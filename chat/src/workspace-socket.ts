import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import { WebSocket, type RawData } from "ws";
import type { Frame, Log } from "./session.js";
import type { WorkspaceAdmission, WorkspaceRouter } from "./workspaces.js";

// Reading a view neither starts work nor participates in the workspace switch gate.
// Unknown methods remain protected; thread/resume is intentionally not a read.
const READ_METHODS = new Set([
  "thread/read",
  "thread/list",
  "thread/loaded/list",
  "thread/turns/list",
  "thread/items/list",
  "thread/queue/list",
  "thread/backgroundTerminals/list",
  "thread/goal/get",
  "model/list",
]);

/** Small socket contract shared by raw proxies and JSON-RPC transports. */
export interface NativeSocket {
  readonly readyState: number;
  readonly bufferedAmount: number;
  on(event: string, listener: (...args: any[]) => void): unknown;
  once(event: string, listener: (...args: any[]) => void): unknown;
  off(event: string, listener: (...args: any[]) => void): unknown;
  send(
    data: string | Buffer,
    options: { binary?: boolean; compress?: boolean },
    callback?: (error?: Error) => void,
  ): void;
  pause(): void;
  resume(): void;
  ping(): void;
  close(code?: number, reason?: string): void;
  terminate(): void;
}
export interface WorkspaceSocketOptions {
  router: WorkspaceRouter;
  runtimeId: string;
  workspaceId: string;
  headers?: Record<string, string>;
  /** Reload the binding after switching, including thread/start completed on another connection. */
  threadId(): Promise<string | null>;
  /** Persist the binding BEFORE releasing thread/start's in-flight admission. */
  bindThread(threadId: string): Promise<void>;
  maxPayload?: number;
  maxBufferedBytes?: number;
  timeoutMs?: number;
  routeRefreshMs?: number;
  /** Host checks the concrete Pod/process identity before opening a new connection. */
  validateTarget?(target: string): Promise<void>;
  log?: Log;
}

/**
 * A virtual native socket. Mutating requests are admitted on their current workspace target
 * before sending. If a switch completed while the caller waited, initialize/resume the connection
 * and send the original frame there once. No native input is retried after it has been sent.
 */
export class WorkspaceSocket extends EventEmitter implements NativeSocket {
  private socket?: WebSocket;
  private target?: string;
  private ended = false;
  private opened = false;
  private paused = false;
  private queuedBytes = 0;
  private outgoing = Promise.resolve();
  private incoming = Promise.resolve();
  private initialization?: Record<string, unknown>;
  private resumeParams: Record<string, unknown> = {};
  private subscribed = false;
  private initialized = false;
  private checking = false;
  private readonly refresh: ReturnType<typeof setInterval>;
  private readonly pending = new Map<
    string | number,
    {
      admission?: WorkspaceAdmission;
      method: string;
      params?: Record<string, unknown>;
    }
  >();
  private readonly internal = new Map<
    string,
    { resolve(value: unknown): void; reject(error: Error): void }
  >();
  readonly ready: Promise<void>;

  constructor(private readonly options: WorkspaceSocketOptions) {
    super();
    // Keep transport errors observable without EventEmitter's unhandled-error process crash.
    this.on("error", () => {});
    this.ready = options.router
      .route(options.runtimeId, options.workspaceId)
      .then((target) => this.connect(target, false))
      .then(() => {
        if (!this.ended) {
          this.opened = true;
          this.emit("open");
        }
      });
    void this.ready.catch((error) => {
      this.emit("error", error);
      this.terminate();
    });
    this.refresh = setInterval(() => {
      if (!this.initialized || this.ended || this.checking) return;
      this.checking = true;
      void this.options.router
        .route(this.options.runtimeId, this.options.workspaceId)
        .then((target) => {
          if (this.ended || target === this.target) {
            this.checking = false;
            return;
          }
          // Serialize a passive subscription move with caller sends. Admission is acquired afresh
          // inside the chain, so a newer switch cannot send a resume to an obsolete target.
          this.outgoing = this.outgoing.then(async () => {
            let admission: WorkspaceAdmission | undefined;
            try {
              if (this.ended) return;
              admission = await this.options.router.admit(
                this.options.runtimeId,
                this.options.workspaceId,
              );
              await this.connect(admission.target, true);
              await admission.complete();
            } catch (error) {
              this.options.log?.("workspace_subscription_move_failed", {
                error: String(error),
              });
              this.emit("error", error);
              this.terminate();
            } finally {
              this.checking = false;
            }
          });
        })
        .catch((error) => {
          this.checking = false;
          this.options.log?.("workspace_route_refresh_failed", {
            error: String(error),
          });
        });
    }, options.routeRefreshMs ?? 1000);
    this.refresh.unref();
  }
  get readyState() {
    return this.ended
      ? WebSocket.CLOSED
      : this.opened
        ? WebSocket.OPEN
        : WebSocket.CONNECTING;
  }
  get bufferedAmount() {
    return this.queuedBytes + (this.socket?.bufferedAmount ?? 0);
  }
  pause() {
    this.paused = true;
    this.socket?.pause();
  }
  resume() {
    this.paused = false;
    this.socket?.resume();
  }
  ping() {
    // A protocol heartbeat belongs to the underlying connection; emit a virtual pong on success.
    if (this.socket?.readyState === WebSocket.OPEN) this.socket.ping();
  }
  close(code = 1000, reason = "") {
    if (this.ended) return;
    this.ended = true;
    clearInterval(this.refresh);
    this.socket?.terminate();
    for (const waiter of this.internal.values())
      waiter.reject(new Error("Workspace socket closed"));
    this.internal.clear();
    // Unknown inputs are never replayed. Their bounded reservation expires independently;
    // actual conversation activity remains observable after this view disconnects.
    this.emit("close", code, Buffer.from(reason));
  }
  terminate() {
    this.close(1006);
  }

  send(
    data: string | Buffer,
    _options: { binary?: boolean; compress?: boolean },
    callback?: (error?: Error) => void,
  ) {
    const text = data.toString();
    const bytes = Buffer.byteLength(text);
    if (
      this.ended ||
      this.bufferedAmount + bytes >
        (this.options.maxBufferedBytes ?? 4 * 1024 * 1024)
    ) {
      callback?.(new Error("Workspace socket unavailable or backpressured"));
      return;
    }
    this.queuedBytes += bytes;
    this.outgoing = this.outgoing.then(async () => {
      let admission: WorkspaceAdmission | undefined;
      let sent = false;
      try {
        await this.ready;
        if (this.ended) throw new Error("Workspace socket closed");
        const frame = JSON.parse(text) as Frame;
        if (frame.method === "initialize")
          this.initialization = frame.params as Record<string, unknown>;
        const setup =
          frame.method === "initialize" || frame.method === "initialized";
        if (
          typeof frame.method === "string" &&
          !setup &&
          !READ_METHODS.has(frame.method)
        ) {
          admission = await this.options.router.admit(
            this.options.runtimeId,
            this.options.workspaceId,
          );
          await this.connect(admission.target, true);
        } else if (!setup && !this.pending.size) {
          await this.connect(
            await this.options.router.route(
              this.options.runtimeId,
              this.options.workspaceId,
            ),
            true,
          );
        }
        if (this.ended || this.socket?.readyState !== WebSocket.OPEN)
          throw new Error("Workspace socket closed");
        const id = frame.id;
        if (
          typeof frame.method === "string" &&
          (typeof id === "string" || typeof id === "number")
        ) {
          if (this.pending.has(id) || this.pending.size >= 256)
            throw new Error("Duplicate request ID or pending limit");
          this.pending.set(id, {
            admission,
            method: frame.method,
            params:
              frame.params &&
              typeof frame.params === "object" &&
              !Array.isArray(frame.params)
                ? (frame.params as Record<string, unknown>)
                : undefined,
          });
        }
        sent = true;
        await new Promise<void>((resolve, reject) =>
          this.socket!.send(text, { compress: false }, (error) =>
            error ? reject(error) : resolve(),
          ),
        );
        if (frame.method === "initialized") this.initialized = true;
        if (admission && frame.id === undefined) await admission.complete();
        callback?.();
      } catch (error) {
        if (!sent)
          await admission?.complete().catch((e) =>
            this.options.log?.("inflight_cleanup_failed", {
              error: String(e),
            }),
          );
        callback?.(error instanceof Error ? error : new Error(String(error)));
        this.emit("error", error);
        this.terminate();
      } finally {
        this.queuedBytes -= bytes;
      }
    });
  }

  private async connect(target: string, resume: boolean) {
    if (this.target === target && this.socket?.readyState === WebSocket.OPEN)
      return;
    if (this.pending.size)
      throw new Error("Cannot change workspace target with pending requests");
    await this.options.validateTarget?.(target);
    const previous = this.socket;
    const socket = new WebSocket(target, {
      headers: this.options.headers,
      maxPayload: this.options.maxPayload ?? 1024 * 1024,
      handshakeTimeout: this.options.timeoutMs ?? 10_000,
      perMessageDeflate: false,
      followRedirects: false,
    });
    this.socket = socket;
    this.target = target;
    previous?.terminate();
    socket.on("error", (error) => {
      if (this.socket === socket && !this.ended) this.emit("error", error);
    });
    socket.on("pong", () => {
      if (this.socket === socket) this.emit("pong");
    });
    socket.on("message", (data: RawData, binary: boolean) => {
      if (this.socket !== socket || this.ended) return;
      this.incoming = this.incoming
        .then(async () => {
          if (binary) throw new Error("Binary native frame");
          const frame = JSON.parse(data.toString()) as Frame;
          const id = frame.id;
          if (
            typeof id === "string" &&
            this.internal.has(id) &&
            frame.method === undefined
          ) {
            const waiter = this.internal.get(id)!;
            this.internal.delete(id);
            if ("error" in frame)
              waiter.reject(
                new Error(
                  `Native setup rejected: ${JSON.stringify(frame.error)}`,
                ),
              );
            else waiter.resolve(frame.result);
            return;
          }
          if (
            (typeof id === "number" || typeof id === "string") &&
            frame.method === undefined
          ) {
            const request = this.pending.get(id);
            if (request) {
              if (request.method === "thread/start" && !("error" in frame)) {
                const thread = (
                  frame.result as { thread?: { id?: string } } | undefined
                )?.thread;
                if (!thread?.id)
                  throw new Error("Missing native thread binding");
                await this.options.bindThread(thread.id);
              }
              if (!("error" in frame)) {
                if (
                  request.method === "thread/start" ||
                  request.method === "thread/resume"
                ) {
                  this.subscribed = true;
                  if (request.method === "thread/start") this.resumeParams = {};
                  // Reapply the host's connection context on a cold owner. PG retains model
                  // selection; explicit old model overrides must not reset later turn changes.
                  for (const key of [
                    "config",
                    "cwd",
                    "runtimeWorkspaceRoots",
                    "approvalPolicy",
                    "approvalsReviewer",
                    "sandbox",
                    "permissions",
                    "baseInstructions",
                    "developerInstructions",
                  ])
                    if (request.params && Object.hasOwn(request.params, key))
                      this.resumeParams[key] = request.params[key];
                }
                if (request.method === "thread/unsubscribe")
                  this.subscribed = false;
              }
              await request.admission?.complete();
              this.pending.delete(id);
            }
          }
          this.emit("message", data, false);
        })
        .catch((error) => {
          this.emit("error", error);
          this.terminate();
        });
    });
    socket.on("close", () => {
      if (this.socket !== socket || this.ended) return;
      // Migration reconnects are transparent to a waiting sender. Unexpected disconnects expose
      // the unknown outcome to the host; its recovery scans history without resending inputs.
      this.options.router
        .route(this.options.runtimeId, this.options.workspaceId)
        .then((current) => {
          if (this.socket !== socket || this.ended) return;
          if (current !== target && !this.pending.size) {
            this.target = undefined;
            this.emit("moved", current);
          } else this.close(4502, "upstream closed");
        })
        .catch((error) => {
          this.emit("error", error);
          this.close(4502, "upstream closed");
        });
    });
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
      socket.once("close", () =>
        reject(new Error("Workspace upstream closed during setup")),
      );
    });
    if (this.ended) {
      socket.terminate();
      throw new Error("Workspace socket closed");
    }
    if (resume && this.initialization) {
      await this.setup("initialize", this.initialization);
      socket.send(JSON.stringify({ method: "initialized" }));
      const threadId = await this.options.threadId();
      if (threadId && this.subscribed)
        await this.setup("thread/resume", {
          ...this.resumeParams,
          threadId,
          excludeTurns: true,
        });
      this.emit("reconnected", target);
      if (threadId)
        this.emit(
          "message",
          Buffer.from(
            JSON.stringify({
              method: "hcodex/workspaceMoved",
              params: { threadId },
            }),
          ),
          false,
        );
    }
    if (this.paused) socket.pause();
  }
  private setup(
    method: string,
    params: Record<string, unknown>,
  ): Promise<unknown> {
    const id = `hcodex:setup:${randomUUID()}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.internal.delete(id);
        reject(new Error(`Workspace setup timeout: ${method}`));
      }, this.options.timeoutMs ?? 10_000);
      this.internal.set(id, {
        resolve: (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
      this.socket!.send(JSON.stringify({ id, method, params }), (error) => {
        if (error) {
          clearTimeout(timer);
          this.internal.delete(id);
          reject(error);
        }
      });
    });
  }
}

/** Structural transport factory for any JSON-RPC client; no dependency on a host SDK. */
export function createWorkspaceTransport(options: WorkspaceSocketOptions) {
  return (handlers: { message(text: string): void; closed(): void }) => {
    const socket = new WorkspaceSocket(options);
    const closed = new Promise<void>((resolve) =>
      socket.once("close", () => {
        resolve();
        handlers.closed();
      }),
    );
    socket.on("message", (data: RawData) => handlers.message(data.toString()));
    socket.on("error", () => socket.terminate());
    return {
      ready: socket.ready,
      closed,
      get isOpen() {
        return socket.readyState === WebSocket.OPEN;
      },
      get bufferedAmount() {
        return socket.bufferedAmount;
      },
      send(text: string, failed: () => void) {
        socket.send(text, {}, (error) => {
          if (error) failed();
        });
      },
      close() {
        socket.terminate();
      },
    };
  };
}
