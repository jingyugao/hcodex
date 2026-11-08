import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import pg from "pg";
import WebSocket from "ws";
import { createConversationStore } from "../src/store.js";
import { WorkspaceSocket, type NativeSocket } from "../src/workspace-socket.js";
import {
  createWorkspaceRouter,
  inspectWorkspace,
  type WorkspaceRouter,
} from "../src/workspaces.js";

type Frame = {
  id?: number | string;
  method?: string;
  params?: any;
  result?: any;
  error?: any;
};
class Rpc {
  private sequence = 0;
  private readonly pending = new Map<
    number | string,
    { resolve(value: any): void; reject(error: Error): void }
  >();
  readonly events: Frame[] = [];
  constructor(readonly socket: NativeSocket | WebSocket) {
    socket.on("message", (value: Buffer) => {
      const frame = JSON.parse(value.toString()) as Frame;
      if (frame.id !== undefined && frame.method === undefined) {
        const pending = this.pending.get(frame.id);
        if (!pending) return;
        this.pending.delete(frame.id);
        if (frame.error)
          pending.reject(
            new Error(`RPC rejected: ${JSON.stringify(frame.error)}`),
          );
        else pending.resolve(frame.result);
      } else if (frame.method) this.events.push(frame);
    });
    socket.on("close", () => {
      for (const pending of this.pending.values())
        pending.reject(new Error("RPC connection closed; outcome unknown"));
      this.pending.clear();
    });
    socket.on("error", () => {});
  }
  async request(method: string, params: Record<string, unknown>): Promise<any> {
    const id = ++this.sequence;
    const response = new Promise<any>((resolve, reject) =>
      this.pending.set(id, { resolve, reject }),
    );
    this.socket.send(
      JSON.stringify({ id, method, params }),
      {},
      (error?: Error) => {
        if (error) {
          this.pending.get(id)?.reject(error);
          this.pending.delete(id);
        }
      },
    );
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        response,
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () =>
              reject(new Error(`RPC timed out: ${method}; outcome unknown`)),
            30_000,
          );
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }
  async initialize() {
    await this.request("initialize", {
      clientInfo: { name: "workspace-e2e", version: "1" },
      capabilities: { experimentalApi: true },
    });
    this.socket.send(JSON.stringify({ method: "initialized" }), {});
  }
  close() {
    this.socket.terminate();
  }
}
async function until<T>(
  name: string,
  fn: () => T | Promise<T>,
  timeoutMs = 30_000,
): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value) return value as NonNullable<T>;
    await delay(20);
  }
  throw new Error(`Timeout: ${name}`);
}
function emit(
  response: ServerResponse,
  type: string,
  value: Record<string, unknown>,
) {
  response.write(
    `event: ${type}\ndata: ${JSON.stringify({ type, ...value })}\n\n`,
  );
}
function reply(response: ServerResponse, text: string) {
  const item = {
    type: "message",
    id: `msg_${randomUUID()}`,
    role: "assistant",
    content: [{ type: "output_text", text }],
  };
  emit(response, "response.output_item.added", { output_index: 0, item });
  emit(response, "response.output_item.done", { output_index: 0, item });
  emit(response, "response.completed", {
    response: {
      id: `resp_${randomUUID()}`,
      status: "completed",
      output: [item],
      usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
    },
  });
  response.end();
}
async function openRaw(url: string): Promise<Rpc> {
  const ws = new WebSocket(url, { handshakeTimeout: 10_000 });
  await Promise.race([
    once(ws, "open"),
    once(ws, "error").then(([e]) => {
      throw e;
    }),
  ]);
  const rpc = new Rpc(ws);
  await rpc.initialize();
  return rpc;
}
async function stop(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), 2000);
  try {
    await exited;
  } finally {
    clearTimeout(timer);
  }
}
const databaseUrl = process.env.HCODEX_PG_TEST_URL;
const binary = process.env.HCODEX_APP_SERVER_BINARY;
test(
  "two SDK instances transfer one idle workspace between real app servers without replay",
  {
    skip:
      !databaseUrl || !binary
        ? "HCODEX_PG_TEST_URL and HCODEX_APP_SERVER_BINARY are required"
        : false,
    timeout: 180_000,
  },
  async () => {
    const target = new URL(databaseUrl!);
    assert.ok(["postgres:", "postgresql:"].includes(target.protocol));
    assert.equal(
      target.pathname,
      "/hcodex_github_ci",
      "use only the dedicated hcodex_github_ci test database",
    );
    assert.equal(target.search, "");
    assert.equal(target.hash, "");
    const schema = `workspace_e2e_${randomUUID().replaceAll("-", "")}`;
    const admin = new pg.Pool({
      connectionString: databaseUrl,
      connectionTimeoutMillis: 5000,
    });
    let schemaCreated = false;
    const pools: pg.Pool[] = [],
      children: ChildProcess[] = [],
      sockets: Rpc[] = [];
    const modelErrors: string[] = [];
    const modelCalls = new Map<string, number>();
    let releaseFirst: (() => void) | undefined,
      releaseOther: (() => void) | undefined,
      releaseInspection: (() => void) | undefined;
    let inspectionBarrierEntered = false,
      barrierEnabled = false,
      inspections = 0;
    const root = await mkdtemp(join(tmpdir(), "hcodex-workspace-e2e-"));
    const home = join(root, "codex-home"),
      workspace = join(root, "workspaces", "one");
    const model = createServer((request, response) => {
      void (async () => {
        if (!request.url?.split("?")[0]?.endsWith("/responses")) {
          response.writeHead(404).end();
          return;
        }
        const chunks: Buffer[] = [];
        for await (const chunk of request) chunks.push(Buffer.from(chunk));
        const body = JSON.parse(Buffer.concat(chunks).toString());
        const last = (body.input ?? [])
          .filter((item: any) => item.role === "user")
          .at(-1);
        const text = (last?.content ?? [])
          .map((item: any) => item.text ?? "")
          .join("\n");
        const stage = text.match(/WORKSPACE_(HOLD|OTHER|AFTER|BG|RACE)/)?.[1];
        if (!stage) throw new Error("Unexpected mock model input");
        modelCalls.set(stage, (modelCalls.get(stage) ?? 0) + 1);
        response.writeHead(200, { "content-type": "text/event-stream" });
        emit(response, "response.created", {
          response: { id: `resp_${randomUUID()}` },
        });
        if (stage === "HOLD")
          await new Promise<void>((resolve) => {
            releaseFirst = resolve;
          });
        if (stage === "OTHER")
          await new Promise<void>((resolve) => {
            releaseOther = resolve;
          });
        if (
          stage === "BG" &&
          !(body.input ?? []).some(
            (item: any) =>
              item.type === "function_call_output" &&
              item.call_id === "call-workspace-bg",
          )
        ) {
          const advertised = (body.tools ?? []).find(
            (tool: any) =>
              tool.name === "exec_command" ||
              (tool.type === "namespace" &&
                tool.tools?.some(
                  (child: any) => child.name === "exec_command",
                )),
          );
          if (!advertised) throw new Error("exec_command was not advertised");
          const item = {
            type: "function_call",
            id: `fc_${randomUUID()}`,
            call_id: "call-workspace-bg",
            name: "exec_command",
            ...(advertised.type === "namespace"
              ? { namespace: advertised.name }
              : {}),
            arguments: JSON.stringify({ cmd: "sleep 8", yield_time_ms: 100 }),
          };
          emit(response, "response.output_item.added", {
            output_index: 0,
            item,
          });
          emit(response, "response.output_item.done", {
            output_index: 0,
            item,
          });
          emit(response, "response.completed", {
            response: {
              id: `resp_${randomUUID()}`,
              status: "completed",
              output: [item],
              usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
            },
          });
          response.end();
        } else reply(response, `DONE_${stage}`);
      })().catch((error: Error) => {
        modelErrors.push(error.message);
        response.destroy();
      });
    });
    try {
      await admin.query(`CREATE SCHEMA ${schema}`);
      schemaCreated = true;
      for (let i = 0; i < 2; i++)
        pools.push(
          new pg.Pool({
            connectionString: databaseUrl,
            options: `-c search_path=${schema}`,
            connectionTimeoutMillis: 5000,
          }),
        );
      const poolA = pools[0]!,
        poolB = pools[1]!;
      const store = createConversationStore(poolA);
      await store.migrate();
      const runtimeId = randomUUID(),
        workspaceId = randomUUID(),
        conversationId = randomUUID();
      await store.create({
        id: conversationId,
        ownerId: randomUUID(),
        agentId: randomUUID(),
        workspaceId,
        runtimeId,
        title: "Workspace migration e2e",
        source: "web",
      });
      const nativeUrl = new URL(databaseUrl!);
      nativeUrl.searchParams.set("options[search_path]", schema);
      model.listen(0, "127.0.0.1");
      await once(model, "listening");
      const address = model.address();
      assert.ok(address && typeof address !== "string");
      const connectionConfig = {
        "model_providers.mock_provider.base_url": `http://127.0.0.1:${address.port}/v1`,
      };
      await mkdir(home, { recursive: true });
      await mkdir(workspace, { recursive: true });
      await writeFile(
        join(home, "config.toml"),
        `model = "mock-model"\nmodel_provider = "mock_provider"\napproval_policy = "never"\nsandbox_mode = "danger-full-access"\n[features]\nenable_request_compression = false\nmulti_agent = false\nmulti_agent_v2 = false\ngoals = false\napps = false\nmemories = false\n[model_providers.mock_provider]\nname = "Isolated workspace integration model"\nbase_url = "http://127.0.0.1:1/missing-connection-context"\nwire_api = "responses"\nrequires_openai_auth = false\nsupports_websockets = false\nrequest_max_retries = 0\nstream_max_retries = 0\n`,
      );
      async function launch(owner: string): Promise<string> {
        const sqlite = join(root, `sqlite-${owner}`);
        await mkdir(sqlite);
        const env: NodeJS.ProcessEnv = {
          ...process.env,
          CODEX_HOME: home,
          CODEX_SQLITE_HOME: sqlite,
          HCODEX_THREAD_STORE: nativeUrl.toString(),
          HCODEX_OWNER_ID: owner,
          RUST_LOG: "warn",
        };
        for (const name of [
          "HCODEX_POOL_ID",
          "HCODEX_OWNER_URL",
          "HCODEX_WORKSPACE_ROOT",
          "HCODEX_HANDOFF_STORE",
          "HCODEX_HANDOFF_ON_SIGTERM",
        ])
          delete env[name];
        const child = spawn(
          resolve(binary!),
          ["--listen", "ws://127.0.0.1:0"],
          { env, stdio: ["ignore", "ignore", "pipe"] },
        );
        children.push(child);
        let log = "";
        child.stderr!.on("data", (chunk) => {
          log = (
            log +
            chunk
              .toString()
              .replaceAll(databaseUrl!, "[PG_URL]")
              .replaceAll(target.password, "[redacted]")
          ).slice(-20_000);
        });
        const endpoint = await until(`app server ${owner}`, () => {
          if (child.exitCode !== null || child.signalCode !== null)
            throw new Error(`server exited: ${log}`);
          return log
            .replace(/\x1b\[[0-9;]*m/g, "")
            .match(/ws:\/\/127\.0\.0\.1:\d+/)?.[0];
        });
        return `${endpoint}/?instance=${owner}`;
      }
      const serverA = await launch("pod-a"),
        serverB = await launch("pod-b");
      const inspect = async (
        _runtime: string,
        url: string,
        ids: readonly string[],
      ) => {
        if (barrierEnabled && ++inspections === 2) {
          inspectionBarrierEntered = true;
          await new Promise<void>((resolve) => {
            releaseInspection = resolve;
          });
        }
        const client = await openRaw(url);
        try {
          return await inspectWorkspace(client, ids);
        } finally {
          client.close();
        }
      };
      const routerA = createWorkspaceRouter(poolA, {
        inspect,
        waitMs: 10_000,
        pollMs: 20,
        switchMs: 10_000,
      });
      const routerB = createWorkspaceRouter(poolB, {
        inspect,
        waitMs: 10_000,
        pollMs: 20,
        switchMs: 10_000,
      });
      await routerA.migrate();
      await routerA.setTargets(runtimeId, [serverA]);
      async function virtual(
        router: WorkspaceRouter,
        bindingId = conversationId,
        bindingWorkspace = workspaceId,
      ): Promise<Rpc> {
        const socket = new WorkspaceSocket({
          router,
          runtimeId,
          workspaceId: bindingWorkspace,
          timeoutMs: 10_000,
          threadId: async () => (await store.get(bindingId))?.threadId ?? null,
          bindThread: async (threadId) => {
            await store.bindThread(bindingId, runtimeId, threadId);
          },
        });
        const client = new Rpc(socket);
        sockets.push(client);
        await socket.ready;
        await client.initialize();
        return client;
      }
      const clientA = await virtual(routerA);
      const started = await clientA.request("thread/start", {
        config: connectionConfig,
        cwd: workspace,
        model: "mock-model",
        approvalPolicy: "never",
        sandbox: "danger-full-access",
      });
      const threadId: string = started.thread.id;
      assert.equal((await store.get(conversationId))?.threadId, threadId);
      const clientB = await virtual(routerB);
      await clientB.request("thread/resume", {
        threadId,
        excludeTurns: true,
        config: connectionConfig,
      });
      // Keep a separate workspace executing on A while the tested workspace moves to B.
      const otherWorkspaceId = randomUUID(),
        otherConversationId = randomUUID();
      const otherWorkspace = join(root, "workspaces", "other");
      await mkdir(otherWorkspace, { recursive: true });
      await store.create({
        id: otherConversationId,
        ownerId: randomUUID(),
        agentId: randomUUID(),
        workspaceId: otherWorkspaceId,
        runtimeId,
        title: "Other workspace",
        source: "web",
      });
      const otherClient = await virtual(
        routerA,
        otherConversationId,
        otherWorkspaceId,
      );
      const otherStarted = await otherClient.request("thread/start", {
        config: connectionConfig,
        cwd: otherWorkspace,
        model: "mock-model",
        approvalPolicy: "never",
        sandbox: "danger-full-access",
      });
      const otherThreadId: string = otherStarted.thread.id;
      const otherTurn = await otherClient.request("turn/start", {
        threadId: otherThreadId,
        input: [{ type: "text", text: "WORKSPACE_OTHER" }],
      });
      await until("other workspace active on A", () => releaseOther);
      const first = await clientA.request("turn/start", {
        threadId,
        input: [{ type: "text", text: "WORKSPACE_HOLD" }],
      });
      const firstId: string = first.turn.id;
      await until("first model request", () => releaseFirst);
      await routerA.setTargets(runtimeId, [serverB], [serverA]);
      assert.equal(
        await routerB.switchWorkspace(runtimeId, workspaceId, serverA, serverB),
        "still_active",
      );
      assert.equal(await routerA.route(runtimeId, workspaceId), serverA);
      releaseFirst!();
      await until("first turn completed", () =>
        clientA.events.find(
          (event) =>
            event.method === "turn/completed" &&
            event.params?.turn?.id === firstId &&
            event.params.turn.status === "completed",
        ),
      );
      assert.equal(
        await routerB.switchWorkspace(runtimeId, workspaceId, serverA, serverB),
        "switched",
      );
      assert.equal(await routerA.route(runtimeId, workspaceId), serverB);
      const moved = await until(
        "passive subscriber reconnects without sending",
        () =>
          clientB.events.find(
            (event) =>
              event.method === "hcodex/workspaceMoved" &&
              event.params?.threadId === threadId,
          ),
        10_000,
      );
      assert.deepEqual(
        moved.params,
        { threadId },
        "move notification must not expose owner address",
      );
      assert.equal(
        children[0]!.exitCode,
        null,
        "old pod is still running another workspace",
      );
      const after = await clientA.request("turn/start", {
        threadId,
        input: [{ type: "text", text: "WORKSPACE_AFTER" }],
      });
      await until("turn after migration", () =>
        clientA.events.find(
          (event) =>
            event.method === "turn/completed" &&
            event.params?.turn?.id === after.turn.id &&
            event.params.turn.status === "completed",
        ),
      );
      await until("passive subscriber receives new owner's turn", () =>
        clientB.events.find(
          (event) =>
            event.method === "turn/completed" &&
            event.params?.turn?.id === after.turn.id &&
            event.params.turn.status === "completed",
        ),
      );
      releaseOther!();
      await until("other workspace completes on old owner", () =>
        otherClient.events.find(
          (event) =>
            event.method === "turn/completed" &&
            event.params?.turn?.id === otherTurn.turn.id &&
            event.params.turn.status === "completed",
        ),
      );
      const page = await clientA.request("thread/turns/list", {
        threadId,
        limit: 50,
        sortDirection: "asc",
        itemsView: "full",
      });
      assert.ok(JSON.stringify(page.data).includes("WORKSPACE_HOLD"));
      assert.ok(JSON.stringify(page.data).includes("WORKSPACE_AFTER"));
      assert.equal(
        (
          await poolA.query(
            "SELECT owner_id FROM codex_fork_threads WHERE thread_id=$1",
            [threadId],
          )
        ).rows[0]?.owner_id,
        "pod-b",
      );
      // A slot is only reused after its old process is gone. A fresh process cannot retain the
      // previous writer's cached epoch, even when the workspace returns to the same logical slot.
      await stop(children[0]!);
      const replacementA = await launch("pod-a2");
      await routerA.setTargets(runtimeId, [replacementA], [serverB]);
      const background = await clientA.request("turn/start", {
        threadId,
        input: [{ type: "text", text: "WORKSPACE_BG" }],
      });
      await until("background command turn completed", () =>
        clientA.events.find(
          (event) =>
            event.method === "turn/completed" &&
            event.params?.turn?.id === background.turn.id &&
            event.params.turn.status === "completed",
        ),
      );
      const backgroundInspector = await openRaw(serverB);
      try {
        const terminals = await backgroundInspector.request(
          "thread/backgroundTerminals/list",
          { threadId, limit: 1 },
        );
        assert.ok(
          terminals.data.length > 0,
          "test command must remain alive after the turn",
        );
      } finally {
        backgroundInspector.close();
      }
      assert.equal(
        await routerB.switchWorkspace(
          runtimeId,
          workspaceId,
          serverB,
          replacementA,
        ),
        "still_active",
      );
      await until(
        "background process exits",
        async () => !(await routerA.isActive(runtimeId, workspaceId)),
        20_000,
      );
      barrierEnabled = true;
      inspections = 0;
      const switching = routerA.switchWorkspace(
        runtimeId,
        workspaceId,
        serverB,
        replacementA,
      );
      await until("second activity check", () => inspectionBarrierEntered);
      let raceAnswered = false;
      const race = clientB
        .request("turn/start", {
          threadId,
          input: [{ type: "text", text: "WORKSPACE_RACE" }],
        })
        .then((value) => {
          raceAnswered = true;
          return value;
        });
      await delay(150);
      assert.equal(
        raceAnswered,
        false,
        "new input waits while the workspace is switching",
      );
      assert.equal(modelCalls.get("RACE") ?? 0, 0);
      releaseInspection!();
      assert.equal(await switching, "switched");
      const raced = await race;
      await until("raced turn completed", () =>
        clientB.events.find(
          (event) =>
            event.method === "turn/completed" &&
            event.params?.turn?.id === raced.turn.id &&
            event.params.turn.status === "completed",
        ),
      );
      assert.equal(await routerB.route(runtimeId, workspaceId), replacementA);
      assert.equal(
        (
          await poolA.query(
            "SELECT owner_id FROM codex_fork_threads WHERE thread_id=$1",
            [threadId],
          )
        ).rows[0]?.owner_id,
        "pod-a2",
      );
      assert.deepEqual(Object.fromEntries(modelCalls), {
        OTHER: 1,
        HOLD: 1,
        AFTER: 1,
        BG: 2,
        RACE: 1,
      });
      assert.deepEqual(modelErrors, []);
    } finally {
      releaseFirst?.();
      releaseOther?.();
      releaseInspection?.();
      for (const client of sockets) client.close();
      await Promise.allSettled(children.map(stop));
      if (model.listening)
        await new Promise<void>((resolve) => model.close(() => resolve()));
      await Promise.allSettled(pools.map((pool) => pool.end()));
      if (schemaCreated) await admin.query(`DROP SCHEMA ${schema} CASCADE`);
      await admin.end();
      await rm(root, { recursive: true, force: true });
    }
  },
);
