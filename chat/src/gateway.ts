import { timingSafeEqual } from "node:crypto";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";

import { WebSocket, WebSocketServer, type RawData } from "ws";

import { replicaUrl, type ReplicaResolver } from "./routing.js";
import { createInputSession, type Frame, type InputStore, type Log } from "./session.js";

/** Close code sent downstream when the app-server connection ends: reconnect to continue. */
export const UPSTREAM_CLOSED = 4502;
const UPSTREAM_CONNECT_TIMEOUT_MS = 10_000;

const isObject = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);

function parse(data: RawData): Frame | undefined {
  try {
    const value = JSON.parse(data.toString());
    return isObject(value) ? (value as Frame) : undefined;
  } catch {
    return undefined;
  }
}

function sameToken(given: string | undefined, expected: string): boolean {
  const a = Buffer.from(given ?? "");
  const b = Buffer.from(`Bearer ${expected}`);
  return a.length === b.length && timingSafeEqual(a, b);
}

function reject(socket: Duplex, status: number, message: string) {
  socket.end(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
}

function upstreamHeaders(value: string | string[] | undefined): Record<string, string> {
  if (typeof value !== "string" || !value) return {};
  const headers = JSON.parse(value) as unknown;
  if (!isObject(headers) || Object.values(headers).some((v) => typeof v !== "string"))
    throw new Error("X-Hcodex-Upstream-Headers must be a JSON object of strings");
  return headers as Record<string, string>;
}

function openUpstream(url: string, headers: Record<string, string>, maxPayload: number): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const upstream = new WebSocket(url, { headers, maxPayload, handshakeTimeout: UPSTREAM_CONNECT_TIMEOUT_MS });
    upstream.once("open", () => resolve(upstream));
    upstream.once("error", reject);
  });
}

/**
 * Standalone native-protocol proxy between trusted hosts and app-server replicas, for hosts that
 * run hcodex chat as its own service. Hosts relaying connections themselves use
 * {@link createInputSession} instead.
 */
export function createGateway(options: {
  store: InputStore;
  resolver: ReplicaResolver;
  hostToken: string;
  maxPayload?: number;
  heartbeatMs?: number;
  log?: Log;
}) {
  const { store, resolver, hostToken } = options;
  const maxPayload = options.maxPayload ?? 64 * 1024 * 1024;
  const heartbeatMs = options.heartbeatMs ?? 30_000;
  const log: Log = options.log ?? (() => {});
  const server = new WebSocketServer({ noServer: true, maxPayload });
  const sockets = new Set<WebSocket>();

  // Peers that miss a heartbeat are dead (for example after a node failure).
  const alive = new WeakMap<WebSocket, boolean>();
  const watch = (socket: WebSocket) => {
    alive.set(socket, true);
    socket.on("pong", () => alive.set(socket, true));
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  };
  const heartbeat = setInterval(() => {
    for (const socket of sockets) {
      if (!alive.get(socket)) {
        socket.terminate();
        continue;
      }
      alive.set(socket, false);
      socket.ping();
    }
  }, heartbeatMs);
  heartbeat.unref();

  function relay(client: WebSocket, upstream: WebSocket, conversationId: string) {
    const session = createInputSession({ store, conversationId, log });
    let chain = Promise.resolve();
    const send = (socket: WebSocket, frame: Frame | undefined) => {
      if (frame && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(frame));
    };

    async function fromClient(data: RawData, binary: boolean) {
      const frame = binary ? undefined : parse(data);
      if (!frame) {
        if (upstream.readyState === WebSocket.OPEN) upstream.send(data, { binary });
        return;
      }
      const out = await session.fromClient(frame);
      send(upstream, out.upstream);
      send(client, out.client);
    }

    function fromUpstream(data: RawData, binary: boolean) {
      const frame = binary ? undefined : parse(data);
      const out = frame ? session.fromUpstream(frame) : { forward: true };
      send(upstream, out.upstream);
      if (out.forward && client.readyState === WebSocket.OPEN) client.send(data, { binary });
    }

    client.on("message", (data, binary) => {
      chain = chain.then(() => fromClient(data, binary)).catch((error: unknown) => {
        log("client_frame_failed", { conversationId, error: String(error) });
      });
    });
    upstream.on("message", fromUpstream);
    upstream.once("close", (code) => {
      log("upstream_closed", { conversationId, code });
      if (client.readyState === WebSocket.OPEN) client.close(UPSTREAM_CLOSED, "upstream closed");
    });
    client.once("close", () => {
      if (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING) upstream.close();
    });
    upstream.on("error", (error) => log("upstream_error", { conversationId, error: error.message }));
    client.on("error", (error) => log("client_error", { conversationId, error: error.message }));
  }

  return {
    /** Handles an HTTP upgrade on the gateway path. */
    async handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer) {
      if (!sameToken(request.headers.authorization, hostToken)) return reject(socket, 401, "Unauthorized");
      const conversationId = request.headers["x-hcodex-conversation"];
      const pool = request.headers["x-hcodex-upstream"];
      if (typeof conversationId !== "string" || !conversationId ||
          (typeof pool !== "string" || !pool))
        return reject(socket, 400, "Bad Request");
      let upstream: WebSocket;
      try {
        const conversation = await store.get(conversationId);
        if (!conversation) return reject(socket, 404, "Not Found");
        const headers = upstreamHeaders(request.headers["x-hcodex-upstream-headers"]);
        const url = await replicaUrl(resolver, pool, conversationId);
        upstream = await openUpstream(url, headers, maxPayload);
        log("upstream_opened", { conversationId, upstream: new URL(url).host });
      } catch (error) {
        log("upstream_unavailable", { conversationId, error: String(error) });
        return reject(socket, 502, "Bad Gateway");
      }
      // The client may go away before its upgrade completes; never leak the upstream.
      socket.once("close", () => upstream.close());
      server.handleUpgrade(request, socket, head, (client) => {
        watch(client);
        watch(upstream);
        relay(client, upstream, conversationId);
        if (upstream.readyState !== WebSocket.OPEN) client.close(UPSTREAM_CLOSED, "upstream closed");
      });
    },

    /** Closes every connection, for example on shutdown; clients reconnect elsewhere. */
    close(code = 1012, reason = "restarting") {
      clearInterval(heartbeat);
      for (const socket of sockets) socket.close(code, reason);
    },
  };
}
