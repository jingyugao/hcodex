import { createServer } from "node:http";

import pg from "pg";

import { createGateway } from "./gateway.js";
import { createHttpApi } from "./http.js";
import { createReplicaResolver } from "./routing.js";
import { createConversationStore } from "./store.js";

const log = (event: string, fields: Record<string, unknown> = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), event, ...fields }));

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

const hostToken = required("HCODEX_CHAT_HOST_TOKEN");
const [host, port] = (process.env.HCODEX_CHAT_LISTEN ?? "0.0.0.0:4910").split(/:(?=\d+$)/);
const pool = new pg.Pool({
  connectionString: required("HCODEX_CHAT_STORE"),
  max: Number(process.env.HCODEX_CHAT_STORE_POOL_SIZE ?? 10),
  connectionTimeoutMillis: 5_000,
  idleTimeoutMillis: 30_000,
  statement_timeout: 10_000,
});
pool.on("error", (error) => log("store_pool_error", { error: error.message }));

const store = createConversationStore(pool);
await store.migrate();
const api = createHttpApi({ store, hostToken });
const gateway = createGateway({ store, resolver: createReplicaResolver(), hostToken, log });
let draining = false;

const server = createServer((request, response) => {
  if (request.url === "/readyz") {
    response.writeHead(draining ? 503 : 200).end();
    return;
  }
  void api(request, response);
});
server.on("upgrade", (request, socket, head) => {
  if (draining || new URL(request.url ?? "/", "http://hcodex").pathname !== "/v1/native") {
    socket.end("HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    return;
  }
  void gateway.handleUpgrade(request, socket, head).catch((error: unknown) => {
    log("upgrade_failed", { error: String(error) });
    socket.destroy();
  });
});
server.listen(Number(port), host, () => log("listening", { host, port }));

// Stop taking work, let the load balancer notice, then close connections; clients reconnect.
const shutdown = (signal: string) => {
  if (draining) return;
  draining = true;
  log("draining", { signal });
  const grace = Number(process.env.HCODEX_CHAT_DRAIN_SECS ?? 5) * 1000;
  setTimeout(() => {
    gateway.close();
    server.close(() => void pool.end().finally(() => process.exit(0)));
    setTimeout(() => process.exit(0), 5_000).unref();
  }, grace).unref();
};
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
