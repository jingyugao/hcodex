import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";

import { WebSocket, WebSocketServer } from "ws";

import { createGateway, UPSTREAM_CLOSED } from "../src/gateway.ts";
import { createReplicaResolver } from "../src/routing.ts";
import { inputAuthor } from "../src/rules.ts";
import type { Conversation } from "../src/types.ts";

const owner = "11111111-1111-4111-8111-111111111111";
const other = "22222222-2222-4222-8222-222222222222";
const token = "host-token";
const conversation = { id: "conv-1", ownerId: owner, threadId: "thr-1", archived: false } as Conversation;
const recorded: string[] = [];

/** Fake app-server: records frames and answers every request with an empty success. */
const upstreamFrames: Record<string, any>[] = [];
const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
upstream.on("connection", (socket) => {
  socket.on("message", (data) => {
    const frame = JSON.parse(data.toString());
    upstreamFrames.push(frame);
    if (frame.method === "close-me") return socket.close();
    const result = frame.method === "thread/queue/add" ? { queuedSubmission: { id: "q1" } } : { turn: { id: "turn-1" } };
    socket.send(JSON.stringify({ id: frame.id, result }));
  });
});

const gateway = createGateway({
  store: {
    get: async (id) => (id === conversation.id ? conversation : undefined),
    recordInput: async (_conversationId, userId) => void recorded.push(userId),
  },
  resolver: createReplicaResolver(),
  hostToken: token,
});
const server = createServer();
server.on("upgrade", (request, socket, head) => void gateway.handleUpgrade(request, socket, head));
let gatewayUrl = "";

before(async () => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  gatewayUrl = `ws://127.0.0.1:${(server.address() as AddressInfo).port}/v1/native`;
});
after(() => {
  gateway.close();
  server.close();
  upstream.close();
});

function connect(headers: Record<string, string> = {}) {
  const port = (upstream.address() as AddressInfo).port;
  const client = new WebSocket(gatewayUrl, {
    headers: {
      authorization: `Bearer ${token}`,
      "x-hcodex-conversation": conversation.id,
      "x-hcodex-upstream": `ws://127.0.0.1:${port}`,
      ...headers,
    },
  });
  const inbox: Record<string, any>[] = [];
  const waiters: (() => void)[] = [];
  client.on("message", (data) => {
    inbox.push(JSON.parse(data.toString()));
    waiters.splice(0).forEach((wake) => wake());
  });
  const next = async () => {
    while (!inbox.length) await new Promise<void>((resolve) => waiters.push(resolve));
    return inbox.shift()!;
  };
  return { client, next, opened: new Promise((resolve, reject) => (client.once("open", resolve), client.once("error", reject))) };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 50));
const input = (id: number, method: string, author: string) =>
  JSON.stringify({
    id,
    method,
    params: { threadId: "thr-1", input: [{ type: "text", text: "hi" }], hcodex: { authorUserId: author, env: { TOKEN: author } } },
  });

test("applies the send rules and keeps the gateway's own requests to itself", async () => {
  const { client, next, opened } = connect();
  await opened;

  client.send(input(1, "turn/start", owner));
  assert.deepEqual(await next(), { id: 1, result: { turn: { id: "turn-1" } } });
  const start = upstreamFrames.at(-1)!;
  assert.equal(start.method, "turn/start");
  assert.equal(inputAuthor(start.params.clientUserMessageId), owner);
  assert.deepEqual(start.params.responsesapiClientMetadata, { "fork.env.TOKEN": owner });
  assert.equal("hcodex" in start.params, false);

  client.send(input(2, "turn/start", other));
  assert.deepEqual(await next(), { id: 2, result: { queuedSubmission: { id: "q1" } } });
  await settle();
  const [queued, nudge] = upstreamFrames.slice(-2);
  assert.equal(queued!.method, "thread/queue/add");
  assert.equal(nudge!.method, "thread/queue/start");
  assert.deepEqual(nudge!.params, { threadId: "thr-1" });

  client.send(input(3, "turn/steer", other));
  assert.match((await next()).error.message, /owner/);
  client.send(JSON.stringify({ id: 4, method: "turn/start", params: { threadId: "thr-1", input: [] } }));
  assert.match((await next()).error.message, /identity/);

  // Any other frame loses the identity field, and its response comes back unchanged.
  client.send(JSON.stringify({ id: 5, method: "thread/turns/list", params: { threadId: "thr-1", hcodex: { env: { TOKEN: "x" } } } }));
  assert.equal((await next()).id, 5, "the queue/start response must not reach the client");
  assert.deepEqual(upstreamFrames.at(-1)!.params, { threadId: "thr-1" });
  assert.deepEqual(recorded, [owner, other]);
  client.close();
});

test("closes the client with a reconnect code when the app-server goes away", async () => {
  const { client, opened } = connect();
  await opened;
  const closed = new Promise<number>((resolve) => client.once("close", resolve));
  client.send(JSON.stringify({ id: 1, method: "close-me" }));
  assert.equal(await closed, UPSTREAM_CLOSED);
});

test("rejects connections without the host token or with an unknown conversation", async () => {
  for (const headers of [{ authorization: "Bearer wrong" }, { "x-hcodex-conversation": "missing" }]) {
    const { opened } = connect(headers);
    await assert.rejects(opened as Promise<unknown>, /Unexpected server response: (401|404)/);
  }
});
