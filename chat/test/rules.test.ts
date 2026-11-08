import assert from "node:assert/strict";
import { test } from "node:test";

import { encodeInputEnv, inputAuthor, planInput } from "../src/rules.ts";

const owner = "11111111-1111-4111-8111-111111111111";
const other = "22222222-2222-4222-8222-222222222222";
const conversation = { ownerId: owner, threadId: "thr", archived: false };
const params = (author: string) => ({
  threadId: "thr",
  input: [{ type: "text", text: "hi" }],
  clientUserMessageId: "client-chosen",
  hcodex: { authorUserId: author, env: { TOKEN: "t" } },
});

test("owner starts turns; others queue with their own env; only the owner steers", () => {
  const start = planInput(conversation, "turn/start", params(owner), { authorUserId: owner, env: { TOKEN: "o" } });
  assert.equal(start.method, "turn/start");
  assert.equal(start.startQueue, false);
  assert.equal(inputAuthor(start.params.clientUserMessageId), owner);
  assert.deepEqual(start.params.responsesapiClientMetadata, { "fork.env.TOKEN": "o" });
  assert.equal("hcodex" in start.params, false);

  const queued = planInput(conversation, "turn/start", params(other), { authorUserId: other, env: { TOKEN: "x" } });
  assert.equal(queued.method, "thread/queue/add");
  assert.equal(queued.startQueue, true);
  assert.deepEqual(Object.keys(queued.params).sort(), ["clientUserMessageId", "input", "responsesapiClientMetadata", "threadId"]);
  assert.equal(inputAuthor(queued.params.clientUserMessageId), other);

  assert.equal(planInput(conversation, "turn/steer", params(owner), { authorUserId: owner, env: {} }).method, "turn/steer");
  assert.throws(() => planInput(conversation, "turn/steer", params(other), { authorUserId: other, env: {} }), /owner/);
  assert.throws(() => planInput({ ...conversation, archived: true }, "turn/start", params(owner), { authorUserId: owner, env: {} }), /archived/);
  assert.throws(() => planInput(conversation, "turn/start", { ...params(owner), threadId: "x" }, { authorUserId: owner, env: {} }), /another thread/);
});

test("env must fit the limits Codex keeps instead of being dropped silently", () => {
  assert.throws(() => encodeInputEnv({ "BAD-NAME": "v" }), /name/);
  assert.throws(() => encodeInputEnv({ A: "x".repeat(129) }), /value/);
  assert.throws(() => encodeInputEnv({ A: "a\0b" }), /value/);
  assert.throws(() => encodeInputEnv(Object.fromEntries(Array.from({ length: 17 }, (_, i) => [`V${i}`, "v"]))), /16/);
  assert.equal(inputAuthor("not-an-input-id"), undefined);
});
