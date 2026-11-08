import assert from "node:assert/strict";
import { test } from "node:test";

import { inputAuthor } from "../src/rules.ts";
import { withoutInputAuthor } from "../src/input.ts";
import { submitInput } from "../src/session.ts";
import type { Conversation } from "../src/types.ts";

const owner = "11111111-1111-4111-8111-111111111111";
const other = "22222222-2222-4222-8222-222222222222";
const conversation = { id: "conv-1", ownerId: owner, threadId: "thr-1", archived: false } as Conversation;

test("submitInput: the owner starts a turn, others queue and start the queue", async () => {
  const calls: [string, Record<string, any>][] = [];
  const recorded: string[] = [];
  const store = { get: async () => conversation, recordInput: async (_c: string, user: string) => void recorded.push(user) };
  const requester = {
    request: async (method: string, params: Record<string, unknown>) => {
      calls.push([method, params]);
      if (method === "thread/queue/start") throw new Error("turn already active"); // tolerated
      return { ok: method };
    },
  };
  const params = (author: string) => ({ threadId: "thr-1", input: [{ type: "text", text: "hi" }], hcodex: { authorUserId: author, authorName: author === owner ? "Owner" : "Other", env: { USER_ID: author } } });

  assert.deepEqual(
    await submitInput({ store, requester, conversationId: "conv-1", method: "turn/start", params: params(owner) }),
    { ok: "turn/start" },
  );
  assert.deepEqual(
    await submitInput({ store, requester, conversationId: "conv-1", method: "turn/start", params: params(other) }),
    { ok: "thread/queue/add" },
  );
  assert.deepEqual(calls.map(([method]) => method), ["turn/start", "thread/queue/add", "thread/queue/start"]);
  const [, queued] = calls[1]!;
  assert.equal(inputAuthor(queued.clientUserMessageId), other);
  assert.deepEqual(queued.responsesapiClientMetadata, { "fork.env.USER_ID": other });
  assert.equal("hcodex" in calls[0]![1], false);
  for (const [index, name] of ["Owner", "Other"].entries()) {
    const sent = calls[index]![1];
    assert.deepEqual(withoutInputAuthor(sent.input, sent.clientUserMessageId), [{ type: "text", text: "hi" }]);
    assert.ok(sent.input[0].text.includes(JSON.stringify({ userId: index === 0 ? owner : other, name })));
  }
  assert.deepEqual(recorded, [owner, other]);
});
