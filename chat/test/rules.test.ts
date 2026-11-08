import assert from "node:assert/strict";
import { test } from "node:test";

import { encodeInputEnv, inputAuthor, inputIdentity, planInput } from "../src/rules.ts";

import { withInputAuthor, withoutInputAuthor } from "../src/input.ts";

const owner = "11111111-1111-4111-8111-111111111111";
const other = "22222222-2222-4222-8222-222222222222";
const conversation = { ownerId: owner, threadId: "thr", archived: false };
const params = (author: string) => ({
  threadId: "thr",
  input: [{ type: "text", text: "hi" }],
  clientUserMessageId: "client-chosen",
  hcodex: { authorUserId: author, authorName: "Sender", env: { TOKEN: "t" } },
});

test("owner starts turns; others queue with their own env; only the owner steers", () => {
  const start = planInput(conversation, "turn/start", params(owner), { authorUserId: owner, authorName: "Owner", env: { TOKEN: "o" } });
  assert.equal(start.method, "turn/start");
  assert.equal(start.startQueue, false);
  assert.equal(inputAuthor(start.params.clientUserMessageId), owner);
  assert.deepEqual(start.params.responsesapiClientMetadata, { "fork.env.TOKEN": "o" });
  assert.equal("hcodex" in start.params, false);

  const queued = planInput(conversation, "turn/start", params(other), { authorUserId: other, authorName: "Other", env: { TOKEN: "x" } });
  assert.equal(queued.method, "thread/queue/add");
  assert.equal(queued.startQueue, true);
  assert.deepEqual(Object.keys(queued.params).sort(), ["clientUserMessageId", "input", "responsesapiClientMetadata", "threadId"]);
  assert.equal(inputAuthor(queued.params.clientUserMessageId), other);

  assert.equal(planInput(conversation, "turn/steer", params(owner), { authorUserId: owner, authorName: "Owner", env: {} }).method, "turn/steer");
  assert.throws(() => planInput(conversation, "turn/steer", params(other), { authorUserId: other, authorName: "Other", env: {} }), /owner/);
  assert.throws(() => planInput({ ...conversation, archived: true }, "turn/start", params(owner), { authorUserId: owner, authorName: "Owner", env: {} }), /archived/);
  assert.throws(() => planInput(conversation, "turn/start", { ...params(owner), threadId: "x" }, { authorUserId: owner, authorName: "Owner", env: {} }), /another thread/);
});

test("env must fit the limits Codex keeps instead of being dropped silently", () => {
  assert.throws(() => encodeInputEnv({ "BAD-NAME": "v" }), /name/);
  assert.throws(() => encodeInputEnv({ A: "x".repeat(129) }), /value/);
  assert.throws(() => encodeInputEnv({ A: "a\0b" }), /value/);
  assert.throws(() => encodeInputEnv(Object.fromEntries(Array.from({ length: 17 }, (_, i) => [`V${i}`, "v"]))), /16/);
  assert.equal(inputAuthor("not-an-input-id"), undefined);
});

test("all send rules wrap the trusted author and preserve the raw input", () => {
  const raw = [{ type: "text", text: "hi" }, { type: "image", url: "image" }];
  const before = structuredClone(raw);
  for (const [userId, method, expected] of [
    [owner, "turn/start", "turn/start"],
    [owner, "turn/steer", "turn/steer"],
    [other, "turn/start", "thread/queue/add"],
  ] as const) {
    const identity = inputIdentity({ authorUserId: userId, authorName: '张三 "Alice"\n第二行' });
    const request = { ...params(userId), input: raw, hcodex: { authorUserId: other, authorName: "forged" } };
    const plan = planInput(conversation, method, request, identity);
    assert.equal(plan.method, expected);
    assert.deepEqual(plan.params.input, withInputAuthor(raw, { userId, name: identity.authorName }));
    assert.deepEqual(withoutInputAuthor(plan.params.input as unknown[], plan.inputId), raw);
    assert.equal("hcodex" in plan.params, false);
    assert.deepEqual(request.input, before);
  }
  assert.throws(() => inputIdentity({ authorUserId: owner }), /authorName/);
  assert.throws(() => inputIdentity({ authorUserId: owner, authorName: 123 }), /authorName/);
  for (const input of [undefined, [], "text"])
    assert.throws(() => planInput(conversation, "turn/start", { ...params(owner), input }, {
      authorUserId: owner, authorName: "Owner", env: {},
    }), /non-empty array/);
});

const author = {
  userId: "12345678-1234-1234-1234-123456789abc",
  name: '张三 "Alice"\n第二行',
};
const clientId = `${author.userId}:abcdefab-abcd-abcd-abcd-abcdefabcdef`;

test("round trips a separately framed header without changing input", () => {
  const original = [
    { type: "text", text: "你好", text_elements: [] },
    { type: "image", url: "image" },
  ];
  const before = structuredClone(original);
  const encoded = withInputAuthor(original, author);
  assert.deepEqual(original, before);
  assert.deepEqual(encoded[0], {
    type: "text",
    text: `<hcodex-input-author>\n${JSON.stringify(author)}\n</hcodex-input-author>\n\n`,
    text_elements: [],
  });
  assert.deepEqual(withoutInputAuthor(encoded, clientId), original);
  assert.equal(encoded.length, 3);
});

test("legacy concatenated input strips one header and keeps user content and other parts", () => {
  const nested = (withInputAuthor([], author)[0] as { text: string }).text;
  const original = [
    { type: "text", text: `${nested}用户文本`, text_elements: [] },
    { type: "image", url: "image" },
  ];
  const encoded = withInputAuthor(original, author);
  const combined = [
    { ...(encoded[0] as object), text: nested + original[0].text },
    original[1],
  ];
  const before = structuredClone(combined);
  assert.deepEqual(withoutInputAuthor(combined, clientId), original);
  assert.deepEqual(combined, before);
});

test("untrusted, malformed and mismatched headers remain untouched", () => {
  const valid = withInputAuthor([{ type: "text", text: "body" }], author);
  for (const id of [
    null,
    "bad",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:abcdefab-abcd-abcd-abcd-abcdefabcdef",
  ])
    assert.equal(withoutInputAuthor(valid, id), valid);
  for (const json of [
    "not json",
    JSON.stringify({ ...author, extra: true }),
    JSON.stringify({ userId: author.userId, name: 3 }),
    JSON.stringify({ name: author.name }),
  ]) {
    const input = [
      {
        type: "text",
        text: `<hcodex-input-author>\n${json}\n</hcodex-input-author>\n\nbody`,
      },
    ];
    assert.equal(withoutInputAuthor(input, clientId), input);
  }
  const original = [{ type: "text", text: "ordinary message" }];
  assert.equal(withoutInputAuthor(original, clientId), original);
  const image = [{ type: "image", text: (valid[0] as { text: string }).text }];
  assert.equal(withoutInputAuthor(image, clientId), image);
});
