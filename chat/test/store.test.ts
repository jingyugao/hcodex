// Against a real Postgres; set HCODEX_PG_TEST_URL to run (CI provides one).
import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import pg from "pg";

import { createConversationStore, type ConversationStore } from "../src/store.ts";
import type { NewConversation } from "../src/types.ts";

const url = process.env.HCODEX_PG_TEST_URL;
const schema = `hcodex_chat_test_${process.pid}_${Date.now()}`;
let pool: pg.Pool;
let store: ConversationStore;

before(async () => {
  if (!url) return;
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  await admin.query(`CREATE SCHEMA ${schema}`);
  await admin.end();
  pool = new pg.Pool({ connectionString: url, options: `-c search_path=${schema}` });
  store = createConversationStore(pool);
  await store.migrate();
  await store.migrate(); // idempotent
});
after(async () => {
  if (!url) return;
  await pool.query(`DROP SCHEMA ${schema} CASCADE`);
  await pool.end();
});

const conversation = (id: string, ownerId: string, extra: Partial<NewConversation> = {}): NewConversation => ({
  id,
  ownerId,
  agentId: "agent-1",
  workspaceId: "ws-1",
  runtimeId: "rt-1",
  title: `Report ${id}`,
  source: "web",
  ...extra,
});
const viewer = (userId: string) => ({ userId });
const ids = (page: { items: { id: string }[] }) => page.items.map((item) => item.id);
const search = (query: Record<string, unknown>) =>
  store.search({ relation: "all", archived: "active", limit: 30, ...query } as never);

test("visibility, relations, filters, metadata and cursor pages", { skip: !url && "HCODEX_PG_TEST_URL is not set" }, async () => {
  await store.create(conversation("a-own", "alice", { metadata: { tags: ["q3"] } }));
  await store.create(conversation("b-joined", "bob", { title: "Weekly sales" }));
  await store.create(conversation("c-not-joined", "bob"));
  await store.recordInput("b-joined", "alice"); // alice joins bob's conversation; it becomes the latest

  // Only conversations the viewer takes part in; the owner always does.
  assert.deepEqual(ids(await search({ viewer: viewer("alice") })), ["b-joined", "a-own"]);
  assert.deepEqual(ids(await search({ viewer: viewer("alice"), relation: "created" })), ["a-own"]);
  assert.deepEqual(ids(await search({ viewer: viewer("alice"), relation: "participated" })), ["b-joined"]);
  assert.deepEqual(ids(await search({ viewer: viewer("alice"), query: "SALES" })), ["b-joined"]);
  const page = await search({ viewer: viewer("alice") });
  assert.equal(page.items[0]!.participantCount, 2);
  assert.deepEqual(page.items[1]!.metadata, { tags: ["q3"] });

  const first = await search({ viewer: viewer("alice"), limit: 1 });
  assert.deepEqual(ids(first), ["b-joined"]);
  const second = await search({ viewer: viewer("alice"), limit: 1, cursor: first.nextCursor });
  assert.deepEqual(ids(second), ["a-own"]);
  assert.equal(second.nextCursor, null);
  await assert.rejects(search({ viewer: viewer("bob"), limit: 1, cursor: first.nextCursor }), /cursor/);

  await store.update("a-own", { archived: true, metadata: { tags: [] } });
  assert.deepEqual(ids(await search({ viewer: viewer("alice"), relation: "created", archived: "archived" })), ["a-own"]);
  assert.deepEqual((await store.get("a-own"))?.metadata, { tags: [] }, "metadata is replaced whole");
  assert.deepEqual(await store.owners(["alice", "bob", "zoe"], viewer("carol")), []);
  assert.deepEqual((await store.owners(["alice", "bob", "zoe"], viewer("alice"))).sort(), ["alice", "bob"]);
  assert.deepEqual((await store.getMany(["c-not-joined", "missing"])).map((c) => c.id), ["c-not-joined"]);
  assert.equal((await store.getParticipant("b-joined", "alice"))?.userId, "alice");
  assert.equal(await store.getParticipant("c-not-joined", "alice"), undefined);
});

test("thread binding happens once and origins map to one conversation", { skip: !url && "HCODEX_PG_TEST_URL is not set" }, async () => {
  await store.create(conversation("bind", "alice"));
  await store.bindThread("bind", "rt-1", "thread-1");
  await store.bindThread("bind", "rt-1", "thread-1"); // same binding again is fine
  await assert.rejects(store.bindThread("bind", "rt-1", "thread-2"), /cannot be changed/);
  await store.create(conversation("bind-2", "alice"));
  await assert.rejects(store.bindThread("bind-2", "rt-1", "thread-1"), /already bound/);
  assert.equal((await store.getByThread("rt-1", "thread-1"))?.id, "bind");

  const origin = { namespace: "feishu", key: "chat-1" };
  const [x, y] = await Promise.all([
    store.ensureForOrigin(origin, conversation("origin-x", "alice")),
    store.ensureForOrigin(origin, conversation("origin-y", "alice")),
  ]);
  assert.equal(x.id, y.id);
  assert.deepEqual((await store.listParticipants(x.id)).map((p) => p.userId), ["alice"]);
});
