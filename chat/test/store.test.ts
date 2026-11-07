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

const conversation = (id: string, ownerId: string, tenant: string, extra: Partial<NewConversation> = {}): NewConversation => ({
  id,
  ownerId,
  tenant,
  agentId: "agent-1",
  workspaceId: "ws-1",
  runtimeId: "rt-1",
  title: `Report ${id}`,
  source: "web",
  ...extra,
});
const viewer = (userId: string, tenant: string) => ({ userId, tenant });
const ids = (page: { items: { id: string }[] }) => page.items.map((item) => item.id);
const search = (query: Record<string, unknown>) =>
  store.search({ scope: "mine", relation: "all", archived: "active", limit: 30, ...query } as never);

test("visibility, relations, filters and cursor pages", { skip: !url && "HCODEX_PG_TEST_URL is not set" }, async () => {
  await store.create(conversation("a-own", "alice", "t1"));
  await store.create(conversation("b-team", "bob", "t1", { title: "Weekly sales" }));
  await store.create(conversation("c-other-tenant", "carol", "t2"));
  await store.create(conversation("d-private", "dave", ""));
  await store.recordInput("b-team", "alice"); // alice joins bob's conversation; it becomes the latest

  assert.deepEqual(ids(await search({ viewer: viewer("alice", "t1") })), ["b-team", "a-own"]);
  assert.deepEqual(ids(await search({ viewer: viewer("alice", "t1"), relation: "created" })), ["a-own"]);
  assert.deepEqual(ids(await search({ viewer: viewer("alice", "t1"), relation: "participated" })), ["b-team"]);
  assert.deepEqual(ids(await search({ viewer: viewer("alice", "t1"), scope: "team" })), ["b-team", "a-own"]);
  assert.deepEqual(ids(await search({ viewer: viewer("alice", ""), scope: "team" })), ["a-own"], "no tenant: own only");
  assert.deepEqual(ids(await search({ viewer: viewer("alice", "t1"), scope: "team", query: "SALES" })), ["b-team"]);
  const team = await search({ viewer: viewer("alice", "t1"), scope: "team" });
  assert.equal(team.items[0]!.participantCount, 2);

  const first = await search({ viewer: viewer("alice", "t1"), scope: "team", limit: 1 });
  assert.deepEqual(ids(first), ["b-team"]);
  const second = await search({ viewer: viewer("alice", "t1"), scope: "team", limit: 1, cursor: first.nextCursor });
  assert.deepEqual(ids(second), ["a-own"]);
  assert.equal(second.nextCursor, null);
  await assert.rejects(search({ viewer: viewer("bob", "t1"), scope: "team", limit: 1, cursor: first.nextCursor }), /cursor/);

  await store.update("a-own", { archived: true });
  assert.deepEqual(ids(await search({ viewer: viewer("alice", "t1"), relation: "created", archived: "archived" })), ["a-own"]);
  assert.deepEqual(await store.owners(["alice", "zoe"]), ["alice"]);
});

test("thread binding happens once and origins map to one conversation", { skip: !url && "HCODEX_PG_TEST_URL is not set" }, async () => {
  await store.create(conversation("bind", "alice", "t1"));
  await store.bindThread("bind", "rt-1", "thread-1");
  await store.bindThread("bind", "rt-1", "thread-1"); // same binding again is fine
  await assert.rejects(store.bindThread("bind", "rt-1", "thread-2"), /cannot be changed/);
  await store.create(conversation("bind-2", "alice", "t1"));
  await assert.rejects(store.bindThread("bind-2", "rt-1", "thread-1"), /already bound/);
  assert.equal((await store.getByThread("rt-1", "thread-1"))?.id, "bind");

  const origin = { namespace: "feishu", key: "chat-1" };
  const [x, y] = await Promise.all([
    store.ensureForOrigin(origin, conversation("origin-x", "alice", "t1")),
    store.ensureForOrigin(origin, conversation("origin-y", "alice", "t1")),
  ]);
  assert.equal(x.id, y.id);
  assert.deepEqual((await store.listParticipants(x.id)).map((p) => p.userId), ["alice"]);
});
