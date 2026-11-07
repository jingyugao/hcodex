import assert from "node:assert/strict";
import { test } from "node:test";

import { createReplicaResolver, rendezvous, replicaUrl } from "../src/routing.ts";

test("adding a replica only moves the conversations it wins", () => {
  const before = ["10.0.0.1", "10.0.0.2", "10.0.0.3"];
  const after = [...before, "10.0.0.4"];
  const keys = Array.from({ length: 2000 }, (_, i) => `conversation-${i}`);
  let moved = 0;
  for (const key of keys) {
    const from = rendezvous(key, before);
    const to = rendezvous(key, after);
    assert.equal(rendezvous(key, [...before].reverse()), from, "order of replicas must not matter");
    if (from !== to) {
      moved++;
      assert.equal(to, "10.0.0.4");
    }
  }
  assert.ok(moved > 300 && moved < 700, `about a quarter should move, moved ${moved}`);
});

test("keeps the last answer when DNS fails, and replaces the host in the pool URL", async () => {
  let fail = false;
  const resolver = createReplicaResolver({
    refreshMs: 0,
    lookup: async () => {
      if (fail) throw new Error("dns down");
      return ["10.0.0.2", "10.0.0.1"];
    },
  });
  const first = await replicaUrl(resolver, "ws://runtime-a.colony.svc:4900/path?x=1", "c1");
  fail = true;
  assert.equal(await replicaUrl(resolver, "ws://runtime-a.colony.svc:4900/path?x=1", "c1"), first);
  assert.match(first, /^ws:\/\/10\.0\.0\.[12]:4900\/path\?x=1$/);
});
