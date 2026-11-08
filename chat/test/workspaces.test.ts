import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import pg from "pg";
import { createConversationStore } from "../src/store.ts";
import { createWorkspaceRouter, inspectWorkspace } from "../src/workspaces.ts";

const url = process.env.HCODEX_PG_TEST_URL;
const schema = `hcodex_workspaces_test_${process.pid}_${Date.now()}`;
let pool: pg.Pool;
let second: pg.Pool;
let inspect: () => Promise<boolean> = async () => false;
let router: ReturnType<typeof createWorkspaceRouter>;
let peer: ReturnType<typeof createWorkspaceRouter>;
const oldTarget = "ws://old:4500/";
const newTarget = "ws://new:4500/";
const skipped = !url && "HCODEX_PG_TEST_URL is not set";
before(async () => {
  if (!url) return;
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  try {
    await admin.query(`CREATE SCHEMA ${schema}`);
  } finally {
    await admin.end();
  }
  pool = new pg.Pool({
    connectionString: url,
    options: `-c search_path=${schema}`,
  });
  second = new pg.Pool({
    connectionString: url,
    options: `-c search_path=${schema}`,
  });
  await createConversationStore(pool).migrate();
  const options = {
    inspect: async () => inspect(),
    pollMs: 2,
    switchMs: 1000,
    inflightMs: 1000,
    waitMs: 3000,
  };
  router = createWorkspaceRouter(pool, options);
  peer = createWorkspaceRouter(second, options);
  await Promise.all([router.migrate(), peer.migrate()]);
});
after(async () => {
  if (!url) return;
  await second?.end();
  if (pool) {
    try {
      await pool.query(`DROP SCHEMA ${schema} CASCADE`);
    } finally {
      await pool.end();
    }
  }
});

test(
  "allocation is shared and changing pool targets only affects new workspaces",
  { skip: skipped },
  async () => {
    inspect = async () => false;
    await router.setTargets("allocation", [oldTarget]);
    const assigned = await Promise.all(
      Array.from({ length: 24 }, (_, i) =>
        (i % 2 ? router : peer).route("allocation", "same"),
      ),
    );
    assert.deepEqual(new Set(assigned), new Set([oldTarget]));
    assert.equal((await router.listRoutes("allocation")).length, 1);
    assert.equal(await peer.setTargets("allocation", [newTarget], []), false);
    assert.equal(
      await peer.setTargets("allocation", [newTarget], [oldTarget]),
      true,
    );
    assert.equal(await router.route("allocation", "same"), oldTarget);
    assert.equal(await peer.route("allocation", "new"), newTarget);
    assert.equal(
      await router.switchWorkspace("allocation", "same", oldTarget, newTarget),
      "switched",
    );
    assert.equal(await peer.route("allocation", "same"), newTarget);
  },
);

test(
  "activity only reflects native conversations; input admission still coordinates a switch",
  { skip: skipped },
  async () => {
    inspect = async () => false;
    await router.setTargets("inflight", [oldTarget]);
    const input = await router.admit("inflight", "w");
    await peer.setTargets("inflight", [newTarget]);
    assert.equal(
      await peer.isActive("inflight", "w"),
      false,
      "RPC bookkeeping must not make a native idle conversation active",
    );
    const switching = peer.switchWorkspace(
      "inflight",
      "w",
      oldTarget,
      newTarget,
    );
    // Wait until the gate is persistently closed, then submit via the other SDK.
    while (
      !(
        await pool.query(
          "SELECT 1 FROM hcodex_workspace_routes WHERE runtime_id=$1 AND switch_token IS NOT NULL",
          ["inflight"],
        )
      ).rowCount
    )
      await sleep(1);
    const waiting = router.admit("inflight", "w");
    await input.complete();
    assert.equal(await switching, "switched");
    const admitted = await waiting;
    assert.equal(admitted.target, newTarget);
    await admitted.complete();
  },
);

test(
  "an abandoned input reservation expires without pinning an idle conversation",
  { skip: skipped },
  async () => {
    inspect = async () => false;
    const shortReservation = createWorkspaceRouter(pool, {
      inspect: async () => inspect(),
      inflightMs: 5,
      pollMs: 2,
    });
    await router.setTargets("abandoned", [oldTarget]);
    await shortReservation.admit("abandoned", "w");
    await router.setTargets("abandoned", [newTarget]);
    await sleep(15);
    assert.equal(await router.isActive("abandoned", "w"), false);
    assert.equal(
      await peer.switchWorkspace("abandoned", "w", oldTarget, newTarget),
      "switched",
    );
    assert.equal(
      Number(
        (
          await pool.query(
            "SELECT count(*) FROM hcodex_workspace_inflight WHERE runtime_id=$1",
            ["abandoned"],
          )
        ).rows[0].count,
      ),
      0,
    );
  },
);

test(
  "a separate SDK process waits on the PG switching gate and admits once to the new target",
  { skip: skipped, timeout: 20_000 },
  async () => {
    const runtime = "cross-process";
    inspect = async () => false;
    await router.setTargets(runtime, [oldTarget]);
    await router.route(runtime, "w");
    await router.setTargets(runtime, [newTarget], [oldTarget]);

    // The worker imports the real SDK and opens its own connection pool. Credentials travel only
    // in inherited environment; messages and failures never include the connection string.
    const source = `
    import pg from "pg";
    const { createWorkspaceRouter } = await import(process.env.HCODEX_TEST_WORKSPACES_URL);
    const pool = new pg.Pool({ connectionString: process.env.HCODEX_PG_TEST_URL,
      options: "-c search_path=" + process.env.HCODEX_TEST_SCHEMA });
    const router = createWorkspaceRouter(pool, { inspect: async () => false, pollMs: 5, waitMs: 5000 });
    process.on("message", async (message) => {
      if (message !== "admit") return;
      try {
        process.send({ type: "attempting" });
        const admission = await router.admit("cross-process", "w");
        process.send({ type: "admitted", target: admission.target, epoch: admission.epoch });
        await admission.complete();
        await pool.end();
        process.send({ type: "done" }, () => process.disconnect());
      } catch {
        await pool.end();
        process.send({ type: "error" }, () => { process.exitCode = 1; process.disconnect(); });
      }
    });
    process.send({ type: "ready" });
  `;
    const worker = spawn(
      process.execPath,
      ["--import", "tsx", "--input-type=module", "--eval", source],
      {
        cwd: fileURLToPath(new URL("..", import.meta.url)),
        env: {
          ...process.env,
          HCODEX_TEST_SCHEMA: schema,
          HCODEX_TEST_WORKSPACES_URL: new URL(
            "../src/workspaces.ts",
            import.meta.url,
          ).href,
        },
        stdio: ["ignore", "ignore", "ignore", "ipc"],
      },
    );
    const messages: Array<{ type?: string; target?: string; epoch?: string }> =
      [];
    worker.on("message", (message) =>
      messages.push(
        message as { type?: string; target?: string; epoch?: string },
      ),
    );
    const exit = new Promise<{
      code: number | null;
      signal: NodeJS.Signals | null;
    }>((resolve) => {
      worker.once("exit", (code, signal) => resolve({ code, signal }));
      worker.once("error", () => resolve({ code: -1, signal: null }));
    });
    let unblock: ((active: boolean) => void) | undefined;
    let switching: ReturnType<typeof router.switchWorkspace> | undefined;
    try {
      const waitFor = async (predicate: () => boolean) => {
        const deadline = Date.now() + 5000;
        while (!predicate()) {
          if (worker.exitCode !== null || worker.signalCode !== null)
            throw new Error("SDK worker exited prematurely");
          if (Date.now() >= deadline)
            throw new Error("SDK worker synchronization timeout");
          await sleep(5);
        }
      };
      await waitFor(() => messages.some((message) => message.type === "ready"));
      let checks = 0;
      inspect = async () =>
        ++checks === 2
          ? new Promise<boolean>((resolve) => {
              unblock = resolve;
            })
          : false;
      switching = router.switchWorkspace(runtime, "w", oldTarget, newTarget);
      await waitFor(() => Boolean(unblock));
      assert.equal(
        (
          await pool.query(
            "SELECT switch_token IS NOT NULL AS closed FROM hcodex_workspace_routes WHERE runtime_id=$1 AND workspace_id=$2",
            [runtime, "w"],
          )
        ).rows[0]?.closed,
        true,
      );
      worker.send("admit");
      await waitFor(() =>
        messages.some((message) => message.type === "attempting"),
      );
      await sleep(75);
      assert.equal(
        messages.some((message) => message.type === "admitted"),
        false,
        "cross-process admission must wait at the closed gate",
      );
      assert.equal(
        Number(
          (
            await pool.query(
              "SELECT count(*) FROM hcodex_workspace_inflight WHERE runtime_id=$1 AND workspace_id=$2",
              [runtime, "w"],
            )
          ).rows[0]?.count,
        ),
        0,
      );
      unblock!(false);
      assert.equal(await switching, "switched");
      await waitFor(() => messages.some((message) => message.type === "done"));
      const admissions = messages.filter(
        (message) => message.type === "admitted",
      );
      assert.equal(admissions.length, 1);
      assert.equal(admissions[0]?.target, newTarget);
      assert.equal(admissions[0]?.epoch, "2");
      assert.equal((await exit).code, 0);
      assert.equal(
        Number(
          (
            await pool.query(
              "SELECT count(*) FROM hcodex_workspace_inflight WHERE runtime_id=$1 AND workspace_id=$2",
              [runtime, "w"],
            )
          ).rows[0]?.count,
        ),
        0,
      );
    } finally {
      unblock?.(false);
      await switching?.catch(() => {});
      inspect = async () => false;
      if (worker.exitCode === null && worker.signalCode === null)
        worker.kill("SIGKILL");
      await exit;
    }
  },
);

test(
  "second activity check reopens the old target without losing a waiting input",
  { skip: skipped },
  async () => {
    await router.setTargets("active", [oldTarget]);
    await router.route("active", "w");
    await router.setTargets("active", [newTarget]);
    let calls = 0;
    inspect = async () => ++calls === 2;
    assert.equal(
      await router.switchWorkspace("active", "w", oldTarget, newTarget),
      "still_active",
    );
    const next = await peer.admit("active", "w");
    assert.equal(next.target, oldTarget);
    await next.complete();
  },
);

test(
  "an expired switching worker cannot commit after a sender reopens admission",
  { skip: skipped },
  async () => {
    await router.setTargets("fencing", [oldTarget]);
    await router.route("fencing", "w");
    await router.setTargets("fencing", [newTarget]);
    let resolve!: (active: boolean) => void;
    let checks = 0;
    inspect = async () =>
      ++checks === 2
        ? new Promise<boolean>((done) => {
            resolve = done;
          })
        : false;
    const shortLease = createWorkspaceRouter(pool, {
      inspect: async () => inspect(),
      switchMs: 100,
      pollMs: 2,
    });
    const switching = shortLease.switchWorkspace(
      "fencing",
      "w",
      oldTarget,
      newTarget,
    );
    while (!resolve) await sleep(1);
    await sleep(110);
    const input = await peer.admit("fencing", "w");
    resolve(false);
    assert.equal(await switching, "changed");
    assert.equal(input.target, oldTarget);
    assert.equal(await router.route("fencing", "w"), oldTarget);
    await input.complete();
  },
);

test(
  "confirmed process retirement reassigns routes without replaying inputs",
  { skip: skipped },
  async () => {
    inspect = async () => false;
    await router.setTargets("dead", [oldTarget]);
    await router.admit("dead", "w"); // Simulated process crash: no acknowledgement.
    await assert.rejects(
      router.retireTarget("dead", oldTarget),
      /removed from allocation/,
    );
    await router.setTargets("dead", [newTarget]);
    await router.retireTarget("dead", oldTarget);
    assert.equal(await peer.route("dead", "w"), newTarget);
    assert.equal(await peer.isActive("dead", "w"), false);
  },
);

test("unloaded threads still check the persistent queue; terminal and goal activity is included", async () => {
  const calls: string[] = [];
  let queued = true;
  let loaded = false;
  let systemError = false;
  let terminal = false;
  let goal = false;
  const client = {
    async request(method: string) {
      calls.push(method);
      if (method === "thread/queue/list") return { data: queued ? [{}] : [] };
      if (method === "thread/read")
        return {
          thread: {
            status: {
              type: systemError ? "systemError" : loaded ? "idle" : "notLoaded",
            },
          },
        };
      if (method === "thread/backgroundTerminals/list")
        return { data: terminal ? [{}] : [] };
      if (method === "thread/goal/get")
        return { goal: goal ? { status: "active" } : null };
      throw new Error("unexpected request");
    },
  };
  assert.equal(await inspectWorkspace(client, ["thread"]), true);
  assert.deepEqual(calls, ["thread/queue/list"]);
  queued = false;
  assert.equal(await inspectWorkspace(client, ["thread"]), false);
  loaded = terminal = true;
  assert.equal(await inspectWorkspace(client, ["thread"]), true);
  systemError = true;
  assert.equal(await inspectWorkspace(client, ["thread"]), true);
  terminal = false;
  assert.equal(await inspectWorkspace(client, ["thread"]), false);
  goal = true;
  assert.equal(
    await inspectWorkspace(client, ["thread"], { checkGoals: true }),
    true,
  );
});

test("queue failures are ignored only after native archive membership is verified", async () => {
  const failure = new Error("queue unavailable");
  let archived = true;
  const client = {
    async request(method: string, params?: Record<string, unknown>) {
      if (method === "thread/queue/list") throw failure;
      if (method === "thread/read")
        return { thread: { cwd: "/workspace", status: { type: "notLoaded" } } };
      if (method === "thread/list") {
        assert.equal(params?.archived, true);
        return { data: archived ? [{ id: "archived" }] : [], nextCursor: null };
      }
      throw new Error("unexpected request");
    },
  };
  assert.equal(await inspectWorkspace(client, ["archived"]), false);
  archived = false;
  await assert.rejects(inspectWorkspace(client, ["archived"]), failure);
});
