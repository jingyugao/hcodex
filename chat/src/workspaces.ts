import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { setTimeout as sleep } from "node:timers/promises";
import type pg from "pg";

import { rendezvous } from "./routing.js";
import type { NativeRequester } from "./session.js";
import type { Log } from "./session.js";

export interface WorkspaceRoute {
  runtimeId: string;
  workspaceId: string;
  target: string;
  epoch: string;
}
export interface WorkspaceAdmission extends WorkspaceRoute {
  /** Call only after a definite response, or when the request was definitely not sent. */
  complete(): Promise<void>;
}
export type SwitchResult = "switched" | "still_active" | "changed";
export interface WorkspaceRouter {
  route(runtimeId: string, workspaceId: string): Promise<string>;
  admit(runtimeId: string, workspaceId: string): Promise<WorkspaceAdmission>;
  isActive(runtimeId: string, workspaceId: string): Promise<boolean>;
  /** newTarget is a fresh process. A former owner must be stopped/recreated before reuse. */
  switchWorkspace(
    runtimeId: string,
    workspaceId: string,
    oldTarget: string,
    newTarget: string,
  ): Promise<SwitchResult>;
  listRoutes(runtimeId: string): Promise<WorkspaceRoute[]>;
  getTargets(runtimeId: string): Promise<string[]>;
  /** CAS the targets for NEW workspaces; existing workspaces stay where they are. */
  setTargets(
    runtimeId: string,
    targets: readonly string[],
    expected?: readonly string[],
  ): Promise<boolean>;
  /** Host must first prove this exact target process has terminated. Never replays inputs. */
  retireTarget(runtimeId: string, target: string): Promise<void>;
  migrate(): Promise<void>;
}

/** Native inspection never resumes a thread or acquires its writer. Goals are opt-in. */
export async function inspectWorkspace(
  client: NativeRequester,
  threadIds: readonly string[],
  options: { checkGoals?: boolean } = {},
): Promise<boolean> {
  for (const threadId of threadIds) {
    let queue: { data: unknown[] };
    try {
      queue = (await client.request("thread/queue/list", {
        threadId,
        limit: 1,
      })) as { data: unknown[] };
    } catch (error) {
      // Native queue APIs reject archived sessions. Verify archive membership using the public
      // listing API; never infer it from error text or hide a dependency failure.
      if (await isArchived(client, threadId)) continue;
      throw error;
    }
    if (!Array.isArray(queue.data)) throw new Error("Invalid queue response");
    if (queue.data.length) return true;
    const result = (await client.request("thread/read", {
      threadId,
      includeTurns: false,
    })) as {
      thread: { status: { type: string } };
    };
    const status = result.thread?.status?.type;
    if (status === "active") return true;
    if (status !== "idle" && status !== "notLoaded" && status !== "systemError")
      throw new Error(`Unknown thread activity: ${status}`);
    if (status !== "notLoaded") {
      const terminals = (await client.request(
        "thread/backgroundTerminals/list",
        { threadId, limit: 1 },
      )) as { data: unknown[] };
      if (!Array.isArray(terminals.data))
        throw new Error("Invalid background terminals response");
      if (terminals.data.length) return true;
    }
    if (options.checkGoals) {
      const result = (await client.request("thread/goal/get", {
        threadId,
      })) as { goal?: { status: string } | null };
      if (result.goal?.status === "active") return true;
    }
  }
  return false;
}

async function isArchived(
  client: NativeRequester,
  threadId: string,
): Promise<boolean> {
  const read = (await client.request("thread/read", {
    threadId,
    includeTurns: false,
  })) as {
    thread: { cwd?: string; status: { type: string } };
  };
  if (
    read.thread?.status?.type !== "notLoaded" ||
    typeof read.thread.cwd !== "string"
  )
    return false;
  let cursor: string | undefined;
  for (let page = 0; page < 16; page++) {
    const listed = (await client.request("thread/list", {
      archived: true,
      cwd: read.thread.cwd,
      modelProviders: [],
      limit: 100,
      ...(cursor ? { cursor } : {}),
    })) as { data: { id: string }[]; nextCursor?: string | null };
    if (!Array.isArray(listed.data))
      throw new Error("Invalid archived thread list");
    if (listed.data.some((thread) => thread.id === threadId)) return true;
    if (!listed.nextCursor || listed.nextCursor === cursor) return false;
    cursor = listed.nextCursor;
  }
  return false;
}

type RouteRow = {
  runtime_id: string;
  workspace_id: string;
  target: string;
  epoch: string;
  switch_token: string | null;
};
const route = (r: RouteRow): WorkspaceRoute => ({
  runtimeId: r.runtime_id,
  workspaceId: r.workspace_id,
  target: r.target,
  epoch: r.epoch,
});
const canonical = (targets: readonly string[]) => [...new Set(targets)].sort();

/**
 * Native conversation state determines activity. Short-lived input reservations only coordinate
 * sending with switching; disconnected callers cannot leave a permanent activity marker.
 * Switching leases may expire safely: every commit checks its unique token and epoch.
 */
export function createWorkspaceRouter(
  pool: pg.Pool,
  options: {
    inspect(
      runtimeId: string,
      target: string,
      threadIds: readonly string[],
    ): Promise<boolean>;
    waitMs?: number;
    pollMs?: number;
    inflightMs?: number;
    switchMs?: number;
    log?: Log;
  },
): WorkspaceRouter {
  const waitMs = options.waitMs ?? 60_000;
  const pollMs = options.pollMs ?? 100;
  const switchMs = options.switchMs ?? 15_000;
  const transaction = async <T>(
    fn: (db: pg.PoolClient) => Promise<T>,
  ): Promise<T> => {
    const db = await pool.connect();
    try {
      await db.query("BEGIN");
      await db.query(
        "SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '10s'",
      );
      const result = await fn(db);
      await db.query("COMMIT");
      return result;
    } catch (error) {
      await db.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      db.release();
    }
  };
  const lockedRoute = async (
    db: pg.PoolClient,
    runtimeId: string,
    workspaceId: string,
  ): Promise<RouteRow> => {
    // Lock the pool first, in the same order as allocation and retirement.
    const config = await db.query<{ targets: string[] }>(
      "SELECT targets FROM hcodex_runtime_routes WHERE runtime_id=$1 FOR SHARE",
      [runtimeId],
    );
    if (!config.rows[0]?.targets.length)
      throw new Error(`No workspace targets: ${runtimeId}`);
    await db.query(
      `INSERT INTO hcodex_workspace_routes(runtime_id,workspace_id,target) VALUES($1,$2,$3)
       ON CONFLICT DO NOTHING`,
      [runtimeId, workspaceId, rendezvous(workspaceId, config.rows[0].targets)],
    );
    await db.query(
      `UPDATE hcodex_workspace_routes SET switch_token=NULL,switch_expires_at=NULL
       WHERE runtime_id=$1 AND workspace_id=$2 AND switch_expires_at < clock_timestamp()`,
      [runtimeId, workspaceId],
    );
    return (
      await db.query<RouteRow>(
        "SELECT * FROM hcodex_workspace_routes WHERE runtime_id=$1 AND workspace_id=$2 FOR UPDATE",
        [runtimeId, workspaceId],
      )
    ).rows[0]!;
  };
  const threads = async (runtimeId: string, workspaceId: string) =>
    (
      await pool.query<{ thread_id: string }>(
        "SELECT DISTINCT thread_id FROM hcodex_conversations WHERE runtime_id=$1 AND workspace_id=$2 AND thread_id IS NOT NULL",
        [runtimeId, workspaceId],
      )
    ).rows.map((r) => r.thread_id);
  const pending = async (runtimeId: string, workspaceId: string) =>
    (
      await pool.query(
        "SELECT 1 FROM hcodex_workspace_inflight WHERE runtime_id=$1 AND workspace_id=$2 AND expires_at > clock_timestamp() LIMIT 1",
        [runtimeId, workspaceId],
      )
    ).rowCount !== 0;

  const router: WorkspaceRouter = {
    async migrate() {
      const sql = await readFile(
        new URL("../sql/0002_workspaces.sql", import.meta.url),
        "utf8",
      );
      await transaction(async (db) => {
        await db.query("SELECT pg_advisory_xact_lock($1)", [0x68636f646578]);
        await db.query(sql);
      });
    },
    async setTargets(runtimeId, targets, expected) {
      const next = canonical(targets);
      if (!next.length)
        throw new Error("At least one workspace target is required");
      for (const target of next) {
        const url = new URL(target);
        if (
          !["ws:", "wss:"].includes(url.protocol) ||
          url.username ||
          url.password ||
          url.hash
        )
          throw new Error("Invalid workspace target");
      }
      return transaction(async (db) => {
        await db.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [
          `hcodex:pool:${runtimeId}`,
        ]);
        const old = (
          await db.query<{ targets: string[] }>(
            "SELECT targets FROM hcodex_runtime_routes WHERE runtime_id=$1 FOR UPDATE",
            [runtimeId],
          )
        ).rows[0];
        if (
          expected &&
          JSON.stringify(canonical(old?.targets ?? [])) !==
            JSON.stringify(canonical(expected))
        )
          return false;
        await db.query(
          `INSERT INTO hcodex_runtime_routes(runtime_id,targets) VALUES($1,$2)
           ON CONFLICT(runtime_id) DO UPDATE SET targets=EXCLUDED.targets,revision=hcodex_runtime_routes.revision+1`,
          [runtimeId, next],
        );
        return true;
      });
    },
    async route(runtimeId, workspaceId) {
      const current = (
        await pool.query<{ target: string }>(
          "SELECT target FROM hcodex_workspace_routes WHERE runtime_id=$1 AND workspace_id=$2",
          [runtimeId, workspaceId],
        )
      ).rows[0];
      if (current) return current.target;
      return (
        await transaction((db) => lockedRoute(db, runtimeId, workspaceId))
      ).target;
    },
    async admit(runtimeId, workspaceId) {
      const until = Date.now() + waitMs;
      for (;;) {
        const input = await transaction(async (db) => {
          const current = await lockedRoute(db, runtimeId, workspaceId);
          if (current.switch_token) return undefined;
          const id = randomUUID();
          await db.query(
            `INSERT INTO hcodex_workspace_inflight(id,runtime_id,workspace_id,target,expires_at)
             VALUES($1,$2,$3,$4,clock_timestamp()+$5::double precision*interval '1 millisecond')`,
            [
              id,
              runtimeId,
              workspaceId,
              current.target,
              options.inflightMs ?? 60_000,
            ],
          );
          return {
            ...route(current),
            complete: async () => {
              await pool.query(
                "DELETE FROM hcodex_workspace_inflight WHERE id=$1",
                [id],
              );
            },
          };
        });
        if (input) return input;
        if (Date.now() >= until)
          throw new Error(`Workspace switching timeout: ${workspaceId}`);
        await sleep(pollMs);
      }
    },
    async isActive(runtimeId, workspaceId) {
      const target = await router.route(runtimeId, workspaceId);
      return options.inspect(
        runtimeId,
        target,
        await threads(runtimeId, workspaceId),
      );
    },
    async listRoutes(runtimeId) {
      return (
        await pool.query<RouteRow>(
          "SELECT * FROM hcodex_workspace_routes WHERE runtime_id=$1 ORDER BY workspace_id",
          [runtimeId],
        )
      ).rows.map(route);
    },
    async getTargets(runtimeId) {
      return (
        (
          await pool.query<{ targets: string[] }>(
            "SELECT targets FROM hcodex_runtime_routes WHERE runtime_id=$1",
            [runtimeId],
          )
        ).rows[0]?.targets ?? []
      );
    },
    async switchWorkspace(runtimeId, workspaceId, oldTarget, newTarget) {
      if (oldTarget === newTarget) return "switched";
      // Cheap first check. The authoritative check is after closing admission and draining sends.
      if (
        await options.inspect(
          runtimeId,
          oldTarget,
          await threads(runtimeId, workspaceId),
        )
      )
        return "still_active";
      const token = randomUUID();
      const acquired = await transaction(async (db) => {
        const current = await lockedRoute(db, runtimeId, workspaceId);
        if (current.target !== oldTarget) return { result: "changed" as const };
        if (current.switch_token) return { result: "still_active" as const };
        const targets = (
          await db.query<{ targets: string[] }>(
            "SELECT targets FROM hcodex_runtime_routes WHERE runtime_id=$1",
            [runtimeId],
          )
        ).rows[0]!.targets;
        if (!targets.includes(newTarget))
          throw new Error("New workspace target is not accepting workspaces");
        await db.query(
          `UPDATE hcodex_workspace_routes SET switch_token=$3,
             switch_expires_at=clock_timestamp()+$4::double precision*interval '1 millisecond'
           WHERE runtime_id=$1 AND workspace_id=$2`,
          [runtimeId, workspaceId, token, switchMs],
        );
        return { epoch: current.epoch };
      });
      if ("result" in acquired) return acquired.result!;
      try {
        // A crashed caller may leave an input reservation. Expiry permits another native
        // activity check, never replay of an input whose outcome is unknown.
        await pool.query(
          "DELETE FROM hcodex_workspace_inflight WHERE runtime_id=$1 AND workspace_id=$2 AND expires_at <= clock_timestamp()",
          [runtimeId, workspaceId],
        );
        const until = Date.now() + switchMs / 2;
        while (await pending(runtimeId, workspaceId)) {
          if (Date.now() >= until) return "still_active";
          await sleep(pollMs);
        }
        if (
          await options.inspect(
            runtimeId,
            oldTarget,
            await threads(runtimeId, workspaceId),
          )
        )
          return "still_active";
        return await transaction(async (db) => {
          await db.query(
            "SELECT targets FROM hcodex_runtime_routes WHERE runtime_id=$1 FOR SHARE",
            [runtimeId],
          );
          const changed = await db.query(
            `UPDATE hcodex_workspace_routes SET target=$3,epoch=epoch+1,switch_token=NULL,switch_expires_at=NULL
             WHERE runtime_id=$1 AND workspace_id=$2 AND target=$4 AND epoch=$5 AND switch_token=$6
               AND switch_expires_at > clock_timestamp()
               AND (SELECT $3=ANY(targets) FROM hcodex_runtime_routes WHERE runtime_id=$1)`,
            [
              runtimeId,
              workspaceId,
              newTarget,
              oldTarget,
              acquired.epoch,
              token,
            ],
          );
          if (!changed.rowCount) {
            const observed = (
              await db.query(
                `SELECT target=$3 AS old_target,epoch=$4 AS same_epoch,switch_token=$5 AS same_token,
               switch_expires_at>clock_timestamp() AS valid,
               (SELECT $6=ANY(targets) FROM hcodex_runtime_routes WHERE runtime_id=$1) AS allowed
               FROM hcodex_workspace_routes WHERE runtime_id=$1 AND workspace_id=$2`,
                [
                  runtimeId,
                  workspaceId,
                  oldTarget,
                  acquired.epoch,
                  token,
                  newTarget,
                ],
              )
            ).rows[0];
            options.log?.("workspace_switch_superseded", {
              runtimeId,
              workspaceId,
              ...observed,
            });
          }
          return changed.rowCount ? "switched" : "changed";
        });
      } finally {
        await pool.query(
          `UPDATE hcodex_workspace_routes SET switch_token=NULL,switch_expires_at=NULL
           WHERE runtime_id=$1 AND workspace_id=$2 AND switch_token=$3`,
          [runtimeId, workspaceId, token],
        );
      }
    },
    async retireTarget(runtimeId, target) {
      // This API is deliberately separate from TTL expiry. Only the host knows Pod/process death.
      await transaction(async (db) => {
        const config = (
          await db.query<{ targets: string[] }>(
            "SELECT targets FROM hcodex_runtime_routes WHERE runtime_id=$1 FOR UPDATE",
            [runtimeId],
          )
        ).rows[0];
        if (!config?.targets.length || config.targets.includes(target))
          throw new Error(
            "Retired target must be removed from allocation first",
          );
        const rows = (
          await db.query<RouteRow>(
            "SELECT * FROM hcodex_workspace_routes WHERE runtime_id=$1 AND target=$2 FOR UPDATE",
            [runtimeId, target],
          )
        ).rows;
        for (const row of rows) {
          await db.query(
            "DELETE FROM hcodex_workspace_inflight WHERE runtime_id=$1 AND workspace_id=$2",
            [runtimeId, row.workspace_id],
          );
          await db.query(
            `UPDATE hcodex_workspace_routes SET target=$3,epoch=epoch+1,switch_token=NULL,switch_expires_at=NULL
             WHERE runtime_id=$1 AND workspace_id=$2`,
            [
              runtimeId,
              row.workspace_id,
              rendezvous(row.workspace_id, config.targets),
            ],
          );
        }
      });
    },
  };
  return router;
}
