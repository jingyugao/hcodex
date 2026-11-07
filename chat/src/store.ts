import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import type pg from "pg";

import {
  ConversationConflict,
  ConversationNotFound,
  InvalidRequest,
  type Conversation,
  type ConversationOrigin,
  type NewConversation,
  type Participant,
  type SearchPage,
  type SearchQuery,
} from "./types.js";

type Row = Record<string, any>;
const MIGRATION_LOCK = 0x68636f646578; // "hcodex"
const UNIQUE_VIOLATION = "23505";
const unique = (error: unknown) => (error as { code?: string })?.code === UNIQUE_VIOLATION;
const iso = (value: Date | string) => new Date(value).toISOString();
/** SQL condition: the viewer (`$1`) takes part in conversation `c`. */
const TAKES_PART =
  "EXISTS (SELECT 1 FROM hcodex_conversation_participants p WHERE p.conversation_id = c.id AND p.user_id = $1)";

const conversation = (r: Row): Conversation => ({
  id: r.id,
  ownerId: r.owner_id,
  agentId: r.agent_id,
  workspaceId: r.workspace_id,
  runtimeId: r.runtime_id,
  threadId: r.thread_id ?? null,
  title: r.title,
  source: r.source,
  archived: r.archived,
  metadata: r.metadata,
  createdAt: iso(r.created_at),
  updatedAt: iso(r.updated_at),
  lastActivityAt: iso(r.last_activity_at),
});
const participant = (r: Row): Participant => ({
  conversationId: r.conversation_id,
  userId: r.user_id,
  joinedAt: iso(r.joined_at),
  lastActiveAt: iso(r.last_active_at),
});

/** Rows as exported from another store, for a one-time import. */
export interface ImportBatch {
  conversations?: (Omit<Conversation, "metadata"> & { metadata?: Record<string, unknown> })[];
  participants?: Participant[];
  origins?: (ConversationOrigin & { conversationId: string })[];
}

export type ConversationStore = ReturnType<typeof createConversationStore>;

/** Conversation, participant and origin storage. Schema migration runs once per process. */
export function createConversationStore(pool: pg.Pool) {
  const one = async (sql: string, args: unknown[]) => (await pool.query(sql, args)).rows[0] as Row | undefined;
  const find = async (id: string) => {
    const row = await one("SELECT * FROM hcodex_conversations WHERE id = $1", [id]);
    return row ? conversation(row) : undefined;
  };
  const mustFind = async (id: string) => {
    const value = await find(id);
    if (!value) throw new ConversationNotFound();
    return value;
  };
  const insert = async (db: pg.PoolClient, input: NewConversation) => {
    await db.query(
      `INSERT INTO hcodex_conversations (id, owner_id, agent_id, workspace_id, runtime_id, title, source, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [input.id, input.ownerId, input.agentId, input.workspaceId, input.runtimeId, input.title, input.source,
        input.metadata ?? {}],
    );
    await db.query(
      `INSERT INTO hcodex_conversation_participants (conversation_id, user_id) VALUES ($1, $2)
       ON CONFLICT (conversation_id, user_id) DO UPDATE SET last_active_at = now()`,
      [input.id, input.ownerId],
    );
  };
  const transaction = async <T>(fn: (db: pg.PoolClient) => Promise<T>): Promise<T> => {
    const db = await pool.connect();
    try {
      await db.query("BEGIN");
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

  return {
    /** Creates the tables if needed; concurrent replicas serialize on an advisory lock. */
    async migrate() {
      const sql = await readFile(new URL("../sql/0001_init.sql", import.meta.url), "utf8");
      await transaction(async (db) => {
        await db.query("SELECT pg_advisory_xact_lock($1)", [MIGRATION_LOCK]);
        await db.query(sql);
      });
    },

    async create(input: NewConversation): Promise<Conversation> {
      try {
        await transaction((db) => insert(db, input));
      } catch (error) {
        if (unique(error)) throw new ConversationConflict("Conversation already exists");
        throw error;
      }
      return mustFind(input.id);
    },

    /** One conversation per external origin; concurrent callers get the same conversation. */
    async ensureForOrigin(origin: ConversationOrigin, input: NewConversation): Promise<Conversation> {
      try {
        // The origin references the conversation, so insert it first; a taken origin rolls both back.
        await transaction(async (db) => {
          await insert(db, input);
          await db.query(
            "INSERT INTO hcodex_conversation_origins (namespace, key, conversation_id) VALUES ($1, $2, $3)",
            [origin.namespace, origin.key, input.id],
          );
        });
        return mustFind(input.id);
      } catch (error) {
        if (!unique(error)) throw error;
        const found = await one(
          "SELECT conversation_id FROM hcodex_conversation_origins WHERE namespace = $1 AND key = $2",
          [origin.namespace, origin.key],
        );
        if (!found) throw new ConversationConflict("Conversation already exists");
        return mustFind(found.conversation_id);
      }
    },

    get: find,

    /** The conversations among these IDs that exist, in no particular order. */
    async getMany(ids: string[]): Promise<Conversation[]> {
      if (!ids.length) return [];
      const { rows } = await pool.query("SELECT * FROM hcodex_conversations WHERE id = ANY($1::text[])", [ids]);
      return rows.map(conversation);
    },

    async getByThread(runtimeId: string, threadId: string): Promise<Conversation | undefined> {
      const row = await one("SELECT * FROM hcodex_conversations WHERE runtime_id = $1 AND thread_id = $2", [
        runtimeId,
        threadId,
      ]);
      return row ? conversation(row) : undefined;
    },

    /** `metadata` replaces the stored value whole. */
    async update(
      id: string,
      patch: { title?: string; archived?: boolean; metadata?: Record<string, unknown> },
    ): Promise<Conversation> {
      const row = await one(
        `UPDATE hcodex_conversations SET title = COALESCE($2, title), archived = COALESCE($3, archived),
           metadata = COALESCE($4, metadata), updated_at = now() WHERE id = $1 RETURNING *`,
        [id, patch.title ?? null, patch.archived ?? null, patch.metadata ?? null],
      );
      if (!row) throw new ConversationNotFound();
      return conversation(row);
    },

    /** Compare-and-set; repeating the same binding is a no-op, any other change a conflict. */
    async bindThread(id: string, runtimeId: string, threadId: string): Promise<void> {
      let bound: Row | undefined;
      try {
        bound = await one(
          `UPDATE hcodex_conversations SET thread_id = $3, updated_at = now()
           WHERE id = $1 AND runtime_id = $2 AND NOT archived AND thread_id IS NULL RETURNING id`,
          [id, runtimeId, threadId],
        );
      } catch (error) {
        if (unique(error)) throw new ConversationConflict("Thread is already bound");
        throw error;
      }
      if (bound) return;
      const current = await mustFind(id);
      if (current.archived || current.runtimeId !== runtimeId || current.threadId !== threadId)
        throw new ConversationConflict("Thread binding cannot be changed");
    },

    /** An accepted input: refresh the author as a participant and move the activity forward. */
    async recordInput(conversationId: string, userId: string): Promise<void> {
      await pool.query(
        `WITH touched AS (
           INSERT INTO hcodex_conversation_participants (conversation_id, user_id) VALUES ($1, $2)
           ON CONFLICT (conversation_id, user_id) DO UPDATE SET last_active_at = now()
         )
         UPDATE hcodex_conversations SET last_activity_at = GREATEST(last_activity_at, now()) WHERE id = $1`,
        [conversationId, userId],
      );
    },

    async addParticipant(conversationId: string, userId: string): Promise<void> {
      await pool.query(
        `INSERT INTO hcodex_conversation_participants (conversation_id, user_id) VALUES ($1, $2)
         ON CONFLICT (conversation_id, user_id) DO UPDATE SET last_active_at = now()`,
        [conversationId, userId],
      );
    },

    async getParticipant(conversationId: string, userId: string): Promise<Participant | undefined> {
      const row = await one(
        "SELECT * FROM hcodex_conversation_participants WHERE conversation_id = $1 AND user_id = $2",
        [conversationId, userId],
      );
      return row ? participant(row) : undefined;
    },

    async listParticipants(conversationId: string, query: { after?: string; limit?: number } = {}): Promise<Participant[]> {
      const limit = Math.max(1, Math.min(100, Math.floor(query.limit ?? 50)));
      const { rows } = await pool.query(
        `SELECT * FROM hcodex_conversation_participants WHERE conversation_id = $1 AND user_id > $2
         ORDER BY user_id LIMIT $3`,
        [conversationId, query.after ?? "", limit],
      );
      return rows.map(participant);
    },

    /** The viewer's conversations (those they take part in) newest first. */
    async search(query: SearchQuery): Promise<SearchPage> {
      const { cursor, limit, ...filters } = query;
      const filter = createHash("sha256").update(JSON.stringify(filters)).digest("hex");
      const args: unknown[] = [query.viewer.userId];
      const arg = (value: unknown) => `$${args.push(value)}`;
      const clauses = [TAKES_PART];
      if (query.relation === "created") clauses.push("c.owner_id = $1");
      if (query.relation === "participated") clauses.push("c.owner_id <> $1");
      if (query.agentId) clauses.push(`c.agent_id = ${arg(query.agentId)}`);
      if (query.ownerId) clauses.push(`c.owner_id = ${arg(query.ownerId)}`);
      if (query.query)
        clauses.push(`c.title ILIKE ${arg(`%${query.query.replace(/[=%_]/g, (c) => `=${c}`)}%`)} ESCAPE '='`);
      if (query.source) clauses.push(`c.source = ${arg(query.source)}`);
      if (query.archived !== "all") clauses.push(`c.archived = ${arg(query.archived === "archived")}`);
      if (cursor) {
        let position: { at: string; id: string; filter: string };
        try {
          position = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
          if (position.filter !== filter || typeof position.at !== "string" || typeof position.id !== "string")
            throw new Error();
        } catch {
          throw new InvalidRequest("Invalid cursor");
        }
        clauses.push(`(c.last_activity_at, c.id) < (${arg(position.at)}::timestamptz, ${arg(position.id)})`);
      }
      const { rows } = await pool.query(
        `SELECT c.*,
           (SELECT count(*) FROM hcodex_conversation_participants p WHERE p.conversation_id = c.id)::int
             AS participant_count
         FROM hcodex_conversations c
         WHERE ${clauses.join(" AND ")}
         ORDER BY c.last_activity_at DESC, c.id DESC
         LIMIT ${arg(limit + 1)}`,
        args,
      );
      const items = rows.slice(0, limit).map((row) => ({ ...conversation(row), participantCount: row.participant_count }));
      const last = items.at(-1);
      return {
        items,
        nextCursor:
          rows.length > limit && last
            ? Buffer.from(JSON.stringify({ at: last.lastActivityAt, id: last.id, filter })).toString("base64url")
            : null,
      };
    },

    /** Which of these users own a conversation the viewer takes part in. */
    async owners(ownerIds: string[], viewer: SearchQuery["viewer"]): Promise<string[]> {
      if (!ownerIds.length) return [];
      const { rows } = await pool.query(
        `SELECT DISTINCT c.owner_id FROM hcodex_conversations c
         WHERE ${TAKES_PART} AND c.owner_id = ANY($2::text[])`,
        [viewer.userId, ownerIds],
      );
      return rows.map((row) => row.owner_id as string);
    },

    /** Upserts rows exported from another store, keeping their IDs and timestamps. */
    async import(batch: ImportBatch): Promise<void> {
      await transaction(async (db) => {
        for (const c of batch.conversations ?? [])
          await db.query(
            `INSERT INTO hcodex_conversations (id, owner_id, agent_id, workspace_id, runtime_id, thread_id,
               title, source, archived, metadata, created_at, updated_at, last_activity_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT (id) DO UPDATE SET owner_id = EXCLUDED.owner_id,
               agent_id = EXCLUDED.agent_id, workspace_id = EXCLUDED.workspace_id, runtime_id = EXCLUDED.runtime_id,
               thread_id = EXCLUDED.thread_id, title = EXCLUDED.title, source = EXCLUDED.source,
               archived = EXCLUDED.archived, metadata = EXCLUDED.metadata, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
               last_activity_at = EXCLUDED.last_activity_at`,
            [c.id, c.ownerId, c.agentId, c.workspaceId, c.runtimeId, c.threadId, c.title, c.source,
              c.archived, c.metadata ?? {}, c.createdAt, c.updatedAt, c.lastActivityAt],
          );
        for (const p of batch.participants ?? [])
          await db.query(
            `INSERT INTO hcodex_conversation_participants (conversation_id, user_id, joined_at, last_active_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (conversation_id, user_id) DO UPDATE SET joined_at = EXCLUDED.joined_at,
               last_active_at = EXCLUDED.last_active_at`,
            [p.conversationId, p.userId, p.joinedAt, p.lastActiveAt],
          );
        for (const o of batch.origins ?? [])
          await db.query(
            `INSERT INTO hcodex_conversation_origins (namespace, key, conversation_id) VALUES ($1, $2, $3)
             ON CONFLICT (namespace, key) DO UPDATE SET conversation_id = EXCLUDED.conversation_id`,
            [o.namespace, o.key, o.conversationId],
          );
      });
    },
  };
}
