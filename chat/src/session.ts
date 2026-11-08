import { randomUUID } from "node:crypto";

import { inputIdentity, planInput } from "./rules.js";
import type { Conversation } from "./types.js";

/** The store operations input handling needs. */
export interface InputStore {
  get(id: string): Promise<Conversation | undefined>;
  recordInput(conversationId: string, userId: string): Promise<void>;
}

export type Log = (event: string, fields?: Record<string, unknown>) => void;

/** A parsed JSON-RPC frame of the native app-server protocol. */
export type Frame = Record<string, unknown> & { id?: unknown; method?: unknown; params?: unknown };

/** Request IDs of frames the session sends itself; their responses are not forwarded. */
const INTERNAL_ID = "hcodex:";
const MAX_PENDING_INPUTS = 256;

const isObject = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);
const requestId = (value: unknown): value is string | number =>
  typeof value === "string" || typeof value === "number";

/**
 * Applies the send rule to one client connection of one conversation, frame by frame, for hosts
 * that relay the native protocol themselves. Feed client frames in arrival order, one at a time.
 *
 * - `turn/start` / `turn/steer` carry the author in `params.hcodex = {authorUserId, env}`: the
 *   owner starts or steers, others queue (`thread/queue/add`, then `thread/queue/start`). The
 *   input gets a `<author>:<uuid>` ID and the env becomes `fork.env.*` metadata.
 * - Other frames pass through without `params.hcodex`.
 * - Accepted inputs record the author as a participant; failures are logged, never retried.
 */
export function createInputSession(options: { store: InputStore; conversationId: string; log?: Log }) {
  const { store, conversationId } = options;
  const log: Log = options.log ?? (() => {});
  /** Inputs awaiting their native response: request ID → author and follow-up. */
  const pending = new Map<string | number, { author: string; threadId: unknown; startQueue: boolean }>();

  return {
    /** A client frame: what to send upstream, or an error response for the client. */
    async fromClient(frame: Frame): Promise<{ upstream?: Frame; client?: Frame }> {
      const params = isObject(frame.params) ? frame.params : undefined;
      const method = frame.method;
      if ((method === "turn/start" || method === "turn/steer") && requestId(frame.id) && params) {
        try {
          if (pending.size >= MAX_PENDING_INPUTS || pending.has(frame.id))
            throw new Error("Too many pending inputs or duplicate request ID");
          const conversation = await store.get(conversationId);
          if (!conversation) throw new Error("Conversation not found");
          const identity = inputIdentity(params.hcodex);
          const plan = planInput(conversation, method, params, identity);
          pending.set(frame.id, { author: identity.authorUserId, threadId: plan.params.threadId, startQueue: plan.startQueue });
          return { upstream: { ...frame, method: plan.method, params: plan.params } };
        } catch (error) {
          return { client: { id: frame.id, error: { code: -32600, message: (error as Error).message } } };
        }
      }
      if (params && "hcodex" in params) {
        const { hcodex: _identity, ...rest } = params;
        return { upstream: { ...frame, params: rest } };
      }
      return { upstream: frame };
    },

    /** An upstream frame: whether to forward it to the client, and any follow-up to send upstream. */
    fromUpstream(frame: Frame): { forward: boolean; upstream?: Frame } {
      if (frame.method !== undefined || !requestId(frame.id)) return { forward: true };
      if (typeof frame.id === "string" && frame.id.startsWith(INTERNAL_ID)) return { forward: false };
      const input = pending.get(frame.id);
      if (!input) return { forward: true };
      pending.delete(frame.id);
      if ("error" in frame) return { forward: true };
      store
        .recordInput(conversationId, input.author)
        .catch((error: unknown) => log("record_input_failed", { conversationId, error: String(error) }));
      // Rejected while a turn is active; only needed when the thread is idle.
      if (!input.startQueue) return { forward: true };
      return {
        forward: true,
        upstream: { id: `${INTERNAL_ID}${randomUUID()}`, method: "thread/queue/start", params: { threadId: input.threadId } },
      };
    },
  };
}

export type InputSession = ReturnType<typeof createInputSession>;

/** Sends native requests and resolves with their results; rejects on JSON-RPC errors. */
export interface NativeRequester {
  request(method: string, params: Record<string, unknown>): Promise<unknown>;
}

/**
 * The send rule for hosts that call app-server as a JSON-RPC client (bots, jobs) instead of
 * relaying a user's connection. Returns the native result (`turn` or `queuedSubmission`).
 */
export async function submitInput(options: {
  store: InputStore;
  requester: NativeRequester;
  conversationId: string;
  method: "turn/start" | "turn/steer";
  /** Native params plus `hcodex: {authorUserId, env}`. */
  params: Record<string, unknown>;
  log?: Log;
}): Promise<unknown> {
  const { store, requester, conversationId, params } = options;
  const log: Log = options.log ?? (() => {});
  const conversation = await store.get(conversationId);
  if (!conversation) throw new Error("Conversation not found");
  const identity = inputIdentity(params.hcodex);
  const plan = planInput(conversation, options.method, params, identity);
  const result = await requester.request(plan.method, plan.params);
  await store
    .recordInput(conversationId, identity.authorUserId)
    .catch((error: unknown) => log("record_input_failed", { conversationId, error: String(error) }));
  if (plan.startQueue)
    // Rejected while a turn is active, which is fine: the queue drains when it ends.
    await requester
      .request("thread/queue/start", { threadId: plan.params.threadId })
      .catch((error: unknown) => log("queue_start_skipped", { conversationId, error: String(error) }));
  return result;
}
