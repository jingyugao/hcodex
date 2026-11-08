import { randomUUID } from "node:crypto";

import { withInputAuthor } from "./input.js";
import { ConversationConflict, InvalidRequest } from "./types.js";

export { inputAuthor } from "./input.js";

/** Metadata prefix the Codex fork turns into child-process environment variables. */
export const FORK_ENV_PREFIX = "fork.env.";
// Upstream drops metadata beyond these limits; reject instead of losing env silently.
const MAX_ENV_ENTRIES = 16;
const MAX_ENV_KEY_BYTES = 64;
const MAX_ENV_VALUE_BYTES = 128;
const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const bytes = (value: string) => Buffer.byteLength(value, "utf8");

/** The author of one input, supplied by the trusted host in `params.hcodex`. */
export interface InputIdentity {
  authorUserId: string;
  authorName: string;
  env: Record<string, string>;
}

/** Native `clientUserMessageId`: the author is recoverable from history without a table. */
export function newInputId(userId: string): string {
  if (!UUID.test(userId)) throw new InvalidRequest("Invalid input author");
  return `${userId.toLowerCase()}:${randomUUID()}`;
}

/** `responsesapiClientMetadata` carrying the author's complete env as `fork.env.*`. */
export function encodeInputEnv(env: Record<string, string>): Record<string, string> {
  const entries = Object.entries(env);
  if (entries.length > MAX_ENV_ENTRIES)
    throw new InvalidRequest(`Input env exceeds ${MAX_ENV_ENTRIES} entries`);
  const metadata: Record<string, string> = {};
  for (const [name, value] of entries) {
    const key = `${FORK_ENV_PREFIX}${name}`;
    if (!ENV_NAME.test(name) || bytes(key) > MAX_ENV_KEY_BYTES)
      throw new InvalidRequest(`Invalid input env name: ${name}`);
    if (typeof value !== "string" || value.includes("\0") || bytes(value) > MAX_ENV_VALUE_BYTES)
      throw new InvalidRequest(`Invalid input env value: ${name}`);
    metadata[key] = value;
  }
  return metadata;
}

/** Reads and validates the host's `params.hcodex` identity. */
export function inputIdentity(value: unknown): InputIdentity {
  const identity = value as Partial<InputIdentity> | undefined;
  if (!identity || typeof identity !== "object" || typeof identity.authorUserId !== "string" ||
    typeof identity.authorName !== "string")
    throw new InvalidRequest("Input identity (params.hcodex) requires authorUserId and authorName");
  const env = identity.env ?? {};
  if (typeof env !== "object" || Array.isArray(env))
    throw new InvalidRequest("Input env must be an object");
  return { authorUserId: identity.authorUserId, authorName: identity.authorName, env: env as Record<string, string> };
}

export type InputMethod = "turn/start" | "turn/steer" | "thread/queue/add";

export interface PlannedInput {
  method: InputMethod;
  params: Record<string, unknown>;
  /** Native ID of the input, `<author>:<uuid>`. */
  inputId: string;
  /** After a successful queue add, start the queue in case the thread is idle. */
  startQueue: boolean;
}

/**
 * One rule for every channel. The owner starts a turn (joining the active one) or steers it;
 * other participants queue, so each queued turn runs with its own author's env.
 */
export function planInput(
  conversation: { ownerId: string; threadId: string | null; archived: boolean },
  method: "turn/start" | "turn/steer",
  params: Record<string, unknown>,
  identity: InputIdentity,
): PlannedInput {
  if (conversation.archived) throw new ConversationConflict("Conversation is archived");
  if (!conversation.threadId || params.threadId !== conversation.threadId)
    throw new ConversationConflict("Input targets another thread");
  const owner = conversation.ownerId === identity.authorUserId;
  if (method === "turn/steer" && !owner)
    throw new ConversationConflict("Only the conversation owner can steer");
  if (!Array.isArray(params.input) || params.input.length === 0)
    throw new InvalidRequest("Input must be a non-empty array");
  const inputId = newInputId(identity.authorUserId);
  const { hcodex: _identity, clientUserMessageId: _clientId, responsesapiClientMetadata: _metadata, ...rest } = params;
  const next: Record<string, unknown> = {
    ...rest,
    input: withInputAuthor(params.input, { userId: identity.authorUserId, name: identity.authorName }),
    clientUserMessageId: inputId,
    responsesapiClientMetadata: encodeInputEnv(identity.env),
  };
  if (method === "turn/steer") return { method, params: next, inputId, startQueue: false };
  if (owner) return { method: "turn/start", params: next, inputId, startQueue: false };
  return {
    method: "thread/queue/add",
    params: {
      threadId: next.threadId,
      input: next.input,
      clientUserMessageId: inputId,
      responsesapiClientMetadata: next.responsesapiClientMetadata,
    },
    inputId,
    startQueue: true,
  };
}
