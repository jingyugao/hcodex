import { randomUUID, timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

import { z } from "zod";

import type { ConversationStore } from "./store.js";
import { ConversationConflict, ConversationNotFound, InvalidRequest } from "./types.js";

const MAX_BODY_BYTES = 8 * 1024 * 1024;
const id = z.string().min(1).max(128);
const viewerInput = z.object({ userId: id }).strict();
const MAX_METADATA_BYTES = 16 * 1024;
const metadata = z
  .record(z.string(), z.unknown())
  .refine((value) => Buffer.byteLength(JSON.stringify(value)) <= MAX_METADATA_BYTES, {
    message: `metadata exceeds ${MAX_METADATA_BYTES} bytes`,
  });
const conversationFields = z
  .object({
    id: id.optional(),
    ownerId: id,
    agentId: id,
    workspaceId: id,
    runtimeId: id,
    title: z.string().trim().min(1).max(200),
    source: z.string().min(1).max(64),
    metadata: metadata.optional(),
  })
  .strict();
const searchInput = z
  .object({
    viewer: viewerInput,
    relation: z.enum(["all", "created", "participated"]).default("all"),
    agentId: id.optional(),
    ownerId: id.optional(),
    query: z.string().trim().max(200).optional(),
    source: z.string().max(64).optional(),
    archived: z.enum(["active", "archived", "all"]).default("active"),
    cursor: z.string().max(1024).optional(),
    limit: z.number().int().min(1).max(100).default(30),
  })
  .strict();
const timestamp = z.string().datetime();
const importInput = z
  .object({
    conversations: z
      .array(
        conversationFields
          .extend({
            id,
            threadId: z.string().max(128).nullable(),
            archived: z.boolean(),
            createdAt: timestamp,
            updatedAt: timestamp,
            lastActivityAt: timestamp,
          })
          .strict(),
      )
      .max(1000)
      .optional(),
    participants: z
      .array(z.object({ conversationId: id, userId: id, joinedAt: timestamp, lastActiveAt: timestamp }).strict())
      .max(10000)
      .optional(),
    origins: z
      .array(z.object({ namespace: z.string().min(1).max(100), key: z.string().min(1).max(255), conversationId: id }).strict())
      .max(10000)
      .optional(),
  })
  .strict();

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function body(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    size += (chunk as Buffer).length;
    if (size > MAX_BODY_BYTES) throw new HttpError(413, "Request body too large");
    chunks.push(chunk as Buffer);
  }
  if (!size) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new HttpError(400, "Invalid JSON body");
  }
}

function send(response: ServerResponse, status: number, value?: unknown) {
  const payload = value === undefined ? "" : JSON.stringify(value);
  response.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(payload),
  });
  response.end(payload);
}

/** JSON API for hosts. Authorization decisions stay with the host; it passes the viewer. */
export function createHttpApi(options: { store: ConversationStore; hostToken: string }) {
  const { store } = options;
  const expected = Buffer.from(`Bearer ${options.hostToken}`);
  const routes: [string, RegExp, (match: string[], request: IncomingMessage, url: URL) => Promise<[number, unknown?]>][] = [
    ["POST", /^\/v1\/conversations$/, async (_m, request) => {
      const input = conversationFields.parse(await body(request));
      return [201, await store.create({ ...input, id: input.id ?? randomUUID() })];
    }],
    ["POST", /^\/v1\/conversations\/search$/, async (_m, request) => [200, await store.search(searchInput.parse(await body(request)))]],
    ["POST", /^\/v1\/conversations\/owners$/, async (_m, request) => {
      const { ownerIds, viewer } = z
        .object({ ownerIds: z.array(id).max(1000), viewer: viewerInput })
        .strict()
        .parse(await body(request));
      return [200, { ownerIds: await store.owners(ownerIds, viewer) }];
    }],
    ["POST", /^\/v1\/conversations\/batch$/, async (_m, request) => {
      const { ids } = z.object({ ids: z.array(id).max(200) }).strict().parse(await body(request));
      return [200, { items: await store.getMany(ids) }];
    }],
    ["PUT", /^\/v1\/origins\/([^/]+)\/([^/]+)$/, async ([namespace, key], request) => {
      const input = conversationFields.parse(await body(request));
      const origin = z
        .object({ namespace: z.string().min(1).max(100), key: z.string().min(1).max(255) })
        .parse({ namespace: decodeURIComponent(namespace!), key: decodeURIComponent(key!) });
      return [200, await store.ensureForOrigin(origin, { ...input, id: input.id ?? randomUUID() })];
    }],
    ["GET", /^\/v1\/conversations\/([^/]+)$/, async ([conversationId]) => {
      const value = await store.get(decodeURIComponent(conversationId!));
      if (!value) throw new ConversationNotFound();
      return [200, value];
    }],
    ["PATCH", /^\/v1\/conversations\/([^/]+)$/, async ([conversationId], request) => {
      const patch = z
        .object({
          title: z.string().trim().min(1).max(200).optional(),
          archived: z.boolean().optional(),
          metadata: metadata.optional(),
        })
        .strict()
        .parse(await body(request));
      return [200, await store.update(decodeURIComponent(conversationId!), patch)];
    }],
    ["POST", /^\/v1\/conversations\/([^/]+)\/thread$/, async ([conversationId], request) => {
      const { runtimeId, threadId } = z.object({ runtimeId: id, threadId: id }).strict().parse(await body(request));
      await store.bindThread(decodeURIComponent(conversationId!), runtimeId, threadId);
      return [204];
    }],
    ["GET", /^\/v1\/threads\/([^/]+)\/([^/]+)\/conversation$/, async ([runtimeId, threadId]) => {
      const value = await store.getByThread(decodeURIComponent(runtimeId!), decodeURIComponent(threadId!));
      if (!value) throw new ConversationNotFound();
      return [200, value];
    }],
    ["POST", /^\/v1\/conversations\/([^/]+)\/participants$/, async ([conversationId], request) => {
      const { userId } = z.object({ userId: id }).strict().parse(await body(request));
      await store.addParticipant(decodeURIComponent(conversationId!), userId);
      return [204];
    }],
    ["GET", /^\/v1\/conversations\/([^/]+)\/participants\/([^/]+)$/, async ([conversationId, userId]) => {
      const value = await store.getParticipant(decodeURIComponent(conversationId!), decodeURIComponent(userId!));
      if (!value) throw new ConversationNotFound();
      return [200, value];
    }],
    ["GET", /^\/v1\/conversations\/([^/]+)\/participants$/, async ([conversationId], _request, url) => {
      const query = z
        .object({ after: z.string().max(128).optional(), limit: z.coerce.number().int().min(1).max(100).optional() })
        .parse(Object.fromEntries(url.searchParams));
      return [200, { data: await store.listParticipants(decodeURIComponent(conversationId!), query) }];
    }],
    ["POST", /^\/v1\/import$/, async (_m, request) => {
      await store.import(importInput.parse(await body(request)));
      return [204];
    }],
  ];

  return async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    try {
      const given = Buffer.from(request.headers.authorization ?? "");
      if (given.length !== expected.length || !timingSafeEqual(given, expected)) throw new HttpError(401, "Unauthorized");
      const url = new URL(request.url ?? "/", "http://hcodex");
      for (const [method, pattern, handler] of routes) {
        const match = pattern.exec(url.pathname);
        if (!match || request.method !== method) continue;
        const [status, value] = await handler(match.slice(1), request, url);
        return send(response, status, value);
      }
      throw new HttpError(404, "Not found");
    } catch (error) {
      if (error instanceof HttpError) return send(response, error.status, { error: error.message });
      if (error instanceof z.ZodError) return send(response, 400, { error: error.issues.map((i) => i.message).join("; ") });
      if (error instanceof InvalidRequest) return send(response, 400, { error: error.message });
      if (error instanceof ConversationNotFound) return send(response, 404, { error: error.message });
      if (error instanceof ConversationConflict) return send(response, 409, { error: error.message });
      send(response, 500, { error: "Internal error" });
      console.error(JSON.stringify({ event: "http_error", error: String(error) }));
    }
  };
}
