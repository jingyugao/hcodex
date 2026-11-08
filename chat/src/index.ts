/**
 * hcodex chat SDK: multi-user conversations on Codex app-server.
 *
 * - Storage: {@link createConversationStore} (Postgres; conversations, participants, origins).
 * - Send rule: {@link createInputSession} for hosts relaying the native protocol frame by frame,
 *   {@link submitInput} for hosts calling app-server as a JSON-RPC client.
 * - Routing: {@link replicaUrl} picks a conversation's app-server replica.
 * - Standalone parts, for hosts that deploy it as its own service: {@link createHttpApi} and
 *   {@link createGateway}.
 */
export * from "./types.js";
export { withoutInputAuthor } from "./input.js";
export {
  createConversationStore,
  type ConversationStore,
  type ImportBatch,
} from "./store.js";
export {
  FORK_ENV_PREFIX,
  encodeInputEnv,
  inputAuthor,
  inputIdentity,
  newInputId,
  planInput,
  type InputIdentity,
  type InputMethod,
  type PlannedInput,
} from "./rules.js";
export {
  createInputSession,
  submitInput,
  type Frame,
  type InputSession,
  type InputStore,
  type Log,
  type NativeRequester,
} from "./session.js";
export {
  createReplicaResolver,
  createWorkspaceRouter,
  rendezvous,
  replicaUrl,
  type ReplicaResolver,
  type WorkspaceRouter,
} from "./routing.js";
export { UPSTREAM_CLOSED, createGateway } from "./gateway.js";
export { createHttpApi } from "./http.js";
export {
  inspectWorkspace,
  type WorkspaceRoute,
  type WorkspaceAdmission,
  type SwitchResult,
} from "./workspaces.js";
export {
  WorkspaceSocket,
  createWorkspaceTransport,
  type WorkspaceSocketOptions,
  type NativeSocket,
} from "./workspace-socket.js";
