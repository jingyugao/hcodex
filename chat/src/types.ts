/** A multi-user conversation. History, queueing and execution stay in Codex. */
export interface Conversation {
  id: string;
  /** Immutable creator; the only participant whose inputs start or join turns directly. */
  ownerId: string;
  /** Owner's tenant at creation; '' keeps the conversation visible to the owner only. */
  tenant: string;
  agentId: string;
  workspaceId: string;
  /** App-server pool (runtime) the thread lives in. */
  runtimeId: string;
  threadId: string | null;
  title: string;
  source: string;
  archived: boolean;
  createdAt: string;
  updatedAt: string;
  /** Last accepted input; a list sort key, not Codex activity. */
  lastActivityAt: string;
}

export type NewConversation = Pick<
  Conversation,
  | "id"
  | "ownerId"
  | "tenant"
  | "agentId"
  | "workspaceId"
  | "runtimeId"
  | "title"
  | "source"
>;

export interface Participant {
  conversationId: string;
  userId: string;
  joinedAt: string;
  lastActiveAt: string;
}

export interface ConversationOrigin {
  namespace: string;
  key: string;
}

export interface SearchQuery {
  viewer: { userId: string; tenant: string };
  /** `mine` limits to conversations the viewer takes part in; `team` to everything visible. */
  scope: "mine" | "team";
  relation: "all" | "created" | "participated";
  agentId?: string;
  ownerId?: string;
  /** Case-insensitive title substring. */
  query?: string;
  source?: string;
  archived: "active" | "archived" | "all";
  cursor?: string;
  limit: number;
}

export interface SearchPage {
  items: (Conversation & { participantCount: number })[];
  nextCursor: string | null;
}

export class ConversationNotFound extends Error {
  constructor() {
    super("Conversation not found");
    this.name = "ConversationNotFound";
  }
}

export class ConversationConflict extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ConversationConflict";
  }
}

export class InvalidRequest extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidRequest";
  }
}
