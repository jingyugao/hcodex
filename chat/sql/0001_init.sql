-- Multi-user conversations. History, queueing and execution stay in Codex.
-- Millisecond timestamps: list cursors carry JavaScript (millisecond) times.
CREATE TABLE IF NOT EXISTS hcodex_conversations (
  id text PRIMARY KEY,
  owner_id text NOT NULL,
  -- Owner's tenant when the conversation was created; '' means visible to the owner only.
  tenant text NOT NULL DEFAULT '',
  agent_id text NOT NULL,
  workspace_id text NOT NULL,
  runtime_id text NOT NULL,
  thread_id text NULL,
  title text NOT NULL,
  source text NOT NULL,
  archived boolean NOT NULL DEFAULT false,
  created_at timestamptz(3) NOT NULL DEFAULT now(),
  updated_at timestamptz(3) NOT NULL DEFAULT now(),
  last_activity_at timestamptz(3) NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS hcodex_conversations_thread
  ON hcodex_conversations (runtime_id, thread_id) WHERE thread_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS hcodex_conversations_activity
  ON hcodex_conversations (last_activity_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS hcodex_conversations_owner_activity
  ON hcodex_conversations (owner_id, last_activity_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS hcodex_conversations_tenant_activity
  ON hcodex_conversations (tenant, last_activity_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS hcodex_conversations_agent_activity
  ON hcodex_conversations (agent_id, last_activity_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS hcodex_conversation_participants (
  conversation_id text NOT NULL REFERENCES hcodex_conversations (id) ON DELETE CASCADE,
  user_id text NOT NULL,
  joined_at timestamptz(3) NOT NULL DEFAULT now(),
  last_active_at timestamptz(3) NOT NULL DEFAULT now(),
  PRIMARY KEY (conversation_id, user_id)
);
CREATE INDEX IF NOT EXISTS hcodex_conversation_participants_user
  ON hcodex_conversation_participants (user_id, conversation_id);

CREATE TABLE IF NOT EXISTS hcodex_conversation_origins (
  namespace text NOT NULL,
  key text NOT NULL,
  conversation_id text NOT NULL REFERENCES hcodex_conversations (id) ON DELETE CASCADE,
  PRIMARY KEY (namespace, key)
);
CREATE INDEX IF NOT EXISTS hcodex_conversation_origins_conversation
  ON hcodex_conversation_origins (conversation_id);
