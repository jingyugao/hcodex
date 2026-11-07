-- Thread rows. `base` holds creation-time facts (serialized `ThreadBase`), `metadata` holds the
-- merged `ThreadMetadataPatch`. The remaining scalar columns are projections of those two JSON
-- documents, recomputed by the store on every write, used for filtering and ordering.
CREATE TABLE IF NOT EXISTS codex_fork_threads (
    thread_id        TEXT PRIMARY KEY,
    owner_id         TEXT NOT NULL,
    epoch            BIGINT NOT NULL DEFAULT 0,
    next_seq         BIGINT NOT NULL DEFAULT 0,
    base             JSONB NOT NULL,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    parent_thread_id TEXT,
    model_provider   TEXT NOT NULL DEFAULT '',
    source_json      TEXT NOT NULL DEFAULT '',
    cwd              TEXT NOT NULL DEFAULT '',
    search_text      TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    recency_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS codex_fork_threads_created_idx
    ON codex_fork_threads (created_at, thread_id);
CREATE INDEX IF NOT EXISTS codex_fork_threads_updated_idx
    ON codex_fork_threads (updated_at, thread_id);
CREATE INDEX IF NOT EXISTS codex_fork_threads_recency_idx
    ON codex_fork_threads (recency_at, thread_id);
CREATE INDEX IF NOT EXISTS codex_fork_threads_parent_idx
    ON codex_fork_threads (parent_thread_id);

-- Durable replay history: one serialized `RolloutItem` per row, ordered by `seq`.
CREATE TABLE IF NOT EXISTS codex_fork_thread_items (
    thread_id TEXT NOT NULL REFERENCES codex_fork_threads (thread_id) ON DELETE CASCADE,
    seq       BIGINT NOT NULL,
    item      JSONB NOT NULL,
    PRIMARY KEY (thread_id, seq)
);

-- Cross-pod handoff of an unfinished turn. At most one pending handoff per thread.
CREATE TABLE IF NOT EXISTS codex_fork_turn_handoffs (
    thread_id    TEXT PRIMARY KEY,
    turn_id      TEXT NOT NULL,
    source_owner TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
