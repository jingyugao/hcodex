-- Queue payloads keep Codex's serialized wire shape, including per-input metadata.
CREATE TABLE codex_fork_queued_items (
    id          TEXT PRIMARY KEY,
    thread_id   TEXT NOT NULL REFERENCES codex_fork_threads (thread_id) ON DELETE CASCADE,
    payload     TEXT NOT NULL,
    queue_order BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (thread_id, queue_order) DEFERRABLE INITIALLY DEFERRED
);

-- Allocate revisions with a transactional row update, not a sequence: a later revision must
-- never commit before an earlier one, or a watcher could permanently skip the earlier change.
CREATE TABLE codex_fork_queue_clock (
    id       INTEGER PRIMARY KEY CHECK (id = 1),
    revision BIGINT NOT NULL DEFAULT 0
);
INSERT INTO codex_fork_queue_clock (id) VALUES (1);

CREATE TABLE codex_fork_queue_revisions (
    thread_id TEXT PRIMARY KEY REFERENCES codex_fork_threads (thread_id) ON DELETE CASCADE,
    revision  BIGINT NOT NULL
);
CREATE INDEX codex_fork_queue_revisions_revision_idx ON codex_fork_queue_revisions (revision);
