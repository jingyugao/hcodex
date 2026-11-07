-- Remote sandbox bound to a thread for its whole life. Written once, never updated.
CREATE TABLE IF NOT EXISTS codex_fork_thread_sandboxes (
    thread_id  TEXT PRIMARY KEY,
    binding    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
