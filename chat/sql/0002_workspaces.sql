-- Routing belongs to SDK callers. App-server has no workspace controller.
CREATE TABLE IF NOT EXISTS hcodex_runtime_routes (
  runtime_id text PRIMARY KEY,
  targets text[] NOT NULL,
  revision bigint NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS hcodex_workspace_routes (
  runtime_id text NOT NULL,
  workspace_id text NOT NULL,
  target text NOT NULL,
  epoch bigint NOT NULL DEFAULT 1,
  switch_token uuid,
  switch_expires_at timestamptz,
  PRIMARY KEY (runtime_id, workspace_id)
);
CREATE INDEX IF NOT EXISTS hcodex_workspace_routes_target
  ON hcodex_workspace_routes (runtime_id, target);
CREATE TABLE IF NOT EXISTS hcodex_workspace_inflight (
  id uuid PRIMARY KEY,
  runtime_id text NOT NULL,
  workspace_id text NOT NULL,
  target text NOT NULL,
  expires_at timestamptz NOT NULL,
  FOREIGN KEY (runtime_id, workspace_id)
    REFERENCES hcodex_workspace_routes ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS hcodex_workspace_inflight_workspace
  ON hcodex_workspace_inflight (runtime_id, workspace_id);
