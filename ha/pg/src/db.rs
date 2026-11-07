//! Shared database plumbing: migrations, error mapping, and rollout item encoding.

use std::sync::Arc;

use codex_rollout::RolloutItem;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use tokio::sync::OnceCell;

/// Migration bookkeeping table, kept separate from any other sqlx user of the same schema.
const MIGRATIONS_TABLE: &str = "_hcodex_pg_migrations";

/// Key used to wrap serialized items that Postgres JSONB cannot store directly.
const RAW_ITEM_KEY: &str = "$codex_fork_raw";

fn migrator() -> Migrator {
    let mut migrator = sqlx_macros::migrate!("./migrations");
    migrator.dangerous_set_table_name(MIGRATIONS_TABLE);
    migrator
}

/// Runs the embedded migrations at most once per guard, on first use.
#[derive(Clone, Default)]
pub(crate) struct MigrationGuard {
    done: Arc<OnceCell<()>>,
}

impl MigrationGuard {
    pub(crate) async fn ensure(&self, pool: &PgPool) -> ThreadStoreResult<()> {
        self.done
            .get_or_try_init(|| async {
                migrator()
                    .run(pool)
                    .await
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!("failed to run hcodex-pg migrations: {err}"),
                    })
            })
            .await
            .map(|_| ())
    }
}

/// Maps a database error for an operation that does not write (or that is safe to retry).
pub(crate) fn read_err(operation: &str, err: sqlx::Error) -> ThreadStoreError {
    match err {
        sqlx::Error::PoolTimedOut | sqlx::Error::Io(_) | sqlx::Error::PoolClosed => {
            ThreadStoreError::Retryable {
                message: format!("{operation}: {err}"),
            }
        }
        err => ThreadStoreError::Internal {
            message: format!("{operation}: {err}"),
        },
    }
}

/// Maps a database error for a write. Connection failures after a write was sent may have
/// committed, so they are reported as an unknown outcome.
pub(crate) fn write_err(operation: &str, err: sqlx::Error) -> ThreadStoreError {
    match err {
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => ThreadStoreError::Retryable {
            message: format!("{operation}: {err}"),
        },
        sqlx::Error::Io(_) => ThreadStoreError::OutcomeUnknown {
            message: format!("{operation}: {err}"),
        },
        err => ThreadStoreError::Internal {
            message: format!("{operation}: {err}"),
        },
    }
}

pub(crate) fn serde_err(what: &str, err: serde_json::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to (de)serialize {what}: {err}"),
    }
}

/// Encodes a rollout item as JSON text suitable for a `::jsonb` cast.
///
/// JSONB rejects the `\u0000` escape, which can legitimately appear in tool output. Such items are
/// stored losslessly as a JSON string holding the original serialized text.
pub(crate) fn encode_item(item: &RolloutItem) -> ThreadStoreResult<String> {
    let text = serde_json::to_string(item).map_err(|err| serde_err("rollout item", err))?;
    if !text.contains("\\u0000") {
        return Ok(text);
    }
    serde_json::to_string(&serde_json::json!({ RAW_ITEM_KEY: text }))
        .map_err(|err| serde_err("rollout item", err))
}

pub(crate) fn decode_item(value: Value) -> ThreadStoreResult<RolloutItem> {
    if let Value::Object(map) = &value
        && map.len() == 1
        && let Some(Value::String(raw)) = map.get(RAW_ITEM_KEY)
    {
        return serde_json::from_str(raw).map_err(|err| serde_err("rollout item", err));
    }
    serde_json::from_value(value).map_err(|err| serde_err("rollout item", err))
}
