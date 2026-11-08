//! Cross-pod handoff records for unfinished turns.

use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use serde::Deserialize;
use serde::Serialize;
use sqlx::PgPool;
use sqlx::Row;
use sqlx::types::Json;

use crate::db::MigrationGuard;
use crate::db::read_err;
use crate::db::write_err;

/// Cross-pod handoff record for an unfinished turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HandoffRecord {
    pub thread_id: String,
    pub turn_id: String,
    pub source_owner: String,
    pub payload: serde_json::Value,
}

/// Stores at most one pending [`HandoffRecord`] per thread.
#[derive(Clone)]
pub struct PgHandoffStore {
    pool: PgPool,
    migrations: MigrationGuard,
}

impl PgHandoffStore {
    /// Uses `pool` (typically [`crate::PgThreadStore::pool`]); migrations run on first use.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            migrations: MigrationGuard::default(),
        }
    }

    /// Inserts or replaces the pending handoff for `record.thread_id`.
    pub async fn put(&self, record: HandoffRecord) -> Result<(), ThreadStoreError> {
        self.migrations.ensure(&self.pool).await?;
        sqlx::query(
            "INSERT INTO codex_fork_turn_handoffs (thread_id, turn_id, source_owner, payload) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (thread_id) DO UPDATE SET turn_id = EXCLUDED.turn_id, \
             source_owner = EXCLUDED.source_owner, payload = EXCLUDED.payload, \
             created_at = now()",
        )
        .bind(record.thread_id)
        .bind(record.turn_id)
        .bind(record.source_owner)
        .bind(Json(record.payload))
        .execute(&self.pool)
        .await
        .map_err(|err| write_err("failed to store turn handoff", err))?;
        Ok(())
    }

    /// Atomically claims and removes the pending handoff for `thread_id`. Concurrent callers
    /// observe it at most once.
    pub async fn take(&self, thread_id: &str) -> Result<Option<HandoffRecord>, ThreadStoreError> {
        self.migrations.ensure(&self.pool).await?;
        let row = sqlx::query(
            "DELETE FROM codex_fork_turn_handoffs WHERE thread_id = $1 \
             RETURNING thread_id, turn_id, source_owner, payload",
        )
        .bind(thread_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| write_err("failed to claim turn handoff", err))?;
        row.map(|row| -> ThreadStoreResult<HandoffRecord> {
            let get_err = |err| read_err("failed to decode turn handoff", err);
            let Json(payload): Json<serde_json::Value> = row.try_get("payload").map_err(get_err)?;
            Ok(HandoffRecord {
                thread_id: row.try_get("thread_id").map_err(get_err)?,
                turn_id: row.try_get("turn_id").map_err(get_err)?,
                source_owner: row.try_get("source_owner").map_err(get_err)?,
                payload,
            })
        })
        .transpose()
    }
}
