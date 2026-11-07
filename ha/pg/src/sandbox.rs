//! Write-once thread → sandbox bindings.

use codex_thread_store::ThreadStoreError;
use sqlx::PgPool;
use sqlx::Row;
use sqlx::types::Json;

use crate::db::MigrationGuard;
use crate::db::read_err;
use crate::db::write_err;

/// Stores at most one sandbox binding per thread; the first write wins.
#[derive(Clone)]
pub struct PgSandboxStore {
    pool: PgPool,
    migrations: MigrationGuard,
}

impl PgSandboxStore {
    /// Uses `pool` (typically shared with the thread store); migrations run on first use.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            migrations: MigrationGuard::default(),
        }
    }

    /// Returns the binding recorded for `thread_id`.
    pub async fn get(
        &self,
        thread_id: &str,
    ) -> Result<Option<serde_json::Value>, ThreadStoreError> {
        self.migrations.ensure(&self.pool).await?;
        let row =
            sqlx::query("SELECT binding FROM codex_fork_thread_sandboxes WHERE thread_id = $1")
                .bind(thread_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| read_err("failed to read sandbox binding", err))?;
        row.map(|row| {
            row.try_get::<Json<serde_json::Value>, _>("binding")
                .map(|Json(binding)| binding)
                .map_err(|err| read_err("failed to decode sandbox binding", err))
        })
        .transpose()
    }

    /// Records `binding` unless one exists. Returns whether this call wrote it.
    pub async fn put_if_absent(
        &self,
        thread_id: &str,
        binding: serde_json::Value,
    ) -> Result<bool, ThreadStoreError> {
        self.migrations.ensure(&self.pool).await?;
        let result = sqlx::query(
            "INSERT INTO codex_fork_thread_sandboxes (thread_id, binding) VALUES ($1, $2) \
             ON CONFLICT (thread_id) DO NOTHING",
        )
        .bind(thread_id)
        .bind(Json(binding))
        .execute(&self.pool)
        .await
        .map_err(|err| write_err("failed to store sandbox binding", err))?;
        Ok(result.rows_affected() == 1)
    }
}
