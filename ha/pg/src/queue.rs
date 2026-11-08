//! PostgreSQL implementation of Codex's ordered, thread-scoped message queue.

use codex_protocol::ThreadId;
use codex_thread_store::MAX_QUEUE_ITEMS;
use codex_thread_store::QueueStore;
use codex_thread_store::QueuedUserSubmissionRecord;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreFuture;
use codex_thread_store::ThreadStoreResult;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Transaction;
use uuid::Uuid;

use crate::db::MigrationGuard;
use crate::db::read_err;
use crate::db::write_err;

/// Shares the thread database. Queue mutations lock the parent thread row, so independent
/// processes agree on ordering, capacity and reorders. Thread deletion cascades to its queue.
/// Turn dispatch and acknowledgement retain Codex's existing queue-service semantics.
#[derive(Clone)]
pub struct PgQueueStore {
    pool: PgPool,
    migrations: MigrationGuard,
}

impl PgQueueStore {
    /// Uses the thread store's pool; embedded migrations run before the first operation.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            migrations: MigrationGuard::default(),
        }
    }

    async fn begin(&self) -> ThreadStoreResult<Transaction<'static, Postgres>> {
        self.migrations.ensure(&self.pool).await?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| read_err("failed to begin queue transaction", err))?;
        // Bound waits on the thread and revision rows; a stalled writer must not hang callers.
        sqlx::query(
            "SELECT set_config('lock_timeout', '5s', true), \
             set_config('statement_timeout', '10s', true)",
        )
        .execute(&mut *tx)
        .await
        .map_err(|err| read_err("failed to configure queue transaction timeouts", err))?;
        Ok(tx)
    }

    async fn lock_thread(
        tx: &mut Transaction<'_, Postgres>,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<bool> {
        sqlx::query_scalar::<_, String>(
            "SELECT thread_id FROM codex_fork_threads WHERE thread_id = $1 FOR UPDATE",
        )
        .bind(thread_id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .map(|row| row.is_some())
        .map_err(|err| read_err("failed to lock thread queue", err))
    }

    async fn changed(
        tx: &mut Transaction<'_, Postgres>,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<()> {
        let revision: i64 = sqlx::query_scalar(
            "UPDATE codex_fork_queue_clock SET revision = revision + 1 WHERE id = 1 RETURNING revision",
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|err| write_err("failed to advance queue revision", err))?;
        sqlx::query(
            "INSERT INTO codex_fork_queue_revisions (thread_id, revision) VALUES ($1, $2) \
             ON CONFLICT (thread_id) DO UPDATE SET revision = EXCLUDED.revision",
        )
        .bind(thread_id.to_string())
        .bind(revision)
        .execute(&mut **tx)
        .await
        .map_err(|err| write_err("failed to record queue revision", err))?;
        Ok(())
    }

    async fn commit(tx: Transaction<'_, Postgres>) -> ThreadStoreResult<()> {
        tx.commit()
            .await
            .map_err(|err| write_err("failed to commit queue changes", err))
    }

    async fn enqueue_impl(
        &self,
        thread_id: ThreadId,
        payload: String,
    ) -> ThreadStoreResult<QueuedUserSubmissionRecord> {
        let mut tx = self.begin().await?;
        if !Self::lock_thread(&mut tx, thread_id).await? {
            return Err(ThreadStoreError::ThreadNotFound { thread_id });
        }
        let (count, last_order): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(MAX(queue_order), -1) \
             FROM codex_fork_queued_items WHERE thread_id = $1",
        )
        .bind(thread_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| read_err("failed to inspect thread queue", err))?;
        if count >= MAX_QUEUE_ITEMS as i64 {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("queue cannot contain more than {MAX_QUEUE_ITEMS} submissions"),
            });
        }
        let order = last_order
            .checked_add(1)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "queue order overflow".to_string(),
            })?;
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO codex_fork_queued_items (id, thread_id, payload, queue_order) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&id)
        .bind(thread_id.to_string())
        .bind(&payload)
        .bind(order)
        .execute(&mut *tx)
        .await
        .map_err(|err| write_err("failed to enqueue user input", err))?;
        Self::changed(&mut tx, thread_id).await?;
        Self::commit(tx).await?;
        Ok(QueuedUserSubmissionRecord {
            id,
            thread_id,
            payload,
        })
    }

    async fn update_impl(
        &self,
        thread_id: ThreadId,
        item_id: String,
        payload: String,
    ) -> ThreadStoreResult<Option<QueuedUserSubmissionRecord>> {
        let mut tx = self.begin().await?;
        if !Self::lock_thread(&mut tx, thread_id).await? {
            return Ok(None);
        }
        let result = sqlx::query(
            "UPDATE codex_fork_queued_items SET payload = $3, updated_at = now() \
             WHERE thread_id = $1 AND id = $2",
        )
        .bind(thread_id.to_string())
        .bind(&item_id)
        .bind(&payload)
        .execute(&mut *tx)
        .await
        .map_err(|err| write_err("failed to update queued user input", err))?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }
        Self::changed(&mut tx, thread_id).await?;
        Self::commit(tx).await?;
        Ok(Some(QueuedUserSubmissionRecord {
            id: item_id,
            thread_id,
            payload,
        }))
    }

    async fn delete_impl(&self, thread_id: ThreadId, item_id: String) -> ThreadStoreResult<bool> {
        let mut tx = self.begin().await?;
        if !Self::lock_thread(&mut tx, thread_id).await? {
            return Ok(false);
        }
        let result =
            sqlx::query("DELETE FROM codex_fork_queued_items WHERE thread_id = $1 AND id = $2")
                .bind(thread_id.to_string())
                .bind(item_id)
                .execute(&mut *tx)
                .await
                .map_err(|err| write_err("failed to delete queued user input", err))?;
        if result.rows_affected() == 0 {
            return Ok(false);
        }
        Self::changed(&mut tx, thread_id).await?;
        Self::commit(tx).await?;
        Ok(true)
    }

    async fn reorder_impl(
        &self,
        thread_id: ThreadId,
        item_ids: Vec<String>,
    ) -> ThreadStoreResult<()> {
        let mut tx = self.begin().await?;
        let exists = Self::lock_thread(&mut tx, thread_id).await?;
        let current: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM codex_fork_queued_items WHERE thread_id = $1 ORDER BY queue_order",
        )
        .bind(thread_id.to_string())
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| read_err("failed to read queue order", err))?;
        let mut expected = current.clone();
        expected.sort();
        let mut requested = item_ids.clone();
        requested.sort();
        if expected != requested {
            return Err(ThreadStoreError::InvalidRequest {
                message: "queue reorder must include every queued submission exactly once"
                    .to_string(),
            });
        }
        if !exists || current == item_ids {
            return Ok(());
        }
        sqlx::query(
            "UPDATE codex_fork_queued_items AS item \
             SET queue_order = desired.ordinal - 1, updated_at = now() \
             FROM UNNEST($2::text[]) WITH ORDINALITY AS desired(id, ordinal) \
             WHERE item.thread_id = $1 AND item.id = desired.id",
        )
        .bind(thread_id.to_string())
        .bind(&item_ids)
        .execute(&mut *tx)
        .await
        .map_err(|err| write_err("failed to reorder queued user input", err))?;
        Self::changed(&mut tx, thread_id).await?;
        Self::commit(tx).await
    }
}

impl QueueStore for PgQueueStore {
    fn change_version(&self) -> ThreadStoreFuture<'_, i64> {
        Box::pin(async move {
            self.migrations.ensure(&self.pool).await?;
            sqlx::query_scalar("SELECT revision FROM codex_fork_queue_clock WHERE id = 1")
                .fetch_one(&self.pool)
                .await
                .map_err(|err| read_err("failed to read queue version", err))
        })
    }

    fn changes_since<'a>(
        &'a self,
        revision: i64,
        thread_ids: &'a [ThreadId],
    ) -> ThreadStoreFuture<'a, Vec<(ThreadId, i64)>> {
        Box::pin(async move {
            if thread_ids.is_empty() {
                return Ok(Vec::new());
            }
            self.migrations.ensure(&self.pool).await?;
            let thread_ids: Vec<String> = thread_ids.iter().map(ToString::to_string).collect();
            let rows: Vec<(String, i64)> = sqlx::query_as(
                "SELECT thread_id, revision FROM codex_fork_queue_revisions \
                 WHERE revision > $1 AND thread_id = ANY($2::text[]) ORDER BY revision",
            )
            .bind(revision)
            .bind(&thread_ids)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| read_err("failed to read changed thread queues", err))?;
            rows.into_iter()
                .map(|(id, revision)| {
                    ThreadId::from_string(&id)
                        .map(|id| (id, revision))
                        .map_err(|err| ThreadStoreError::Internal {
                            message: format!("invalid queued thread id {id}: {err}"),
                        })
                })
                .collect()
        })
    }

    fn enqueue(
        &self,
        thread_id: ThreadId,
        payload: String,
    ) -> ThreadStoreFuture<'_, QueuedUserSubmissionRecord> {
        Box::pin(self.enqueue_impl(thread_id, payload))
    }

    fn list_page(
        &self,
        thread_id: ThreadId,
        offset: usize,
        limit: usize,
    ) -> ThreadStoreFuture<'_, Vec<QueuedUserSubmissionRecord>> {
        Box::pin(async move {
            let bound = |value| {
                i64::try_from(value).map_err(|_| ThreadStoreError::InvalidRequest {
                    message: "queue page bound is too large".to_string(),
                })
            };
            let offset = bound(offset)?;
            let limit = bound(limit)?;
            self.migrations.ensure(&self.pool).await?;
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT id, payload FROM codex_fork_queued_items \
                 WHERE thread_id = $1 ORDER BY queue_order LIMIT $2 OFFSET $3",
            )
            .bind(thread_id.to_string())
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| read_err("failed to read thread queue", err))?;
            Ok(rows
                .into_iter()
                .map(|(id, payload)| QueuedUserSubmissionRecord {
                    id,
                    thread_id,
                    payload,
                })
                .collect())
        })
    }

    fn update(
        &self,
        thread_id: ThreadId,
        item_id: String,
        payload: String,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>> {
        Box::pin(self.update_impl(thread_id, item_id, payload))
    }

    fn delete(&self, thread_id: ThreadId, item_id: String) -> ThreadStoreFuture<'_, bool> {
        Box::pin(self.delete_impl(thread_id, item_id))
    }

    fn reorder(&self, thread_id: ThreadId, item_ids: Vec<String>) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.reorder_impl(thread_id, item_ids))
    }
}
