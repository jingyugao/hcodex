//! [`ThreadStore`] implementation backed by PostgreSQL.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionContextWindow;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::into_persisted_rollout_items;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::ArchiveThreadParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::DeleteThreadParams;
use codex_thread_store::ListThreadsParams;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadByRolloutPathParams;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::SortDirection;
use codex_thread_store::StoredModelContext;
use codex_thread_store::StoredThread;
use codex_thread_store::StoredThreadHistory;
use codex_thread_store::ThreadMetadataPatch;
use codex_thread_store::ThreadPage;
use codex_thread_store::ThreadRelationFilter;
use codex_thread_store::ThreadSortKey;
use codex_thread_store::ThreadStore;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreFuture;
use codex_thread_store::ThreadStoreResult;
use codex_thread_store::UpdateThreadMetadataParams;
use codex_utils_absolute_path::AbsolutePathBuf;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::QueryBuilder;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use sqlx::types::Json;

use crate::db::MigrationGuard;
use crate::db::decode_item;
use crate::db::encode_item;
use crate::db::read_err;
use crate::db::serde_err;
use crate::db::write_err;
use crate::projection::RowTimes;
use crate::projection::ThreadBase;
use crate::projection::project;
use crate::projection::source_json;
use crate::projection::stored_thread;

/// Columns selected for every thread row read.
macro_rules! thread_columns {
    () => {
        "thread_id, owner_id, epoch, base, metadata, created_at, updated_at, recency_at, archived_at"
    };
}

/// Connection settings for [`PgThreadStore`].
#[derive(Clone, Debug)]
pub struct PgThreadStoreConfig {
    /// Postgres connection URL, e.g. `postgres://user:pass@host:5432/db`.
    pub url: String,
    /// Identity of this process for ownership fencing (see [`crate::owner_id_from_env`]).
    pub owner_id: String,
    /// Model provider reported for threads that never recorded one.
    pub default_model_provider_id: String,
}

/// Writer state for a thread this store currently has open.
#[derive(Clone, Copy, Debug)]
struct LiveWriter {
    epoch: i64,
    history_mode: ThreadHistoryMode,
}

/// PostgreSQL-backed [`ThreadStore`].
///
/// Every write is durable when the call returns, so flush/persist are fence checks only.
pub struct PgThreadStore {
    pool: PgPool,
    owner_id: String,
    default_model_provider_id: String,
    migrations: MigrationGuard,
    live: Mutex<HashMap<ThreadId, LiveWriter>>,
}

struct ThreadRow {
    thread_id: ThreadId,
    owner_id: String,
    epoch: i64,
    base: ThreadBase,
    metadata: ThreadMetadataPatch,
    times: RowTimes,
}

impl ThreadRow {
    fn from_row(row: &PgRow) -> ThreadStoreResult<Self> {
        let get_err = |err: sqlx::Error| read_err("failed to decode thread row", err);
        let thread_id: String = row.try_get("thread_id").map_err(get_err)?;
        let thread_id =
            ThreadId::from_string(&thread_id).map_err(|err| ThreadStoreError::Internal {
                message: format!("invalid stored thread id {thread_id}: {err}"),
            })?;
        let Json(base): Json<ThreadBase> = row.try_get("base").map_err(get_err)?;
        let Json(metadata): Json<ThreadMetadataPatch> = row.try_get("metadata").map_err(get_err)?;
        Ok(Self {
            thread_id,
            owner_id: row.try_get("owner_id").map_err(get_err)?,
            epoch: row.try_get("epoch").map_err(get_err)?,
            base,
            metadata,
            times: RowTimes {
                created_at: row.try_get("created_at").map_err(get_err)?,
                updated_at: row.try_get("updated_at").map_err(get_err)?,
                recency_at: row.try_get("recency_at").map_err(get_err)?,
                archived_at: row.try_get("archived_at").map_err(get_err)?,
            },
        })
    }
}

impl PgThreadStore {
    /// Creates a store without connecting. The pool connects lazily and migrations run on the
    /// first operation, so this can be called from sync code.
    pub fn new(config: PgThreadStoreConfig) -> Result<Self, ThreadStoreError> {
        if config.owner_id.trim().is_empty() {
            return Err(ThreadStoreError::InvalidRequest {
                message: "hcodex-pg owner_id must not be empty".to_string(),
            });
        }
        let pool = crate::connect_lazy(&config.url)?;
        Ok(Self {
            pool,
            owner_id: config.owner_id,
            default_model_provider_id: config.default_model_provider_id,
            migrations: MigrationGuard::default(),
            live: Mutex::new(HashMap::new()),
        })
    }

    /// The underlying pool, e.g. for constructing a [`crate::PgHandoffStore`].
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Owner id this store claims threads with.
    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    fn live_guard(&self) -> MutexGuard<'_, HashMap<ThreadId, LiveWriter>> {
        match self.live.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn live_writer(&self, thread_id: ThreadId) -> Option<LiveWriter> {
        self.live_guard().get(&thread_id).copied()
    }

    fn require_live_writer(&self, thread_id: ThreadId) -> ThreadStoreResult<LiveWriter> {
        self.live_writer(thread_id)
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: format!("thread {thread_id} has no live writer in this store"),
            })
    }

    async fn ready(&self) -> ThreadStoreResult<()> {
        self.migrations.ensure(&self.pool).await
    }

    async fn begin(&self) -> ThreadStoreResult<Transaction<'static, Postgres>> {
        self.ready().await?;
        self.pool
            .begin()
            .await
            .map_err(|err| read_err("failed to begin transaction", err))
    }

    fn stale_writer_error(
        &self,
        thread_id: ThreadId,
        writer: LiveWriter,
        current_owner: &str,
        current_epoch: i64,
    ) -> ThreadStoreError {
        ThreadStoreError::Conflict {
            message: format!(
                "stale writer for thread {thread_id}: owner {} holds epoch {}, but owner \
                 {current_owner} claimed epoch {current_epoch}",
                self.owner_id, writer.epoch
            ),
        }
    }

    /// Verifies (inside `tx`, with the row locked) that this store still owns `thread_id`.
    async fn check_fence(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        thread_id: ThreadId,
        writer: LiveWriter,
    ) -> ThreadStoreResult<()> {
        let row = sqlx::query(
            "SELECT owner_id, epoch FROM codex_fork_threads WHERE thread_id = $1 FOR UPDATE",
        )
        .bind(thread_id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| read_err("failed to check thread ownership", err))?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        let owner: String = row
            .try_get("owner_id")
            .map_err(|err| read_err("failed to decode owner", err))?;
        let epoch: i64 = row
            .try_get("epoch")
            .map_err(|err| read_err("failed to decode epoch", err))?;
        if epoch != writer.epoch || owner != self.owner_id {
            return Err(self.stale_writer_error(thread_id, writer, &owner, epoch));
        }
        Ok(())
    }

    async fn check_fence_standalone(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let writer = self.require_live_writer(thread_id)?;
        let mut tx = self.begin().await?;
        self.check_fence(&mut tx, thread_id, writer).await?;
        tx.commit()
            .await
            .map_err(|err| read_err("failed to commit fence check", err))
    }

    async fn fetch_row(
        &self,
        executor: impl sqlx::PgExecutor<'_>,
        thread_id: ThreadId,
        for_update: bool,
    ) -> ThreadStoreResult<Option<ThreadRow>> {
        let sql = if for_update {
            concat!(
                "SELECT ",
                thread_columns!(),
                " FROM codex_fork_threads WHERE thread_id = $1 FOR UPDATE"
            )
        } else {
            concat!(
                "SELECT ",
                thread_columns!(),
                " FROM codex_fork_threads WHERE thread_id = $1"
            )
        };
        sqlx::query(sql)
            .bind(thread_id.to_string())
            .fetch_optional(executor)
            .await
            .map_err(|err| read_err("failed to read thread", err))?
            .as_ref()
            .map(ThreadRow::from_row)
            .transpose()
    }

    async fn fetch_visible_row(
        &self,
        thread_id: ThreadId,
        include_archived: bool,
    ) -> ThreadStoreResult<ThreadRow> {
        self.ready().await?;
        self.fetch_row(&self.pool, thread_id, false)
            .await?
            .filter(|row| include_archived || row.times.archived_at.is_none())
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })
    }

    async fn fetch_items(
        &self,
        executor: impl sqlx::PgExecutor<'_>,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Vec<RolloutItem>> {
        let rows: Vec<Json<serde_json::Value>> = sqlx::query_scalar(
            "SELECT item FROM codex_fork_thread_items WHERE thread_id = $1 ORDER BY seq",
        )
        .bind(thread_id.to_string())
        .fetch_all(executor)
        .await
        .map_err(|err| read_err("failed to read thread history", err))?;
        rows.into_iter()
            .map(|Json(value)| decode_item(value))
            .collect()
    }

    /// Inserts `items` starting at `first_seq`.
    async fn insert_items(
        tx: &mut Transaction<'static, Postgres>,
        thread_id: ThreadId,
        first_seq: i64,
        items: &[RolloutItem],
    ) -> ThreadStoreResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let encoded = items
            .iter()
            .map(encode_item)
            .collect::<ThreadStoreResult<Vec<_>>>()?;
        let seqs = (0..encoded.len())
            .map(|offset| first_seq + offset as i64)
            .collect::<Vec<i64>>();
        sqlx::query(
            "INSERT INTO codex_fork_thread_items (thread_id, seq, item) \
             SELECT $1, seq, item::jsonb FROM UNNEST($2::bigint[], $3::text[]) AS t(seq, item)",
        )
        .bind(thread_id.to_string())
        .bind(seqs)
        .bind(encoded)
        .execute(&mut **tx)
        .await
        .map_err(|err| write_err("failed to append thread items", err))?;
        Ok(())
    }

    /// Inserts a brand-new thread row claimed by this owner at epoch 1.
    async fn insert_thread_row(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        thread_id: ThreadId,
        base: &ThreadBase,
        next_seq: i64,
    ) -> ThreadStoreResult<Option<i64>> {
        let metadata = ThreadMetadataPatch::default();
        let projection = project(base, &metadata, &self.default_model_provider_id);
        let base_json = serde_json::to_value(base).map_err(|err| serde_err("thread base", err))?;
        let metadata_json =
            serde_json::to_value(&metadata).map_err(|err| serde_err("thread metadata", err))?;
        sqlx::query_scalar(
            "INSERT INTO codex_fork_threads \
             (thread_id, owner_id, epoch, next_seq, base, metadata, parent_thread_id, \
              model_provider, source_json, cwd, search_text) \
             VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (thread_id) DO NOTHING RETURNING epoch",
        )
        .bind(thread_id.to_string())
        .bind(&self.owner_id)
        .bind(next_seq)
        .bind(Json(base_json))
        .bind(Json(metadata_json))
        .bind(projection.parent_thread_id)
        .bind(projection.model_provider)
        .bind(projection.source_json)
        .bind(projection.cwd)
        .bind(projection.search_text)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| write_err("failed to create thread", err))
    }

    async fn create_thread_impl(&self, params: CreateThreadParams) -> ThreadStoreResult<()> {
        let thread_id = params.thread_id;
        let base = ThreadBase::from_create_params(&params);
        let session_meta = session_meta_item(&params);
        let mut tx = self.begin().await?;
        let epoch = self
            .insert_thread_row(&mut tx, thread_id, &base, 1)
            .await?
            .ok_or_else(|| ThreadStoreError::Conflict {
                message: format!("thread {thread_id} already exists"),
            })?;
        Self::insert_items(&mut tx, thread_id, 0, &[session_meta]).await?;
        tx.commit()
            .await
            .map_err(|err| write_err("failed to commit thread creation", err))?;
        self.live_guard().insert(
            thread_id,
            LiveWriter {
                epoch,
                history_mode: base.history_mode,
            },
        );
        Ok(())
    }

    async fn resume_thread_impl(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreResult<Arc<Vec<RolloutItem>>> {
        let thread_id = params.thread_id;
        let mut tx = self.begin().await?;
        let existing = self.fetch_row(&mut *tx, thread_id, true).await?;
        let (epoch, history_mode, history) = match existing {
            Some(row) => {
                if row.times.archived_at.is_some() && !params.include_archived {
                    return Err(ThreadStoreError::ThreadNotFound { thread_id });
                }
                let epoch: i64 = sqlx::query_scalar(
                    "UPDATE codex_fork_threads SET epoch = epoch + 1, owner_id = $2 \
                     WHERE thread_id = $1 RETURNING epoch",
                )
                .bind(thread_id.to_string())
                .bind(&self.owner_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|err| write_err("failed to claim thread", err))?;
                let history = match params.history {
                    // An explicit override replaces the stored history.
                    Some(history) if !is_canonical_snapshot(&history, thread_id) => {
                        let items = Arc::unwrap_or_clone(history);
                        sqlx::query("DELETE FROM codex_fork_thread_items WHERE thread_id = $1")
                            .bind(thread_id.to_string())
                            .execute(&mut *tx)
                            .await
                            .map_err(|err| write_err("failed to replace thread history", err))?;
                        Self::insert_items(&mut tx, thread_id, 0, &items).await?;
                        sqlx::query(
                            "UPDATE codex_fork_threads SET next_seq = $2 WHERE thread_id = $1",
                        )
                        .bind(thread_id.to_string())
                        .bind(items.len() as i64)
                        .execute(&mut *tx)
                        .await
                        .map_err(|err| write_err("failed to replace thread history", err))?;
                        items
                    }
                    // Stored history is authoritative under writer ownership; caller snapshots
                    // may predate writes committed by a previous owner.
                    _ => self.fetch_items(&mut *tx, thread_id).await?,
                };
                (epoch, row.base.history_mode, history)
            }
            None => {
                let Some(history) = params.history else {
                    return Err(ThreadStoreError::ThreadNotFound { thread_id });
                };
                let items = Arc::unwrap_or_clone(history);
                let base = ThreadBase::from_history(&items, &self.default_model_provider_id);
                let epoch = self
                    .insert_thread_row(&mut tx, thread_id, &base, items.len() as i64)
                    .await?
                    .ok_or_else(|| ThreadStoreError::Conflict {
                        message: format!("thread {thread_id} was created concurrently"),
                    })?;
                Self::insert_items(&mut tx, thread_id, 0, &items).await?;
                (epoch, base.history_mode, items)
            }
        };
        tx.commit()
            .await
            .map_err(|err| write_err("failed to commit thread resume", err))?;
        self.live_guard().insert(
            thread_id,
            LiveWriter {
                epoch,
                history_mode,
            },
        );
        Ok(Arc::new(history))
    }

    async fn append_items_impl(&self, params: AppendThreadItemsParams) -> ThreadStoreResult<()> {
        if params.items.is_empty() {
            return Ok(());
        }
        let thread_id = params.thread_id;
        let writer = self.require_live_writer(thread_id)?;
        let items = into_persisted_rollout_items(params.items, writer.history_mode);
        if items.is_empty() {
            return Ok(());
        }
        let mut tx = self.begin().await?;
        let first_seq: Option<i64> = sqlx::query_scalar(
            "UPDATE codex_fork_threads SET next_seq = next_seq + $3, updated_at = now() \
             WHERE thread_id = $1 AND epoch = $2 AND owner_id = $4 RETURNING next_seq - $3",
        )
        .bind(thread_id.to_string())
        .bind(writer.epoch)
        .bind(items.len() as i64)
        .bind(&self.owner_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| write_err("failed to reserve item sequence", err))?;
        let Some(first_seq) = first_seq else {
            // Distinguish deletion from a stale epoch for a precise error.
            self.check_fence(&mut tx, thread_id, writer).await?;
            return Err(ThreadStoreError::Internal {
                message: format!("failed to append to thread {thread_id}"),
            });
        };
        Self::insert_items(&mut tx, thread_id, first_seq, &items).await?;
        tx.commit()
            .await
            .map_err(|err| write_err("failed to commit appended items", err))
    }

    async fn update_thread_metadata_impl(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreResult<StoredThread> {
        if params.patch.project_id.is_some() {
            return Err(ThreadStoreError::Unsupported {
                operation: "projects",
            });
        }
        let thread_id = params.thread_id;
        let mut tx = self.begin().await?;
        let row = self
            .fetch_row(&mut *tx, thread_id, true)
            .await?
            .filter(|row| params.include_archived || row.times.archived_at.is_none())
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        if let Some(writer) = self.live_writer(thread_id)
            && (row.epoch != writer.epoch || row.owner_id != self.owner_id)
        {
            return Err(self.stale_writer_error(thread_id, writer, &row.owner_id, row.epoch));
        }
        let patch = params.patch;
        let created_at = patch.created_at.unwrap_or(row.times.created_at);
        let updated_at = patch.updated_at.unwrap_or(row.times.updated_at);
        let recency_at = patch
            .advance_recency_at
            .or(patch.updated_at)
            .map_or(row.times.recency_at, |at| at.max(row.times.recency_at));
        let mut metadata = row.metadata;
        metadata.merge(patch);
        let projection = project(&row.base, &metadata, &self.default_model_provider_id);
        let metadata_json =
            serde_json::to_value(&metadata).map_err(|err| serde_err("thread metadata", err))?;
        sqlx::query(
            "UPDATE codex_fork_threads SET metadata = $2, model_provider = $3, source_json = $4, \
             cwd = $5, search_text = $6, created_at = $7, updated_at = $8, recency_at = $9 \
             WHERE thread_id = $1",
        )
        .bind(thread_id.to_string())
        .bind(Json(metadata_json))
        .bind(projection.model_provider)
        .bind(projection.source_json)
        .bind(projection.cwd)
        .bind(projection.search_text)
        .bind(created_at)
        .bind(updated_at)
        .bind(recency_at)
        .execute(&mut *tx)
        .await
        .map_err(|err| write_err("failed to update thread metadata", err))?;
        tx.commit()
            .await
            .map_err(|err| write_err("failed to commit thread metadata", err))?;
        Ok(stored_thread(
            thread_id,
            &row.base,
            &metadata,
            RowTimes {
                created_at,
                updated_at,
                recency_at,
                archived_at: row.times.archived_at,
            },
            &self.default_model_provider_id,
            /*history*/ None,
        ))
    }

    fn to_stored_thread(&self, row: ThreadRow, history: Option<Vec<RolloutItem>>) -> StoredThread {
        stored_thread(
            row.thread_id,
            &row.base,
            &row.metadata,
            row.times,
            &self.default_model_provider_id,
            history,
        )
    }

    async fn read_thread_impl(&self, params: ReadThreadParams) -> ThreadStoreResult<StoredThread> {
        let row = self
            .fetch_visible_row(params.thread_id, params.include_archived)
            .await?;
        let history = if params.include_history {
            reject_paginated(row.base.history_mode)?;
            Some(self.fetch_items(&self.pool, params.thread_id).await?)
        } else {
            None
        };
        Ok(self.to_stored_thread(row, history))
    }

    async fn load_items_impl(
        &self,
        params: &LoadThreadHistoryParams,
        allow_paginated: bool,
    ) -> ThreadStoreResult<Vec<RolloutItem>> {
        let row = self
            .fetch_visible_row(params.thread_id, params.include_archived)
            .await?;
        if !allow_paginated {
            reject_paginated(row.base.history_mode)?;
        }
        self.fetch_items(&self.pool, params.thread_id).await
    }

    async fn list_threads_impl(&self, params: ListThreadsParams) -> ThreadStoreResult<ThreadPage> {
        let sort_column = match params.sort_key {
            ThreadSortKey::CreatedAt => "created_at",
            ThreadSortKey::UpdatedAt => "updated_at",
            ThreadSortKey::RecencyAt => "recency_at",
            ThreadSortKey::SectionPosition => {
                // This store has no sections, so no thread can be in the requested section.
                return match params.section {
                    Some(Some(_)) => Ok(ThreadPage {
                        items: Vec::new(),
                        next_cursor: None,
                    }),
                    _ => Err(ThreadStoreError::InvalidRequest {
                        message: "section-position sorting requires a section filter".to_owned(),
                    }),
                };
            }
        };
        if matches!(
            params.relation_filter,
            Some(ThreadRelationFilter::AttachedThreadsOf(_))
        ) {
            return Err(ThreadStoreError::Unsupported {
                operation: "attached threads",
            });
        }
        let cursor = params.cursor.as_deref().map(parse_cursor).transpose()?;
        let page_size = params.page_size.max(1);
        self.ready().await?;

        let mut query = QueryBuilder::<Postgres>::new(concat!(
            "SELECT ",
            thread_columns!(),
            " FROM codex_fork_threads WHERE "
        ));
        query.push(if params.archived {
            "archived_at IS NOT NULL"
        } else {
            "archived_at IS NULL"
        });
        if !params.allowed_sources.is_empty() {
            let sources = params
                .allowed_sources
                .iter()
                .map(source_json)
                .collect::<Vec<_>>();
            query
                .push(" AND source_json = ANY(")
                .push_bind(sources)
                .push(")");
        }
        if let Some(providers) = params.model_providers.filter(|p| !p.is_empty()) {
            query
                .push(" AND model_provider = ANY(")
                .push_bind(providers)
                .push(")");
        }
        if let Some(cwds) = params.cwd_filters {
            let cwds = cwds
                .iter()
                .map(|cwd| cwd.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            query.push(" AND cwd = ANY(").push_bind(cwds).push(")");
        }
        if matches!(params.section, Some(Some(_))) || matches!(params.project_id, Some(Some(_))) {
            // Sections and projects are not supported, so every thread is unassigned.
            query.push(" AND FALSE");
        }
        if let Some(term) = params.search_term.filter(|term| !term.trim().is_empty()) {
            query
                .push(" AND strpos(lower(search_text), lower(")
                .push_bind(term)
                .push(")) > 0");
        }
        match params.relation_filter {
            Some(ThreadRelationFilter::DirectChildrenOf(parent)) => {
                query
                    .push(" AND parent_thread_id = ")
                    .push_bind(parent.to_string());
            }
            Some(ThreadRelationFilter::DescendantsOf(ancestor)) => {
                query
                    .push(
                        " AND thread_id IN (WITH RECURSIVE descendants(id) AS (\
                         SELECT thread_id FROM codex_fork_threads WHERE parent_thread_id = ",
                    )
                    .push_bind(ancestor.to_string())
                    .push(
                        " UNION SELECT t.thread_id FROM codex_fork_threads t \
                         JOIN descendants d ON t.parent_thread_id = d.id) \
                         SELECT id FROM descendants)",
                    );
            }
            Some(ThreadRelationFilter::AttachedThreadsOf(_)) | None => {}
        }
        let (comparison, direction) = match params.sort_direction {
            SortDirection::Asc => (">", "ASC"),
            SortDirection::Desc => ("<", "DESC"),
        };
        if let Some((cursor_at, cursor_id)) = cursor {
            query
                .push(format!(" AND ({sort_column}, thread_id) {comparison} ("))
                .push_bind(cursor_at)
                .push(", ")
                .push_bind(cursor_id)
                .push(")");
        }
        query
            .push(format!(
                " ORDER BY {sort_column} {direction}, thread_id {direction} LIMIT "
            ))
            .push_bind(page_size as i64 + 1);

        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|err| read_err("failed to list threads", err))?;
        let mut items = rows
            .iter()
            .map(ThreadRow::from_row)
            .collect::<ThreadStoreResult<Vec<_>>>()?;
        let next_cursor = if items.len() > page_size {
            items.truncate(page_size);
            items.last().map(|row| {
                let at = match params.sort_key {
                    ThreadSortKey::UpdatedAt => row.times.updated_at,
                    ThreadSortKey::RecencyAt => row.times.recency_at,
                    ThreadSortKey::CreatedAt | ThreadSortKey::SectionPosition => {
                        row.times.created_at
                    }
                };
                format!("{}|{}", at.timestamp_micros(), row.thread_id)
            })
        } else {
            None
        };
        Ok(ThreadPage {
            items: items
                .into_iter()
                .map(|row| self.to_stored_thread(row, None))
                .collect(),
            next_cursor,
        })
    }

    async fn archive_thread_impl(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        self.ready().await?;
        let result = sqlx::query(
            "UPDATE codex_fork_threads SET archived_at = COALESCE(archived_at, now()) \
             WHERE thread_id = $1",
        )
        .bind(thread_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|err| write_err("failed to archive thread", err))?;
        if result.rows_affected() == 0 {
            return Err(ThreadStoreError::ThreadNotFound { thread_id });
        }
        Ok(())
    }

    async fn unarchive_thread_impl(&self, thread_id: ThreadId) -> ThreadStoreResult<StoredThread> {
        self.ready().await?;
        let row = sqlx::query(concat!(
            "UPDATE codex_fork_threads SET archived_at = NULL WHERE thread_id = $1 RETURNING ",
            thread_columns!()
        ))
        .bind(thread_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| write_err("failed to unarchive thread", err))?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        Ok(self.to_stored_thread(ThreadRow::from_row(&row)?, None))
    }

    async fn delete_thread_impl(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let mut tx = self.begin().await?;
        let result = sqlx::query("DELETE FROM codex_fork_threads WHERE thread_id = $1")
            .bind(thread_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|err| write_err("failed to delete thread", err))?;
        sqlx::query("DELETE FROM codex_fork_turn_handoffs WHERE thread_id = $1")
            .bind(thread_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|err| write_err("failed to delete thread handoff", err))?;
        tx.commit()
            .await
            .map_err(|err| write_err("failed to commit thread deletion", err))?;
        self.live_guard().remove(&thread_id);
        if result.rows_affected() == 0 {
            return Err(ThreadStoreError::ThreadNotFound { thread_id });
        }
        Ok(())
    }

    async fn shutdown_thread_impl(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let result = self.check_fence_standalone(thread_id).await;
        if !matches!(result, Err(ThreadStoreError::InvalidRequest { .. })) {
            self.live_guard().remove(&thread_id);
        }
        result
    }
}

impl ThreadStore for PgThreadStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.create_thread_impl(params))
    }

    fn resume_thread(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreFuture<'_, Arc<Vec<RolloutItem>>> {
        Box::pin(self.resume_thread_impl(params))
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.append_items_impl(params))
    }

    fn persist_thread(
        &self,
        thread_id: ThreadId,
        _context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.check_fence_standalone(thread_id))
    }

    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.check_fence_standalone(thread_id))
    }

    fn shutdown_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.shutdown_thread_impl(thread_id))
    }

    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.live_guard().remove(&thread_id);
            Ok(())
        })
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        Box::pin(async move {
            let items = self
                .load_items_impl(&params, /*allow_paginated*/ false)
                .await?;
            Ok(StoredThreadHistory {
                thread_id: params.thread_id,
                items,
                revision: None,
            })
        })
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        Box::pin(async move {
            let items = self
                .load_items_impl(&params, /*allow_paginated*/ true)
                .await?;
            Ok(StoredModelContext {
                thread_id: params.thread_id,
                items,
                revision: None,
            })
        })
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(self.read_thread_impl(params))
    }

    fn read_thread_by_rollout_path(
        &self,
        _params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "read_thread_by_rollout_path",
            })
        })
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        Box::pin(self.list_threads_impl(params))
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        Box::pin(async move { self.update_thread_metadata_impl(params).await.map(Some) })
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.archive_thread_impl(params.thread_id))
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(self.unarchive_thread_impl(params.thread_id))
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.delete_thread_impl(params.thread_id))
    }
}

fn reject_paginated(history_mode: ThreadHistoryMode) -> ThreadStoreResult<()> {
    if matches!(history_mode, ThreadHistoryMode::Paginated) {
        return Err(ThreadStoreError::Unsupported {
            operation: "paginated_threads",
        });
    }
    Ok(())
}

/// A canonical snapshot starts with this thread's own session header; anything else is an
/// explicit history override.
fn is_canonical_snapshot(history: &[RolloutItem], thread_id: ThreadId) -> bool {
    matches!(
        history.first(),
        Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == thread_id
    )
}

fn parse_cursor(cursor: &str) -> ThreadStoreResult<(DateTime<Utc>, String)> {
    let invalid = || ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {cursor}"),
    };
    let (micros, thread_id) = cursor.split_once('|').ok_or_else(invalid)?;
    let at = micros
        .parse::<i64>()
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_micros)
        .ok_or_else(invalid)?;
    ThreadId::from_string(thread_id).map_err(|_| invalid())?;
    Ok((at, thread_id.to_string()))
}

/// The session header written as the first history item, mirroring the in-memory store.
fn session_meta_item(params: &CreateThreadParams) -> RolloutItem {
    let session_meta = SessionMeta {
        attached_thread: params.attached_thread,
        session_id: params.session_id,
        id: params.thread_id,
        forked_from_id: params.forked_from_id,
        parent_thread_id: params.parent_thread_id,
        timestamp: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        cwd: params.metadata.cwd.clone().unwrap_or_default(),
        runtime_workspace_roots: params
            .runtime_workspace_roots
            .as_ref()
            .map(|roots| roots.iter().map(AbsolutePathBuf::to_path_buf).collect()),
        agent_nickname: params.source.get_nickname(),
        agent_role: params.source.get_agent_role(),
        agent_path: params.source.get_agent_path().map(Into::into),
        originator: params.originator.clone(),
        cli_version: env!("CARGO_PKG_VERSION").to_string(),
        creator_user_id: params.creator_user_id.clone(),
        creator_account_id: params.creator_account_id.clone(),
        source: params.source.clone(),
        thread_source: params.thread_source.clone(),
        model_provider: Some(params.metadata.model_provider.clone()),
        base_instructions: Some(params.base_instructions.clone()),
        dynamic_tools: (!params.dynamic_tools.is_empty()).then(|| params.dynamic_tools.clone()),
        selected_capability_roots: params.selected_capability_roots.clone(),
        memory_mode: matches!(params.metadata.memory_mode, ThreadMemoryMode::Disabled)
            .then_some("disabled".to_string()),
        history_mode: params.history_mode,
        history_base: params.history_base,
        subagent_history_start_ordinal: params.subagent_history_start_ordinal,
        multi_agent_version: params.multi_agent_version,
        context_window: Some(SessionContextWindow::new(params.initial_window_id.clone())),
        ..SessionMeta::default()
    };
    RolloutItem::SessionMeta(Box::new(SessionMetaLine {
        meta: session_meta,
        git: None,
    }))
}
