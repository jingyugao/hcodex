//! Integration tests against a real Postgres. Set `HCODEX_PG_TEST_URL` to run them.
//!
//! Each test runs in its own freshly created schema so tests can run in parallel.

use std::sync::Arc;

use hcodex_pg::HandoffRecord;
use hcodex_pg::PgHandoffStore;
use hcodex_pg::PgThreadStore;
use hcodex_pg::PgThreadStoreConfig;
use codex_protocol::ContextWindowId;
use codex_protocol::ThreadId;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::ArchiveThreadParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::DeleteThreadParams;
use codex_thread_store::InMemoryThreadStore;
use codex_thread_store::ListThreadsParams;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::SortDirection;
use codex_thread_store::ThreadMetadataPatch;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_thread_store::ThreadRelationFilter;
use codex_thread_store::ThreadSortKey;
use codex_thread_store::ThreadStore;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::UpdateThreadMetadataParams;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

const TEST_URL_ENV: &str = "HCODEX_PG_TEST_URL";

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type TestResult = Result<(), BoxError>;

/// Creates a unique schema and returns a URL whose connections use it.
async fn schema_url() -> Result<Option<String>, BoxError> {
    let Ok(base) = std::env::var(TEST_URL_ENV) else {
        eprintln!("skipping: {TEST_URL_ENV} is not set");
        return Ok(None);
    };
    let schema = format!("codex_fork_test_{}", uuid::Uuid::new_v4().simple());
    let pool = hcodex_pg::connect_lazy(&base)?;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&pool)
        .await?;
    pool.close().await;
    let separator = if base.contains('?') { '&' } else { '?' };
    Ok(Some(format!(
        "{base}{separator}options[search_path]={schema}"
    )))
}

fn store(url: &str, owner: &str) -> Result<PgThreadStore, ThreadStoreError> {
    PgThreadStore::new(PgThreadStoreConfig {
        url: url.to_string(),
        owner_id: owner.to_string(),
        default_model_provider_id: "openai".to_string(),
    })
}

fn create_params(thread_id: ThreadId, parent: Option<ThreadId>) -> CreateThreadParams {
    CreateThreadParams {
        attached_thread: false,
        creator_user_id: None,
        creator_account_id: None,
        session_id: thread_id.into(),
        thread_id,
        extra_config: None,
        forked_from_id: None,
        parent_thread_id: parent,
        source: SessionSource::Exec,
        thread_source: None,
        originator: "test_originator".to_string(),
        base_instructions: BaseInstructions::default(),
        dynamic_tools: Vec::new(),
        selected_capability_roots: Vec::new(),
        multi_agent_version: None,
        history_mode: ThreadHistoryMode::Legacy,
        history_base: None,
        subagent_history_start_ordinal: None,
        initial_window_id: ContextWindowId::new_uuid_v7().to_string(),
        runtime_workspace_roots: None,
        metadata: metadata(),
    }
}

fn metadata() -> ThreadPersistenceMetadata {
    ThreadPersistenceMetadata {
        cwd: None,
        model_provider: "test-provider".to_string(),
        memory_mode: ThreadMemoryMode::Enabled,
    }
}

fn resume_params(thread_id: ThreadId) -> ResumeThreadParams {
    ResumeThreadParams {
        thread_id,
        rollout_path: None,
        history: None,
        history_revision: None,
        include_archived: false,
        metadata: metadata(),
    }
}

fn message(role: &str, text: &str) -> Result<RolloutItem, serde_json::Error> {
    let content_type = if role == "user" {
        "input_text"
    } else {
        "output_text"
    };
    let item: ResponseItem = serde_json::from_value(json!({
        "type": "message",
        "role": role,
        "content": [{ "type": content_type, "text": text }],
    }))?;
    Ok(RolloutItem::ResponseItem(item.into()))
}

/// Serializes history, blanking fields the stores legitimately fill differently.
fn normalized(items: &[RolloutItem]) -> Result<Vec<Value>, serde_json::Error> {
    items
        .iter()
        .map(|item| {
            let mut value = serde_json::to_value(item)?;
            if let Some(meta) = value.get_mut("payload")
                && meta.get("timestamp").is_some()
            {
                meta["timestamp"] = json!("");
                meta["cli_version"] = json!("");
                meta["context_window"] = json!(null);
            }
            Ok(value)
        })
        .collect()
}

async fn run_lifecycle(
    store: &dyn ThreadStore,
    thread_id: ThreadId,
) -> Result<Vec<RolloutItem>, BoxError> {
    store.create_thread(create_params(thread_id, None)).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                message("user", "hello")?,
                message("assistant", "hi \u{0} there")?,
            ],
        })
        .await?;
    store.flush_thread(thread_id).await?;
    store.shutdown_thread(thread_id).await?;
    let resumed = store.resume_thread(resume_params(thread_id)).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![message("user", "again")?],
        })
        .await?;
    store.shutdown_thread(thread_id).await?;
    let history = store
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await?;
    assert_eq!(resumed.len() + 1, history.items.len());
    Ok(history.items)
}

#[tokio::test]
async fn lifecycle_round_trip_matches_in_memory_store() -> TestResult {
    let Some(url) = schema_url().await? else {
        return Ok(());
    };
    let pg = store(&url, "pod-a")?;
    let in_memory = InMemoryThreadStore::default();
    let thread_id = ThreadId::new();

    let pg_history = run_lifecycle(&pg, thread_id).await?;
    let memory_history = run_lifecycle(&in_memory, thread_id).await?;

    assert_eq!(pg_history.len(), 4);
    assert_eq!(normalized(&pg_history)?, normalized(&memory_history)?);
    Ok(())
}

#[tokio::test]
async fn read_list_archive_and_delete() -> TestResult {
    let Some(url) = schema_url().await? else {
        return Ok(());
    };
    let pg = store(&url, "pod-a")?;
    let parent = ThreadId::new();
    let child = ThreadId::new();
    let grandchild = ThreadId::new();
    pg.create_thread(create_params(parent, None)).await?;
    pg.create_thread(create_params(child, Some(parent))).await?;
    pg.create_thread(create_params(grandchild, Some(child)))
        .await?;
    let renamed = pg
        .update_thread_metadata(UpdateThreadMetadataParams {
            thread_id: parent,
            patch: ThreadMetadataPatch {
                name: Some(Some("Renamed".to_string())),
                preview: Some("first question".to_string()),
                ..Default::default()
            },
            include_archived: false,
        })
        .await?
        .expect("materialized thread");
    assert_eq!(renamed.name.as_deref(), Some("Renamed"));

    let read = pg
        .read_thread(ReadThreadParams {
            thread_id: parent,
            include_archived: false,
            include_history: true,
        })
        .await?;
    assert_eq!(read.name.as_deref(), Some("Renamed"));
    assert_eq!(read.preview, "first question");
    assert_eq!(read.model_provider, "test-provider");
    assert_eq!(read.originator.as_deref(), Some("test_originator"));
    assert_eq!(read.history.map(|history| history.items.len()), Some(1));

    let list = |relation_filter, archived, page_size, cursor| ListThreadsParams {
        page_size,
        cursor,
        sort_key: ThreadSortKey::CreatedAt,
        sort_direction: SortDirection::Asc,
        allowed_sources: Vec::new(),
        model_providers: None,
        cwd_filters: None,
        section: None,
        project_id: None,
        archived,
        search_term: None,
        relation_filter,
        use_state_db_only: false,
    };
    let ids = |page: codex_thread_store::ThreadPage| {
        page.items
            .into_iter()
            .map(|thread| thread.thread_id)
            .collect::<Vec<_>>()
    };

    // Pagination walks every thread exactly once in creation order.
    let first = pg.list_threads(list(None, false, 2, None)).await?;
    let cursor = first.next_cursor.clone();
    assert!(cursor.is_some());
    let mut all = ids(first);
    let second = pg.list_threads(list(None, false, 2, cursor)).await?;
    assert_eq!(second.next_cursor, None);
    all.extend(ids(second));
    assert_eq!(all, vec![parent, child, grandchild]);

    let descendants = pg
        .list_threads(list(
            Some(ThreadRelationFilter::DescendantsOf(parent)),
            false,
            10,
            None,
        ))
        .await?;
    assert_eq!(ids(descendants), vec![child, grandchild]);
    let mut search = list(None, false, 10, None);
    search.search_term = Some("renam".to_string());
    assert_eq!(ids(pg.list_threads(search).await?), vec![parent]);

    pg.archive_thread(ArchiveThreadParams { thread_id: child })
        .await?;
    assert_eq!(
        ids(pg.list_threads(list(None, true, 10, None)).await?),
        vec![child]
    );
    assert!(matches!(
        pg.read_thread(ReadThreadParams {
            thread_id: child,
            include_archived: false,
            include_history: false,
        })
        .await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));
    let restored = pg
        .unarchive_thread(ArchiveThreadParams { thread_id: child })
        .await?;
    assert_eq!(restored.archived_at, None);

    pg.delete_thread(DeleteThreadParams { thread_id: child })
        .await?;
    assert!(matches!(
        pg.delete_thread(DeleteThreadParams { thread_id: child })
            .await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));
    assert_eq!(
        ids(pg.list_threads(list(None, false, 10, None)).await?),
        vec![parent, grandchild]
    );
    Ok(())
}

#[tokio::test]
async fn stale_writer_is_rejected_after_another_owner_resumes() -> TestResult {
    let Some(url) = schema_url().await? else {
        return Ok(());
    };
    let pod_a = store(&url, "pod-a")?;
    let pod_b = store(&url, "pod-b")?;
    let thread_id = ThreadId::new();
    pod_a.create_thread(create_params(thread_id, None)).await?;
    pod_a
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![message("user", "from a")?],
        })
        .await?;

    // Pod B takes over without pod A shutting down; it sees A's committed writes.
    let history = pod_b.resume_thread(resume_params(thread_id)).await?;
    assert_eq!(history.len(), 2);

    let stale_append = pod_a
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![message("user", "late write from a")?],
        })
        .await;
    assert!(
        matches!(stale_append, Err(ThreadStoreError::Conflict { .. })),
        "{stale_append:?}"
    );
    assert!(matches!(
        pod_a.flush_thread(thread_id).await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert!(matches!(
        pod_a
            .update_thread_metadata(UpdateThreadMetadataParams {
                thread_id,
                patch: ThreadMetadataPatch {
                    name: Some(Some("stale".to_string())),
                    ..Default::default()
                },
                include_archived: false,
            })
            .await,
        Err(ThreadStoreError::Conflict { .. })
    ));

    pod_b
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![message("user", "from b")?],
        })
        .await?;
    let history = pod_b
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await?;
    assert_eq!(history.items.len(), 3);
    Ok(())
}

#[tokio::test]
async fn handoff_is_claimed_exactly_once() -> TestResult {
    let Some(url) = schema_url().await? else {
        return Ok(());
    };
    // The handoff store works on its own pool, without a thread store having run migrations.
    let handoffs = PgHandoffStore::new(hcodex_pg::connect_lazy(&url)?);
    let thread_id = ThreadId::new().to_string();
    let record = |turn_id: &str| HandoffRecord {
        thread_id: thread_id.clone(),
        turn_id: turn_id.to_string(),
        source_owner: "pod-a".to_string(),
        payload: json!({ "pending_input": ["continue"] }),
    };
    handoffs.put(record("turn-1")).await?;
    handoffs.put(record("turn-2")).await?;

    let handoffs = Arc::new(handoffs);
    let claims = (0..8)
        .map(|_| {
            let handoffs = Arc::clone(&handoffs);
            let thread_id = thread_id.clone();
            tokio::spawn(async move { handoffs.take(&thread_id).await })
        })
        .collect::<Vec<_>>();
    let mut claimed = Vec::new();
    for claim in claims {
        if let Some(record) = claim.await?? {
            claimed.push(record);
        }
    }
    assert_eq!(claimed, vec![record("turn-2")]);
    assert_eq!(handoffs.take(&thread_id).await?, None);
    Ok(())
}
