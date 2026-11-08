//! Upstream queued items carry Responses client metadata (and so `fork.env.*`) into the turn
//! that starts them, whether started explicitly or by idle dispatch.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use anyhow::Context;
use codex_core::StartIfIdleSubmission;
use codex_core::TurnInput;
use codex_extension_api::ExtensionData;
use codex_extension_api::NoopExtensionEventSink;
use codex_extension_api::ThreadIdleCause;
use codex_extension_api::ThreadIdleInput;
use codex_extension_api::ThreadLifecycleContributor;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::user_input::UserInput;
use codex_queue_extension::QueuedItemService;
use codex_thread_store::LocalQueueStore;
use codex_thread_store::QueueStore;
use core_test_support::responses;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::StreamingSseServer;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event_match;
use pretty_assertions::assert_eq;
use tokio::sync::oneshot;

fn user_input(text: &str) -> TurnInput {
    TurnInput::UserInput {
        content: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
        client_id: Some(format!("{text}:client")),
    }
}

fn metadata(user: &str) -> HashMap<String, String> {
    HashMap::from([
        ("fork.env.COLONY_USER".to_string(), user.to_string()),
        ("source".to_string(), "colony".to_string()),
    ])
}

fn expected_env(user: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("COLONY_USER".to_string(), user.to_string())])
}

fn queue_for(test: &TestCodex) -> anyhow::Result<Arc<dyn QueueStore>> {
    let runtime = test.codex.state_db().context("state runtime unavailable")?;
    Ok(Arc::new(LocalQueueStore::new(runtime)))
}

/// One streamed response held open until the returned sender fires.
async fn gated_server() -> (StreamingSseServer, oneshot::Sender<()>) {
    let (release, gate) = oneshot::channel();
    let (server, _completions) = start_streaming_sse_server(vec![vec![
        StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![responses::ev_response_created("resp-1")]),
        },
        StreamingSseChunk {
            gate: Some(gate),
            body: responses::sse(vec![responses::ev_completed("resp-1")]),
        },
    ]])
    .await;
    (server, release)
}

async fn finish_turn(test: &TestCodex, release: oneshot::Sender<()>) {
    assert!(release.send(()).is_ok(), "response gate should remain open");
    wait_for_event_match(test.codex.as_ref(), |event| {
        matches!(event, EventMsg::TurnComplete(_)).then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_start_applies_item_metadata_and_update_keeps_it() -> anyhow::Result<()> {
    let (server, release) = gated_server().await;
    let test = test_codex().build_with_streaming_server(&server).await?;
    let thread_id = test.session_configured.thread_id;
    let queue = queue_for(&test)?;
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    let queued = service
        .enqueue_with_metadata(thread_id, user_input("first"), Some(metadata("u1")))
        .await?;
    // The queued input itself is stored unchanged.
    assert_eq!(user_input("first"), queued.input);

    let updated = service
        .update(thread_id, queued.id.clone(), user_input("edited"))
        .await?
        .context("queued item should still exist")?;
    assert_eq!(vec![updated], service.list(thread_id).await?);
    let payload = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await?
        .remove(0)
        .payload;
    assert!(payload.contains("fork.env.COLONY_USER"), "{payload}");

    let submission = service
        .start(
            test.codex.as_ref(),
            /*queued_item_id*/ None,
            /*trace*/ None,
        )
        .await?;
    assert!(matches!(submission, StartIfIdleSubmission::Started { .. }));
    let (_, env) = test
        .codex
        .fork_active_turn_env()
        .await
        .context("queued turn should be active")?;
    assert_eq!(expected_env("u1"), env);
    assert_eq!(
        Vec::<codex_queue_extension::QueuedItem>::new(),
        service.list(thread_id).await?
    );

    finish_turn(&test, release).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_dispatch_applies_item_metadata() -> anyhow::Result<()> {
    let (server, release) = gated_server().await;
    let test = test_codex().build_with_streaming_server(&server).await?;
    let thread_id = test.session_configured.thread_id;
    let service = QueuedItemService::new(
        queue_for(&test)?,
        Arc::downgrade(&test.thread_manager),
        Arc::new(NoopExtensionEventSink),
    );

    service
        .enqueue_with_metadata(thread_id, user_input("queued"), Some(metadata("u2")))
        .await?;
    emit_idle(&service, thread_id).await;

    let env = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some((_, env)) = test.codex.fork_active_turn_env().await {
                return env;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    assert_eq!(expected_env("u2"), env);

    finish_turn(&test, release).await;
    Ok(())
}

async fn emit_idle(service: &QueuedItemService, thread_id: ThreadId) {
    let session_store = ExtensionData::new("session");
    let thread_store = ExtensionData::new(thread_id.to_string());
    <QueuedItemService as ThreadLifecycleContributor<()>>::on_thread_idle(
        service,
        ThreadIdleInput {
            cause: ThreadIdleCause::Completed,
            session_store: &session_store,
            thread_store: &thread_store,
        },
    )
    .await;
}
