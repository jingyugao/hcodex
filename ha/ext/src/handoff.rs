//! Moves an unfinished root turn between app-server processes.
//!
//! Source: [`TurnHandoff::suspend_thread`] records a handoff, then uses Core's
//! `suspend_turn_and_shutdown`, which flushes history and stops the turn without a terminal
//! event so its turn ID stays recoverable. Target: after a client resumes the thread, the first
//! idle notification claims the record and calls `recover_turn_if_idle` with the saved
//! environment selection and per-turn env.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use codex_core::CodexThread;
use codex_core::RecoverTurnRequest;
use codex_core::StartIfIdleSubmission;
use codex_core::SuspendTurnOutcome;
use codex_core::ThreadManager;
use codex_core::WithTurnExtensionData;
use codex_core::fork_hooks::ForkTurnEnv;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ThreadIdleInput;
use codex_extension_api::ThreadLifecycleContributor;
use codex_extension_api::ThreadResumeInput;
use codex_extension_api::ThreadStopInput;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EnvironmentConfigState;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::TurnEnvironmentRequest;
use codex_protocol::protocol::TurnEnvironmentRequests;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_path_uri::PathUri;
use serde::Deserialize;
use serde::Serialize;
use tracing::info;
use tracing::warn;

use crate::store::HandoffRecord;
use crate::store::HandoffStore;

const PAYLOAD_VERSION: u32 = 1;

/// State the target needs beyond persisted history to continue a suspended turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HandoffPayload {
    pub version: u32,
    /// Environment selection active on the source. Core does not persist remote environment
    /// selections, so the target re-applies them; their configuration comes from the target.
    pub environments: Option<HandoffEnvironments>,
    /// Per-turn env (`fork.env.*`) of the suspended turn.
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HandoffEnvironments {
    pub legacy_fallback_cwd: AbsolutePathBuf,
    pub environments: Vec<HandoffEnvironment>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HandoffEnvironment {
    pub environment_id: String,
    pub cwd: PathUri,
    pub workspace_roots: Vec<PathUri>,
}

impl From<TurnEnvironmentRequests> for HandoffEnvironments {
    fn from(requests: TurnEnvironmentRequests) -> Self {
        Self {
            legacy_fallback_cwd: requests.legacy_fallback_cwd,
            environments: requests
                .environment_requests
                .into_iter()
                .map(|request| HandoffEnvironment {
                    environment_id: request.environment_id,
                    cwd: request.cwd,
                    workspace_roots: request.workspace_roots,
                })
                .collect(),
        }
    }
}

impl From<HandoffEnvironments> for TurnEnvironmentRequests {
    fn from(environments: HandoffEnvironments) -> Self {
        TurnEnvironmentRequests::new(
            environments.legacy_fallback_cwd,
            environments
                .environments
                .into_iter()
                .map(|environment| TurnEnvironmentRequest {
                    environment_id: environment.environment_id,
                    cwd: environment.cwd,
                    workspace_roots: environment.workspace_roots,
                    config: EnvironmentConfigState::FromThread,
                })
                .collect(),
        )
    }
}

/// Result of asking this process to give up one thread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SuspendOutcome {
    /// The turn stopped and a handoff record names it.
    HandedOff { turn_id: String },
    /// No regular root turn was running, so nothing needs recovery.
    Idle,
}

pub struct TurnHandoff {
    thread_manager: Weak<ThreadManager>,
    store: Arc<dyn HandoffStore>,
    owner_id: String,
    /// Threads resumed by this process whose handoff record has not been checked yet.
    resumed: Mutex<HashSet<String>>,
}

impl TurnHandoff {
    pub fn new(
        thread_manager: Weak<ThreadManager>,
        store: Arc<dyn HandoffStore>,
        owner_id: String,
    ) -> Self {
        Self {
            thread_manager,
            store,
            owner_id,
            resumed: Mutex::new(HashSet::new()),
        }
    }

    fn thread_manager(&self) -> anyhow::Result<Arc<ThreadManager>> {
        self.thread_manager
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("thread manager is unavailable"))
    }

    /// Stops `thread_id`'s running root turn and records it for another process to recover.
    pub async fn suspend_thread(&self, thread_id: ThreadId) -> anyhow::Result<SuspendOutcome> {
        let thread_manager = self.thread_manager()?;
        let thread = thread_manager.get_thread(thread_id).await?;
        if !is_handoff_candidate(&thread).await {
            return Ok(SuspendOutcome::Idle);
        }
        let Some((turn_id, env)) = thread.fork_active_turn_env().await else {
            return Ok(SuspendOutcome::Idle);
        };
        let environments = thread
            .restorable_thread_settings()
            .await
            .environments
            .map(HandoffEnvironments::from);
        let payload = HandoffPayload {
            version: PAYLOAD_VERSION,
            environments,
            env,
        };
        // Record first so a crash after suspension cannot lose the recoverable turn.
        self.put(&thread_id, &turn_id, &payload).await?;

        match thread.suspend_turn_and_shutdown().await {
            Ok(SuspendTurnOutcome::Suspended { turn_id: suspended }) => {
                if suspended != turn_id {
                    // Another turn replaced the observed one; its env was never captured.
                    let payload = HandoffPayload {
                        env: BTreeMap::new(),
                        ..payload
                    };
                    self.put(&thread_id, &suspended, &payload).await?;
                }
                thread_manager.remove_thread(&thread_id).await;
                info!(%thread_id, turn_id = %suspended, "fork handoff: suspended turn");
                Ok(SuspendOutcome::HandedOff { turn_id: suspended })
            }
            Ok(SuspendTurnOutcome::NotActive | SuspendTurnOutcome::UnsupportedTask) => {
                self.discard(&thread_id).await;
                Ok(SuspendOutcome::Idle)
            }
            Err(err) => {
                // The current process keeps ownership when suspension fails.
                self.discard(&thread_id).await;
                Err(err.into())
            }
        }
    }

    /// Whether this process has `thread_id` loaded.
    pub async fn has_thread(&self, thread_id: ThreadId) -> bool {
        match self.thread_manager() {
            Ok(thread_manager) => thread_manager.get_thread(thread_id).await.is_ok(),
            Err(_) => false,
        }
    }

    /// Suspends every running root turn loaded in this process. Returns how many were handed off.
    pub async fn suspend_all_running(&self) -> usize {
        let Ok(thread_manager) = self.thread_manager() else {
            return 0;
        };
        let mut handed_off = 0;
        for thread_id in thread_manager.list_thread_ids().await {
            match self.suspend_thread(thread_id).await {
                Ok(SuspendOutcome::HandedOff { .. }) => handed_off += 1,
                Ok(SuspendOutcome::Idle) => {}
                Err(err) => warn!(%thread_id, "fork handoff: suspend failed: {err:#}"),
            }
        }
        handed_off
    }

    /// Claims a handoff record for a thread resumed here and continues its turn.
    async fn recover(&self, thread_id: &str) -> anyhow::Result<()> {
        let Some(record) = self.store.take(thread_id).await? else {
            return Ok(());
        };
        let result = self.recover_record(&record).await;
        if result.is_err() {
            // Keep the turn recoverable by a later resume.
            if let Err(err) = self.store.put(record).await {
                warn!(%thread_id, "fork handoff: failed to restore record: {err:#}");
            }
        }
        result
    }

    async fn recover_record(&self, record: &HandoffRecord) -> anyhow::Result<()> {
        let payload: HandoffPayload = serde_json::from_value(record.payload.clone())?;
        anyhow::ensure!(
            payload.version == PAYLOAD_VERSION,
            "unsupported handoff payload version {}",
            payload.version
        );
        let thread_id = ThreadId::from_string(&record.thread_id)?;
        let thread = self.thread_manager()?.get_thread(thread_id).await?;

        // Submission init replaces the thread's inherited turn data, so extend it.
        let mut turn_extension_init = thread.config_snapshot().await.turn_extension_init;
        turn_extension_init.insert(ForkTurnEnv {
            turn_id: record.turn_id.clone(),
            env: payload.env,
        });
        let request = RecoverTurnRequest {
            turn_id: record.turn_id.clone(),
            thread_settings: ThreadSettingsOverrides {
                environments: payload.environments.map(TurnEnvironmentRequests::from),
                ..Default::default()
            },
            trace: None,
            prompt_cache_ttl: None,
            cyber_access_program: None,
        };
        match thread
            .recover_turn_if_idle(WithTurnExtensionData::new(request, turn_extension_init))
            .await?
        {
            StartIfIdleSubmission::Started { turn_id, .. } => {
                info!(
                    %thread_id,
                    %turn_id,
                    source_owner = %record.source_owner,
                    "fork handoff: recovered turn"
                );
            }
            StartIfIdleSubmission::NotSubmitted { reason } => {
                // The client already started other work; the suspended turn is superseded.
                warn!(%thread_id, ?reason, "fork handoff: recovery not started");
            }
        }
        Ok(())
    }

    async fn put(
        &self,
        thread_id: &ThreadId,
        turn_id: &str,
        payload: &HandoffPayload,
    ) -> anyhow::Result<()> {
        self.store
            .put(HandoffRecord {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                source_owner: self.owner_id.clone(),
                payload: serde_json::to_value(payload)?,
            })
            .await
    }

    async fn discard(&self, thread_id: &ThreadId) {
        if let Err(err) = self.store.take(&thread_id.to_string()).await {
            warn!(%thread_id, "fork handoff: failed to discard record: {err:#}");
        }
    }

    fn resumed(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.resumed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Only regular root turns can be suspended and recovered under their original ID.
async fn is_handoff_candidate(thread: &CodexThread) -> bool {
    let config = thread.config_snapshot().await;
    !config.ephemeral
        && config.parent_thread_id.is_none()
        && !config.session_source.is_non_root_agent()
}

impl<C: Sync> ThreadLifecycleContributor<C> for TurnHandoff {
    fn on_thread_resume<'a>(&'a self, input: ThreadResumeInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.resumed()
                .insert(input.thread_store.level_id().to_string());
        })
    }

    fn on_thread_idle<'a>(&'a self, input: ThreadIdleInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let thread_id = input.thread_store.level_id().to_string();
            // App-server reports idle after the resume response and replay, so recovery
            // events reach the resuming client.
            if !self.resumed().remove(&thread_id) {
                return;
            }
            if let Err(err) = self.recover(&thread_id).await {
                warn!(%thread_id, "fork handoff: recovery failed: {err:#}");
            }
        })
    }

    fn on_thread_stop<'a>(&'a self, input: ThreadStopInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.resumed().remove(input.thread_store.level_id());
        })
    }
}
