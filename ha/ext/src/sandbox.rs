//! Binds each thread to one remote sandbox for its whole life.
//!
//! The first remote environment selected for a thread (normally through
//! `thread/start.environments`) is recorded once and never changes. A thread created with
//! `thread/fork` inherits its source thread's binding. Whenever a thread is loaded, including a
//! cold resume on another process, the binding is re-registered with this process's environment
//! manager (address from [`SANDBOX_URL_TEMPLATE_ENV_VAR`]) and restored as the thread's sticky
//! environment selection, before the client can start or dequeue any turn.
//!
//! Core keeps neither the environment registry nor remote selections across a cold resume, which
//! is why the binding lives here.

use std::sync::Arc;
use std::sync::Weak;

use codex_core::CodexThread;
use codex_core::ThreadManager;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ThreadLifecycleContributor;
use codex_extension_api::ThreadReadyInput;
use codex_extension_api::ThreadResumeInput;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadSettingsOverrides;
use tracing::info;
use tracing::warn;

use crate::handoff::HandoffEnvironments;
use crate::store::SandboxStore;

/// Exec-server address for a sandbox id; `{id}` is replaced with the environment id.
pub const SANDBOX_URL_TEMPLATE_ENV_VAR: &str = "HCODEX_SANDBOX_URL_TEMPLATE";
/// Binding store (`postgres://…` or `file:<dir>`); defaults to the handoff store.
pub const SANDBOX_STORE_ENV_VAR: &str = "HCODEX_SANDBOX_STORE";
const ID_PLACEHOLDER: &str = "{id}";

pub struct SandboxBinding {
    thread_manager: Weak<ThreadManager>,
    store: Arc<dyn SandboxStore>,
    url_template: String,
}

impl SandboxBinding {
    pub fn new(
        thread_manager: Weak<ThreadManager>,
        store: Arc<dyn SandboxStore>,
        url_template: String,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            url_template.contains(ID_PLACEHOLDER),
            "{SANDBOX_URL_TEMPLATE_ENV_VAR} must contain {ID_PLACEHOLDER}"
        );
        Ok(Self {
            thread_manager,
            store,
            url_template,
        })
    }

    /// Exec-server URL for `environment_id`.
    pub fn sandbox_url(&self, environment_id: &str) -> String {
        self.url_template.replace(ID_PLACEHOLDER, environment_id)
    }

    /// Records the binding of a newly seen thread, or restores an existing one.
    pub async fn sync_thread(&self, thread_id: &str) -> anyhow::Result<()> {
        let manager = self
            .thread_manager
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("thread manager is unavailable"))?;
        let thread = manager
            .get_thread(ThreadId::from_string(thread_id)?)
            .await?;
        let binding = match self.store.get(thread_id).await? {
            Some(binding) => binding,
            None => {
                let Some(binding) = self.capture(&manager, &thread).await? else {
                    return Ok(());
                };
                if self.store.put_if_absent(thread_id, &binding).await? {
                    info!(thread_id, ?binding, "fork sandbox: bound thread");
                    binding
                } else {
                    // Another process bound it first; its record wins.
                    self.store
                        .get(thread_id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("sandbox binding vanished"))?
                }
            }
        };
        self.register(&manager, &binding)?;
        let current = thread
            .restorable_thread_settings()
            .await
            .environments
            .map(HandoffEnvironments::from);
        if current.as_ref() != Some(&binding) {
            thread
                .update_thread_settings(ThreadSettingsOverrides {
                    environments: Some(binding.into()),
                    ..Default::default()
                })
                .await?;
            info!(thread_id, "fork sandbox: restored thread environment");
        }
        Ok(())
    }

    /// The thread's own remote selection, else the binding of the thread it was forked from.
    async fn capture(
        &self,
        manager: &ThreadManager,
        thread: &CodexThread,
    ) -> anyhow::Result<Option<HandoffEnvironments>> {
        let environments = manager.environment_manager();
        if let Some(mut selection) = thread
            .restorable_thread_settings()
            .await
            .environments
            .map(HandoffEnvironments::from)
        {
            // Only remote sandboxes are bound; the local environment is never re-registered.
            selection.environments.retain(|environment| {
                environments
                    .get_environment(&environment.environment_id)
                    .is_some_and(|environment| environment.is_remote())
            });
            if !selection.environments.is_empty() {
                return Ok(Some(selection));
            }
        }
        match thread.config_snapshot().await.forked_from_thread_id {
            Some(source) => self.store.get(&source.to_string()).await,
            None => Ok(None),
        }
    }

    /// Registers bound environments this process does not know yet.
    fn register(
        &self,
        manager: &ThreadManager,
        binding: &HandoffEnvironments,
    ) -> anyhow::Result<()> {
        let environments = manager.environment_manager();
        for environment in &binding.environments {
            let id = &environment.environment_id;
            if environments.get_environment(id).is_none() {
                environments
                    .upsert_environment(
                        id.clone(),
                        self.sandbox_url(id),
                        /*connect_timeout*/ None,
                    )
                    .map_err(|err| anyhow::anyhow!("register sandbox `{id}`: {err}"))?;
            }
        }
        Ok(())
    }
}

impl<C: Sync> ThreadLifecycleContributor<C> for SandboxBinding {
    fn on_thread_ready<'a>(&'a self, input: ThreadReadyInput<'a, C>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let thread_id = input.thread_store.level_id();
            if let Err(err) = self.sync_thread(thread_id).await {
                warn!(thread_id, "fork sandbox: {err:#}");
            }
        })
    }

    fn on_thread_resume<'a>(&'a self, input: ThreadResumeInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let thread_id = input.thread_store.level_id();
            if let Err(err) = self.sync_thread(thread_id).await {
                warn!(thread_id, "fork sandbox: {err:#}");
            }
        })
    }
}
