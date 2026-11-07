//! hcodex high-availability extensions: cross-pod turn handoff (suspend on one app-server,
//! recover on another) and per-thread sandbox binding.
//!
//! Configured through environment variables so upstream config schemas stay untouched:
//!
//! - `HCODEX_HANDOFF_STORE`: `postgres://…` or `file:<dir>`. Unset disables handoff.
//! - `HCODEX_HANDOFF_ON_SIGTERM`: `1` suspends running turns after SIGTERM plus the grace
//!   period, so app-server's graceful drain can exit while the turns move elsewhere.
//! - `HCODEX_HANDOFF_GRACE_SECS`: seconds turns may finish naturally first (default 30).
//! - `HCODEX_OWNER_ID`: this process's identity in records (default `HOSTNAME`).
//!
//! With a Postgres store, `NOTIFY codex_fork_handoff, '<thread_id>'` asks whichever process has
//! that thread loaded to suspend it for handoff.

mod handoff;
mod sandbox;
mod store;

use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use codex_core::ThreadManager;
use codex_extension_api::ExtensionRegistryBuilder;
use tracing::error;
use tracing::info;
use tracing::warn;

pub use handoff::TurnHandoff;
pub use handoff::HandoffEnvironment;
pub use handoff::HandoffEnvironments;
pub use handoff::HandoffPayload;
pub use handoff::SuspendOutcome;
pub use sandbox::SandboxBinding;
pub use sandbox::SANDBOX_STORE_ENV_VAR;
pub use sandbox::SANDBOX_URL_TEMPLATE_ENV_VAR;
pub use store::FileHandoffStore;
pub use store::FileSandboxStore;
pub use store::HandoffRecord;
pub use store::HandoffStore;
pub use store::PgHandoffStore;
pub use store::PgSandboxStore;
pub use store::SandboxStore;

pub const HANDOFF_STORE_ENV_VAR: &str = "HCODEX_HANDOFF_STORE";
pub const HANDOFF_ON_SIGTERM_ENV_VAR: &str = "HCODEX_HANDOFF_ON_SIGTERM";
pub const HANDOFF_GRACE_SECS_ENV_VAR: &str = "HCODEX_HANDOFF_GRACE_SECS";
/// Postgres channel carrying thread IDs to suspend for handoff.
pub const HANDOFF_NOTIFY_CHANNEL: &str = "codex_fork_handoff";
const DEFAULT_GRACE: Duration = Duration::from_secs(30);

/// Installs turn handoff when [`HANDOFF_STORE_ENV_VAR`] is set. Returns the controller so hosts
/// can trigger [`TurnHandoff::suspend_thread`] themselves.
pub fn install<C: Sync + 'static>(
    builder: &mut ExtensionRegistryBuilder<C>,
    thread_manager: Weak<ThreadManager>,
) -> Option<Arc<TurnHandoff>> {
    install_sandbox(builder, thread_manager.clone());
    let spec = std::env::var(HANDOFF_STORE_ENV_VAR).ok()?;
    let (store, pool) = match store_from_spec(&spec) {
        Ok(store) => store,
        Err(err) => {
            warn!("fork handoff disabled: {err:#}");
            return None;
        }
    };
    let handoff = Arc::new(TurnHandoff::new(
        thread_manager,
        store,
        hcodex_pg::owner_id_from_env(),
    ));
    builder.thread_lifecycle_contributor(handoff.clone());
    if env_flag(HANDOFF_ON_SIGTERM_ENV_VAR) {
        spawn_sigterm_handoff(Arc::clone(&handoff), grace_from_env());
    }
    if let Some(pool) = pool {
        spawn_notify_listener(Arc::clone(&handoff), pool);
    }
    Some(handoff)
}

/// Installs per-thread sandbox binding when [`SANDBOX_URL_TEMPLATE_ENV_VAR`] is set. The binding
/// store comes from [`SANDBOX_STORE_ENV_VAR`], else [`HANDOFF_STORE_ENV_VAR`].
fn install_sandbox<C: Sync + 'static>(
    builder: &mut ExtensionRegistryBuilder<C>,
    thread_manager: Weak<ThreadManager>,
) {
    let Ok(template) = std::env::var(SANDBOX_URL_TEMPLATE_ENV_VAR) else {
        return;
    };
    let result = (|| {
        let spec = std::env::var(SANDBOX_STORE_ENV_VAR)
            .or_else(|_| std::env::var(HANDOFF_STORE_ENV_VAR))
            .map_err(|_| {
                anyhow::anyhow!("set {SANDBOX_STORE_ENV_VAR} or {HANDOFF_STORE_ENV_VAR}")
            })?;
        SandboxBinding::new(
            thread_manager.clone(),
            sandbox_store_from_spec(&spec)?,
            template,
        )
    })();
    // Guard even when binding failed to install: the template states that commands belong in
    // sandboxes.
    spawn_local_environment_guard(thread_manager);
    match result {
        Ok(sandbox) => builder.thread_lifecycle_contributor(Arc::new(sandbox)),
        Err(err) => error!("fork sandbox binding disabled: {err:#}"),
    }
}

/// Exit status when sandbox binding is on but the app-server still has a local environment
/// (`EX_CONFIG`).
const LOCAL_ENVIRONMENT_EXIT_CODE: i32 = 78;
/// Reserved id of the app-server's own environment.
const LOCAL_ENVIRONMENT_ID: &str = "local";
const GUARD_POLL: Duration = Duration::from_millis(20);
const GUARD_MAX_WAIT: Duration = Duration::from_secs(30);

/// With sandbox binding on, commands must never fall back to the app-server container: a thread
/// whose binding failed to load would otherwise run them locally. Exits the process when a local
/// environment is configured (set `include_local = false` in `environments.toml`, or
/// `CODEX_EXEC_SERVER_URL=none` without one).
///
/// The thread manager is constructed around the extension registry, so the check waits until it
/// is available.
fn spawn_local_environment_guard(thread_manager: Weak<ThreadManager>) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        warn!("fork sandbox: no tokio runtime; local environment guard disabled");
        return;
    };
    runtime.spawn(async move {
        let started = tokio::time::Instant::now();
        let manager = loop {
            if let Some(manager) = thread_manager.upgrade() {
                break manager;
            }
            if started.elapsed() > GUARD_MAX_WAIT {
                warn!("fork sandbox: thread manager unavailable; local environment not checked");
                return;
            }
            tokio::time::sleep(GUARD_POLL).await;
        };
        if manager
            .environment_manager()
            .get_environment(LOCAL_ENVIRONMENT_ID)
            .is_some()
        {
            error!(
                "fork sandbox: {SANDBOX_URL_TEMPLATE_ENV_VAR} is set but a local environment is \
                 configured; set include_local = false in environments.toml (or \
                 CODEX_EXEC_SERVER_URL=none) so commands never run in the app-server"
            );
            std::process::exit(LOCAL_ENVIRONMENT_EXIT_CODE);
        }
    });
}

/// Builds a sandbox binding store from `postgres://…`/`postgresql://…` or `file:<dir>`. File
/// bindings live in `<dir>/sandboxes`, so the directory can be shared with handoff records.
pub fn sandbox_store_from_spec(spec: &str) -> anyhow::Result<Arc<dyn SandboxStore>> {
    if let Some(dir) = spec.strip_prefix("file:") {
        anyhow::ensure!(!dir.is_empty(), "file sandbox store needs a directory");
        return Ok(Arc::new(FileSandboxStore::new(
            std::path::Path::new(dir).join("sandboxes"),
        )));
    }
    if spec.starts_with("postgres://") || spec.starts_with("postgresql://") {
        let pool = hcodex_pg::connect_lazy(spec)
            .map_err(|err| anyhow::anyhow!("connect sandbox store: {err}"))?;
        return Ok(Arc::new(PgSandboxStore(Arc::new(
            hcodex_pg::PgSandboxStore::new(pool),
        ))));
    }
    anyhow::bail!("unsupported sandbox store value; use postgres://… or file:<dir>")
}

/// Builds a store from `postgres://…`/`postgresql://…` or `file:<dir>`, returning the pool
/// for Postgres stores.
pub fn store_from_spec(
    spec: &str,
) -> anyhow::Result<(Arc<dyn HandoffStore>, Option<hcodex_pg::sqlx::PgPool>)> {
    if let Some(dir) = spec.strip_prefix("file:") {
        anyhow::ensure!(!dir.is_empty(), "file handoff store needs a directory");
        return Ok((Arc::new(FileHandoffStore::new(dir)), None));
    }
    if spec.starts_with("postgres://") || spec.starts_with("postgresql://") {
        let pool = hcodex_pg::connect_lazy(spec)
            .map_err(|err| anyhow::anyhow!("connect handoff store: {err}"))?;
        let store = PgHandoffStore(Arc::new(hcodex_pg::PgHandoffStore::new(pool.clone())));
        return Ok((Arc::new(store), Some(pool)));
    }
    anyhow::bail!("unsupported {HANDOFF_STORE_ENV_VAR} value; use postgres://… or file:<dir>")
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes"))
}

fn grace_from_env() -> Duration {
    std::env::var(HANDOFF_GRACE_SECS_ENV_VAR)
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_GRACE)
}

/// Upstream drain stops admitting turns on SIGTERM and exits once none run. After the grace
/// period, suspend whatever is still running so the drain can finish.
fn spawn_sigterm_handoff(handoff: Arc<TurnHandoff>, grace: Duration) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        warn!("fork handoff: no tokio runtime; SIGTERM handoff disabled");
        return;
    };
    runtime.spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::SignalKind;
            use tokio::signal::unix::signal;
            let Ok(mut terminate) = signal(SignalKind::terminate()) else {
                warn!("fork handoff: cannot listen for SIGTERM");
                return;
            };
            terminate.recv().await;
        }
        #[cfg(not(unix))]
        {
            let _ = (&handoff, grace);
            return;
        }
        #[cfg(unix)]
        {
            info!(
                ?grace,
                "fork handoff: SIGTERM received; waiting before suspending turns"
            );
            tokio::time::sleep(grace).await;
            let handed_off = handoff.suspend_all_running().await;
            info!(handed_off, "fork handoff: suspended running turns");
        }
    });
}

/// Suspends threads named on [`HANDOFF_NOTIFY_CHANNEL`] that this process has loaded.
/// Reconnects after connection loss; notifications sent while disconnected are lost, so
/// controllers should retry until the thread's owner changes.
fn spawn_notify_listener(handoff: Arc<TurnHandoff>, pool: hcodex_pg::sqlx::PgPool) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        warn!("fork handoff: no tokio runtime; NOTIFY listener disabled");
        return;
    };
    runtime.spawn(async move {
        loop {
            if let Err(err) = listen_once(&handoff, &pool).await {
                warn!("fork handoff: NOTIFY listener failed: {err:#}");
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn listen_once(
    handoff: &Arc<TurnHandoff>,
    pool: &hcodex_pg::sqlx::PgPool,
) -> anyhow::Result<()> {
    let mut listener = hcodex_pg::sqlx::postgres::PgListener::connect_with(pool).await?;
    listener.listen(HANDOFF_NOTIFY_CHANNEL).await?;
    loop {
        let notification = listener.recv().await?;
        let Ok(thread_id) = codex_protocol::ThreadId::from_string(notification.payload()) else {
            warn!(
                payload = notification.payload(),
                "fork handoff: ignoring invalid thread id"
            );
            continue;
        };
        let handoff = Arc::clone(handoff);
        tokio::spawn(async move {
            // Threads loaded elsewhere fail lookup here; their owner handles the request.
            if handoff.has_thread(thread_id).await {
                match handoff.suspend_thread(thread_id).await {
                    Ok(outcome) => info!(%thread_id, ?outcome, "fork handoff: NOTIFY handled"),
                    Err(err) => warn!(%thread_id, "fork handoff: NOTIFY suspend failed: {err:#}"),
                }
            }
        });
    }
}
