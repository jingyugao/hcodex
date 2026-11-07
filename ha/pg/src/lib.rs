//! PostgreSQL persistence for hcodex: Codex threads, cross-pod turn handoff and sandbox bindings.
//!
//! [`PgThreadStore`] implements [`codex_thread_store::ThreadStore`] so that all conversation data
//! lives in Postgres. Live writers are fenced by a per-thread `epoch`: opening a thread for writes
//! (create or resume) bumps the epoch, and every subsequent write by that store is conditioned on
//! the epoch it claimed, so a stale pod cannot keep writing after another pod took the thread over.
//!
//! [`PgHandoffStore`] shares the same database and offers an exactly-once claim of a pending
//! unfinished-turn handoff record. [`PgSandboxStore`] keeps write-once thread → sandbox bindings.

mod db;
mod handoff;
mod projection;
mod queue;
mod sandbox;
mod store;

pub use handoff::HandoffRecord;
pub use handoff::PgHandoffStore;
pub use queue::PgQueueStore;
pub use sandbox::PgSandboxStore;
/// Re-exported so dependents share this crate's driver feature setup.
pub use sqlx;
pub use store::PgThreadStore;
pub use store::PgThreadStoreConfig;

/// Creates a pool without connecting (usable from sync code). Stores built on it run their
/// migrations lazily before their first query.
#[expect(
    clippy::disallowed_methods,
    reason = "the disallowed pool constructors guard SQLite; this is a Postgres pool"
)]
pub fn connect_lazy(url: &str) -> Result<sqlx::PgPool, codex_thread_store::ThreadStoreError> {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect_lazy(url)
        .map_err(|err| codex_thread_store::ThreadStoreError::InvalidRequest {
            message: format!("invalid postgres connection url: {err}"),
        })
}

/// Environment variable that overrides the owner id used for ownership fencing.
pub const OWNER_ID_ENV_VAR: &str = "HCODEX_OWNER_ID";

/// Returns the owner id for this process: `HCODEX_OWNER_ID`, else `HOSTNAME`, else a random
/// UUID.
pub fn owner_id_from_env() -> String {
    [OWNER_ID_ENV_VAR, "HOSTNAME"]
        .into_iter()
        .filter_map(|name| std::env::var(name).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

/// Switches the thread store to Postgres when set to a `postgres://…` URL. Read from the
/// environment so upstream config types stay untouched.
pub const THREAD_STORE_ENV_VAR: &str = "HCODEX_THREAD_STORE";

/// Builds the Postgres thread store when [`THREAD_STORE_ENV_VAR`] is set, else `None` so the
/// configured upstream store is used.
///
/// Panics when the variable is set but the store cannot be built: falling back to local storage
/// would silently split thread history across processes.
pub fn thread_store_from_env(
    default_model_provider_id: &str,
) -> Option<std::sync::Arc<dyn codex_thread_store::ThreadStore>> {
    PgThreadStore::from_env(default_model_provider_id).map(|store| std::sync::Arc::new(store) as _)
}

impl PgThreadStore {
    /// Builds the configured Postgres store without erasing its type, so a host can share its
    /// connection pool with [`PgQueueStore`]. Invalid configuration never falls back to disk.
    pub fn from_env(default_model_provider_id: &str) -> Option<Self> {
        let url = std::env::var(THREAD_STORE_ENV_VAR)
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        Some(
            Self::new(PgThreadStoreConfig {
                url,
                owner_id: owner_id_from_env(),
                default_model_provider_id: default_model_provider_id.to_string(),
            })
            .unwrap_or_else(|err| {
                panic!("cannot build postgres thread store from {THREAD_STORE_ENV_VAR}: {err}")
            }),
        )
    }
}
