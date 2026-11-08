//! Wires the high-availability layer into codex through the fork's registration hooks.

use std::sync::Arc;
use std::sync::OnceLock;

/// Registers the Postgres thread and queue stores (`HCODEX_THREAD_STORE`) and the handoff and sandbox
/// extensions (`HCODEX_HANDOFF_*`, `HCODEX_SANDBOX_*`). Each stays inert unless its environment
/// variables are set. Must run before the app-server starts.
pub(crate) fn register() {
    let postgres = Arc::new(OnceLock::<Option<Arc<hcodex_pg::PgThreadStore>>>::new());
    let queue_postgres = Arc::clone(&postgres);
    codex_core::fork_hooks::set_thread_store_factory(move |config| {
        postgres
            .get_or_init(|| {
                hcodex_pg::PgThreadStore::from_env(&config.model_provider_id).map(Arc::new)
            })
            .as_ref()
            .map(|store| Arc::clone(store) as _)
    });
    codex_app_server::fork_hooks::set_queue_store_factory(move |config| {
        let store = queue_postgres
            .get_or_init(|| {
                hcodex_pg::PgThreadStore::from_env(&config.model_provider_id).map(Arc::new)
            })
            .as_ref()?;
        Some(Arc::new(hcodex_pg::PgQueueStore::new(store.pool().clone())))
    });
    codex_app_server::fork_hooks::add_extension_installer(|builder, thread_manager| {
        hcodex_ha::install(builder, thread_manager);
    });
}
