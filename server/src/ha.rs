//! Wires the high-availability layer into codex through the fork's registration hooks.

/// Registers the Postgres thread store (`HCODEX_THREAD_STORE`) and the handoff and sandbox
/// extensions (`HCODEX_HANDOFF_*`, `HCODEX_SANDBOX_*`). Each stays inert unless its environment
/// variables are set. Must run before the app-server starts.
pub(crate) fn register() {
    codex_core::fork_hooks::set_thread_store_factory(|config| {
        hcodex_pg::thread_store_from_env(&config.model_provider_id)
    });
    codex_app_server::fork_hooks::add_extension_installer(|builder, thread_manager| {
        hcodex_ha::install(builder, thread_manager);
    });
}
