//! Durable handoff records that let another app-server recover a suspended turn.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use futures::future::BoxFuture;
use serde::Deserialize;
use serde::Serialize;

/// One suspended turn waiting for its next owner. At most one record exists per thread.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HandoffRecord {
    pub thread_id: String,
    pub turn_id: String,
    pub source_owner: String,
    /// Versioned [`crate::HandoffPayload`] encoded as JSON.
    pub payload: serde_json::Value,
}

/// Storage for handoff records shared by every app-server that can own a thread.
pub trait HandoffStore: Send + Sync {
    /// Inserts or replaces the record for `record.thread_id`.
    fn put(&self, record: HandoffRecord) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Removes and returns the record for `thread_id`. Concurrent callers must never both
    /// receive the same record, because each would recover the turn.
    fn take<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Option<HandoffRecord>>>;
}

/// Directory-backed store for single-host deployments and tests.
///
/// `take` claims a record with an atomic rename, so concurrent claimers on one filesystem
/// receive it at most once.
pub struct FileHandoffStore {
    dir: PathBuf,
}

impl FileHandoffStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn record_path(&self, thread_id: &str) -> anyhow::Result<PathBuf> {
        // Thread IDs are UUIDs; reject anything that could escape the directory.
        anyhow::ensure!(
            !thread_id.is_empty()
                && thread_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "invalid thread id for handoff record: {thread_id:?}"
        );
        Ok(self.dir.join(format!("{thread_id}.json")))
    }
}

impl HandoffStore for FileHandoffStore {
    fn put(&self, record: HandoffRecord) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let path = self.record_path(&record.thread_id)?;
            tokio::fs::create_dir_all(&self.dir)
                .await
                .with_context(|| format!("create handoff dir {}", self.dir.display()))?;
            let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
            tokio::fs::write(&tmp, serde_json::to_vec(&record)?)
                .await
                .with_context(|| format!("write handoff record {}", tmp.display()))?;
            tokio::fs::rename(&tmp, &path)
                .await
                .with_context(|| format!("publish handoff record {}", path.display()))?;
            Ok(())
        })
    }

    fn take<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Option<HandoffRecord>>> {
        Box::pin(async move {
            let path = self.record_path(thread_id)?;
            let claimed = path.with_extension(format!("claimed-{}", uuid::Uuid::new_v4()));
            match tokio::fs::rename(&path, &claimed).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(err) => {
                    return Err(err).with_context(|| format!("claim {}", path.display()));
                }
            }
            let bytes = tokio::fs::read(&claimed).await?;
            let _ = tokio::fs::remove_file(&claimed).await;
            Ok(Some(serde_json::from_slice(&bytes)?))
        })
    }
}

/// Postgres-backed store sharing the hcodex thread-store database.
pub struct PgHandoffStore(pub Arc<hcodex_pg::PgHandoffStore>);

impl HandoffStore for PgHandoffStore {
    fn put(&self, record: HandoffRecord) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            self.0
                .put(hcodex_pg::HandoffRecord {
                    thread_id: record.thread_id,
                    turn_id: record.turn_id,
                    source_owner: record.source_owner,
                    payload: record.payload,
                })
                .await
                .map_err(|err| anyhow::anyhow!("put handoff record: {err}"))
        })
    }

    fn take<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Option<HandoffRecord>>> {
        Box::pin(async move {
            let record = self
                .0
                .take(thread_id)
                .await
                .map_err(|err| anyhow::anyhow!("take handoff record: {err}"))?;
            Ok(record.map(|record| HandoffRecord {
                thread_id: record.thread_id,
                turn_id: record.turn_id,
                source_owner: record.source_owner,
                payload: record.payload,
            }))
        })
    }
}

/// Write-once storage for thread → sandbox bindings, shared by every app-server.
pub trait SandboxStore: Send + Sync {
    fn get<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Option<crate::HandoffEnvironments>>>;

    /// Records `binding` unless one exists. Returns whether this call wrote it.
    fn put_if_absent<'a>(
        &'a self,
        thread_id: &'a str,
        binding: &'a crate::HandoffEnvironments,
    ) -> BoxFuture<'a, anyhow::Result<bool>>;
}

/// Directory-backed bindings: one `<thread_id>.json` per thread, created exclusively.
pub struct FileSandboxStore {
    dir: PathBuf,
}

impl FileSandboxStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn path(&self, thread_id: &str) -> anyhow::Result<PathBuf> {
        anyhow::ensure!(
            !thread_id.is_empty()
                && thread_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "invalid thread id for sandbox binding: {thread_id:?}"
        );
        Ok(self.dir.join(format!("{thread_id}.json")))
    }
}

impl SandboxStore for FileSandboxStore {
    fn get<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Option<crate::HandoffEnvironments>>> {
        Box::pin(async move {
            let path = self.path(thread_id)?;
            match tokio::fs::read(&path).await {
                Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
            }
        })
    }

    fn put_if_absent<'a>(
        &'a self,
        thread_id: &'a str,
        binding: &'a crate::HandoffEnvironments,
    ) -> BoxFuture<'a, anyhow::Result<bool>> {
        Box::pin(async move {
            let path = self.path(thread_id)?;
            tokio::fs::create_dir_all(&self.dir)
                .await
                .with_context(|| format!("create sandbox dir {}", self.dir.display()))?;
            // Write a complete temp file, then hard-link it: the link fails if a binding exists,
            // so readers never observe a partial file and the first writer wins.
            let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
            tokio::fs::write(&tmp, serde_json::to_vec(binding)?)
                .await
                .with_context(|| format!("write {}", tmp.display()))?;
            let linked = tokio::fs::hard_link(&tmp, &path).await;
            let _ = tokio::fs::remove_file(&tmp).await;
            match linked {
                Ok(()) => Ok(true),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
                Err(err) => Err(err).with_context(|| format!("publish {}", path.display())),
            }
        })
    }
}

/// Postgres-backed bindings sharing the hcodex thread-store database.
pub struct PgSandboxStore(pub Arc<hcodex_pg::PgSandboxStore>);

impl SandboxStore for PgSandboxStore {
    fn get<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Option<crate::HandoffEnvironments>>> {
        Box::pin(async move {
            let binding = self
                .0
                .get(thread_id)
                .await
                .map_err(|err| anyhow::anyhow!("read sandbox binding: {err}"))?;
            Ok(binding.map(serde_json::from_value).transpose()?)
        })
    }

    fn put_if_absent<'a>(
        &'a self,
        thread_id: &'a str,
        binding: &'a crate::HandoffEnvironments,
    ) -> BoxFuture<'a, anyhow::Result<bool>> {
        Box::pin(async move {
            self.0
                .put_if_absent(thread_id, serde_json::to_value(binding)?)
                .await
                .map_err(|err| anyhow::anyhow!("store sandbox binding: {err}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn file_store_take_claims_a_record_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileHandoffStore::new(dir.path());
        let record = HandoffRecord {
            thread_id: "0198a0b0-0000-7000-8000-000000000001".to_string(),
            turn_id: "turn-1".to_string(),
            source_owner: "pod-a".to_string(),
            payload: serde_json::json!({ "version": 1 }),
        };
        store.put(record.clone()).await.expect("put");

        let (first, second) =
            tokio::join!(store.take(&record.thread_id), store.take(&record.thread_id));
        let mut claimed = [first.expect("take"), second.expect("take")];
        claimed.sort_by_key(Option::is_none);

        assert_eq!(claimed, [Some(record), None]);
        assert!(store.take("../escape").await.is_err());
    }

    #[tokio::test]
    async fn file_sandbox_store_keeps_the_first_binding() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileSandboxStore::new(dir.path());
        let thread_id = "0198a0b0-0000-7000-8000-000000000002";
        let workspace =
            codex_utils_absolute_path::AbsolutePathBuf::from_absolute_path("/workspace")
                .expect("absolute");
        let binding = |id: &str| crate::HandoffEnvironments {
            legacy_fallback_cwd: workspace.clone(),
            environments: vec![crate::HandoffEnvironment {
                environment_id: id.to_string(),
                cwd: codex_utils_path_uri::PathUri::from_abs_path(&workspace),
                workspace_roots: Vec::new(),
            }],
        };

        assert_eq!(store.get(thread_id).await.expect("get"), None);
        assert!(
            store
                .put_if_absent(thread_id, &binding("sbx-a"))
                .await
                .expect("put")
        );
        assert!(
            !store
                .put_if_absent(thread_id, &binding("sbx-b"))
                .await
                .expect("put")
        );
        assert_eq!(
            store.get(thread_id).await.expect("get"),
            Some(binding("sbx-a"))
        );
        assert!(store.get("../escape").await.is_err());
    }
}
