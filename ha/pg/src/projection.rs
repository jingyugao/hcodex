//! Conversion between stored rows and [`StoredThread`].

use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::GitInfo;
use codex_protocol::protocol::GitSha;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSource;
use codex_rollout::RolloutItem;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::ExtraConfig;
use codex_thread_store::StoredThread;
use codex_thread_store::StoredThreadHistory;
use codex_thread_store::ThreadMetadataPatch;
use serde::Deserialize;
use serde::Serialize;

/// Creation-time facts about a thread, stored as the `base` JSONB column.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ThreadBase {
    #[serde(default)]
    pub(crate) forked_from_id: Option<ThreadId>,
    #[serde(default)]
    pub(crate) parent_thread_id: Option<ThreadId>,
    #[serde(default)]
    pub(crate) source: SessionSource,
    #[serde(default)]
    pub(crate) thread_source: Option<ThreadSource>,
    #[serde(default)]
    pub(crate) originator: String,
    #[serde(default)]
    pub(crate) creator_user_id: Option<String>,
    #[serde(default)]
    pub(crate) creator_account_id: Option<String>,
    #[serde(default)]
    pub(crate) history_mode: ThreadHistoryMode,
    #[serde(default)]
    pub(crate) extra_config: Option<ExtraConfig>,
    #[serde(default)]
    pub(crate) cwd: Option<PathBuf>,
    #[serde(default)]
    pub(crate) model_provider: Option<String>,
    #[serde(default)]
    pub(crate) cli_version: Option<String>,
}

impl ThreadBase {
    pub(crate) fn from_create_params(params: &CreateThreadParams) -> Self {
        Self {
            forked_from_id: params.forked_from_id,
            parent_thread_id: params.parent_thread_id,
            source: params.source.clone(),
            thread_source: params.thread_source.clone(),
            originator: params.originator.clone(),
            creator_user_id: params.creator_user_id.clone(),
            creator_account_id: params.creator_account_id.clone(),
            history_mode: params.history_mode,
            extra_config: params.extra_config.clone(),
            cwd: params.metadata.cwd.clone(),
            model_provider: Some(params.metadata.model_provider.clone()),
            cli_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        }
    }

    /// Derives a base from replay history (first `SessionMeta`) for threads that were imported
    /// through `resume_thread` instead of created by this store.
    pub(crate) fn from_history(items: &[RolloutItem], fallback_model_provider: &str) -> Self {
        let meta = items.iter().find_map(|item| match item {
            RolloutItem::SessionMeta(line) => Some(&line.meta),
            _ => None,
        });
        match meta {
            Some(meta) => Self::from_session_meta(meta),
            None => Self {
                forked_from_id: None,
                parent_thread_id: None,
                source: SessionSource::default(),
                thread_source: None,
                originator: String::new(),
                creator_user_id: None,
                creator_account_id: None,
                history_mode: ThreadHistoryMode::default(),
                extra_config: None,
                cwd: None,
                model_provider: Some(fallback_model_provider.to_string()),
                cli_version: None,
            },
        }
    }

    fn from_session_meta(meta: &SessionMeta) -> Self {
        Self {
            forked_from_id: meta.forked_from_id,
            parent_thread_id: meta.parent_thread_id,
            source: meta.source.clone(),
            thread_source: meta.thread_source.clone(),
            originator: meta.originator.clone(),
            creator_user_id: meta.creator_user_id.clone(),
            creator_account_id: meta.creator_account_id.clone(),
            history_mode: meta.history_mode,
            extra_config: None,
            cwd: (!meta.cwd.as_os_str().is_empty()).then(|| meta.cwd.clone()),
            model_provider: meta.model_provider.clone(),
            cli_version: (!meta.cli_version.is_empty()).then(|| meta.cli_version.clone()),
        }
    }
}

/// Scalar columns derived from `base` + `metadata`, recomputed on every write.
pub(crate) struct Projection {
    pub(crate) parent_thread_id: Option<String>,
    pub(crate) model_provider: String,
    pub(crate) source_json: String,
    pub(crate) cwd: String,
    pub(crate) search_text: String,
}

pub(crate) fn source_json(source: &SessionSource) -> String {
    match serde_json::to_value(source) {
        Ok(serde_json::Value::String(source)) => source,
        Ok(source) => source.to_string(),
        Err(_) => String::new(),
    }
}

pub(crate) fn effective_model_provider(
    base: &ThreadBase,
    metadata: &ThreadMetadataPatch,
    default_model_provider: &str,
) -> String {
    metadata
        .model_provider
        .clone()
        .or_else(|| base.model_provider.clone())
        .filter(|provider| !provider.is_empty())
        .unwrap_or_else(|| default_model_provider.to_string())
}

fn effective_cwd(base: &ThreadBase, metadata: &ThreadMetadataPatch) -> PathBuf {
    metadata
        .cwd
        .clone()
        .or_else(|| base.cwd.clone())
        .unwrap_or_default()
}

pub(crate) fn project(
    base: &ThreadBase,
    metadata: &ThreadMetadataPatch,
    default_model_provider: &str,
) -> Projection {
    let source = metadata.source.as_ref().unwrap_or(&base.source);
    let search_text = [
        metadata.name.clone().flatten(),
        metadata.title.clone(),
        metadata.preview.clone(),
        metadata.first_user_message.clone(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n");
    Projection {
        parent_thread_id: base.parent_thread_id.map(|id| id.to_string()),
        model_provider: effective_model_provider(base, metadata, default_model_provider),
        source_json: source_json(source),
        cwd: effective_cwd(base, metadata).to_string_lossy().into_owned(),
        search_text,
    }
}

/// Row-level timestamps that live in columns rather than in the metadata document.
pub(crate) struct RowTimes {
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) recency_at: DateTime<Utc>,
    pub(crate) archived_at: Option<DateTime<Utc>>,
}

pub(crate) fn stored_thread(
    thread_id: ThreadId,
    base: &ThreadBase,
    metadata: &ThreadMetadataPatch,
    times: RowTimes,
    default_model_provider: &str,
    history: Option<Vec<RolloutItem>>,
) -> StoredThread {
    let source = metadata
        .source
        .clone()
        .unwrap_or_else(|| base.source.clone());
    let originator = if base.originator.is_empty() {
        metadata.originator.clone()
    } else {
        Some(base.originator.clone())
    };
    StoredThread {
        creator_user_id: base
            .creator_user_id
            .clone()
            .or_else(|| metadata.creator_user_id.clone()),
        creator_account_id: base
            .creator_account_id
            .clone()
            .or_else(|| metadata.creator_account_id.clone()),
        originator: originator.filter(|originator| !originator.is_empty()),
        thread_id,
        extra_config: base.extra_config.clone(),
        rollout_path: None,
        forked_from_id: base.forked_from_id,
        parent_thread_id: base.parent_thread_id,
        preview: metadata.preview.clone().unwrap_or_default(),
        name: metadata.name.clone().flatten(),
        model_provider: effective_model_provider(base, metadata, default_model_provider),
        model: metadata.model.clone(),
        reasoning_effort: metadata.reasoning_effort.clone().flatten(),
        created_at: times.created_at,
        updated_at: times.updated_at,
        recency_at: times.recency_at,
        archived_at: times.archived_at,
        section: None,
        section_position: None,
        section_entered_at: None,
        project_id: None,
        daybreak_enabled: metadata.daybreak_enabled,
        cwd: effective_cwd(base, metadata),
        cli_version: metadata
            .cli_version
            .clone()
            .or_else(|| base.cli_version.clone())
            .unwrap_or_default(),
        history_mode: base.history_mode,
        thread_source: metadata
            .thread_source
            .clone()
            .unwrap_or_else(|| base.thread_source.clone()),
        agent_nickname: metadata
            .agent_nickname
            .clone()
            .unwrap_or_else(|| source.get_nickname()),
        agent_role: metadata
            .agent_role
            .clone()
            .unwrap_or_else(|| source.get_agent_role()),
        agent_path: metadata
            .agent_path
            .clone()
            .unwrap_or_else(|| source.get_agent_path().map(Into::into)),
        source,
        git_info: git_info_from_patch(metadata),
        approval_mode: metadata.approval_mode.unwrap_or(AskForApproval::Never),
        permission_profile: metadata
            .permission_profile
            .clone()
            .unwrap_or_else(PermissionProfile::read_only),
        token_usage: metadata.token_usage.clone(),
        first_user_message: metadata.first_user_message.clone(),
        history: history.map(|items| StoredThreadHistory {
            thread_id,
            items,
            revision: None,
        }),
    }
}

fn git_info_from_patch(patch: &ThreadMetadataPatch) -> Option<GitInfo> {
    let git_info = patch.git_info.as_ref()?;
    let sha = git_info.sha.clone().flatten();
    let branch = git_info.branch.clone().flatten();
    let origin_url = git_info.origin_url.clone().flatten();
    if sha.is_none() && branch.is_none() && origin_url.is_none() {
        return None;
    }
    Some(GitInfo {
        commit_hash: sha.as_deref().map(GitSha::new),
        branch,
        repository_url: origin_url,
    })
}
