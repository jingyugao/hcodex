//! End-to-end turn handoff between two app-server processes.
//!
//! Requires the binary: `cargo build -p hcodex-server --bin codex-app-server`.

#![cfg(unix)]

use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use core_test_support::responses;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Child;
use tokio::process::Command;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::matchers::method;
use wiremock::matchers::path_regex;

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const READ_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_hands_off_running_turn_to_next_server_with_its_env() -> Result<()> {
    let codex_home = TempDir::new()?;
    let handoff_dir = TempDir::new()?;
    let store = format!("file:{}", handoff_dir.path().display());
    run_handoff(
        codex_home.path(),
        codex_home.path(),
        &store,
        /*thread_store*/ None,
    )
    .await?;
    assert!(std::fs::read_dir(handoff_dir.path())?.next().is_none());
    Ok(())
}

/// Production shape: pods share only Postgres and use the same CODEX_HOME path, each starting
/// with it empty, and neither keeps conversation files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_pods_hand_off_without_local_conversation_files() -> Result<()> {
    let Ok(url) = std::env::var("HCODEX_PG_TEST_URL") else {
        eprintln!("skipping: HCODEX_PG_TEST_URL is not set");
        return Ok(());
    };
    let root = TempDir::new()?;
    let home = root.path().join("codex-home");
    std::fs::create_dir_all(&home)?;
    run_handoff(&home, &home, &url, Some(&url)).await?;
    let files = conversation_files(&home)?;
    assert!(
        files.is_empty(),
        "conversation files under {}: {files:?}",
        home.display()
    );
    Ok(())
}

/// Starts a turn with fork env on pod A, SIGTERMs A mid-sampling, then resumes the thread on
/// pod B and requires B to finish the same turn with the same env.
/// `thread_store` is a Postgres URL for the thread store; `None` keeps local storage.
async fn run_handoff(
    home_a: &Path,
    home_b: &Path,
    store: &str,
    thread_store: Option<&str>,
) -> Result<()> {
    let server = responses::start_mock_server().await;
    // Source server: the first sampling request never completes before SIGTERM.
    mount(
        &server,
        1,
        final_message("too late"),
        Duration::from_secs(120),
    )
    .await;
    // Target server: the recovered turn echoes the per-turn env, then finishes.
    let echo = responses::sse(vec![
        responses::ev_response_created("resp-2"),
        responses::ev_exec_command_call("call-echo", "echo fork-env=$TRACE_ID"),
        responses::ev_completed("resp-2"),
    ]);
    mount(&server, 2, echo, Duration::ZERO).await;
    mount(&server, 3, final_message("recovered"), Duration::ZERO).await;
    write_config(home_a, &server.uri())?;

    let (mut pod_a, addr) = spawn_server(home_a, store, thread_store, "pod-a").await?;
    let mut ws = connect(addr).await?;
    initialize(&mut ws).await?;
    let thread = request(
        &mut ws,
        2,
        "thread/start",
        serde_json::to_value(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })?,
    )
    .await?;
    let thread_id = thread.result["thread"]["id"]
        .as_str()
        .context("thread id")?
        .to_string();
    let turn = request(
        &mut ws,
        3,
        "turn/start",
        serde_json::to_value(TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![UserInput::Text {
                text: "Hello".to_string(),
                text_elements: Vec::new(),
            }],
            responsesapi_client_metadata: Some(
                [("fork.env.TRACE_ID".to_string(), "trace-123".to_string())].into(),
            ),
            ..Default::default()
        })?,
    )
    .await?;
    let turn_id = turn.result["turn"]["id"]
        .as_str()
        .context("turn id")?
        .to_string();
    wait_for_requests(&server, 1).await?;

    kill(&pod_a, "-TERM")?;
    let status = timeout(Duration::from_secs(20), pod_a.wait())
        .await
        .context("pod A did not exit after handing off")??;
    assert!(status.success(), "pod A exit: {status}");
    if home_b == home_a && thread_store.is_some() {
        // A new pod starts with an empty CODEX_HOME at the same path.
        std::fs::remove_dir_all(home_b)?;
        std::fs::create_dir_all(home_b)?;
    }
    write_config(home_b, &server.uri())?;

    let (_pod_b, addr) = spawn_server(home_b, store, thread_store, "pod-b").await?;
    let mut ws = connect(addr).await?;
    initialize(&mut ws).await?;
    request(
        &mut ws,
        2,
        "thread/resume",
        serde_json::to_value(ThreadResumeParams {
            thread_id,
            ..Default::default()
        })?,
    )
    .await?;
    let completed = notification(&mut ws, "turn/completed").await?;
    let params = completed.params.context("turn/completed params")?;
    assert_eq!(params["turn"]["id"], json!(turn_id));
    assert_eq!(params["turn"]["status"], json!("completed"));

    let bodies = response_bodies(&server).await?;
    assert_eq!(bodies.len(), 3, "one source and two recovered requests");
    // Recovery must resend the interrupted request's input unchanged: no context is re-injected
    // when pods share the CODEX_HOME path, and replaying model output depends on it.
    let input =
        |body: &str| -> Result<Value> { Ok(serde_json::from_str::<Value>(body)?["input"].clone()) };
    assert_eq!(
        input(&bodies[1])?,
        input(&bodies[0])?,
        "recovered request input changed"
    );
    assert!(
        bodies[2].contains("fork-env=trace-123"),
        "recovered exec output should carry the per-turn env"
    );
    assert!(
        !bodies.iter().any(|body| body.contains("fork.env.")),
        "fork env metadata must not reach the model provider"
    );
    Ok(())
}

/// Rollout and history files that would hold conversation content.
/// Codex conversation (rollout) files under `dir`, compressed or not. Other `.jsonl` files, such as
/// fixtures in plugin repositories Codex clones at startup, are not conversation data.
fn conversation_files(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("rollout-")
                        && (name.ends_with(".jsonl") || name.ends_with(".jsonl.zst"))
                })
            {
                found.push(path);
            }
        }
    }
    Ok(found)
}

fn final_message(text: &str) -> String {
    responses::sse(vec![
        responses::ev_response_created("resp"),
        responses::ev_assistant_message("msg", text),
        responses::ev_completed("resp"),
    ])
}

async fn mount(server: &MockServer, priority: u8, body: String, delay: Duration) {
    Mock::given(method("POST"))
        .and(path_regex(".*/responses$"))
        .respond_with(responses::sse_response(body).set_delay(delay))
        .with_priority(priority)
        .up_to_n_times(1)
        .mount(server)
        .await;
}

fn write_config(codex_home: &Path, server_uri: &str) -> std::io::Result<()> {
    std::fs::write(
        codex_home.join("config.toml"),
        format!(
            r#"model = "mock-model"
approval_policy = "never"
# These tests cover handoff and env, not sandboxing; avoid needing bubblewrap.
sandbox_mode = "danger-full-access"
model_provider = "mock_provider"

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "{server_uri}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"#
        ),
    )
}

async fn spawn_server(
    codex_home: &Path,
    store: &str,
    thread_store: Option<&str>,
    owner: &str,
) -> Result<(Child, SocketAddr)> {
    let program = codex_utils_cargo_bin::cargo_bin("codex-app-server")
        .context("build codex-app-server first")?;
    // Debug builds accept a flag that skips plugin startup work; release builds reject it.
    let help = std::process::Command::new(&program)
        .arg("--help")
        .output()?;
    let mut command = Command::new(program);
    command.args(["--listen", "ws://127.0.0.1:0"]);
    if String::from_utf8_lossy(&help.stdout).contains("--disable-plugin-startup-tasks-for-tests") {
        command.arg("--disable-plugin-startup-tasks-for-tests");
    }
    if let Some(url) = thread_store {
        command.env(hcodex_pg::THREAD_STORE_ENV_VAR, url);
    }
    let mut child = command
        .env("CODEX_HOME", codex_home)
        .env("RUST_LOG", "warn,hcodex_ha=info")
        .env(hcodex_ha::HANDOFF_STORE_ENV_VAR, store)
        .env(hcodex_ha::HANDOFF_ON_SIGTERM_ENV_VAR, "1")
        .env(hcodex_ha::HANDOFF_GRACE_SECS_ENV_VAR, "0")
        .env("HCODEX_OWNER_ID", owner)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stderr = BufReader::new(child.stderr.take().context("stderr")?).lines();
    let addr = loop {
        let line = timeout(READ_TIMEOUT, stderr.next_line())
            .await
            .context("waiting for bind address")??
            .context("app-server exited before binding")?;
        eprintln!("[{owner}] {line}");
        if let Some(addr) = strip_ansi(&line)
            .split_whitespace()
            .find_map(|word| word.strip_prefix("ws://")?.parse::<SocketAddr>().ok())
        {
            break addr;
        }
    };
    let owner = owner.to_string();
    tokio::spawn(async move {
        while let Ok(Some(line)) = stderr.next_line().await {
            eprintln!("[{owner}] {line}");
        }
    });
    Ok((child, addr))
}

fn strip_ansi(line: &str) -> String {
    let mut stripped = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for next in chars.by_ref() {
                if ('@'..='~').contains(&next) {
                    break;
                }
            }
            continue;
        }
        stripped.push(ch);
    }
    stripped
}

async fn connect(addr: SocketAddr) -> Result<Ws> {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}")).await?;
    Ok(ws)
}

async fn initialize(ws: &mut Ws) -> Result<()> {
    request(
        ws,
        1,
        "initialize",
        json!({
            "clientInfo": { "name": "fork-handoff-e2e", "version": "0.1.0" },
            "capabilities": { "experimentalApi": true },
        }),
    )
    .await?;
    Ok(())
}

async fn request(ws: &mut Ws, id: i64, method: &str, params: Value) -> Result<JSONRPCResponse> {
    let message = JSONRPCMessage::Request(JSONRPCRequest {
        id: RequestId::Integer(id),
        method: method.to_string(),
        params: Some(params),
        trace: None,
    });
    ws.send(Message::Text(serde_json::to_string(&message)?.into()))
        .await?;
    loop {
        match read(ws).await? {
            JSONRPCMessage::Response(response) if response.id == RequestId::Integer(id) => {
                return Ok(response);
            }
            JSONRPCMessage::Error(error) if error.id == RequestId::Integer(id) => {
                bail!("{method} failed: {:?}", error.error);
            }
            _ => {}
        }
    }
}

async fn notification(ws: &mut Ws, method: &str) -> Result<JSONRPCNotification> {
    loop {
        if let JSONRPCMessage::Notification(notification) = read(ws).await?
            && notification.method == method
        {
            return Ok(notification);
        }
    }
}

async fn read(ws: &mut Ws) -> Result<JSONRPCMessage> {
    loop {
        let frame = timeout(READ_TIMEOUT, ws.next())
            .await
            .context("timed out waiting for websocket frame")?
            .context("websocket closed")??;
        if let Message::Text(text) = frame {
            return Ok(serde_json::from_str(text.as_ref())?);
        }
    }
}

async fn wait_for_requests(server: &MockServer, count: usize) -> Result<()> {
    let deadline = Instant::now() + READ_TIMEOUT;
    while response_bodies(server).await?.len() < count {
        if Instant::now() >= deadline {
            bail!("timed out waiting for {count} /responses request(s)");
        }
        sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

async fn response_bodies(server: &MockServer) -> Result<Vec<String>> {
    Ok(server
        .received_requests()
        .await
        .context("mock server requests")?
        .into_iter()
        .filter(|request| request.url.path().ends_with("/responses"))
        .map(|request| String::from_utf8_lossy(&request.body).into_owned())
        .collect())
}

fn kill(child: &Child, signal: &str) -> Result<()> {
    let pid = child.id().context("pid")?.to_string();
    let status = std::process::Command::new("kill")
        .args([signal, &pid])
        .status()?;
    anyhow::ensure!(status.success(), "kill {signal} failed");
    Ok(())
}
