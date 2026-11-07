//! End-to-end thread → sandbox binding with a real `codex-exec-server` as the sandbox.
//!
//! App-servers run with `CODEX_EXEC_SERVER_URL=none`, so they have no local environment: any
//! command that produces output ran in the sandbox.
//!
//! Requires the binaries in this target directory (see README "测试"):
//! `cargo build -p hcodex-server --bin codex-app-server`,
//! `codex-exec-server` and, for the code-mode test, `codex-code-mode-host` built from
//! `codex/codex-rs/Cargo.toml` (app-server finds the host next to its own executable).

#![cfg(unix)]

use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use core_test_support::responses;
use futures::SinkExt;
use futures::StreamExt;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Child;
use tokio::process::Command;
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
const SANDBOX_ID: &str = "sbx-e2e";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_keeps_its_sandbox_across_cold_resume_and_fork() -> Result<()> {
    let codex_home = TempDir::new()?;
    let store_dir = TempDir::new()?;
    let workspace = TempDir::new()?;
    let store = format!("file:{}", store_dir.path().display());
    let (_sandbox, sandbox_addr) = spawn_exec_server().await?;
    let template = format!("ws://{sandbox_addr}/?sandbox={{id}}");

    let server = responses::start_mock_server().await;
    // Three turns, each: one command, then a final message. Shell arithmetic keeps the expected
    // output out of the command text the model request also carries.
    for (index, (call, command)) in [
        ("call-1", "echo $((40+1))-started"),
        ("call-2", "echo $((40+2))-resumed"),
        ("call-3", "echo $((40+3))-forked"),
    ]
    .into_iter()
    .enumerate()
    {
        let priority = u8::try_from(index * 2 + 1)?;
        let exec = responses::sse(vec![
            responses::ev_response_created("resp"),
            responses::ev_exec_command_call(call, command),
            responses::ev_completed("resp"),
        ]);
        mount(&server, priority, exec).await;
        mount(&server, priority + 1, final_message("done")).await;
    }
    write_config(codex_home.path(), &server.uri())?;

    // Pod A: register the sandbox once, bind it at thread/start, run a command there.
    let (mut pod_a, addr) = spawn_server(codex_home.path(), &store, &template, "pod-a").await?;
    let mut ws = connect(addr).await?;
    initialize(&mut ws).await?;
    request(
        &mut ws,
        2,
        "environment/add",
        json!({ "environmentId": SANDBOX_ID, "execServerUrl": template.replace("{id}", SANDBOX_ID) }),
    )
    .await?;
    let thread = request(
        &mut ws,
        3,
        "thread/start",
        json!({
            "model": "mock-model",
            "environments": [{ "environmentId": SANDBOX_ID, "cwd": workspace.path() }],
        }),
    )
    .await?;
    let thread_id = thread.result["thread"]["id"]
        .as_str()
        .context("thread id")?
        .to_string();
    run_turn(&mut ws, 4, &thread_id).await?;
    assert_output(&server, "41-started").await?;
    let binding = std::fs::read_to_string(
        store_dir
            .path()
            .join("sandboxes")
            .join(format!("{thread_id}.json")),
    )
    .context("binding should be recorded")?;
    assert!(binding.contains(SANDBOX_ID), "{binding}");

    // Pod B: no environment/add and no environments param; the binding must restore both.
    kill(&pod_a)?;
    let _ = pod_a.wait().await;
    let (_pod_b, addr) = spawn_server(codex_home.path(), &store, &template, "pod-b").await?;
    let mut ws = connect(addr).await?;
    initialize(&mut ws).await?;
    request(&mut ws, 2, "thread/resume", json!({ "threadId": thread_id })).await?;
    run_turn(&mut ws, 3, &thread_id).await?;
    assert_output(&server, "42-resumed").await?;

    // A fork inherits the source thread's sandbox.
    let forked = request(&mut ws, 4, "thread/fork", json!({ "threadId": thread_id })).await?;
    let forked_id = forked.result["thread"]["id"]
        .as_str()
        .context("forked thread id")?
        .to_string();
    run_turn(&mut ws, 5, &forked_id).await?;
    assert_output(&server, "43-forked").await?;
    assert!(
        store_dir
            .path()
            .join("sandboxes")
            .join(format!("{forked_id}.json"))
            .exists(),
        "fork should record its inherited binding"
    );
    Ok(())
}

/// Virtual slots: sandboxes are listed statically in `environments.toml` (a gateway maps each slot
/// to a real exec-server), so clients never call `environment/add`, on the first pod or after a
/// cold resume on another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_slots_need_no_environment_add() -> Result<()> {
    let codex_home = TempDir::new()?;
    let store_dir = TempDir::new()?;
    let workspace = TempDir::new()?;
    let store = format!("file:{}", store_dir.path().display());
    let (_sandbox, sandbox_addr) = spawn_exec_server().await?;
    let template = format!("ws://{sandbox_addr}/?sandbox={{id}}");

    let server = responses::start_mock_server().await;
    for (index, (call, command)) in [
        ("call-1", "echo $((50+1))-slot"),
        ("call-2", "echo $((50+2))-slot-resumed"),
    ]
    .into_iter()
    .enumerate()
    {
        let priority = u8::try_from(index * 2 + 1)?;
        let exec = responses::sse(vec![
            responses::ev_response_created("resp"),
            responses::ev_exec_command_call(call, command),
            responses::ev_completed("resp"),
        ]);
        mount(&server, priority, exec).await;
        mount(&server, priority + 1, final_message("done")).await;
    }
    write_config(codex_home.path(), &server.uri())?;
    let mut slots = String::from("include_local = false\n");
    for slot in ["slot-0", "slot-1"] {
        slots.push_str(&format!(
            "\n[[environments]]\nid = \"{slot}\"\nurl = \"{}\"\n",
            template.replace("{id}", slot)
        ));
    }
    std::fs::write(codex_home.path().join("environments.toml"), slots)?;

    let (mut pod_a, addr) = spawn_server(codex_home.path(), &store, &template, "pod-a").await?;
    let mut ws = connect(addr).await?;
    initialize(&mut ws).await?;
    let thread = request(
        &mut ws,
        2,
        "thread/start",
        json!({
            "model": "mock-model",
            "environments": [{ "environmentId": "slot-1", "cwd": workspace.path() }],
        }),
    )
    .await?;
    let thread_id = thread.result["thread"]["id"]
        .as_str()
        .context("thread id")?
        .to_string();
    run_turn(&mut ws, 3, &thread_id).await?;
    assert_output(&server, "51-slot").await?;

    kill(&pod_a)?;
    let _ = pod_a.wait().await;
    let (_pod_b, addr) = spawn_server(codex_home.path(), &store, &template, "pod-b").await?;
    let mut ws = connect(addr).await?;
    initialize(&mut ws).await?;
    request(&mut ws, 2, "thread/resume", json!({ "threadId": thread_id })).await?;
    run_turn(&mut ws, 3, &thread_id).await?;
    assert_output(&server, "52-slot-resumed").await?;
    let binding = std::fs::read_to_string(
        store_dir
            .path()
            .join("sandboxes")
            .join(format!("{thread_id}.json")),
    )?;
    assert!(binding.contains("slot-1"), "{binding}");
    Ok(())
}

/// With sandbox binding on, an app-server that still has a local environment refuses to run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_environment_is_refused_when_binding_is_on() -> Result<()> {
    let codex_home = TempDir::new()?;
    let store_dir = TempDir::new()?;
    let server = responses::start_mock_server().await;
    write_config(codex_home.path(), &server.uri())?;
    let program = codex_utils_cargo_bin::cargo_bin("codex-app-server")
        .context("build codex-app-server first")?;
    // No CODEX_EXEC_SERVER_URL and no environments.toml: the local environment is present.
    let mut child = Command::new(program)
        .args(["--listen", "ws://127.0.0.1:0"])
        .env("CODEX_HOME", codex_home.path())
        .env("RUST_LOG", "warn")
        .env_remove("CODEX_EXEC_SERVER_URL")
        .env(
            hcodex_ha::SANDBOX_URL_TEMPLATE_ENV_VAR,
            "ws://127.0.0.1:1/?sandbox={id}",
        )
        .env(
            hcodex_ha::SANDBOX_STORE_ENV_VAR,
            format!("file:{}", store_dir.path().display()),
        )
        .env_remove(hcodex_ha::HANDOFF_STORE_ENV_VAR)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let status = timeout(READ_TIMEOUT, child.wait())
        .await
        .context("app-server should exit")??;
    assert_eq!(status.code(), Some(78), "{status}");
    Ok(())
}

/// Code mode: one JS cell runs two commands in the sandbox, with the turn's fork env.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn code_mode_cell_runs_several_commands_in_the_sandbox() -> Result<()> {
    if codex_utils_cargo_bin::cargo_bin("codex-code-mode-host").is_err() {
        eprintln!("skipping: build codex-code-mode-host first");
        return Ok(());
    }
    let codex_home = TempDir::new()?;
    let store_dir = TempDir::new()?;
    let workspace = TempDir::new()?;
    let store = format!("file:{}", store_dir.path().display());
    let (_sandbox, sandbox_addr) = spawn_exec_server().await?;
    let template = format!("ws://{sandbox_addr}/?sandbox={{id}}");

    let server = responses::start_mock_server().await;
    let code = r#"
const first = await tools.exec_command({ cmd: "echo $((40+4))-$COLONY_USER" });
const second = await tools.exec_command({ cmd: "echo $((40+5))-second" });
text(first.output + "|" + second.output);
"#;
    let cell = responses::sse(vec![
        responses::ev_response_created("resp"),
        responses::ev_custom_tool_call("call-js", "exec", code),
        responses::ev_completed("resp"),
    ]);
    mount(&server, 1, cell).await;
    mount(&server, 2, final_message("done")).await;
    write_config(codex_home.path(), &server.uri())?;
    let config_path = codex_home.path().join("config.toml");
    let config = std::fs::read_to_string(&config_path)?;
    std::fs::write(&config_path, format!("{config}\n[features]\ncode_mode = true\n"))?;

    let (_pod, addr) = spawn_server(codex_home.path(), &store, &template, "pod-js").await?;
    let mut ws = connect(addr).await?;
    initialize(&mut ws).await?;
    request(
        &mut ws,
        2,
        "environment/add",
        json!({ "environmentId": SANDBOX_ID, "execServerUrl": template.replace("{id}", SANDBOX_ID) }),
    )
    .await?;
    let thread = request(
        &mut ws,
        3,
        "thread/start",
        json!({
            "model": "mock-model",
            "environments": [{ "environmentId": SANDBOX_ID, "cwd": workspace.path() }],
        }),
    )
    .await?;
    let thread_id = thread.result["thread"]["id"]
        .as_str()
        .context("thread id")?
        .to_string();
    run_turn_with_metadata(
        &mut ws,
        4,
        &thread_id,
        json!({ "fork.env.COLONY_USER": "u1" }),
    )
    .await?;
    assert_output(&server, "44-u1").await?;
    assert_output(&server, "45-second").await?;
    Ok(())
}

async fn run_turn(ws: &mut Ws, id: i64, thread_id: &str) -> Result<()> {
    run_turn_with_metadata(ws, id, thread_id, Value::Null).await
}

async fn run_turn_with_metadata(
    ws: &mut Ws,
    id: i64,
    thread_id: &str,
    metadata: Value,
) -> Result<()> {
    request(
        ws,
        id,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "run it", "text_elements": [] }],
            "responsesapiClientMetadata": metadata,
        }),
    )
    .await?;
    loop {
        if let JSONRPCMessage::Notification(notification) = read(ws).await?
            && notification.method == "turn/completed"
        {
            let params = notification.params.context("turn/completed params")?;
            if params["threadId"] == json!(thread_id) {
                return Ok(());
            }
        }
    }
}

/// The command output reached the model, which only happens when the sandbox ran it.
async fn assert_output(server: &MockServer, expected: &str) -> Result<()> {
    let bodies: Vec<String> = server
        .received_requests()
        .await
        .context("mock server requests")?
        .into_iter()
        .filter(|request| request.url.path().ends_with("/responses"))
        .map(|request| String::from_utf8_lossy(&request.body).into_owned())
        .collect();
    anyhow::ensure!(
        bodies.iter().any(|body| body.contains(expected)),
        "no model request carried `{expected}`; last body: {}",
        bodies.last().map(String::as_str).unwrap_or_default()
    );
    Ok(())
}

fn final_message(text: &str) -> String {
    responses::sse(vec![
        responses::ev_response_created("resp"),
        responses::ev_assistant_message("msg", text),
        responses::ev_completed("resp"),
    ])
}

async fn mount(server: &MockServer, priority: u8, body: String) {
    Mock::given(method("POST"))
        .and(path_regex(".*/responses$"))
        .respond_with(responses::sse_response(body))
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

async fn spawn_exec_server() -> Result<(Child, SocketAddr)> {
    let program = codex_utils_cargo_bin::cargo_bin("codex-exec-server")
        .context("build codex-exec-server first")?;
    let mut child = Command::new(program)
        .args(["--listen", "ws://127.0.0.1:0"])
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdout = BufReader::new(child.stdout.take().context("stdout")?).lines();
    let addr = loop {
        let line = timeout(READ_TIMEOUT, stdout.next_line())
            .await
            .context("waiting for exec-server address")??
            .context("exec-server exited before binding")?;
        if let Some(addr) = line.trim().strip_prefix("ws://") {
            break addr.parse()?;
        }
    };
    tokio::spawn(async move { while let Ok(Some(_)) = stdout.next_line().await {} });
    Ok((child, addr))
}

async fn spawn_server(
    codex_home: &Path,
    store: &str,
    template: &str,
    owner: &str,
) -> Result<(Child, SocketAddr)> {
    let program = codex_utils_cargo_bin::cargo_bin("codex-app-server")
        .context("build codex-app-server first")?;
    let help = std::process::Command::new(&program)
        .arg("--help")
        .output()?;
    let mut command = Command::new(program);
    command.args(["--listen", "ws://127.0.0.1:0"]);
    if String::from_utf8_lossy(&help.stdout).contains("--disable-plugin-startup-tasks-for-tests") {
        command.arg("--disable-plugin-startup-tasks-for-tests");
    }
    let mut child = command
        .env("CODEX_HOME", codex_home)
        .env("RUST_LOG", "warn,hcodex_ha=info")
        .env("CODEX_EXEC_SERVER_URL", "none")
        .env(hcodex_ha::SANDBOX_URL_TEMPLATE_ENV_VAR, template)
        .env(hcodex_ha::SANDBOX_STORE_ENV_VAR, store)
        .env_remove(hcodex_ha::HANDOFF_STORE_ENV_VAR)
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
            "clientInfo": { "name": "fork-sandbox-e2e", "version": "0.1.0" },
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

fn kill(child: &Child) -> Result<()> {
    let pid = child.id().context("pid")?.to_string();
    let status = std::process::Command::new("kill")
        .args(["-KILL", &pid])
        .status()?;
    anyhow::ensure!(status.success(), "kill failed");
    Ok(())
}
