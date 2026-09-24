//! Key/PATH-gated live integration tests for the cursor backend — wasi-model
//! "run 3" (the `cursor-sdk-bridge` agent acceptance gate).
//!
//! Mirrors the genai backend's `live.rs`: each test spawns a real
//! `cursor-sdk-bridge`, drives a completion through the
//! `omnia:model/completion` boundary, and parses the answer back.
//!
//! All tests are `#[ignore]`d so they never run or spawn a process in CI; run
//! them with `cargo nextest run --run-ignored all` alongside an installed
//! `cursor-sdk-bridge` and a `CURSOR_API_KEY`. The rows that watch the
//! spawned processes install the process's tracing subscriber, so they run
//! one per process, as nextest does.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result};
use omnia::Backend as _;
use omnia_cursor::{Client, ConnectOptions};
use omnia_wasi_model::{
    Answer, Error, Format, Function, Grants, Mcp, Message, Request, Role, Schema, Tool,
    WasiModelCtx,
};
use serde_json::{Value, json};
use support::{
    CHECK_WORD, SENTINEL, TOOL_SENTINEL, checking_tool_host, local_path_tool_host, no_tool_host,
    serve,
};
use tokio::net::TcpListener;
use tracing_subscriber::layer::SubscriberExt as _;

/// How long the pool may take to be rid of every process once the answers
/// are in: each lease's process is shut down and waited for first, and a
/// slot reopens only then.
const GONE: Duration = Duration::from_secs(15);

/// The answer text as the JSON object the prompts ask for.
fn object(answer: &Answer) -> Value {
    let value: Value = serde_json::from_str(&answer.answer)
        .unwrap_or_else(|e| panic!("the answer must be JSON ({e}): {}", answer.answer));
    assert!(value.is_object(), "the answer must be a JSON object: {value}");
    value
}

async fn connect() -> Result<Client> {
    Client::connect_with(ConnectOptions {
        timeout_secs: 120,
        inactivity_secs: 120,
        model: "auto".to_owned(),
        max_agents: 4,
    })
    .await
}

fn temp_workspace(label: &str) -> Result<std::path::PathBuf> {
    let workspace =
        std::env::temp_dir().join(format!("omnia-cursor-live-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&workspace)?;
    Ok(workspace)
}

// Connect and drop the client alone, which spawns one worker, completes its
// handshake, and closes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn live_worker_handshake() -> Result<()> {
    let client = connect().await?;
    drop(client);
    Ok(())
}

fn verdict_request() -> Request {
    Request {
        model: None,
        system: Some(
            "You are a terse judge. Decide whether the candidate passes and reply with the \
             required JSON object."
                .to_owned(),
        ),
        messages: vec![Message {
            role: Role::User,
            content: "Judge the trivial candidate and return a verdict of \"pass\" with a \
                      one-line reason.\n\nThe candidate is a no-op; it should pass."
                .to_owned(),
        }],
        generation: None,
        format: Format::Schema(Schema {
            name: "verdict".to_owned(),
            schema: json!({
                "type": "object",
                "properties": {
                    "verdict": { "type": "string", "enum": ["pass", "fail"] },
                    "reason": { "type": "string" },
                },
                "required": ["verdict", "reason"],
                "additionalProperties": false,
            })
            .to_string(),
        }),
        tools: vec![],
        grants: Grants { workspace: None },
        check: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn live_cursor_completes() -> Result<()> {
    let client = connect().await?;
    let answer: Answer = client
        .complete(verdict_request(), local_path_tool_host(temp_workspace("ws")?))
        .await
        .map_err(|e| {
            anyhow::anyhow!("live cursor completion failed (is cursor-sdk-bridge installed?): {e}")
        })?;

    let value = object(&answer);
    assert!(
        value.get("verdict").and_then(Value::as_str).is_some(),
        "run-3 answer must carry a string verdict: {value}"
    );

    Ok(())
}

/// `agents` completions pending together on `client`: every answer arrives
/// and carries a verdict, then every process spawned so far is gone.
async fn fanout(client: &Client, agents: usize, pids: &SpawnedPids) -> Result<()> {
    let pending: Vec<_> = (0..agents)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.complete(verdict_request(), no_tool_host()).await })
        })
        .collect();

    for (index, pending) in pending.into_iter().enumerate() {
        let answer = pending
            .await?
            .map_err(|e| anyhow::anyhow!("live cursor fan-out completion {index} failed: {e}"))?;
        let value = object(&answer);
        assert!(
            value.get("verdict").and_then(Value::as_str).is_some(),
            "completion {index} must carry a string verdict: {value}"
        );
    }
    pids.await_gone().await
}

/// Four completions pending together, the way `emery_sdk::extract` puts its
/// seams up: one worker per agent on the pooled client, none held
/// two, every answer arrives, and every process is gone once they have.
/// `connect()` leaves `max_agents` at four, so nothing here queues.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn live_fanout() -> Result<()> {
    let pids = SpawnedPids::install();
    let client = connect().await?;
    fanout(&client, 4, &pids).await
}

/// `live_fanout` twenty times over on one client: a worker that exits
/// under the fan-out fails its completion with `cursor-sdk-bridge exited`,
/// and a lease that does not release leaves its process up, and a slot
/// closed, for the next round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; slow (20 fan-outs); run with --run-ignored"]
async fn stress_fanout() -> Result<()> {
    let pids = SpawnedPids::install();
    let client = connect().await?;
    for round in 0..20 {
        fanout(&client, 4, &pids).await.with_context(|| format!("fan-out round {round}"))?;
    }
    Ok(())
}

/// The pids of every `cursor-sdk-bridge spawned` event, in order.
#[derive(Clone, Default)]
struct SpawnedPids(Arc<Mutex<Vec<u32>>>);

impl SpawnedPids {
    /// Capture from the process's subscriber; one row per process.
    fn install() -> Self {
        let pids = Self::default();
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(pids.clone()))
            .expect("this test owns the process's subscriber");
        pids
    }

    fn pids(&self) -> Vec<u32> {
        self.0.lock().expect("pids lock").clone()
    }

    /// Wait for every process spawned so far to be gone, and with it every
    /// agent process it forked — the pool whole again, since a slot reopens
    /// only once its process is.
    async fn await_gone(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + GONE;
        loop {
            let up: Vec<u32> = self.pids().into_iter().filter(|pid| group_alive(*pid)).collect();
            if up.is_empty() {
                return Ok(());
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "worker process groups {up:?} still up after {GONE:?}: a lease did not release, \
                 or a worker left an agent process behind"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Whether anything is left of the process group a worker led (`kill -0`
/// on the group): the worker itself, or an agent process it forked.
fn group_alive(pgid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", "--", &format!("-{pgid}")])
        .status()
        .is_ok_and(|status| status.success())
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpawnedPids {
    fn on_event(
        &self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut spawn = Spawn::default();
        event.record(&mut spawn);
        if spawn.spawned
            && let Some(pid) = spawn.pid
        {
            self.0.lock().expect("pids lock").push(pid);
        }
    }
}

#[derive(Default)]
struct Spawn {
    pid: Option<u32>,
    spawned: bool,
}

impl tracing::field::Visit for Spawn {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "pid" {
            self.pid = u32::try_from(value).ok();
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" && format!("{value:?}").contains("cursor-sdk-bridge spawned") {
            self.spawned = true;
        }
    }
}

/// A real worker `kill -9`ed under its opening run: the completion restarts
/// on a fresh process and still answers. The completion's own process is the
/// first spawn, the restart the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn worker_killed_mid_run_recovers() -> Result<()> {
    let pids = SpawnedPids::install();

    // One slot, so the restart also proves the dead lease is reaped first.
    let client = Client::connect_with(ConnectOptions {
        timeout_secs: 120,
        inactivity_secs: 120,
        model: "auto".to_owned(),
        max_agents: 1,
    })
    .await?;
    let completion = {
        let client = client.clone();
        tokio::spawn(async move { client.complete(verdict_request(), no_tool_host()).await })
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while pids.pids().is_empty() {
        anyhow::ensure!(tokio::time::Instant::now() < deadline, "no worker spawned");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let victim = pids.pids()[0];
    // A moment for `CreateAgent` and `Send` to go out, well short of a
    // real answer.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let killed = std::process::Command::new("kill").args(["-9", &victim.to_string()]).status()?;
    anyhow::ensure!(killed.success(), "kill -9 {victim} failed: {killed}");

    let answer = completion
        .await?
        .map_err(|e| anyhow::anyhow!("the completion did not recover from the kill: {e:#}"))?;
    let value = object(&answer);
    assert!(
        value.get("verdict").and_then(Value::as_str).is_some(),
        "the recovered answer must carry a string verdict: {value}"
    );
    let spawned = pids.pids();
    anyhow::ensure!(
        spawned.len() == 2,
        "expected the killed process and the restart; saw {spawned:?} (did the kill land after \
         the answer?)"
    );
    pids.await_gone().await
}

/// A request whose only path to the answer is the `lookup` function tool the
/// stub session answers with [`TOOL_SENTINEL`].
fn tool_request() -> Request {
    Request {
        model: None,
        system: Some("Answer only from tools. Do not guess or fabricate values.".to_owned()),
        messages: vec![Message {
            role: Role::User,
            content: "Call the `lookup` tool to obtain the project secret token, then return \
                      it unchanged."
                .to_owned(),
        }],
        generation: None,
        format: Format::Schema(Schema {
            name: "secret".to_owned(),
            schema: json!({
                "type": "object",
                "properties": { "secret": { "type": "string" } },
                "required": ["secret"],
                "additionalProperties": false,
            })
            .to_string(),
        }),
        tools: vec![Tool::Function(Function {
            name: "lookup".to_owned(),
            description: "Return the project secret token.".to_owned(),
            parameters: json!({ "type": "object", "properties": {} }).to_string(),
        })],
        grants: Grants { workspace: None },
        check: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn live_cursor_function_tool() -> Result<()> {
    let client = connect().await?;
    let answer: Answer = client
        .complete(tool_request(), local_path_tool_host(temp_workspace("tool")?))
        .await
        .map_err(|e| anyhow::anyhow!("live cursor function-tool completion failed: {e}"))?;

    let value = object(&answer);
    assert!(
        value.to_string().contains(TOOL_SENTINEL),
        "the agent must return the session-provided secret; got: {value}"
    );
    Ok(())
}

/// The references-only shape: no lent workspace, built-in tools disabled, the
/// function tool as the only capability.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn no_workspace() -> Result<()> {
    let client = connect().await?;
    let answer: Answer = client
        .complete(tool_request(), no_tool_host())
        .await
        .map_err(|e| anyhow::anyhow!("live cursor no-workspace completion failed: {e}"))?;

    let value = object(&answer);
    assert!(
        value.to_string().contains(TOOL_SENTINEL),
        "the agent must return the session-provided secret; got: {value}"
    );
    Ok(())
}

fn secret_request(url: String) -> Request {
    Request {
        model: None,
        system: Some("Answer only from tools. Do not guess or fabricate values.".to_owned()),
        messages: vec![Message {
            role: Role::User,
            content: "Call the `read_secret` tool on the `omnia` MCP server to obtain the \
                      project secret token, then return it unchanged."
                .to_owned(),
        }],
        generation: None,
        format: Format::Schema(Schema {
            name: "secret".to_owned(),
            schema: json!({
                "type": "object",
                "properties": { "secret": { "type": "string" } },
                "required": ["secret"],
                "additionalProperties": false,
            })
            .to_string(),
        }),
        // Grant the `omnia` MCP server with its endpoint URL; the backend
        // passes it inline through `CreateAgent`'s `mcp_servers`.
        tools: vec![Tool::Mcp(Mcp {
            name: "omnia".to_owned(),
            tools: vec![],
            url,
        })],
        grants: Grants { workspace: None },
        check: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn uses_mcp() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(serve(listener));

    let client = connect().await?;
    let answer: Answer = client
        .complete(secret_request(format!("http://127.0.0.1:{port}/mcp")), no_tool_host())
        .await
        .map_err(|e| anyhow::anyhow!("live cursor MCP completion failed: {e}"))?;

    let value = object(&answer);
    assert!(
        value.to_string().contains(SENTINEL),
        "the agent must return the MCP-provided secret; got: {value}"
    );

    Ok(())
}

/// A prompt whose first answer cannot contain the check's word — the agent
/// only learns it from the correction sent on its session.
fn check_request() -> Request {
    Request {
        model: None,
        system: Some(
            "Reply with a JSON object {\"word\": <a single English word>}. Follow any \
             correction you receive exactly."
                .to_owned(),
        ),
        messages: vec![Message {
            role: Role::User,
            content: "Name a colour.".to_owned(),
        }],
        generation: None,
        format: Format::Json,
        tools: vec![],
        grants: Grants { workspace: None },
        check: true,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn check_corrects() -> Result<()> {
    let client = connect().await?;
    let (tool_host, candidates) = checking_tool_host(1);
    let answer: Answer = client
        .complete(check_request(), tool_host)
        .await
        .map_err(|e| anyhow::anyhow!("live cursor check completion failed: {e}"))?;

    let candidates = candidates.lock().expect("candidates lock").clone();
    assert_eq!(candidates.len(), 2, "one rejection, one acceptance: {candidates:?}");
    assert_eq!(answer.answer, candidates[1], "the accepted candidate is the answer");
    let value = object(&answer);
    assert_eq!(
        value.get("word").and_then(Value::as_str),
        Some(CHECK_WORD),
        "the correction reached the agent's session: {value}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn check_exhausts() -> Result<()> {
    let client = connect().await?;
    let (tool_host, candidates) = checking_tool_host(usize::MAX);
    let error = client
        .complete(check_request(), tool_host)
        .await
        .expect_err("every candidate is rejected, so the round budget ends the completion");

    let correction = match error.downcast_ref::<Error>() {
        Some(Error::BudgetExhausted(correction)) => correction,
        other => panic!("expected the typed budget-exhausted carrying the correction: {other:?}"),
    };
    let candidates = candidates.lock().expect("candidates lock").clone();
    assert_eq!(candidates.len(), 2, "the opening prompt and one correction: {candidates:?}");
    assert!(
        correction.contains(&candidates[1]),
        "the detail names the last rejected candidate: {correction}"
    );
    Ok(())
}
