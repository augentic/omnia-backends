//! Key/PATH-gated live integration tests for the cursor backend — wasi-model
//! "run 3" (the bridge-managed agent acceptance gate).
//!
//! Mirrors the genai backend's `live.rs`: each test spawns a real
//! `cursor-sdk-bridge`, drives a completion through the
//! `omnia:model/completion` boundary, and parses the answer back.
//!
//! All tests are `#[ignore]`d so they never run or spawn a process in CI; run
//! them with `cargo nextest run --run-ignored all` (or `cargo test --
//! --ignored`) alongside an installed `cursor-sdk-bridge` and a `CURSOR_API_KEY`.
//! `upstream_tripwire` additionally needs a hand-started bridge named by
//! `CURSOR_BRIDGE_URL` and `CURSOR_BRIDGE_TOKEN`.

mod support;

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

/// How long the pool may take to reopen every slot once the answers are in:
/// each lease's process is shut down and waited for first.
const REOPEN: Duration = Duration::from_secs(15);

/// Wait for every slot to reopen — every spawned process gone.
async fn await_idle(client: &Client, slots: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + REOPEN;
    while client.idle_slots() != slots {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "{} of {slots} slots idle after {REOPEN:?}: a lease did not release",
            client.idle_slots()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

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
        bridge_bin: "cursor-sdk-bridge".to_owned(),
        bridge_url: None,
        bridge_token: None,
    })
    .await
}

fn temp_workspace(label: &str) -> Result<std::path::PathBuf> {
    let workspace =
        std::env::temp_dir().join(format!("omnia-cursor-live-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&workspace)?;
    Ok(workspace)
}

// Spawn-and-handshake only
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn live_bridge_handshake() -> Result<()> {
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
/// and carries a verdict, then every slot reopens.
async fn fanout(client: &Client, agents: usize) -> Result<()> {
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
    await_idle(client, agents).await
}

/// Four completions pending together, the way `emery_sdk::extract` puts its
/// seams up: one bridge process per agent on the pooled client, none held
/// two, every answer arrives, and every slot is back once they have.
/// `connect()` leaves `max_agents` at four, so nothing here queues.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn live_fanout() -> Result<()> {
    let client = connect().await?;
    fanout(&client, 4).await
}

/// `live_fanout` twenty times over on one client: a bridge that exits
/// under the fan-out fails its completion with `cursor-sdk-bridge exited`,
/// and a lease that does not release leaves a slot closed for the next
/// round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; slow (20 fan-outs); run with --run-ignored"]
async fn stress_fanout() -> Result<()> {
    let client = connect().await?;
    for round in 0..20 {
        fanout(&client, 4).await.with_context(|| format!("fan-out round {round}"))?;
    }
    Ok(())
}

/// Two completions at once on one *attached* bridge — two agents on one
/// process, which the pool never does. Red today by design: the upstream
/// bridge dies of a double-close (`EXC_GUARD`, wait status 9) with two live
/// agents, and since an attached bridge is never seen to die, both
/// completions fail as transport errors while the crash report is the
/// evidence. Green means a bridge release fixed it, and an
/// `agents_per_bridge` knob is worth adding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live tripwire: needs a running cursor-sdk-bridge named by CURSOR_BRIDGE_URL and \
            CURSOR_BRIDGE_TOKEN, plus CURSOR_API_KEY; run with --run-ignored"]
async fn upstream_tripwire() -> Result<()> {
    let bridge_url = std::env::var("CURSOR_BRIDGE_URL").context("CURSOR_BRIDGE_URL is unset")?;
    let bridge_token =
        std::env::var("CURSOR_BRIDGE_TOKEN").context("CURSOR_BRIDGE_TOKEN is unset")?;
    let client = Client::connect_with(ConnectOptions {
        timeout_secs: 120,
        inactivity_secs: 60,
        model: "auto".to_owned(),
        max_agents: 2,
        bridge_bin: "cursor-sdk-bridge".to_owned(),
        bridge_url: Some(bridge_url),
        bridge_token: Some(bridge_token),
    })
    .await?;
    fanout(&client, 2).await
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
