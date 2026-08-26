//! Key/PATH-gated live integration tests for the cursor backend — wasi-model
//! "run 3" (the bridge-managed agent acceptance gate).
//!
//! Mirrors the genai backend's `live.rs`: each test spawns a real
//! `cursor-sdk-bridge`, drives a completion through the
//! `omnia:model/completion` boundary, and parses the validated answer back.
//!
//! All tests are `#[ignore]`d so they never run or spawn a process in CI; run
//! them with `cargo nextest run --run-ignored all` (or `cargo test --
//! --ignored`) alongside an installed `cursor-sdk-bridge` and a `CURSOR_API_KEY`.

mod support;

use anyhow::Result;
use omnia::Backend as _;
use omnia_cursor::{Client, ConnectOptions};
use omnia_wasi_model::{
    Answer, Format, Function, Grants, Mcp, Message, Request, Role, Schema, Tool, WasiModelCtx,
};
use serde_json::json;
use support::{SENTINEL, TOOL_SENTINEL, local_path_tool_host, no_workspace_tool_host, serve};
use tokio::net::TcpListener;

async fn connect() -> Result<Client> {
    Client::connect_with(ConnectOptions {
        timeout_secs: 120,
        inactivity_secs: 120,
        model: None,
    })
    .await
}

fn temp_workspace(label: &str) -> Result<std::path::PathBuf> {
    let workspace =
        std::env::temp_dir().join(format!("omnia-cursor-live-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&workspace)?;
    Ok(workspace)
}

/// Spawn-and-handshake only: ready line, token file, `Ping`, `GetVersion`,
/// and a clean shutdown on drop. Runs against any bridge — the API key is
/// not exercised.
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

    assert!(answer.value.is_object(), "run-3 answer must be a JSON object: {:?}", answer.value);
    assert!(
        answer.value.get("verdict").and_then(serde_json::Value::as_str).is_some(),
        "run-3 answer must carry a string verdict: {:?}",
        answer.value
    );

    Ok(())
}

/// A request whose only path to the answer is the `lookup` function tool the
/// stub session answers with [`TOOL_SENTINEL`].
fn function_tool_request() -> Request {
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
    }
}

/// Proves the whole custom-tool chain: `CreateAgent` declares the guest
/// function tool, the agent calls it, the bridge calls our loopback
/// `CallCustomTool`, and the session's `call_tool` answer reaches the answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn live_cursor_function_tool_round_trip() -> Result<()> {
    let client = connect().await?;
    let answer: Answer = client
        .complete(function_tool_request(), local_path_tool_host(temp_workspace("tool")?))
        .await
        .map_err(|e| anyhow::anyhow!("live cursor function-tool completion failed: {e}"))?;

    assert!(
        answer.value.to_string().contains(TOOL_SENTINEL),
        "the agent must return the session-provided secret; got: {:?}",
        answer.value
    );
    Ok(())
}

/// The references-only shape: no lent workspace, built-in tools disabled, the
/// function tool as the only capability.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn live_cursor_no_workspace() -> Result<()> {
    let client = connect().await?;
    let answer: Answer =
        client
            .complete(function_tool_request(), no_workspace_tool_host())
            .await
            .map_err(|e| anyhow::anyhow!("live cursor no-workspace completion failed: {e}"))?;

    assert!(
        answer.value.to_string().contains(TOOL_SENTINEL),
        "the agent must return the session-provided secret; got: {:?}",
        answer.value
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
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs cursor-sdk-bridge and CURSOR_API_KEY; run with --run-ignored"]
async fn live_cursor_uses_mcp() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(serve(listener));

    let client = connect().await?;
    let answer: Answer = client
        .complete(
            secret_request(format!("http://127.0.0.1:{port}/mcp")),
            local_path_tool_host(temp_workspace("mcp")?),
        )
        .await
        .map_err(|e| anyhow::anyhow!("live cursor MCP completion failed: {e}"))?;

    assert!(
        answer.value.to_string().contains(SENTINEL),
        "the agent must return the MCP-provided secret; got: {:?}",
        answer.value
    );

    Ok(())
}
