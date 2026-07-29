//! Key/PATH-gated live integration test for the cursor backend — wasi-model
//! "run 3" (the spawned-agent acceptance gate).
//!
//! Mirrors the genai backend's `live.rs`: it spawns a real `cursor-agent`
//! against a node-local workspace and parses the validated answer back through the
//! `omnia:model/completion` boundary.
//!
//! Both tests are `#[ignore]`d so they never run or spawn a process in CI; run
//! them with `cargo nextest run --run-ignored` (or `cargo test -- --ignored`)
//! alongside an installed, authenticated `cursor-agent` (`CURSOR_API_KEY` or a
//! prior `cursor-agent login`).

mod support;

use anyhow::Result;
use omnia::Backend as _;
use omnia_cursor::{Client, ConnectOptions};
use omnia_wasi_model::{
    Answer, Format, Grants, Mcp, Message, Request, Role, Schema, Tool, WasiModelCtx,
};
use serde_json::json;
use support::{SENTINEL, local_path_tool_host, serve};
use tokio::net::TcpListener;

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
        grants: Grants {
            references: None,
            workspace: None,
            verify: vec![],
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an installed, authenticated cursor-agent; run with --run-ignored"]
async fn live_cursor_completes() -> Result<()> {
    let workspace =
        std::env::temp_dir().join(format!("omnia-cursor-live-ws-{}", std::process::id()));
    std::fs::create_dir_all(&workspace)?;

    let client = Client::connect_with(ConnectOptions {
        timeout_secs: 120,
        model: None,
    })
    .await?;

    let answer: Answer =
        client.complete(verdict_request(), local_path_tool_host(workspace)).await.map_err(|e| {
            anyhow::anyhow!(
                "live cursor completion failed (is cursor-agent installed and authed?): {e}"
            )
        })?;

    assert!(answer.value.is_object(), "run-3 answer must be a JSON object: {:?}", answer.value);
    assert!(
        answer.value.get("verdict").and_then(serde_json::Value::as_str).is_some(),
        "run-3 answer must carry a string verdict: {:?}",
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
        // Grant the `omnia` MCP server with its endpoint URL; the backend wires
        // the spawned agent directly to it.
        tools: vec![Tool::Mcp(Mcp {
            name: "omnia".to_owned(),
            tools: vec![],
            url,
        })],
        grants: Grants {
            references: None,
            workspace: None,
            verify: vec![],
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an installed, authenticated cursor-agent; run with --run-ignored"]
async fn live_cursor_uses_mcp() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(serve(listener));

    let workspace =
        std::env::temp_dir().join(format!("omnia-cursor-mcp-live-{}", std::process::id()));
    std::fs::create_dir_all(&workspace)?;

    let client = Client::connect_with(ConnectOptions {
        timeout_secs: 120,
        model: None,
    })
    .await?;

    let answer: Answer = client
        .complete(
            secret_request(format!("http://127.0.0.1:{port}/mcp")),
            local_path_tool_host(workspace),
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
