//! Key-gated live integration test for the genai backend — the function-tool
//! session loop against a real provider.
//!
//! The cross-repo companion to omnia's deterministic seam scenarios
//! (`crates/seam-suite/tests/seam/model.rs`): those prove the session
//! machinery with no network; this one proves the genai backend itself —
//! `Request`→`ChatRequest` mapping with a declared function tool, the
//! in-process tool loop forwarding through [`ToolHost::call_tool`], and
//! answer validation — against a real provider.
//!
//! `#[ignore]`d so it never runs or touches the network in CI; run it with
//! `cargo nextest run -p omnia-genai --run-ignored all` alongside a provider
//! key such as `OPENAI_API_KEY`.

use std::sync::Arc;

use anyhow::Result;
use futures::FutureExt as _;
use omnia::Backend as _;
use omnia_genai::Client;
use omnia_wasi_model::{
    Answer, DirEntry, Format, Function, FutureResult, Grants, Message, Request, Role, Tool,
    ToolHost, WasiModelCtx,
};
use serde_json::Value;

/// Deterministic stand-in for the host session: `call_tool("lookup", …)`
/// answers `shelf:{arguments}`. The real guest closure round trip is proved
/// in omnia's seam suite; here we only need the genai backend to drive a
/// declared function tool call and consume its result.
#[derive(Debug)]
struct LiveTools;

impl ToolHost for LiveTools {
    fn call_tool(&self, name: String, arguments: String) -> FutureResult<Result<String, String>> {
        async move {
            anyhow::ensure!(name == "lookup", "only `lookup` is declared, model called `{name}`");
            // The provider sends a JSON arguments object; surface the `name`
            // property when present, the raw document otherwise.
            let key = serde_json::from_str::<Value>(&arguments)
                .ok()
                .and_then(|value| value.get("name").and_then(Value::as_str).map(str::to_owned))
                .unwrap_or(arguments);
            Ok(Ok(format!("shelf:{key}")))
        }
        .boxed()
    }

    fn read(&self, _path: String) -> FutureResult<Vec<u8>> {
        async { Err(anyhow::anyhow!("read is unused in this test")) }.boxed()
    }

    fn list(&self, _path: String) -> FutureResult<Vec<DirEntry>> {
        async { Err(anyhow::anyhow!("list is unused in this test")) }.boxed()
    }

    fn write(&self, _path: String, _bytes: Vec<u8>) -> FutureResult<()> {
        async { Err(anyhow::anyhow!("write is unused in this test")) }.boxed()
    }
}

/// A prompt that forces a `lookup` tool call and a JSON-object answer
/// embedding the tool's result.
fn lookup_request() -> Request {
    Request {
        model: None,
        system: Some(
            "Call the `lookup` tool with name \"alpha\" to fetch a value, then reply with a JSON \
             object {\"resolved\": <the exact string the tool returned>}. Use the tool result \
             verbatim; do not invent it."
                .to_owned(),
        ),
        messages: vec![Message {
            role: Role::User,
            content: "Look up the value named \"alpha\" and report what it returned.".to_owned(),
        }],
        generation: None,
        format: Format::Json,
        tools: vec![Tool::Function(Function {
            name: "lookup".to_owned(),
            description: "Look up a value by name and return its contents.".to_owned(),
            parameters: r#"{
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "The value to look up." }
                },
                "required": ["name"],
                "additionalProperties": false
            }"#
            .to_owned(),
        })],
        grants: Grants { workspace: None },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a provider key (e.g. OPENAI_API_KEY); run with --run-ignored"]
async fn live_genai_function_tool_loop() -> Result<()> {
    let client = Client::connect().await?;
    let answer: Answer =
        client.complete(lookup_request(), Arc::new(LiveTools)).await.map_err(|e| {
            anyhow::anyhow!("live genai completion failed (is the API key valid?): {e}")
        })?;

    let transcript = answer.transcript.as_ref().expect("genai always records a transcript");
    let lookup_turn = transcript
        .turns
        .iter()
        .find(|turn| turn.tool == "lookup")
        .expect("the model must call the declared `lookup` tool");
    assert_eq!(
        lookup_turn.result,
        Value::String("shelf:alpha".to_owned()),
        "the tool result must round-trip the session's answer"
    );

    assert!(answer.value.is_object(), "the answer must be a JSON object: {:?}", answer.value);
    assert!(
        answer.value.to_string().contains("shelf:alpha"),
        "the tool's value must appear in the answer: {:?}",
        answer.value
    );

    Ok(())
}
