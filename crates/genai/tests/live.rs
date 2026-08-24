//! Key-gated live integration tests for the genai backend — the function-tool
//! session loop and the host-injected workspace tools against a real provider.
//!
//! The cross-repo companions to omnia's deterministic ABI scenarios
//! (`crates/abi-tests/tests/model.rs`): those prove the session
//! machinery and the real workspace `ToolHost` with no network; these prove
//! the genai backend itself — `Request`→`ChatRequest` mapping with declared
//! and host-injected tools, the in-process tool loop forwarding through
//! [`ToolHost`], and answer validation — against a real provider.
//!
//! `#[ignore]`d so they never run or touch the network in CI; run them with
//! `cargo nextest run -p omnia-genai --run-ignored all` alongside a provider
//! key such as `OPENAI_API_KEY`.

use std::path::Path;
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
/// by omnia's ABI tests; here we only need the genai backend to drive a
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

/// Sentinel only reachable through the workspace tools: `list` reveals the
/// one file, `read` returns the text carrying it.
const SENTINEL: &str = "omega-7734";

/// Deterministic in-memory workspace stand-in for the host's `BoundToolHost`:
/// `local_path` reports a resolved lend (which makes the backend advertise
/// `read`/`list`), listing the root reveals `refs.md`, and reading it returns
/// the sentinel. The real cap-std workspace is proved by omnia's ABI tests;
/// here we only need the genai backend to discover, read, and use the file.
#[derive(Debug)]
struct LiveWorkspace;

impl ToolHost for LiveWorkspace {
    fn call_tool(&self, name: String, _arguments: String) -> FutureResult<Result<String, String>> {
        async move { Err(anyhow::anyhow!("no function tools are declared, model called `{name}`")) }
            .boxed()
    }

    fn read(&self, path: String) -> FutureResult<Vec<u8>> {
        async move {
            anyhow::ensure!(path == "refs.md", "opening `{path}` in workspace");
            Ok(format!("The reference value is {SENTINEL}.").into_bytes())
        }
        .boxed()
    }

    fn list(&self, path: String) -> FutureResult<Vec<DirEntry>> {
        async move {
            anyhow::ensure!(path.is_empty() || path == ".", "listing `{path}` in workspace");
            Ok(vec![DirEntry {
                name: "refs.md".to_owned(),
                is_directory: false,
            }])
        }
        .boxed()
    }

    fn write(&self, path: String, _bytes: Vec<u8>) -> FutureResult<()> {
        async move { Err(anyhow::anyhow!("write to `{path}` is not granted")) }.boxed()
    }

    fn local_path(&self) -> Option<&Path> {
        Some(Path::new("/unused/live-workspace"))
    }
}

/// A prompt that forces workspace discovery: the file name is never stated,
/// so the model must `list` the root, `read` what it finds, and answer with
/// the value inside.
fn workspace_request() -> Request {
    Request {
        model: None,
        system: Some(
            "A workspace is granted for this task. Discover its single file by listing the \
             workspace root, read that file, then reply with a JSON object {\"reference\": <the \
             exact value the file states>}. Use the file's content verbatim; do not invent it."
                .to_owned(),
        ),
        messages: vec![Message {
            role: Role::User,
            content: "Report the reference value recorded in the workspace.".to_owned(),
        }],
        generation: None,
        format: Format::Json,
        tools: vec![],
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a provider key (e.g. OPENAI_API_KEY); run with --run-ignored"]
async fn live_genai_workspace_tools() -> Result<()> {
    let client = Client::connect().await?;
    let answer: Answer =
        client.complete(workspace_request(), Arc::new(LiveWorkspace)).await.map_err(|e| {
            anyhow::anyhow!("live genai completion failed (is the API key valid?): {e}")
        })?;

    let transcript = answer.transcript.as_ref().expect("genai always records a transcript");
    assert!(
        transcript.turns.iter().any(|turn| turn.tool == "list"),
        "the model must discover the file by listing the workspace root: {transcript:?}"
    );
    let read_turn = transcript
        .turns
        .iter()
        .find(|turn| turn.tool == "read")
        .expect("the model must read the discovered file");
    assert!(
        read_turn.result.as_str().is_some_and(|text| text.contains(SENTINEL)),
        "the read result must carry the file body: {:?}",
        read_turn.result
    );

    assert!(answer.value.is_object(), "the answer must be a JSON object: {:?}", answer.value);
    assert!(
        answer.value.to_string().contains(SENTINEL),
        "the file's value must appear in the answer: {:?}",
        answer.value
    );

    Ok(())
}
