//! Key-gated live integration test for the genai backend — wasi-model
//! "run 2" (the `resolve` acceptance gate).
//!
//! This is the cross-repo companion to omnia's deterministic
//! `resolve_dispatches` test: that one proves the host→guest dispatch machinery
//! with no network; this one proves the genai backend itself — `Request`→`ChatRequest`
//! mapping, the in-process tool loop, `resolve` dispatch through the lent
//! [`ToolHost`], and answer validation — against a real provider.
//!
//! `#[ignore]`d so it never runs or touches the network in CI; run it with
//! `cargo nextest run -p omnia-genai --run-ignored all` alongside a provider key
//! such as `OPENAI_API_KEY`.

use std::sync::Arc;

use anyhow::Result;
use futures::FutureExt as _;
use omnia::Backend as _;
use omnia_genai::Client;
use omnia_wasi_model::{
    Answer, DirEntry, Format, FutureResult, Grants, Message, Reference, Request, Role, ToolHost,
    WasiModelCtx,
};
use serde_json::Value;

/// Deterministic stand-in for the caller's `references` shelf: `resolve(name)`
/// returns `shelf:{name}` bytes, mirroring the omnia `examples/cli-model` shelf
/// guest. The real host→guest dispatch is exercised in omnia; here we only need
/// the genai backend to drive a `resolve` tool call and consume the result.
#[derive(Debug)]
struct LiveShelf;

impl ToolHost for LiveShelf {
    fn resolve(&self, reference: Reference) -> FutureResult<Vec<u8>> {
        async move { Ok(format!("shelf:{}", reference.name).into_bytes()) }.boxed()
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

/// A prompt that forces a `resolve` tool call (a reference target is granted, so
/// the host-injected `resolve` tool is advertised) and a JSON-object answer embedding
/// the resolved value.
fn resolve_request() -> Request {
    Request {
        model: None,
        system: Some(
            "Call the `resolve` tool with name \"alpha\" to fetch a value, then reply with a JSON \
             object {\"resolved\": <the exact string the tool returned>}. Use the tool result \
             verbatim; do not invent it."
                .to_owned(),
        ),
        messages: vec![Message {
            role: Role::User,
            content: "Resolve the reference named \"alpha\" and report what it resolved to."
                .to_owned(),
        }],
        generation: None,
        format: Format::Json,
        tools: vec![],
        grants: Grants {
            references: Some("shelf".to_owned()),
            workspace: None,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a provider key (e.g. OPENAI_API_KEY); run with --run-ignored"]
async fn live_genai_resolves() -> Result<()> {
    let client = Client::connect().await?;
    let answer: Answer =
        client.complete(resolve_request(), Arc::new(LiveShelf)).await.map_err(|e| {
            anyhow::anyhow!("live genai completion failed (is the API key valid?): {e}")
        })?;

    let transcript = answer.transcript.as_ref().expect("genai always records a transcript");
    let resolve_turn = transcript
        .turns
        .iter()
        .find(|turn| turn.tool == "resolve")
        .expect("the model must call the host-injected `resolve` tool");
    assert_eq!(
        resolve_turn.result,
        Value::String("shelf:alpha".to_owned()),
        "the resolve tool result must round-trip the shelf bytes"
    );

    assert!(answer.value.is_object(), "run-2 answer must be a JSON object: {:?}", answer.value);
    assert!(
        answer.value.to_string().contains("shelf:alpha"),
        "the resolved value must appear in the answer: {:?}",
        answer.value
    );

    Ok(())
}
