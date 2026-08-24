//! Loopback `CallCustomTool` server: the bridge calls back here to execute a
//! guest-declared function tool, and the call routes into the completion's
//! session through [`ToolHost::call_tool`].
//!
//! The server binds `127.0.0.1:0`, requires the bearer token handed to the
//! bridge at spawn, and accepts both Connect unary codecs (the bridge picks
//! the content type). Budgets, per-call timeouts, oversize checks, and id
//! correlation are enforced host-side inside `call_tool`; the registry here
//! is a thin `agent_id -> session` map.

mod proto;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{Context as _, Result};
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use axum::routing::post;
use omnia_wasi_model::ToolHost;
use prost::Message as _;
use proto::{CallCustomToolRequest, CallCustomToolResponse, struct_to_value, value_to_struct};
use serde_json::{Value, json};
use tokio::sync::mpsc;

/// The Connect route the bridge is pointed at via `--tool-callback-url`.
const CALL_CUSTOM_TOOL: &str = "/sdk.v1.SdkCustomToolCallbackService/CallCustomTool";

/// Live completions by `agent_id`: the session's tool host plus the abort
/// signal that ends the completion on a hard (non-repairable) tool failure.
#[derive(Debug, Default)]
pub struct Registry {
    entries: Mutex<HashMap<String, Entry>>,
}

#[derive(Clone)]
struct Entry {
    tool_host: Arc<dyn ToolHost>,
    abort: mpsc::UnboundedSender<String>,
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry").finish_non_exhaustive()
    }
}

impl Registry {
    /// Route callbacks for `agent_id` into `tool_host` until the returned
    /// guard drops.
    pub fn register(
        self: &Arc<Self>, agent_id: String, tool_host: Arc<dyn ToolHost>,
        abort: mpsc::UnboundedSender<String>,
    ) -> Registration {
        self.lock().insert(agent_id.clone(), Entry { tool_host, abort });
        Registration {
            registry: Arc::clone(self),
            agent_id,
        }
    }

    fn lookup(&self, agent_id: &str) -> Option<Entry> {
        self.lock().get(agent_id).cloned()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Unregisters its agent on drop.
pub struct Registration {
    registry: Arc<Registry>,
    agent_id: String,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.registry.lock().remove(&self.agent_id);
    }
}

/// The bound loopback server; dropping it stops serving.
pub struct CallbackServer {
    url: String,
    token: String,
    server: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for CallbackServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallbackServer").field("url", &self.url).finish_non_exhaustive()
    }
}

impl Drop for CallbackServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct ServerState {
    registry: Arc<Registry>,
    bearer: String,
}

impl CallbackServer {
    /// Bind `127.0.0.1:0` with a fresh bearer token and start serving.
    ///
    /// # Errors
    ///
    /// Returns an error when the loopback bind fails or the system random
    /// source is unavailable.
    pub async fn spawn(registry: Arc<Registry>) -> Result<Self> {
        let token = random_token()?;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("binding the tool-callback listener")?;
        let addr = listener.local_addr().context("reading the tool-callback address")?;

        let state = Arc::new(ServerState {
            registry,
            bearer: format!("Bearer {token}"),
        });
        let router =
            Router::new().route(CALL_CUSTOM_TOOL, post(call_custom_tool)).with_state(state);
        let server = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, router).await {
                tracing::warn!(%error, "tool-callback server stopped");
            }
        });

        Ok(Self {
            url: format!("http://{addr}{CALL_CUSTOM_TOOL}"),
            token,
            server,
        })
    }

    /// The full callback URL handed to the bridge (`--tool-callback-url`).
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The bearer token handed to the bridge (`--tool-callback-auth-token`).
    pub fn token(&self) -> &str {
        &self.token
    }
}

/// 32 random bytes, hex-encoded, from the OS entropy source.
fn random_token() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| anyhow::anyhow!("gathering entropy: {error}"))?;
    Ok(bytes.iter().fold(String::with_capacity(64), |mut hex, byte| {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
        hex
    }))
}

/// The Connect unary codec of one callback exchange; the response mirrors the
/// request's content type (errors are always JSON, per the Connect spec).
#[derive(Clone, Copy)]
enum Codec {
    Json,
    Proto,
}

async fn call_custom_tool(
    State(state): State<Arc<ServerState>>, headers: HeaderMap, body: Bytes,
) -> Response {
    let authorized = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == state.bearer);
    if !authorized {
        return connect_error(StatusCode::UNAUTHORIZED, "unauthenticated", "bad bearer token");
    }

    let content_type =
        headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok()).unwrap_or_default();
    let codec = if content_type.contains("json") {
        Codec::Json
    } else if content_type.contains("proto") {
        Codec::Proto
    } else {
        return connect_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unknown",
            &format!("unsupported content type `{content_type}`"),
        );
    };

    let call = match decode_call(codec, &body) {
        Ok(call) => call,
        Err(error) => {
            return connect_error(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                &format!("{error:#}"),
            );
        }
    };

    let Some(entry) = state.registry.lookup(&call.agent_id) else {
        return connect_error(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("no live completion for agent `{}`", call.agent_id),
        );
    };

    let arguments = call.args.to_string();
    tracing::debug!(tool = %call.tool_name, agent = %call.agent_id, "custom tool callback");
    match entry.tool_host.call_tool(call.tool_name.clone(), arguments).await {
        // The guest tool answered; non-object output is wrapped because the
        // callback result must be a JSON object on the wire.
        Ok(Ok(output)) => respond(codec, &wrap_output(&output)),
        // The guest tool failed repairably: hand the failure text to the
        // model — the same channel genai feeds back as tool-result text.
        Ok(Err(failure)) => respond(codec, &json!({ "error": failure })),
        // Hard session failure (budget, timeout, unknown tool, closed
        // session): abort the completion; the host's typed error wins in the
        // guest-visible reply.
        Err(error) => {
            let message = format!("tool `{}` failed: {error:#}", call.tool_name);
            let _ = entry.abort.send(message.clone());
            connect_error(StatusCode::CONFLICT, "aborted", &message)
        }
    }
}

struct ToolCall {
    tool_name: String,
    args: Value,
    agent_id: String,
}

fn decode_call(codec: Codec, body: &[u8]) -> Result<ToolCall> {
    match codec {
        Codec::Json => {
            #[derive(Default, serde::Deserialize)]
            #[serde(rename_all = "camelCase", default)]
            struct JsonCall {
                tool_name: String,
                args: Value,
                agent_id: String,
            }
            let call: JsonCall =
                serde_json::from_slice(body).context("decoding the JSON callback body")?;
            Ok(ToolCall {
                tool_name: call.tool_name,
                args: if call.args.is_null() { json!({}) } else { call.args },
                agent_id: call.agent_id,
            })
        }
        Codec::Proto => {
            let call = CallCustomToolRequest::decode(body)
                .context("decoding the protobuf callback body")?;
            Ok(ToolCall {
                tool_name: call.tool_name,
                args: call.args.as_ref().map_or_else(|| json!({}), struct_to_value),
                agent_id: call.agent_id,
            })
        }
    }
}

/// Shape a guest tool's output into the JSON object the callback requires:
/// objects pass through; anything else (including non-JSON text) rides under
/// `"value"` so no content is lost.
fn wrap_output(output: &str) -> Value {
    match serde_json::from_str::<Value>(output) {
        Ok(value @ Value::Object(_)) => value,
        Ok(value) => json!({ "value": value }),
        Err(_) => json!({ "value": output }),
    }
}

fn respond(codec: Codec, result: &Value) -> Response {
    match codec {
        Codec::Json => (
            StatusCode::OK,
            [(CONTENT_TYPE, "application/json")],
            json!({ "result": result }).to_string(),
        )
            .into_response(),
        Codec::Proto => {
            let object = result.as_object().cloned().unwrap_or_default();
            let response = CallCustomToolResponse {
                result: Some(value_to_struct(&object)),
            };
            (StatusCode::OK, [(CONTENT_TYPE, "application/proto")], response.encode_to_vec())
                .into_response()
        }
    }
}

/// A Connect unary error: matching HTTP status, JSON body `{"code","message"}`.
fn connect_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        [(CONTENT_TYPE, "application/json")],
        json!({ "code": code, "message": message }).to_string(),
    )
        .into_response()
}

// Deliberate unit tests: result-wrapping policy plus the callback server
// driven in-process over real HTTP in both codecs and framings — our server,
// not a mocked SDK. `tests/live.rs` proves a real bridge drives it.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use omnia_wasi_model::{DirEntry, FutureResult, ToolHost};
    use prost::Message as _;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    use super::proto::{CallCustomToolRequest, CallCustomToolResponse, value_to_struct};
    use super::{CallbackServer, Registry, wrap_output};

    #[test]
    fn wrap_output_policy() {
        assert_eq!(wrap_output(r#"{"answer":42}"#), json!({ "answer": 42 }));
        assert_eq!(wrap_output("[1,2]"), json!({ "value": [1, 2] }));
        assert_eq!(wrap_output(r#""text""#), json!({ "value": "text" }));
        assert_eq!(wrap_output("not json"), json!({ "value": "not json" }));
    }

    /// Echoes `call_tool` back, or fails per the requested tool name.
    #[derive(Debug)]
    struct SessionStub;

    impl ToolHost for SessionStub {
        fn call_tool(
            &self, name: String, arguments: String,
        ) -> FutureResult<Result<String, String>> {
            Box::pin(async move {
                match name.as_str() {
                    "repairable" => Ok(Err("bad arguments".to_owned())),
                    "hard" => Err(anyhow::anyhow!("tool budget exhausted")),
                    _ => Ok(Ok(json!({ "echo": [name, arguments] }).to_string())),
                }
            })
        }

        fn read(&self, _path: String) -> FutureResult<Vec<u8>> {
            Box::pin(async { Err(anyhow::anyhow!("unused")) })
        }

        fn list(&self, _path: String) -> FutureResult<Vec<DirEntry>> {
            Box::pin(async { Err(anyhow::anyhow!("unused")) })
        }

        fn write(&self, _path: String, _bytes: Vec<u8>) -> FutureResult<()> {
            Box::pin(async { Err(anyhow::anyhow!("unused")) })
        }
    }

    struct Harness {
        server: CallbackServer,
        registration: super::Registration,
        abort_rx: mpsc::UnboundedReceiver<String>,
    }

    async fn serve_one_agent(agent_id: &str) -> Harness {
        let registry = Arc::new(Registry::default());
        let server = CallbackServer::spawn(Arc::clone(&registry)).await.expect("spawn server");
        let (abort_tx, abort_rx) = mpsc::unbounded_channel();
        let registration = registry.register(agent_id.to_owned(), Arc::new(SessionStub), abort_tx);
        Harness {
            server,
            registration,
            abort_rx,
        }
    }

    /// One raw HTTP/1.1 exchange, so the framing is under test control.
    async fn exchange(url: &str, headers: &str, body: &[u8]) -> (u16, String, Vec<u8>) {
        let (host, path) = url
            .strip_prefix("http://")
            .and_then(|rest| rest.split_once('/'))
            .expect("callback url shape");
        let mut socket = TcpStream::connect(host).await.expect("connect");
        let head =
            format!("POST /{path} HTTP/1.1\r\nHost: {host}\r\n{headers}Connection: close\r\n\r\n");
        socket.write_all(head.as_bytes()).await.expect("write head");
        socket.write_all(body).await.expect("write body");

        let mut response = Vec::new();
        socket.read_to_end(&mut response).await.expect("read response");
        let split = response.windows(4).position(|w| w == b"\r\n\r\n").expect("header end");
        let head = String::from_utf8_lossy(&response[..split]).into_owned();
        let status: u16 =
            head.split_whitespace().nth(1).and_then(|s| s.parse().ok()).expect("status");
        let mut payload = response[split + 4..].to_vec();
        if head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
            payload = dechunk(&payload);
        }
        (status, head, payload)
    }

    fn dechunk(mut body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(line_end) = body.windows(2).position(|w| w == b"\r\n") {
            let size = usize::from_str_radix(String::from_utf8_lossy(&body[..line_end]).trim(), 16)
                .unwrap_or(0);
            if size == 0 {
                break;
            }
            out.extend_from_slice(&body[line_end + 2..line_end + 2 + size]);
            body = &body[line_end + 2 + size + 2..];
        }
        out
    }

    fn bearer(server: &CallbackServer) -> String {
        format!("Authorization: Bearer {}\r\n", server.token())
    }

    #[tokio::test]
    async fn json_call_with_content_length() {
        let harness = serve_one_agent("agent-1").await;
        let body =
            json!({ "toolName": "lookup", "args": { "q": "x" }, "agentId": "agent-1" }).to_string();
        let headers = format!(
            "{}Content-Type: application/json\r\nContent-Length: {}\r\n",
            bearer(&harness.server),
            body.len()
        );
        let (status, _, payload) = exchange(harness.server.url(), &headers, body.as_bytes()).await;
        assert_eq!(status, 200);
        let response: Value = serde_json::from_slice(&payload).expect("json response");
        assert_eq!(response["result"]["echo"][0], "lookup");
        assert_eq!(response["result"]["echo"][1], r#"{"q":"x"}"#);
    }

    #[tokio::test]
    async fn json_call_with_chunked_framing() {
        let harness = serve_one_agent("agent-1").await;
        let payload = json!({ "toolName": "lookup", "args": {}, "agentId": "agent-1" }).to_string();
        // Split the body across chunks, as the bridge's callback client may.
        let mut body = Vec::new();
        for chunk in payload.as_bytes().chunks(10) {
            body.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            body.extend_from_slice(chunk);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(b"0\r\n\r\n");

        let headers = format!(
            "{}Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n",
            bearer(&harness.server)
        );
        let (status, _, payload) = exchange(harness.server.url(), &headers, &body).await;
        assert_eq!(status, 200);
        let response: Value = serde_json::from_slice(&payload).expect("json response");
        assert_eq!(response["result"]["echo"][0], "lookup");
    }

    #[tokio::test]
    async fn proto_call_round_trips() {
        let harness = serve_one_agent("agent-1").await;
        let Value::Object(args) = json!({ "q": "x" }) else { unreachable!() };
        let request = CallCustomToolRequest {
            tool_name: "lookup".to_owned(),
            args: Some(value_to_struct(&args)),
            tool_call_id: Some("call-1".to_owned()),
            agent_id: "agent-1".to_owned(),
        };
        let body = request.encode_to_vec();
        let headers = format!(
            "{}Content-Type: application/proto\r\nContent-Length: {}\r\n",
            bearer(&harness.server),
            body.len()
        );
        let (status, head, payload) = exchange(harness.server.url(), &headers, &body).await;
        assert_eq!(status, 200);
        assert!(head.to_ascii_lowercase().contains("content-type: application/proto"), "{head}");
        let response = CallCustomToolResponse::decode(payload.as_slice()).expect("proto response");
        let result = super::proto::struct_to_value(&response.result.expect("result struct"));
        assert_eq!(result["echo"][0], "lookup");
    }

    #[tokio::test]
    async fn repairable_failure_becomes_error_object() {
        let harness = serve_one_agent("agent-1").await;
        let body =
            json!({ "toolName": "repairable", "args": {}, "agentId": "agent-1" }).to_string();
        let headers = format!(
            "{}Content-Type: application/json\r\nContent-Length: {}\r\n",
            bearer(&harness.server),
            body.len()
        );
        let (status, _, payload) = exchange(harness.server.url(), &headers, body.as_bytes()).await;
        assert_eq!(status, 200, "a repairable failure is a successful callback");
        let response: Value = serde_json::from_slice(&payload).expect("json response");
        assert_eq!(response["result"]["error"], "bad arguments");
    }

    #[tokio::test]
    async fn hard_failure_aborts_the_completion() {
        let mut harness = serve_one_agent("agent-1").await;
        let body = json!({ "toolName": "hard", "args": {}, "agentId": "agent-1" }).to_string();
        let headers = format!(
            "{}Content-Type: application/json\r\nContent-Length: {}\r\n",
            bearer(&harness.server),
            body.len()
        );
        let (status, _, payload) = exchange(harness.server.url(), &headers, body.as_bytes()).await;
        assert_eq!(status, 409);
        let response: Value = serde_json::from_slice(&payload).expect("connect error json");
        assert_eq!(response["code"], "aborted");
        let reason = harness.abort_rx.recv().await.expect("the abort signal fired");
        assert!(reason.contains("budget exhausted"), "unexpected reason: {reason}");
    }

    #[tokio::test]
    async fn unknown_agent_is_not_found() {
        let harness = serve_one_agent("agent-1").await;
        let body = json!({ "toolName": "lookup", "args": {}, "agentId": "ghost" }).to_string();
        let headers = format!(
            "{}Content-Type: application/json\r\nContent-Length: {}\r\n",
            bearer(&harness.server),
            body.len()
        );
        let (status, _, payload) = exchange(harness.server.url(), &headers, body.as_bytes()).await;
        assert_eq!(status, 404);
        let response: Value = serde_json::from_slice(&payload).expect("connect error json");
        assert_eq!(response["code"], "not_found");
    }

    #[tokio::test]
    async fn bad_bearer_is_unauthenticated() {
        let harness = serve_one_agent("agent-1").await;
        let body = json!({ "toolName": "lookup", "args": {}, "agentId": "agent-1" }).to_string();
        let headers = format!(
            "Authorization: Bearer wrong\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        );
        let (status, _, payload) = exchange(harness.server.url(), &headers, body.as_bytes()).await;
        assert_eq!(status, 401);
        let response: Value = serde_json::from_slice(&payload).expect("connect error json");
        assert_eq!(response["code"], "unauthenticated");
    }

    #[tokio::test]
    async fn dropped_registration_unregisters() {
        let harness = serve_one_agent("agent-1").await;
        let url = harness.server.url().to_owned();
        let token = bearer(&harness.server);
        drop(harness.registration);

        let body = json!({ "toolName": "lookup", "args": {}, "agentId": "agent-1" }).to_string();
        let headers =
            format!("{token}Content-Type: application/json\r\nContent-Length: {}\r\n", body.len());
        let (status, _, _) = exchange(&url, &headers, body.as_bytes()).await;
        assert_eq!(status, 404, "a finished completion no longer routes callbacks");
    }
}
