//! The protocol `cursor-sdk-bridge` speaks — `sdk.v1` over Connect on a
//! loopback port: the messages this backend exchanges with it, and a client
//! with one typed method per procedure.
//!
//! The messages are the subset of `sdk.v1` this backend uses, in the proto3
//! JSON mapping (camelCase field names, enums by name, `int64` tolerated as
//! string or number). `sdk.v1` evolves additively, so every deserialized
//! shape ignores unknown fields and defaults missing ones — which means a
//! mispaired path and response type would fail silently; the pairing lives
//! only here.
//!
//! Every RPC is `POST {base}/sdk.v1.{Service}/{Method}` in the Connect JSON
//! codec with bearer auth over HTTP/1.1. Unary calls are plain JSON bodies;
//! server streams use the Connect envelope — a 1-byte flag plus a 4-byte
//! big-endian length per message, with flag `0x02` marking the JSON
//! `EndStreamResponse`.
//!
//! A failed call is an [`RpcError`] in one of two classes a caller can tell
//! apart: [`RpcError::Connect`] — the process answered, with a status and
//! code on a unary call or an error frame closing a run stream — and
//! [`RpcError::Transport`] — the socket, the HTTP layer, or the framing gave
//! out below Connect, so the process is not known to have seen the call at
//! all.

use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, ensure};
use bytes::{Bytes, BytesMut};
use http::uri::Scheme;
use http::{StatusCode, Uri};
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::de::{DeserializeOwned, IgnoredAny};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

const END_STREAM: u8 = 0x02;
const COMPRESSED: u8 = 0x01;

// A cloneable `sdk.v1` client bound to one process's loopback endpoint and
// bearer token.
#[derive(Clone)]
pub struct Rpc {
    hyper: HyperClient<HttpConnector, Full<Bytes>>,
    base: String,
    bearer: String,
}

impl Rpc {
    // Bind to `base` and prove the process answers `sdk.v1` (`Ping`, then
    // `GetVersion`). Unbounded: the caller holds the handshake's bound.
    pub async fn connect(base: &str, token: &str) -> Result<Self> {
        ensure_loopback(base)?;
        let rpc = Self {
            hyper: HyperClient::builder(TokioExecutor::new()).build_http(),
            base: base.to_owned(),
            bearer: format!("Bearer {token}"),
        };
        rpc.ping().await?;
        let version = rpc.get_version().await?;
        ensure!(version.protocol_version == "sdk.v1", "unsupported protocol version");
        tracing::debug!(?version.capabilities, "ready");
        Ok(rpc)
    }

    pub async fn ping(&self) -> Result<()> {
        self.unary_empty("SdkBridgeControlService/Ping", &Empty {}).await
    }

    pub async fn get_version(&self) -> Result<GetVersionResponse> {
        self.unary("SdkBridgeControlService/GetVersion", &Empty {}).await
    }

    // Ask the process to exit, giving its agents `grace` to finish (whole
    // seconds, saturating).
    pub async fn shutdown(&self, grace: Duration) -> Result<()> {
        let request = ShutdownRequest {
            grace_seconds: u32::try_from(grace.as_secs()).unwrap_or(u32::MAX),
        };
        self.unary_empty("SdkBridgeControlService/Shutdown", &request).await
    }

    pub async fn create_agent(&self, options: AgentOptions) -> Result<CreateAgentResponse> {
        self.unary("SdkAgentService/CreateAgent", &CreateAgentRequest { options }).await
    }

    // Best-effort cancel of an abandoned run.
    pub async fn cancel_run(&self, run_id: String, agent_id: String) -> Result<()> {
        let request = CancelRunRequest {
            run_id,
            agent_id: Some(agent_id),
        };
        self.unary_empty("SdkAgentService/CancelRun", &request).await
    }

    // Release the live handle; durable rows stay until delete.
    pub async fn close_agent(&self, agent_id: String) -> Result<()> {
        gone_ok(
            self.unary_empty("SdkAgentService/CloseAgent", &CloseAgentRequest { agent_id }).await,
        )
    }

    // Discard durable session state. Local lookup is cwd-scoped, so the
    // options must name the create-time workspace.
    pub async fn delete_agent(
        &self, agent_id: String, options: AgentOperationOptions,
    ) -> Result<()> {
        let request = DeleteAgentRequest { agent_id, options };
        gone_ok(self.unary_empty("SdkAgentService/DeleteAgent", &request).await)
    }

    // One agent turn; the stream yields the run's messages.
    pub async fn send(&self, agent_id: String, text: String) -> Result<RunStream> {
        let request = SendRequest {
            agent_id,
            message: UserMessage { text },
        };
        Ok(RunStream(self.server_stream("SdkAgentService/Send", &request).await?))
    }

    async fn unary_empty<Req: Serialize + Sync>(&self, method: &str, request: &Req) -> Result<()> {
        self.unary::<_, Empty>(method, request).await.map(drop)
    }

    async fn unary<Req: Serialize + Sync, Resp: DeserializeOwned>(
        &self, method: &str, request: &Req,
    ) -> Result<Resp> {
        let body = serde_json::to_vec(request).with_context(|| format!("encoding `{method}`"))?;
        let response = self.call(method, "application/json", body).await?;
        let bytes = read_body(method, "reading the response", response.into_body()).await?;
        serde_json::from_slice(&bytes).with_context(|| format!("decoding `{method}` response"))
    }

    async fn server_stream<Req: Serialize + Sync>(
        &self, method: &str, request: &Req,
    ) -> Result<FrameStream> {
        let body = serde_json::to_vec(request)
            .map_err(anyhow::Error::from)
            .and_then(|payload| envelope(&payload))
            .with_context(|| format!("encoding `{method}`"))?;
        let response = self.call(method, "application/connect+json", body).await?;
        Ok(FrameStream {
            method: method.to_owned(),
            body: response.into_body(),
            buffer: BytesMut::new(),
        })
    }

    async fn call(
        &self, method: &str, content_type: &str, body: Vec<u8>,
    ) -> Result<http::Response<Incoming>> {
        let response = self
            .hyper
            .request(self.post(method, content_type, body)?)
            .await
            .map_err(|error| RpcError::io(method, "sending the request", error))?;

        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let bytes = read_body(method, "reading the error response", response.into_body()).await?;
        Err(RpcError::unary(method, status, &bytes).into())
    }

    fn post(
        &self, method: &str, content_type: &str, body: Vec<u8>,
    ) -> Result<http::Request<Full<Bytes>>> {
        http::Request::post(format!("{}/sdk.v1.{method}", self.base))
            .header(CONTENT_TYPE, content_type)
            .header(AUTHORIZATION, &self.bearer)
            .header("connect-protocol-version", "1")
            .body(Full::new(Bytes::from(body)))
            .with_context(|| format!("building `{method}` request"))
    }
}

impl fmt::Debug for Rpc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rpc").field("base", &self.base).finish_non_exhaustive()
    }
}

// The typed message stream of one `Send` call: envelope framing, end-stream
// errors, keepalives, and unparsable frames are all absorbed here, so a
// yielded message is always real progress.
pub struct RunStream(FrameStream);

impl RunStream {
    // The next run message, or `None` once the run stream ends; a stream
    // that ends with a Connect error is that error.
    pub async fn next(&mut self) -> Result<Option<RunStreamMessage>> {
        while let Some(frame) = self.0.next().await? {
            if frame.is_end_stream() {
                return RpcError::end_stream(&self.0.method, &frame.payload)
                    .map_or(Ok(None), |error| Err(error.into()));
            }
            let message: RunStreamMessage = match serde_json::from_slice(&frame.payload) {
                Ok(message) => message,
                Err(error) => {
                    tracing::debug!(%error, "skipping unparsable stream frame");
                    continue;
                }
            };
            if message.is_keepalive() {
                continue;
            }
            return Ok(Some(message));
        }
        Ok(None)
    }
}

/// One `sdk.v1` RPC failed: the process answered with an error, or the call
/// never got an answer at all.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// The process answered: a non-success status on a unary call, or an
    /// `EndStreamResponse` carrying an error on a stream.
    #[error(fmt = connect_fmt)]
    Connect {
        /// The `Service/Method` the call named.
        method: String,
        /// The HTTP status of a unary failure; a stream's error frame has none.
        status: Option<StatusCode>,
        /// Connect's error code; `unknown` when the body carried none.
        code: String,
        /// Connect's error message, else the raw body.
        message: String,
    },
    /// Below Connect: the request could not be sent, or the response or run
    /// stream could not be read (a socket failure, or a body that ended
    /// inside a frame). The process is not known to have seen the call.
    #[error("sdk.v1 RPC `{method}` transport failed {doing}")]
    Transport {
        /// The `Service/Method` the call named.
        method: String,
        /// What the client was doing when the transport gave out.
        doing: &'static str,
        /// The HTTP client's or the socket's own error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl RpcError {
    // A body that is not Connect's error object (a crash's, a proxy's) is the message itself.
    fn unary(method: &str, status: StatusCode, body: &[u8]) -> Self {
        let answer: ConnectStatus = serde_json::from_slice(body).unwrap_or_default();
        let answer = if answer.message.is_empty() {
            ConnectStatus {
                message: String::from_utf8_lossy(body).trim().to_owned(),
                ..answer
            }
        } else {
            answer
        };
        Self::answered(method, Some(status), answer)
    }

    // An unparsable end frame is a clean end, not an error.
    fn end_stream(method: &str, payload: &[u8]) -> Option<Self> {
        let end: EndStreamResponse = serde_json::from_slice(payload).unwrap_or_default();
        Some(Self::answered(method, None, end.error?))
    }

    // Details are the process's own diagnostics: logged, never carried.
    fn answered(method: &str, status: Option<StatusCode>, answer: ConnectStatus) -> Self {
        let ConnectStatus {
            code,
            message,
            details,
        } = answer;
        if let Some(details) = details {
            tracing::debug!(method, %details, "sdk.v1 error details");
        }
        Self::Connect {
            method: method.to_owned(),
            status,
            code,
            message,
        }
    }

    pub(crate) fn io(
        method: &str, doing: &'static str, source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Transport {
            method: method.to_owned(),
            doing,
            source: Box::new(source),
        }
    }

    // The body ended with a partial envelope still buffered: an unexpected
    // EOF while reading the stream.
    #[must_use]
    pub(crate) fn truncated(method: &str, buffered: usize) -> Self {
        let eof = std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("stream ended mid-frame ({buffered} bytes buffered)"),
        );
        Self::io(method, "reading the stream", eof)
    }

    // Cursor still mis-tags some of these as 500 `internal` "Agent … not found".
    fn is_agent_gone(&self) -> bool {
        let Self::Connect { code, message, .. } = self else {
            return false;
        };
        if code == "not_found" {
            return true;
        }
        let message = message.to_ascii_lowercase();
        message.contains("agent") && message.contains("not found")
    }
}

// `#[error(fmt = ..)]` hands every field over by reference.
#[expect(clippy::ref_option, clippy::trivially_copy_pass_by_ref)]
fn connect_fmt(
    method: &str, status: &Option<StatusCode>, code: &str, message: &str,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    match status {
        Some(status) => write!(f, "sdk.v1 RPC `{method}` failed ({status}, {code}): {message}"),
        None => write!(f, "sdk.v1 RPC `{method}` stream failed ({code}): {message}"),
    }
}

// Connect envelope decoder over a streaming response body.
struct FrameStream {
    method: String,
    body: Incoming,
    buffer: BytesMut,
}

impl FrameStream {
    // `None` only at a clean frame boundary; a body ending mid-frame is a transport error.
    async fn next(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = decode_frame(&mut self.buffer)? {
                return Ok(Some(frame));
            }
            let Some(chunk) = self.body.frame().await else {
                if !self.buffer.is_empty() {
                    return Err(RpcError::truncated(&self.method, self.buffer.len()).into());
                }
                return Ok(None);
            };
            let chunk =
                chunk.map_err(|error| RpcError::io(&self.method, "reading the stream", error))?;
            if let Ok(data) = chunk.into_data() {
                self.buffer.extend_from_slice(&data);
            }
        }
    }
}

#[derive(Debug)]
struct Frame {
    flags: u8,
    payload: Bytes,
}

impl Frame {
    const fn is_end_stream(&self) -> bool {
        self.flags & END_STREAM != 0
    }
}

// The client has no TLS, so credentials travel in the clear: loopback only.
fn ensure_loopback(base: &str) -> Result<()> {
    let uri: Uri = base.parse().context("parsing sdk.v1 URL")?;
    ensure!(
        uri.scheme() == Some(&Scheme::HTTP),
        "sdk.v1 URL must use the http scheme (the client has no TLS)"
    );

    if let Some(authority) = uri.authority() {
        ensure!(!authority.as_str().contains('@'), "sdk.v1 URL must not include userinfo");
    }

    let host = uri.host().context("sdk.v1 URL must include a host")?;
    let host = host.strip_prefix('[').and_then(|host| host.strip_suffix(']')).unwrap_or(host);
    ensure!(
        host.eq_ignore_ascii_case("localhost")
            || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()),
        "sdk.v1 URL must target a loopback host"
    );

    Ok(())
}

fn envelope(payload: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(payload.len()).map_err(|_overflow| {
        anyhow!("a {}-byte message exceeds the Connect frame limit", payload.len())
    })?;

    let mut body = Vec::with_capacity(payload.len() + 5);
    body.push(0);
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(payload);

    Ok(body)
}

fn decode_frame(buffer: &mut BytesMut) -> Result<Option<Frame>> {
    if buffer.len() < 5 {
        return Ok(None);
    }
    let flags = buffer[0];
    ensure!(
        flags & COMPRESSED == 0,
        "cursor-sdk-bridge sent a compressed frame without negotiation"
    );
    let length = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
    if buffer.len() < 5 + length {
        return Ok(None);
    }
    let mut frame = buffer.split_to(5 + length);
    let payload = frame.split_off(5).freeze();
    Ok(Some(Frame { flags, payload }))
}

async fn read_body(method: &str, doing: &'static str, body: Incoming) -> Result<Bytes> {
    let collected = body.collect().await.map_err(|error| RpcError::io(method, doing, error))?;
    Ok(collected.to_bytes())
}

// A missing agent is the desired end state of close/delete.
fn gone_ok(result: Result<()>) -> Result<()> {
    match result {
        Err(error) if error.downcast_ref::<RpcError>().is_some_and(RpcError::is_agent_gone) => {
            Ok(())
        }
        other => other,
    }
}

// --- Agent options ---

// no `Debug`: carries the API key
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentOptions {
    pub model: ModelSelection,
    pub api_key: String,
    pub local: LocalAgentOptions,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub mcp_servers: BTreeMap<String, McpServerConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolList>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelSelection {
    pub id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalAgentOptions {
    pub cwd: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub custom_tools: BTreeMap<String, CustomToolDefinition>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomToolDefinition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerConfig {
    http: HttpMcpServerConfig,
}

impl McpServerConfig {
    pub fn streamable_http(url: &str) -> Self {
        Self {
            http: HttpMcpServerConfig {
                transport: "HTTP_MCP_TRANSPORT_TYPE_HTTP",
                url: url.to_owned(),
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HttpMcpServerConfig {
    #[serde(rename = "type")]
    transport: &'static str,
    url: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolList {
    pub names: Vec<String>,
}

// Local delete/get/archive are cwd-scoped; the key is the same pin
// `CreateAgent` sent so a later call does not depend on the process's env
// fallback.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentOperationOptions {
    pub cwd: String,
    pub api_key: String,
}

// --- Requests and responses ---

#[derive(Serialize, Deserialize, Default)]
#[allow(clippy::empty_structs_with_brackets)] // prevent serialization as `null`
struct Empty {}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GetVersionResponse {
    pub protocol_version: String,
    pub capabilities: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShutdownRequest {
    grace_seconds: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateAgentRequest {
    options: AgentOptions,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CreateAgentResponse {
    pub agent_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CloseAgentRequest {
    agent_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeleteAgentRequest {
    agent_id: String,
    options: AgentOperationOptions,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelRunRequest {
    run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SendRequest {
    agent_id: String,
    message: UserMessage,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UserMessage {
    text: String,
}

// --- Connect errors ---

// Connect's error object; a missing code is `unknown`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ConnectStatus {
    code: String,
    message: String,
    details: Option<Value>,
}

impl Default for ConnectStatus {
    fn default() -> Self {
        Self {
            code: "unknown".to_owned(),
            message: String::new(),
            details: None,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct EndStreamResponse {
    error: Option<ConnectStatus>,
}

// --- Run streaming ---

// One frame of a `Send` stream; an empty frame (no envelope case, no
// offset) is a keepalive, and so is any case this backend does not know.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunStreamMessage {
    pub sdk_message: Option<SdkMessage>,
    pub result: Option<RunStreamResult>,
    // Present on the closing frame.
    pub done: Option<IgnoredAny>,
    offset: Option<String>,
}

impl RunStreamMessage {
    const fn is_keepalive(&self) -> bool {
        self.sdk_message.is_none()
            && self.result.is_none()
            && self.done.is_none()
            && self.offset.is_none()
    }
}

// A typed conversation event; `kind` mirrors the public SDK's message types
// (`system`, `assistant`, `tool_call`, `status`, ...).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SdkMessage {
    #[serde(rename = "type")]
    pub kind: String,
    pub message: Value,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunStreamResult {
    pub run_id: String,
    #[serde(deserialize_with = "run_status")]
    pub status: RunStatus,
    pub error_code: Option<String>,
    pub result: Option<RunResult>,
}

/// `RunLifecycleStatus`, by its proto3 JSON name; a name this backend does
/// not know is `Unknown`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
pub enum RunStatus {
    /// The stream's result carried no status.
    #[default]
    #[serde(rename = "RUN_LIFECYCLE_STATUS_UNSPECIFIED")]
    Unspecified,
    /// The run is being set up.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_CREATING")]
    Creating,
    /// The run is in progress.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_RUNNING")]
    Running,
    /// The run completed with a result.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_FINISHED")]
    Finished,
    /// The run failed.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_ERROR")]
    Error,
    /// The run was cancelled.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_CANCELLED")]
    Cancelled,
    /// The run outlived its server-side lifetime.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_EXPIRED")]
    Expired,
    /// A status this backend does not know.
    #[serde(other)]
    Unknown,
}

impl RunStatus {
    // in proto declaration order, so the index is the wire number
    const BY_NUMBER: [Self; 7] = [
        Self::Unspecified,
        Self::Creating,
        Self::Running,
        Self::Finished,
        Self::Error,
        Self::Cancelled,
        Self::Expired,
    ];
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::Creating => "creating",
            Self::Running => "running",
            Self::Finished => "finished",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
            Self::Unknown => "unknown",
        })
    }
}

// Proto3 JSON writes the name, but a bridge release may write the number.
fn run_status<'de, D: Deserializer<'de>>(deserializer: D) -> Result<RunStatus, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Wire {
        Name(RunStatus),
        Number(usize),
        Other(IgnoredAny),
    }
    Ok(match Wire::deserialize(deserializer)? {
        Wire::Name(status) => status,
        Wire::Number(number) => {
            RunStatus::BY_NUMBER.get(number).copied().unwrap_or(RunStatus::Unknown)
        }
        Wire::Other(_) => RunStatus::Unknown,
    })
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunResult {
    // Final assistant text for a completed run.
    pub result: String,
    pub usage: Option<TokenUsage>,
}

// Billed token counts; `int64` arrives as a string or a number.
#[allow(clippy::struct_field_names)] // names mirror the wire message
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TokenUsage {
    #[serde(deserialize_with = "flexible_i64")]
    pub input_tokens: i64,
    #[serde(deserialize_with = "flexible_i64")]
    pub output_tokens: i64,
    #[serde(deserialize_with = "flexible_i64_opt")]
    pub reasoning_tokens: Option<i64>,
}

fn flexible_i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    Ok(flexible_i64_opt(deserializer)?.unwrap_or_default())
}

fn flexible_i64_opt<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<i64>, D::Error> {
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.and_then(|value| match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }))
}

// Service-free floor: codecs, framing, the URL guard, error decoding. The
// client itself is proven in `tests/worker.rs` (fake bridge) and `tests/live.rs`.
#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use http::StatusCode;
    use serde_json::json;

    use super::{
        ConnectStatus, END_STREAM, EndStreamResponse, RpcError, RunStatus, RunStreamResult,
        TokenUsage, decode_frame, ensure_loopback,
    };

    fn envelope(payload: &[u8]) -> Vec<u8> {
        super::envelope(payload).expect("a test payload fits one frame")
    }

    #[test]
    fn token_encodings() {
        let usage: TokenUsage = serde_json::from_value(json!({
            "inputTokens": "7",
            "outputTokens": 3,
            "reasoningTokens": "not a number",
        }))
        .expect("int64 as string or number");
        assert_eq!((usage.input_tokens, usage.output_tokens), (7, 3));
        assert_eq!(usage.reasoning_tokens, None, "an unparsable count is absent");
    }

    #[test]
    fn connect_status_defaults() {
        let bare: ConnectStatus = serde_json::from_value(json!({})).expect("all defaulted");
        assert_eq!((bare.code.as_str(), bare.message.as_str()), ("unknown", ""));
        assert!(bare.details.is_none());

        let end: EndStreamResponse =
            serde_json::from_value(json!({ "error": null })).expect("a null error parses");
        assert!(end.error.is_none(), "a null error is a clean end");
        let end: EndStreamResponse =
            serde_json::from_value(json!({ "error": { "message": "boom", "details": [] } }))
                .expect("an error with no code");
        let error = end.error.expect("the error");
        assert_eq!((error.code.as_str(), error.message.as_str()), ("unknown", "boom"));
        assert!(error.details.is_some());
    }

    #[test]
    fn run_status_spellings() {
        let status = |value| {
            serde_json::from_value::<RunStreamResult>(json!({ "status": value }))
                .expect("a result with only a status parses")
                .status
        };
        assert_eq!(status(json!("RUN_LIFECYCLE_STATUS_FINISHED")), RunStatus::Finished);
        assert_eq!(status(json!(3)), RunStatus::Finished);
        assert_eq!(status(json!("RUN_LIFECYCLE_STATUS_PAUSED")), RunStatus::Unknown);
        assert_eq!(status(json!(42)), RunStatus::Unknown);
        assert_eq!(status(json!(-1)), RunStatus::Unknown);
        assert_eq!(status(json!(null)), RunStatus::Unknown);
        assert_eq!(status(json!({ "nested": true })), RunStatus::Unknown);

        let absent: RunStreamResult = serde_json::from_value(json!({})).expect("all defaulted");
        assert_eq!(absent.status, RunStatus::Unspecified);
    }

    #[test]
    fn loopback_http_accepted() {
        for url in [
            "http://127.0.0.1",
            "http://127.0.0.1:9",
            "http://127.1.2.3:9",
            "http://[::1]:9",
            "http://localhost:9",
            "http://LOCALHOST:9",
        ] {
            ensure_loopback(url).unwrap_or_else(|error| panic!("{url}: {error}"));
        }
    }

    #[test]
    fn loopback_http_rejected() {
        for (url, needle) in [
            ("http://192.0.2.1:9", "loopback"),
            ("http://8.8.8.8:9", "loopback"),
            ("http://0.0.0.0:9", "loopback"),
            ("http://[::ffff:8.8.8.8]:9", "loopback"),
            ("http://example.com:9", "loopback"),
            ("http://127.0.0.1.example.com:9", "loopback"),
            ("http://localhost.example.com:9", "loopback"),
            ("https://127.0.0.1:9", "http scheme"),
            ("http://user:token@127.0.0.1:9", "userinfo"),
            ("not a url", "parsing"),
        ] {
            let error = ensure_loopback(url).expect_err(url);
            assert!(error.to_string().contains(needle), "{url}: expected {needle:?} in {error}");
        }
    }

    #[test]
    fn envelope_prefix() {
        let body = envelope(br#"{"agentId":"a"}"#);
        assert_eq!(body[0], 0, "a request message frame carries no flags");
        assert_eq!(u32::from_be_bytes([body[1], body[2], body[3], body[4]]), 15);
        assert_eq!(&body[5..], br#"{"agentId":"a"}"#);
    }

    #[test]
    fn frames_decode_incrementally() {
        let mut buffer = BytesMut::new();
        let mut body = envelope(b"one");
        body.extend_from_slice(&envelope(b"two"));

        // Feed byte by byte: no frame until its length is fully buffered.
        let mut frames = Vec::new();
        for byte in body {
            buffer.extend_from_slice(&[byte]);
            while let Some(frame) = decode_frame(&mut buffer).expect("uncompressed frames decode") {
                frames.push(frame);
            }
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload.as_ref(), b"one");
        assert_eq!(frames[1].payload.as_ref(), b"two");
        assert!(buffer.is_empty(), "both frames consumed the buffer exactly");
    }

    #[test]
    fn compressed_frame() {
        let mut body = envelope(b"x");
        body[0] = 0x01;
        let mut buffer = BytesMut::from(body.as_slice());
        let error = decode_frame(&mut buffer).expect_err("compression is never negotiated");
        assert!(error.to_string().contains("compressed"), "unexpected: {error}");
    }

    #[test]
    fn end_stream_flag() {
        let mut message = BytesMut::from(envelope(b"{}").as_slice());
        let frame = decode_frame(&mut message).expect("decodes").expect("one frame");
        assert!(!frame.is_end_stream());

        let mut end = envelope(b"{}");
        end[0] = END_STREAM;
        let mut end = BytesMut::from(end.as_slice());
        let frame = decode_frame(&mut end).expect("decodes").expect("one frame");
        assert!(frame.is_end_stream());
    }

    #[test]
    fn end_stream_error_body() {
        let end = |payload: &[u8]| RpcError::end_stream("SdkAgentService/Send", payload);
        assert!(end(b"{}").is_none(), "no error field, clean end");
        assert!(end(b"not json").is_none(), "an unparsable end frame is not an error");
        let error = end(br#"{"error":{"code":"unauthenticated","message":"Unauthorized"}}"#)
            .expect("an end-stream error fails the stream");
        assert_eq!(
            error.to_string(),
            "sdk.v1 RPC `SdkAgentService/Send` stream failed (unauthenticated): Unauthorized"
        );
    }

    #[test]
    fn connect_error_body() {
        let error = RpcError::unary(
            "SdkAgentService/CreateAgent",
            StatusCode::NOT_FOUND,
            br#"{"code":"not_found","message":"unknown agent"}"#,
        );
        assert_eq!(
            error.to_string(),
            "sdk.v1 RPC `SdkAgentService/CreateAgent` failed (404 Not Found, not_found): unknown \
             agent"
        );

        let error = RpcError::unary(
            "SdkAgentService/CreateAgent",
            StatusCode::INTERNAL_SERVER_ERROR,
            b"plain text\n",
        );
        assert!(error.to_string().ends_with("unknown): plain text"), "{error}");
    }

    #[test]
    fn truncated_stream() {
        let error: anyhow::Error = RpcError::truncated("SdkAgentService/Send", 3).into();
        assert_eq!(
            format!("{error:#}"),
            "sdk.v1 RPC `SdkAgentService/Send` transport failed reading the stream: stream ended \
             mid-frame (3 bytes buffered)"
        );
    }

    #[test]
    fn agent_gone_codes() {
        let unary = |status, body| RpcError::unary("SdkAgentService/DeleteAgent", status, body);

        let typed =
            unary(StatusCode::NOT_FOUND, br#"{"code":"not_found","message":"unknown agent"}"#);
        assert!(typed.is_agent_gone(), "{typed}");

        let mistagged = unary(
            StatusCode::INTERNAL_SERVER_ERROR,
            br#"{"code":"internal","message":"Agent 4448a33a-8b47-41fe-b051-90484e4d00fb not found"}"#,
        );
        assert!(mistagged.is_agent_gone(), "{mistagged}");

        let unimplemented = unary(
            StatusCode::NOT_FOUND,
            br#"{"code":"unimplemented","message":"Method not found"}"#,
        );
        assert!(!unimplemented.is_agent_gone(), "{unimplemented}");

        let other = unary(
            StatusCode::INTERNAL_SERVER_ERROR,
            br#"{"code":"internal","message":"store locked"}"#,
        );
        assert!(!other.is_agent_gone(), "{other}");
    }
}
