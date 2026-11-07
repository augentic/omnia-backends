//! The `sdk.v1` protocol `cursor-sdk-bridge` speaks — Connect JSON over a
//! loopback port — and a client with one typed method per procedure.
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
//! all. Each reaches the caller as a [`Failure::Rpc`].

use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
use bytes::{Buf as _, Bytes, BytesMut};
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

use crate::Failure;

const COMPRESSED: u8 = 0x01;
const END_STREAM: u8 = 0x02;

// A cloneable `sdk.v1` client bound to one process's loopback endpoint and
// bearer token.
#[derive(Clone)]
pub struct Rpc {
    hyper: HyperClient<HttpConnector, Full<Bytes>>,
    base: String,
    bearer: String,
}

impl Rpc {
    // Unbounded: the caller holds the handshake's bound.
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
        tracing::trace!(?version.capabilities, "ready");

        Ok(rpc)
    }

    pub async fn ping(&self) -> Result<()> {
        self.unary_empty("SdkBridgeControlService/Ping", &Empty {}).await
    }

    pub async fn get_version(&self) -> Result<GetVersionResponse> {
        self.unary("SdkBridgeControlService/GetVersion", &Empty {}).await
    }

    pub async fn shutdown(&self, grace: Duration) -> Result<()> {
        let request = ShutdownRequest {
            grace_seconds: u32::try_from(grace.as_secs()).unwrap_or(u32::MAX),
        };
        self.unary_empty("SdkBridgeControlService/Shutdown", &request).await
    }

    pub async fn create_agent(&self, options: AgentOptions) -> Result<CreateAgentResponse> {
        self.unary("SdkAgentService/CreateAgent", &CreateAgentRequest { options }).await
    }

    pub async fn cancel_run(&self, run_id: String, agent_id: String) -> Result<()> {
        let request = CancelRunRequest { run_id, agent_id };
        self.unary_empty("SdkAgentService/CancelRun", &request).await
    }

    // Release the live handle; durable rows stay until delete.
    pub async fn close_agent(&self, agent_id: String) -> Result<()> {
        gone_ok(
            self.unary_empty("SdkAgentService/CloseAgent", &CloseAgentRequest { agent_id }).await,
        )
    }

    // Local lookup is cwd-scoped, so `options` must name the create-time
    // workspace.
    pub async fn delete_agent(
        &self, agent_id: String, options: AgentOperationOptions,
    ) -> Result<()> {
        let request = DeleteAgentRequest { agent_id, options };
        gone_ok(self.unary_empty("SdkAgentService/DeleteAgent", &request).await)
    }

    // The agent's bill so far, across every run; `None` when the bridge
    // reports no counts for it.
    pub async fn get_usage(&self, agent_id: String) -> Result<Option<TokenUsage>> {
        let request = GetUsageRequest { agent_id };
        let response: GetUsageResponse = self.unary("SdkAgentService/GetUsage", &request).await?;
        Ok(response.usage.and_then(|usage| usage.usage))
    }

    pub async fn send(&self, agent_id: String, text: String) -> Result<RunStream> {
        let request = SendRequest {
            agent_id,
            message: UserMessage { text },
        };
        self.server_stream("SdkAgentService/Send", &request).await.map(RunStream)
    }

    async fn unary_empty<Req: Serialize + Sync>(&self, method: &str, request: &Req) -> Result<()> {
        self.unary::<_, IgnoredAny>(method, request).await.map(drop)
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
        let body = envelope(request).with_context(|| format!("framing `{method}`"))?;
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
            .map_err(|error| failed(RpcError::io(method, "sending the request", error)))?;

        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let bytes = read_body(method, "reading the error response", response.into_body()).await?;
        Err(failed(RpcError::unary(method, status, &bytes)))
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

// The typed message stream of one `Send`: framing, end-stream errors,
// keepalives, and unparsable frames are all absorbed here, so a yielded
// message is always real progress.
pub struct RunStream(FrameStream);

impl RunStream {
    // `None` at a clean end; a stream closed by a Connect error is that error.
    pub async fn next(&mut self) -> Result<Option<RunStreamMessage>> {
        while let Some(frame) = self.0.next().await? {
            if frame.is_end_stream() {
                return RpcError::end_stream(&self.0.method, &frame.payload)
                    .map_or(Ok(None), |error| Err(failed(error)));
            }
            let Ok(message) = serde_json::from_slice::<RunStreamMessage>(&frame.payload)
                .inspect_err(|error| tracing::debug!(%error, "skipping unparsable stream frame"))
            else {
                continue;
            };
            if !message.is_keepalive() {
                return Ok(Some(message));
            }
        }
        Ok(None)
    }
}

/// One `sdk.v1` RPC failed: the process answered with an error, or the call
/// never got an answer at all.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// The process answered: a non-success status on a unary call, or an
    /// error `EndStreamResponse` on a stream.
    #[error(
        "sdk.v1 RPC `{method}` failed ({}{code}): {message}",
        .status.map_or(String::new(), |status| format!("{status}, "))
    )]
    Connect {
        /// The `Service/Method` called.
        method: String,
        /// The HTTP status of a unary failure; a stream's error frame has none.
        status: Option<StatusCode>,
        /// Connect's error code; `unknown` when the body carried none.
        code: String,
        /// Connect's error message, else the raw body.
        message: String,
    },
    /// Below Connect: the request could not be sent, or the response or run
    /// stream could not be read; the process is not known to have seen the
    /// call.
    #[error("sdk.v1 RPC `{method}` transport failed {doing}")]
    Transport {
        /// The `Service/Method` called.
        method: String,
        /// What the client was doing when the transport gave out.
        doing: &'static str,
        /// The HTTP client's or the socket's error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl RpcError {
    // A body that is not Connect's error object (a crash's, a proxy's) is the message itself.
    fn unary(method: &str, status: StatusCode, body: &[u8]) -> Self {
        let mut answer: ConnectStatus = serde_json::from_slice(body).unwrap_or_default();
        if answer.message.is_empty() {
            answer.message = String::from_utf8_lossy(body).trim().to_string();
        }
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
        let message = message.to_ascii_lowercase();
        code == "not_found" || (message.contains("agent") && message.contains("not found"))
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
                    return Err(failed(RpcError::truncated(&self.method, self.buffer.len())));
                }
                return Ok(None);
            };
            let chunk = chunk
                .map_err(|error| failed(RpcError::io(&self.method, "reading the stream", error)))?;
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

// One message in the Connect envelope: the flag byte and big-endian length
// stand first, and the JSON is written in place behind them.
fn envelope<Req: Serialize>(request: &Req) -> Result<Vec<u8>> {
    let mut body = vec![0_u8; 5];
    serde_json::to_writer(&mut body, request).context("encoding the message")?;
    let length = u32::try_from(body.len() - 5).with_context(|| {
        format!("a {}-byte message exceeds the Connect frame limit", body.len() - 5)
    })?;
    body[1..5].copy_from_slice(&length.to_be_bytes());
    Ok(body)
}

// The first whole frame in `buffer`, taken off its front; `None` with the
// buffer untouched until the frame's length is buffered in full.
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
    buffer.advance(5);
    let payload = buffer.split_to(length).freeze();
    Ok(Some(Frame { flags, payload }))
}

async fn read_body(method: &str, doing: &'static str, body: Incoming) -> Result<Bytes> {
    let collected =
        body.collect().await.map_err(|error| failed(RpcError::io(method, doing, error)))?;
    Ok(collected.to_bytes())
}

// Every RPC failure is a `Failure`, so a caller downcasts to one type.
fn failed(error: RpcError) -> anyhow::Error {
    Failure::Rpc(error).into()
}

// A missing agent is the desired end state of close/delete.
fn gone_ok(result: Result<()>) -> Result<()> {
    match result {
        Err(error)
            if matches!(
                error.downcast_ref::<Failure>(),
                Some(Failure::Rpc(rpc)) if rpc.is_agent_gone()
            ) =>
        {
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

// The braces keep it a JSON object: a unit struct serializes as `null`.
#[derive(Serialize)]
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
    agent_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetUsageRequest {
    agent_id: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct GetUsageResponse {
    usage: Option<AgentUsage>,
}

// The agent's bill: its total, with a per-run breakdown this backend leaves aside.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct AgentUsage {
    usage: Option<TokenUsage>,
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
    offset: Option<IgnoredAny>,
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

// Billed token counts; `int64` arrives as a string or a number. The bridge's
// `totalTokens` is input plus output plus cache reads, so the cache reads are
// context beyond `inputTokens`, not a part of it; the reasoning tokens are
// among the output. Every optional count is absent when the bridge does not
// report it.
#[expect(clippy::struct_field_names, reason = "the names mirror the wire message")]
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TokenUsage {
    #[serde(deserialize_with = "flexible_i64")]
    pub input_tokens: i64,
    #[serde(deserialize_with = "flexible_i64")]
    pub output_tokens: i64,
    #[serde(deserialize_with = "flexible_i64_opt")]
    pub reasoning_tokens: Option<i64>,
    #[serde(deserialize_with = "flexible_i64_opt")]
    pub cache_read_tokens: Option<i64>,
    #[serde(deserialize_with = "flexible_i64_opt")]
    pub cache_write_tokens: Option<i64>,
    #[serde(deserialize_with = "flexible_i64_opt")]
    pub total_tokens: Option<i64>,
}

fn flexible_i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    Ok(flexible_i64_opt(deserializer)?.unwrap_or_default())
}

fn flexible_i64_opt<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<i64>, D::Error> {
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.as_ref().and_then(lenient_i64))
}

// An `int64` as proto3 JSON spells it — a number, or a string when it may
// not fit a double — read from either; anything else is no count.
pub fn lenient_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}
