//! Connect-over-HTTP/1.1 client for the bridge's loopback endpoint, exposing
//! one typed method per `sdk.v1` procedure. Response shapes deserialize
//! tolerantly (unknown fields ignored, missing fields defaulted), so a
//! mispaired path and response type would fail silently — the pairing lives
//! only here.
//!
//! Every RPC is `POST {base}/sdk.v1.{Service}/{Method}` in the Connect JSON
//! codec with bearer auth. Unary calls are plain JSON bodies; server streams
//! use the Connect envelope — a 1-byte flag plus a 4-byte big-endian length
//! per message, with flag `0x02` marking the JSON `EndStreamResponse`.
//!
//! Failures come in two typed classes a caller can tell apart: a
//! [`ConnectError`] — the bridge answered, with a status and code on a unary
//! call or an error frame closing a run stream — and a [`TransportError`] —
//! the socket, the HTTP layer, or the framing gave out below Connect, so the
//! bridge is not known to have seen the call at all.

use std::fmt;
use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, ensure};
use bytes::{Bytes, BytesMut};
use http::{StatusCode, Uri};
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::messages::{
    AgentOperationOptions, AgentOptions, CancelRunRequest, CloseAgentRequest, ConnectStatus,
    CreateAgentRequest, CreateAgentResponse, DeleteAgentRequest, Empty, EndStreamResponse,
    GetVersionResponse, RunStreamMessage, SendRequest, ShutdownRequest, UserMessage,
};

/// Envelope flag bit marking the end-of-stream frame.
const END_STREAM: u8 = 0x02;
/// Envelope flag bit marking a compressed frame (never negotiated here).
const COMPRESSED: u8 = 0x01;

/// A cloneable `sdk.v1` client bound to one bridge endpoint and bearer token.
#[derive(Clone)]
pub struct Rpc {
    hyper: HyperClient<HttpConnector, Full<Bytes>>,
    base: String,
    bearer: String,
}

impl Rpc {
    /// Bind to `base` and prove the bridge answers `sdk.v1` (`Ping`, then
    /// `GetVersion`). Unbounded: the caller holds the handshake's bound.
    pub async fn connect(base: &str, token: &str) -> Result<Self> {
        // The client is HTTP-only: refuse anything that is not loopback
        // before the bearer token or `DeleteAgent`'s API key go on the wire.
        require_loopback_http(base)?;
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

    /// `Ping`: verify the control endpoint answers.
    pub async fn ping(&self) -> Result<()> {
        self.unary_empty("SdkBridgeControlService/Ping", &Empty {}).await
    }

    /// `GetVersion`: the bridge's protocol and capabilities.
    pub async fn get_version(&self) -> Result<GetVersionResponse> {
        self.unary("SdkBridgeControlService/GetVersion", &Empty {}).await
    }

    /// `Shutdown`: ask the bridge to exit, giving its agents `grace` to
    /// finish (whole seconds, saturating).
    pub async fn shutdown(&self, grace: Duration) -> Result<()> {
        let request = ShutdownRequest {
            grace_seconds: u32::try_from(grace.as_secs()).unwrap_or(u32::MAX),
        };
        self.unary_empty("SdkBridgeControlService/Shutdown", &request).await
    }

    /// `CreateAgent`: one fresh agent from the completion's options.
    pub async fn create_agent(&self, options: AgentOptions) -> Result<CreateAgentResponse> {
        self.unary("SdkAgentService/CreateAgent", &CreateAgentRequest { options }).await
    }

    /// `CancelRun`: best-effort cancel of an abandoned run.
    pub async fn cancel_run(&self, run_id: String, agent_id: String) -> Result<()> {
        let request = CancelRunRequest {
            run_id,
            agent_id: Some(agent_id),
        };
        self.unary_empty("SdkAgentService/CancelRun", &request).await
    }

    /// `CloseAgent`: release the live handle. Durable rows stay until delete.
    pub async fn close_agent(&self, agent_id: String) -> Result<()> {
        gone_ok(
            self.unary_empty("SdkAgentService/CloseAgent", &CloseAgentRequest { agent_id }).await,
        )
    }

    /// `DeleteAgent`: discard durable session state. Local lookup is
    /// cwd-scoped, so the options must name the create-time workspace.
    pub async fn delete_agent(
        &self, agent_id: String, options: AgentOperationOptions,
    ) -> Result<()> {
        let request = DeleteAgentRequest { agent_id, options };
        gone_ok(self.unary_empty("SdkAgentService/DeleteAgent", &request).await)
    }

    /// `Send`: one agent turn; the stream yields the run's messages.
    pub async fn send(&self, agent_id: String, text: String) -> Result<RunStream> {
        let request = SendRequest {
            agent_id,
            message: UserMessage { text },
        };
        Ok(RunStream(self.server_stream("SdkAgentService/Send", &request).await?))
    }

    /// One unary RPC whose response carries nothing.
    async fn unary_empty<Req: Serialize + Sync>(&self, method: &str, request: &Req) -> Result<()> {
        self.unary::<_, Empty>(method, request).await.map(drop)
    }

    /// One unary RPC in the plain JSON codec.
    async fn unary<Req: Serialize + Sync, Resp: DeserializeOwned>(
        &self, method: &str, request: &Req,
    ) -> Result<Resp> {
        let body = serde_json::to_vec(request).with_context(|| format!("encoding `{method}`"))?;
        let response = self.call(method, "application/json", body).await?;
        let bytes = read_body(method, "reading the response", response.into_body()).await?;
        serde_json::from_slice(&bytes).with_context(|| format!("decoding `{method}` response"))
    }

    /// One server-streaming RPC: the request rides as a single enveloped JSON
    /// message; the returned stream yields response envelopes.
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

    /// POST the body and map a non-success status onto a [`ConnectError`].
    async fn call(
        &self, method: &str, content_type: &str, body: Vec<u8>,
    ) -> Result<http::Response<Incoming>> {
        let response = self
            .hyper
            .request(self.post(method, content_type, body)?)
            .await
            .map_err(|error| TransportError::io(method, "sending the request", error))?;

        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let bytes = read_body(method, "reading the error response", response.into_body()).await?;
        Err(ConnectError::unary(method, status, &bytes).into())
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

/// The typed message stream of one `Send` call: envelope framing, end-stream
/// errors, keepalives, and unparsable frames are all absorbed here, so a
/// yielded message is always real progress.
pub struct RunStream(FrameStream);

impl RunStream {
    /// The next run message, or `None` once the run stream ends.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failures or when the stream ends with a
    /// Connect error.
    pub async fn next(&mut self) -> Result<Option<RunStreamMessage>> {
        while let Some(frame) = self.0.next().await? {
            if frame.is_end_stream() {
                return ConnectError::end_stream(&self.0.method, &frame.payload)
                    .map_or(Ok(None), |error| Err(error.into()));
            }
            let message: RunStreamMessage = match serde_json::from_slice(&frame.payload) {
                Ok(message) => message,
                Err(error) => {
                    tracing::debug!(%error, "skipping unparsable stream frame");
                    continue;
                }
            };
            // Keepalives (and unknown envelope cases) are dropped here so the
            // caller's inactivity deadline only rearms on real progress.
            if message.is_keepalive() {
                continue;
            }
            return Ok(Some(message));
        }
        Ok(None)
    }
}

/// The bridge answered one RPC with an error: a non-success status on a
/// unary call, or an `EndStreamResponse` carrying an error on a stream.
#[derive(Debug)]
pub struct ConnectError {
    method: String,
    /// The HTTP status of a unary failure; a stream's error frame has none.
    status: Option<StatusCode>,
    answer: ConnectStatus,
}

impl ConnectError {
    /// A non-200 unary response: Connect's error object, or a body that is
    /// not one (a crash's, a proxy's) as the message itself.
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

    /// The error an `EndStreamResponse` carries, if any: a clean end frame,
    /// or one this backend cannot parse, is not an error.
    fn end_stream(method: &str, payload: &[u8]) -> Option<Self> {
        let end: EndStreamResponse = serde_json::from_slice(payload).unwrap_or_default();
        Some(Self::answered(method, None, end.error?))
    }

    // Details are the bridge's own diagnostics: logged, never carried.
    fn answered(method: &str, status: Option<StatusCode>, mut answer: ConnectStatus) -> Self {
        if let Some(details) = answer.details.take() {
            tracing::debug!(method, %details, "bridge error details");
        }
        Self {
            method: method.to_owned(),
            status,
            answer,
        }
    }

    /// Whether the agent the call named is already gone. Cursor still
    /// mis-tags some of those as 500 `internal` with "Agent … not found".
    fn is_agent_gone(&self) -> bool {
        if self.answer.code == "not_found" {
            return true;
        }
        let message = self.answer.message.to_ascii_lowercase();
        message.contains("agent") && message.contains("not found")
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            method,
            status,
            answer: ConnectStatus { code, message, .. },
        } = self;
        match status {
            Some(status) => write!(f, "bridge RPC `{method}` failed ({status}, {code}): {message}"),
            None => write!(f, "bridge RPC `{method}` stream failed ({code}): {message}"),
        }
    }
}

impl std::error::Error for ConnectError {}

/// A failure below the Connect protocol on one bridge RPC.
///
/// The request could not be sent, the response or run stream could not be
/// read, or the stream ended inside a frame: the bridge never answered the
/// call, unlike a Connect error (`failed (status, code)`) or an end-stream
/// error, both of which are the bridge answering.
#[derive(Debug)]
pub struct TransportError {
    method: String,
    cause: Cause,
}

impl TransportError {
    /// A socket failure while `doing`.
    #[must_use]
    pub fn io(
        method: &str, doing: &'static str, source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            method: method.to_owned(),
            cause: Cause::Io {
                doing,
                source: Box::new(source),
            },
        }
    }

    /// The body ended with a partial envelope still buffered.
    #[must_use]
    pub fn truncated(method: &str, buffered: usize) -> Self {
        Self {
            method: method.to_owned(),
            cause: Cause::Truncated { buffered },
        }
    }
}

// The source is left to `Error::source`, so an `{error:#}` chain names it
// once.
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            Cause::Io { doing, .. } => {
                write!(f, "bridge RPC `{}` transport failed {doing}", self.method)
            }
            Cause::Truncated { buffered } => write!(
                f,
                "bridge RPC `{}` stream ended mid-frame ({buffered} bytes buffered)",
                self.method
            ),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            Cause::Io { source, .. } => Some(source.as_ref()),
            Cause::Truncated { .. } => None,
        }
    }
}

#[derive(Debug)]
enum Cause {
    /// The HTTP client or the socket failed while `doing`.
    Io { doing: &'static str, source: Box<dyn std::error::Error + Send + Sync + 'static> },
    /// The body ended with a partial envelope still buffered.
    Truncated { buffered: usize },
}

/// Incrementally decodes Connect envelopes from a streaming response body.
struct FrameStream {
    method: String,
    body: Incoming,
    buffer: BytesMut,
}

impl FrameStream {
    /// The next envelope, or `None` when the body ends cleanly at a frame
    /// boundary. Fails with a [`TransportError`] on a socket failure or a
    /// truncated frame, and plainly on a compressed frame (compression is
    /// never negotiated).
    async fn next(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = decode_frame(&mut self.buffer)? {
                return Ok(Some(frame));
            }
            let Some(chunk) = self.body.frame().await else {
                if !self.buffer.is_empty() {
                    return Err(TransportError::truncated(&self.method, self.buffer.len()).into());
                }
                return Ok(None);
            };
            let chunk = chunk
                .map_err(|error| TransportError::io(&self.method, "reading the stream", error))?;
            if let Ok(data) = chunk.into_data() {
                self.buffer.extend_from_slice(&data);
            }
        }
    }
}

/// A decoded response envelope: the flag byte and the message payload.
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

/// `http://` to a loopback host: IP literal in `127.0.0.0/8` or `::1`, or
/// the name `localhost`. Anything else would send credentials in the clear.
fn require_loopback_http(base: &str) -> Result<()> {
    let uri: Uri = base.parse().context("parsing bridge URL")?;
    ensure!(
        uri.scheme() == Some(&http::uri::Scheme::HTTP),
        "bridge URL must use the http scheme (the client has no TLS)"
    );
    if let Some(authority) = uri.authority() {
        ensure!(!authority.as_str().contains('@'), "bridge URL must not include userinfo");
    }
    let host = uri.host().context("bridge URL must include a host")?;
    ensure!(is_loopback_host(host), "bridge URL must target a loopback host");
    Ok(())
}

fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    let host = host.strip_prefix('[').and_then(|host| host.strip_suffix(']')).unwrap_or(host);
    host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Wrap one message in the Connect streaming envelope.
///
/// # Errors
///
/// Returns an error when the payload does not fit the envelope's 4-byte
/// length prefix.
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

/// Split one complete envelope off the front of `buffer`, if present.
fn decode_frame(buffer: &mut BytesMut) -> Result<Option<Frame>> {
    if buffer.len() < 5 {
        return Ok(None);
    }
    let flags = buffer[0];
    ensure!(flags & COMPRESSED == 0, "bridge sent a compressed frame without negotiation");
    let length = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
    if buffer.len() < 5 + length {
        return Ok(None);
    }
    let mut frame = buffer.split_to(5 + length);
    let payload = frame.split_off(5).freeze();
    Ok(Some(Frame { flags, payload }))
}

/// Collect a response body whole; a failure below Connect while `doing` is
/// a [`TransportError`].
async fn read_body(method: &str, doing: &'static str, body: Incoming) -> Result<Bytes> {
    let collected =
        body.collect().await.map_err(|error| TransportError::io(method, doing, error))?;
    Ok(collected.to_bytes())
}

/// Close/delete of a missing agent is the desired end state.
fn gone_ok(result: Result<()>) -> Result<()> {
    match result {
        Err(error)
            if error.downcast_ref::<ConnectError>().is_some_and(ConnectError::is_agent_gone) =>
        {
            Ok(())
        }
        other => other,
    }
}

// Deliberate unit tests: the loopback ready-line URL, envelope framing, and
// error decoding (CI floor); `tests/bridge.rs` proves the client against the
// fake bridge and `tests/live.rs` against a real one.
#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use http::StatusCode;

    use super::{ConnectError, END_STREAM, decode_frame, require_loopback_http};

    fn envelope(payload: &[u8]) -> Vec<u8> {
        super::envelope(payload).expect("a test payload fits one frame")
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
            require_loopback_http(url).unwrap_or_else(|error| panic!("{url}: {error}"));
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
            let error = require_loopback_http(url).expect_err(url);
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
        let end = |payload: &[u8]| ConnectError::end_stream("SdkAgentService/Send", payload);
        assert!(end(b"{}").is_none(), "no error field, clean end");
        assert!(end(b"not json").is_none(), "an unparsable end frame is not an error");
        let error = end(br#"{"error":{"code":"unauthenticated","message":"Unauthorized"}}"#)
            .expect("an end-stream error fails the stream");
        assert_eq!(
            error.to_string(),
            "bridge RPC `SdkAgentService/Send` stream failed (unauthenticated): Unauthorized"
        );
    }

    #[test]
    fn connect_error_body() {
        let error = ConnectError::unary(
            "SdkAgentService/CreateAgent",
            StatusCode::NOT_FOUND,
            br#"{"code":"not_found","message":"unknown agent"}"#,
        );
        assert_eq!(
            error.to_string(),
            "bridge RPC `SdkAgentService/CreateAgent` failed (404 Not Found, not_found): unknown \
             agent"
        );

        let error = ConnectError::unary(
            "SdkAgentService/CreateAgent",
            StatusCode::INTERNAL_SERVER_ERROR,
            b"plain text\n",
        );
        assert!(error.to_string().ends_with("unknown): plain text"), "{error}");
    }

    #[test]
    fn agent_gone_codes() {
        let unary = |status, body| ConnectError::unary("SdkAgentService/DeleteAgent", status, body);

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
