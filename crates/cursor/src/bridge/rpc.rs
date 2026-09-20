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
//! Failures come in three classes a caller can tell apart: a Connect error
//! (the bridge answered with a status and code), an end-stream error (the
//! run stream closed with an error frame), and a [`TransportError`] — the
//! socket, the HTTP layer, or the framing gave out below Connect, so the
//! bridge is not known to have seen the call at all.

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use bytes::{Bytes, BytesMut};
use http::Uri;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::messages::{
    AgentOperationOptions, AgentOptions, CancelRunRequest, CloseAgentRequest, CreateAgentRequest,
    CreateAgentResponse, DeleteAgentRequest, Empty, GetVersionResponse, RunStreamMessage,
    SendRequest, ShutdownRequest, UserMessage,
};

/// Envelope flag bit marking the end-of-stream frame.
const END_STREAM: u8 = 0x02;
/// Envelope flag bit marking a compressed frame (never negotiated here).
const COMPRESSED: u8 = 0x01;
/// Bound on the handshake (`Ping`, then `GetVersion`) that proves a bridge
/// answers; a spawned bridge is already past its ready line by then.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

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

#[derive(Debug)]
enum Cause {
    /// The HTTP client or the socket failed while `doing`.
    Io { doing: &'static str, source: Box<dyn std::error::Error + Send + Sync + 'static> },
    /// The body ended with a partial envelope still buffered.
    Truncated { buffered: usize },
}

impl TransportError {
    pub(crate) fn io(
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

    pub(crate) fn truncated(method: &str, buffered: usize) -> Self {
        Self {
            method: method.to_owned(),
            cause: Cause::Truncated { buffered },
        }
    }

    /// The `Service/Method` the failure struck.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }
}

// The source is left to `Error::source`, so an `{error:#}` chain names it
// once.
impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

/// A cloneable `sdk.v1` client bound to one bridge endpoint and bearer token.
#[derive(Clone)]
pub struct Rpc {
    hyper: HyperClient<HttpConnector, Full<Bytes>>,
    base: String,
    bearer: String,
}

impl std::fmt::Debug for Rpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rpc").field("base", &self.base).finish_non_exhaustive()
    }
}

impl Rpc {
    /// Bind to `base` and prove the bridge answers `sdk.v1`.
    pub async fn connect(base: String, token: &str) -> Result<Self> {
        // The client is HTTP-only: refuse anything that is not loopback
        // before the bearer token or `DeleteAgent`'s API key go on the wire.
        require_loopback_http(&base)?;
        let rpc = Self {
            hyper: HyperClient::builder(TokioExecutor::new()).build_http(),
            base,
            bearer: format!("Bearer {token}"),
        };
        let version = tokio::time::timeout(CONNECT_TIMEOUT, async {
            rpc.ping().await?;
            rpc.get_version().await
        })
        .await
        .map_err(|_elapsed| {
            anyhow::anyhow!(
                "bridge did not answer the handshake within {}s",
                CONNECT_TIMEOUT.as_secs()
            )
        })??;
        ensure!(version.protocol_version == "sdk.v1", "unsupported protocol version");
        tracing::debug!(?version.capabilities, "ready");
        Ok(rpc)
    }

    /// `Ping`: verify the control endpoint answers.
    pub async fn ping(&self) -> Result<()> {
        self.unary::<_, Empty>("SdkBridgeControlService/Ping", &Empty {}).await.map(drop)
    }

    /// `GetVersion`: the bridge's version, protocol, and capabilities.
    pub async fn get_version(&self) -> Result<GetVersionResponse> {
        self.unary("SdkBridgeControlService/GetVersion", &Empty {}).await
    }

    /// `Shutdown`: ask the bridge to exit gracefully.
    pub async fn shutdown(&self) -> Result<()> {
        self.unary::<_, Empty>(
            "SdkBridgeControlService/Shutdown",
            &ShutdownRequest { grace_seconds: 1 },
        )
        .await
        .map(drop)
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
        self.unary::<_, Empty>("SdkAgentService/CancelRun", &request).await.map(drop)
    }

    /// `CloseAgent`: release the live handle. Durable rows stay until delete.
    pub async fn close_agent(&self, agent_id: String) -> Result<()> {
        gone_ok(
            self.unary::<_, Empty>("SdkAgentService/CloseAgent", &CloseAgentRequest { agent_id })
                .await
                .map(drop),
        )
    }

    /// `DeleteAgent`: discard durable session state. Local lookup is
    /// cwd-scoped, so `cwd` must be the create-time workspace.
    pub async fn delete_agent(&self, agent_id: String, cwd: String, api_key: String) -> Result<()> {
        gone_ok(
            self.unary::<_, Empty>(
                "SdkAgentService/DeleteAgent",
                &DeleteAgentRequest {
                    agent_id,
                    options: AgentOperationOptions { cwd, api_key },
                },
            )
            .await
            .map(drop),
        )
    }

    /// `Send`: one agent turn; the stream yields the run's messages.
    pub async fn send(&self, agent_id: String, text: String) -> Result<RunStream> {
        let request = SendRequest {
            agent_id,
            message: UserMessage { text },
        };
        Ok(RunStream(self.server_stream("SdkAgentService/Send", &request).await?))
    }

    /// One unary RPC in the plain JSON codec.
    async fn unary<Req: Serialize + Sync, Resp: DeserializeOwned>(
        &self, method: &str, request: &Req,
    ) -> Result<Resp> {
        let body = serde_json::to_vec(request).with_context(|| format!("encoding `{method}`"))?;
        let response = self.call(method, "application/json", body).await?;
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|error| TransportError::io(method, "reading the response", error))?
            .to_bytes();
        serde_json::from_slice(&bytes).with_context(|| format!("decoding `{method}` response"))
    }

    /// One server-streaming RPC: the request rides as a single enveloped JSON
    /// message; the returned stream yields response envelopes.
    async fn server_stream<Req: Serialize + Sync>(
        &self, method: &str, request: &Req,
    ) -> Result<FrameStream> {
        let payload =
            serde_json::to_vec(request).with_context(|| format!("encoding `{method}`"))?;
        let response = self.call(method, "application/connect+json", envelope(&payload)).await?;
        Ok(FrameStream {
            method: method.to_owned(),
            body: response.into_body(),
            buffer: BytesMut::new(),
        })
    }

    /// POST the body and map a non-success status onto a Connect error.
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
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|error| TransportError::io(method, "reading the error response", error))?
            .to_bytes();
        Err(connect_error(method, status, &bytes))
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
fn envelope(payload: &[u8]) -> Vec<u8> {
    // A wrong length prefix would corrupt the stream; fail loudly instead.
    let length = u32::try_from(payload.len()).expect("payload exceeds the Connect frame limit");
    let mut body = Vec::with_capacity(payload.len() + 5);
    body.push(0);
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(payload);
    body
}

/// Close/delete of a missing agent is the desired end state. Cursor still
/// mis-tags some of those as 500 `internal` with "Agent … not found".
fn gone_ok(result: Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if already_gone(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

fn already_gone(error: &anyhow::Error) -> bool {
    let text = error.to_string();
    // `bridge RPC \`…\` failed ({status}, {code}): {message}` — do not
    // match the method name or the HTTP reason phrase (`404 Not Found`).
    let Some((_, rest)) = text.split_once("failed (") else {
        return false;
    };
    let Some((status_and_code, message)) = rest.split_once("): ") else {
        return false;
    };
    let code = status_and_code.rsplit_once(", ").map_or("", |(_, code)| code);
    if code == "not_found" {
        return true;
    }
    let message = message.to_ascii_lowercase();
    message.contains("agent") && message.contains("not found")
}

/// Map a non-200 Connect response — `{"code", "message", ...}` — onto an error.
fn connect_error(method: &str, status: http::StatusCode, body: &[u8]) -> anyhow::Error {
    let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let code = parsed.get("code").and_then(Value::as_str).unwrap_or("unknown");
    let message = parsed
        .get("message")
        .and_then(Value::as_str)
        .map_or_else(|| String::from_utf8_lossy(body).into_owned(), ToOwned::to_owned);
    if let Some(details) = parsed.get("details") {
        tracing::debug!(method, %details, "bridge error details");
    }
    anyhow::anyhow!("bridge RPC `{method}` failed ({status}, {code}): {}", message.trim())
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
                self.0.end_stream_error(&frame.payload)?;
                return Ok(None);
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

    /// Interpret an end-of-stream frame observed on this stream: `Ok` on a
    /// clean end, `Err` when the `EndStreamResponse` carries a Connect error.
    fn end_stream_error(&self, payload: &[u8]) -> Result<()> {
        end_stream_error(&self.method, payload)
    }
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

/// `Ok` on a clean end frame, `Err` when it carries a Connect error.
fn end_stream_error(method: &str, payload: &[u8]) -> Result<()> {
    let parsed: Value = serde_json::from_slice(payload).unwrap_or(Value::Null);
    let Some(error) = parsed.get("error").filter(|error| !error.is_null()) else {
        return Ok(());
    };
    let code = error.get("code").and_then(Value::as_str).unwrap_or("unknown");
    let message = error.get("message").and_then(Value::as_str).unwrap_or_default();
    bail!("bridge RPC `{method}` stream failed ({code}): {message}")
}

// Deliberate unit tests: loopback attach URL, envelope framing, and error
// decoding (CI floor); `tests/bridge.rs` proves the client against the fake
// bridge and `tests/live.rs` against a real one.
#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use hyper_util::client::legacy::Client as HyperClient;
    use hyper_util::rt::TokioExecutor;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::{
        Rpc, TransportError, already_gone, connect_error, decode_frame, end_stream_error, envelope,
        require_loopback_http,
    };

    /// A client bound to `addr` with no handshake.
    fn rpc_at(addr: std::net::SocketAddr) -> Rpc {
        Rpc {
            hyper: HyperClient::builder(TokioExecutor::new()).build_http(),
            base: format!("http://{addr}"),
            bearer: "Bearer test".to_owned(),
        }
    }

    #[tokio::test]
    async fn refused_port() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind loopback");
        let addr = listener.local_addr().expect("local address");
        drop(listener);

        let error = rpc_at(addr).ping().await.expect_err("nothing listens there");
        let transport = error
            .downcast_ref::<TransportError>()
            .unwrap_or_else(|| panic!("a socket failure is typed: {error:?}"));
        assert_eq!(transport.method(), "SdkBridgeControlService/Ping");
        assert!(error.chain().count() >= 2, "the hyper source is kept: {error:?}");
    }

    #[tokio::test]
    async fn stream_truncated_mid_frame() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind loopback");
        let addr = listener.local_addr().expect("local address");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("one connection");
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await;
            // A well-formed chunked body that ends three bytes into an
            // envelope: clean at the HTTP layer, torn at the Connect one.
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/connect+json\r\n\
                      transfer-encoding: chunked\r\n\r\n3\r\n\x00\x00\x00\r\n0\r\n\r\n",
                )
                .await
                .expect("the response is written");
            // Hold the socket until the client is done, so no reset races
            // the response.
            while stream.read(&mut request).await.is_ok_and(|read| read > 0) {}
        });

        let mut run = rpc_at(addr)
            .send("agent-1".to_owned(), "hi".to_owned())
            .await
            .expect("the response head is a success");
        let error = run.next().await.expect_err("three bytes are not a frame");
        let transport = error
            .downcast_ref::<TransportError>()
            .unwrap_or_else(|| panic!("a torn frame is typed: {error:?}"));
        assert_eq!(transport.method(), "SdkAgentService/Send");
        assert!(error.to_string().contains("mid-frame (3 bytes buffered)"), "{error}");
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
        let mut buffer = BytesMut::from(envelope(b"{}").as_slice());
        let mut frame = decode_frame(&mut buffer).expect("decodes").expect("one frame");
        assert!(!frame.is_end_stream());
        frame.flags = 0x02;
        assert!(frame.is_end_stream());
    }

    #[test]
    fn end_stream_error_body() {
        end_stream_error("SdkAgentService/Send", b"{}").expect("no error field, clean end");
        end_stream_error("SdkAgentService/Send", b"not json")
            .expect("an unparsable end frame is not an error");
        let error = end_stream_error(
            "SdkAgentService/Send",
            br#"{"error":{"code":"unauthenticated","message":"Unauthorized"}}"#,
        )
        .expect_err("an end-stream error fails the stream");
        let text = error.to_string();
        assert!(text.contains("unauthenticated") && text.contains("Unauthorized"), "{text}");
    }

    #[test]
    fn connect_error_body() {
        let error = connect_error(
            "SdkAgentService/CreateAgent",
            http::StatusCode::NOT_FOUND,
            br#"{"code":"not_found","message":"unknown agent"}"#,
        );
        let text = error.to_string();
        assert!(text.contains("not_found") && text.contains("unknown agent"), "{text}");

        let error = connect_error(
            "SdkAgentService/CreateAgent",
            http::StatusCode::INTERNAL_SERVER_ERROR,
            b"plain text",
        );
        assert!(error.to_string().contains("plain text"), "{error}");
    }

    #[test]
    fn already_gone_codes() {
        let typed = connect_error(
            "SdkAgentService/DeleteAgent",
            http::StatusCode::NOT_FOUND,
            br#"{"code":"not_found","message":"unknown agent"}"#,
        );
        assert!(already_gone(&typed), "{typed}");

        let mistagged = connect_error(
            "SdkAgentService/DeleteAgent",
            http::StatusCode::INTERNAL_SERVER_ERROR,
            br#"{"code":"internal","message":"Agent 4448a33a-8b47-41fe-b051-90484e4d00fb not found"}"#,
        );
        assert!(already_gone(&mistagged), "{mistagged}");

        let unimplemented = connect_error(
            "SdkAgentService/CloseAgent",
            http::StatusCode::NOT_FOUND,
            br#"{"code":"unimplemented","message":"Method not found"}"#,
        );
        assert!(!already_gone(&unimplemented), "{unimplemented}");

        let other = connect_error(
            "SdkAgentService/DeleteAgent",
            http::StatusCode::INTERNAL_SERVER_ERROR,
            br#"{"code":"internal","message":"store locked"}"#,
        );
        assert!(!already_gone(&other), "{other}");
    }
}
