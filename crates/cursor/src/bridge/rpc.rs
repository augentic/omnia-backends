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

use anyhow::{Context as _, Result, bail, ensure};
use bytes::{Bytes, BytesMut};
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
    AgentOptions, CancelRunRequest, CreateAgentRequest, CreateAgentResponse, DeleteAgentRequest,
    Empty, GetVersionResponse, SendRequest, ShutdownRequest, UserMessage,
};

/// Envelope flag bit marking the end-of-stream frame.
const END_STREAM: u8 = 0x02;
/// Envelope flag bit marking a compressed frame (never negotiated here).
const COMPRESSED: u8 = 0x01;

/// A cloneable `sdk.v1` client bound to one bridge endpoint and bearer token.
#[derive(Clone)]
pub struct Rpc {
    client: HyperClient<HttpConnector, Full<Bytes>>,
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
        let rpc = Self {
            client: HyperClient::builder(TokioExecutor::new()).build_http(),
            base,
            bearer: format!("Bearer {token}"),
        };
        rpc.ping().await?;
        let version = rpc.get_version().await?;
        ensure!(version.protocol_version == "sdk.v1", "unsupported protocol version");
        tracing::info!(?version.capabilities, "ready");
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

    /// `DeleteAgent`: discard the agent and its session state.
    pub async fn delete_agent(&self, agent_id: String) -> Result<()> {
        self.unary::<_, Empty>("SdkAgentService/DeleteAgent", &DeleteAgentRequest { agent_id })
            .await
            .map(drop)
    }

    /// `Send`: one agent turn; the stream yields the run's event envelopes.
    pub async fn send(&self, agent_id: String, text: String) -> Result<FrameStream> {
        let request = SendRequest {
            agent_id,
            message: UserMessage { text },
        };
        self.server_stream("SdkAgentService/Send", &request).await
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
            .with_context(|| format!("reading `{method}` response"))?
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
            .client
            .request(self.post(method, content_type, body)?)
            .await
            .with_context(|| format!("bridge RPC `{method}`"))?;

        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let bytes = response
            .into_body()
            .collect()
            .await
            .with_context(|| format!("reading `{method}` response"))?
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

/// A decoded response envelope: the flag byte and the message payload.
#[derive(Debug)]
pub struct Frame {
    pub flags: u8,
    pub payload: Bytes,
}

impl Frame {
    pub const fn is_end_stream(&self) -> bool {
        self.flags & END_STREAM != 0
    }
}

/// Incrementally decodes Connect envelopes from a streaming response body.
pub struct FrameStream {
    method: String,
    body: Incoming,
    buffer: BytesMut,
}

impl FrameStream {
    /// The next envelope, or `None` when the body ends cleanly at a frame
    /// boundary.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failures, a truncated frame, or a
    /// compressed frame (compression is never negotiated).
    pub async fn next(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = decode_frame(&mut self.buffer)? {
                return Ok(Some(frame));
            }
            let Some(chunk) = self.body.frame().await else {
                ensure!(
                    self.buffer.is_empty(),
                    "bridge RPC `{}` stream ended mid-frame ({} bytes buffered)",
                    self.method,
                    self.buffer.len()
                );
                return Ok(None);
            };
            let chunk = chunk.with_context(|| format!("reading `{}` stream", self.method))?;
            if let Ok(data) = chunk.into_data() {
                self.buffer.extend_from_slice(&data);
            }
        }
    }

    /// Interpret an end-of-stream frame observed on this stream: `Ok` on a
    /// clean end, `Err` when the `EndStreamResponse` carries a Connect error.
    pub fn end_stream_error(&self, payload: &[u8]) -> Result<()> {
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

// Deliberate unit tests: pure envelope framing and error decoding (CI floor);
// `tests/live.rs` proves the client against a real bridge.
#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::{connect_error, decode_frame, end_stream_error, envelope};

    #[test]
    fn envelope_prefixes_flag_and_length() {
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
    fn compressed_frame_is_rejected() {
        let mut body = envelope(b"x");
        body[0] = 0x01;
        let mut buffer = BytesMut::from(body.as_slice());
        let error = decode_frame(&mut buffer).expect_err("compression is never negotiated");
        assert!(error.to_string().contains("compressed"), "unexpected: {error}");
    }

    #[test]
    fn end_stream_flag_is_detected() {
        let mut buffer = BytesMut::from(envelope(b"{}").as_slice());
        let mut frame = decode_frame(&mut buffer).expect("decodes").expect("one frame");
        assert!(!frame.is_end_stream());
        frame.flags = 0x02;
        assert!(frame.is_end_stream());
    }

    #[test]
    fn end_stream_error_surfaces_code_and_message() {
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
    fn connect_error_prefers_the_structured_body() {
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
}
