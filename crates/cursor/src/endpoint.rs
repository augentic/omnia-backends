//! Loopback `CallCustomTool` server: the bridge calls back here to execute a
//! guest-declared function tool, and the call routes into the completion's
//! session through [`ToolHost::call_tool`].
//!
//! The server binds `127.0.0.1:0` and accepts both Connect unary codecs (the
//! bridge picks the content type). Each bridge process is registered under
//! its own bearer token, and that token selects the process's own agent
//! table: agent ids are the bridge's to choose, so two live processes may
//! pick the same one, and a callback routes by the token it carries as well
//! as the id it names. Budgets, per-call timeouts, oversize checks, and id
//! correlation are enforced host-side inside `call_tool`.

mod proto;

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::fmt::{self, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result};
use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt as _, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use omnia_wasi_model::ToolHost;
use prost::Message as _;
use proto::{CallCustomToolRequest, CallCustomToolResponse, struct_to_value, value_to_struct};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::lock;

const PATH: &str = "/sdk.v1.SdkCustomToolCallbackService/CallCustomTool";
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// The bound loopback endpoint; dropping it stops serving.
#[derive(Debug)]
pub struct Endpoint {
    url: String,
    handler: Arc<Handler>,
    server: tokio::task::JoinHandle<()>,
}

impl Endpoint {
    /// Bind `127.0.0.1:0` and start serving.
    ///
    /// # Errors
    ///
    /// Returns an error when the loopback bind fails.
    pub async fn bind() -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("binding the tool-callback listener")?;
        let addr = listener.local_addr().context("reading the tool-callback address")?;

        let handler = Arc::new(Handler::default());
        let server = tokio::spawn(serve(listener, Arc::clone(&handler)));

        Ok(Self {
            url: format!("http://{addr}"),
            handler,
            server,
        })
    }

    /// Register one bridge process: a fresh bearer token for it to call
    /// back with, and its own agent table behind that token. Dropping the
    /// registration revokes the token.
    ///
    /// # Errors
    ///
    /// Returns an error when the system random source is unavailable.
    pub fn register(&self) -> Result<Registration> {
        let token = gen_token()?;
        let sessions = Arc::new(Sessions::default());
        lock(&self.handler.bridges).insert(token.clone(), Arc::clone(&sessions));
        Ok(Registration {
            handler: Arc::clone(&self.handler),
            url: self.url.clone(),
            token,
            sessions,
        })
    }
}

// Accept until the `Endpoint` drops, one connection task per bridge socket.
async fn serve(listener: TcpListener, handler: Arc<Handler>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                tracing::warn!(%error, "tool-callback accept error");
                // don't spin on persistent failure (e.g. fd exhaustion)
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let handler = Arc::clone(&handler);
                async move { Ok::<_, Infallible>(handler.handle(request).await) }
            });
            if let Err(error) =
                http1::Builder::new().serve_connection(TokioIo::new(stream), service).await
            {
                tracing::warn!(%error, "tool-callback connection error");
            }
        });
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// One bridge process's callback identity: the URL and bearer token it is
/// started with, and the agents routed under that token.
#[must_use]
pub struct Registration {
    handler: Arc<Handler>,
    url: String,
    token: String,
    sessions: Arc<Sessions>,
}

impl Registration {
    /// The full callback URL handed to the bridge (`--tool-callback-url`).
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The bearer token handed to the bridge (`--tool-callback-auth-token`).
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Route this bridge's callbacks for `agent_id` into `tool_host` until
    /// the returned guard drops.
    pub fn attach(
        &self, agent_id: String, tool_host: Arc<dyn ToolHost>, abort: mpsc::UnboundedSender<String>,
    ) -> Attached {
        self.sessions.insert(agent_id.clone(), Session { tool_host, abort });
        Attached {
            sessions: Arc::clone(&self.sessions),
            agent_id,
        }
    }
}

impl std::fmt::Debug for Registration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registration")
            .field("url", &self.url)
            .field("sessions", &self.sessions)
            .finish_non_exhaustive()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        lock(&self.handler.bridges).remove(&self.token);
    }
}

/// Detaches its agent on drop.
#[must_use]
pub struct Attached {
    sessions: Arc<Sessions>,
    agent_id: String,
}

impl Drop for Attached {
    fn drop(&mut self) {
        self.sessions.remove(&self.agent_id);
    }
}

#[derive(Default)]
struct Handler {
    /// Registered bridge processes' agent tables, by bearer token.
    bridges: Mutex<HashMap<String, Arc<Sessions>>>,
}

impl Handler {
    /// The agent table of the bridge whose bearer token the request carries.
    fn authorize(&self, headers: &HeaderMap) -> Option<Arc<Sessions>> {
        let token = headers.get(AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ")?;
        lock(&self.bridges).get(token).cloned()
    }

    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let (parts, body) = request.into_parts();

        // Reject on the head alone — no body byte of an unauthenticated
        // request is ever buffered — but discard the body before answering:
        // closing with unread bytes turns the reply into a TCP reset.
        let admitted = if parts.method != Method::POST {
            Err(connect_error(StatusCode::METHOD_NOT_ALLOWED, "unimplemented", "POST required"))
        } else if parts.uri.path() != PATH {
            Err(connect_error(StatusCode::NOT_FOUND, "not_found", "unknown callback path"))
        } else {
            self.authorize(&parts.headers).ok_or_else(|| {
                connect_error(StatusCode::UNAUTHORIZED, "unauthenticated", "bad bearer token")
            })
        };

        let sessions = match admitted {
            Ok(sessions) => sessions,
            Err(reply) => {
                let _ = tokio::time::timeout(DRAIN_TIMEOUT, drain(body, MAX_BODY_BYTES)).await;
                return reply;
            }
        };

        let body = match Limited::new(body, MAX_BODY_BYTES).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(error) if error.is::<LengthLimitError>() => {
                return connect_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "invalid_argument",
                    &format!("request body exceeds {MAX_BODY_BYTES} bytes"),
                );
            }
            Err(error) => {
                return connect_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_argument",
                    &format!("failed to read the request body: {error}"),
                );
            }
        };

        call_tool(&sessions, &parts.headers, body).await
    }
}

impl fmt::Debug for Handler {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handler").field("bridges", &lock(&self.bridges).len()).finish()
    }
}

/// One bridge's live completions by `agent_id`.
#[derive(Debug, Default)]
struct Sessions {
    entries: Mutex<HashMap<String, Session>>,
}

impl Sessions {
    fn insert(&self, agent_id: String, session: Session) {
        lock(&self.entries).insert(agent_id, session);
    }

    fn remove(&self, agent_id: &str) {
        lock(&self.entries).remove(agent_id);
    }

    fn lookup(&self, agent_id: &str) -> Option<Session> {
        lock(&self.entries).get(agent_id).cloned()
    }
}

/// One live completion's callback route: the session's tool host plus the
/// abort signal that ends the completion on a hard (non-repairable) tool
/// failure.
#[derive(Clone, Debug)]
struct Session {
    tool_host: Arc<dyn ToolHost>,
    abort: mpsc::UnboundedSender<String>,
}

#[derive(Clone, Copy)]
enum Codec {
    Json,
    Proto,
}

/// One `CallCustomTool` request, as the JSON codec spells it.
#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ToolCall {
    tool_name: String,
    args: Value,
    agent_id: String,
}

impl ToolCall {
    fn decode(codec: Codec, body: &[u8]) -> Result<Self> {
        let mut call = match codec {
            Codec::Json => {
                serde_json::from_slice(body).context("decoding the JSON callback body")?
            }
            Codec::Proto => {
                let call = CallCustomToolRequest::decode(body)
                    .context("decoding the protobuf callback body")?;
                Self {
                    tool_name: call.tool_name,
                    args: call.args.as_ref().map_or(Value::Null, struct_to_value),
                    agent_id: call.agent_id,
                }
            }
        };
        // a tool takes a JSON object; absent arguments are an empty one
        if call.args.is_null() {
            call.args = json!({});
        }
        Ok(call)
    }
}

// Execute one decoded callback against the calling bridge's agent table.
async fn call_tool(sessions: &Sessions, headers: &HeaderMap, body: Bytes) -> Response<Full<Bytes>> {
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

    let call = match ToolCall::decode(codec, &body) {
        Ok(call) => call,
        Err(error) => {
            return connect_error(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                &format!("{error:#}"),
            );
        }
    };

    let Some(session) = sessions.lookup(&call.agent_id) else {
        return connect_error(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("no live completion for agent `{}`", call.agent_id),
        );
    };

    let arguments = call.args.to_string();
    tracing::info!(monotonic_counter.cursor_custom_tool_calls = 1_u64, "custom tool callback");
    tracing::debug!(tool = %call.tool_name, agent = %call.agent_id, "custom tool callback");

    match session.tool_host.call_tool(call.tool_name.clone(), arguments).await {
        Ok(Ok(output)) => respond(codec, &wrap_output(&output)),
        Ok(Err(failure)) => respond(codec, &json!({ "error": failure })),
        Err(error) => {
            let message = format!("tool `{}` failed: {error:#}", call.tool_name);
            let _ = session.abort.send(message.clone());
            connect_error(StatusCode::CONFLICT, "aborted", &message)
        }
    }
}

// Read and discard up to `limit` body bytes; an oversize or failing body is
// abandoned where it stands.
async fn drain(mut body: Incoming, limit: usize) {
    let mut seen = 0;
    while let Some(Ok(frame)) = body.frame().await {
        if let Ok(data) = frame.into_data() {
            seen += data.len();
            if seen > limit {
                break;
            }
        }
    }
}

// Draw 256 bits of entropy and spell them as lowercase hex.
fn gen_token() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| anyhow::anyhow!("gathering entropy: {error}"))?;
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // writing to a `String` cannot fail
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

// The SDK wants a JSON object for a tool result; anything else the tool
// returned rides under `value`.
fn wrap_output(output: &str) -> Value {
    match serde_json::from_str::<Value>(output) {
        Ok(value @ Value::Object(_)) => value,
        Ok(value) => json!({ "value": value }),
        Err(_) => json!({ "value": output }),
    }
}

fn respond(codec: Codec, result: &Value) -> Response<Full<Bytes>> {
    match codec {
        Codec::Json => {
            reply(StatusCode::OK, "application/json", json!({ "result": result }).to_string())
        }
        Codec::Proto => {
            let response = CallCustomToolResponse {
                result: Some(result.as_object().map(value_to_struct).unwrap_or_default()),
            };
            reply(StatusCode::OK, "application/proto", response.encode_to_vec())
        }
    }
}

fn connect_error(status: StatusCode, code: &str, message: &str) -> Response<Full<Bytes>> {
    reply(status, "application/json", json!({ "code": code, "message": message }).to_string())
}

fn reply(
    status: StatusCode, content_type: &'static str, body: impl Into<Bytes>,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    response.headers_mut().insert(CONTENT_TYPE, http::HeaderValue::from_static(content_type));
    response
}

// The output-wrapping policy and the `Struct` codec — pure translation —
// are unit-tested here; the server itself is exercised by a bridge calling
// back in `tests/model.rs` and `tests/bridge.rs`.
#[cfg(test)]
mod tests {
    use serde_json::{Map, Value, json};

    use super::proto::{struct_to_value, value_to_struct};
    use super::wrap_output;

    #[test]
    fn output_wrapping() {
        assert_eq!(wrap_output(r#"{"answer":42}"#), json!({ "answer": 42 }));
        assert_eq!(wrap_output("[1,2]"), json!({ "value": [1, 2] }));
        assert_eq!(wrap_output(r#""text""#), json!({ "value": "text" }));
        assert_eq!(wrap_output("not json"), json!({ "value": "not json" }));
    }

    fn object(value: Value) -> Map<String, Value> {
        let Value::Object(object) = value else { panic!("an object: {value}") };
        object
    }

    #[test]
    fn struct_roundtrip() {
        let original = json!({
            "text": "hello",
            "count": 3,
            "ratio": 3.5,
            "flag": true,
            "none": null,
            "nested": { "list": [1, -2.5, "two", false, null, { "deep": "yes" }] },
        });
        assert_eq!(struct_to_value(&value_to_struct(&object(original.clone()))), original);
        assert_eq!(struct_to_value(&value_to_struct(&Map::new())), json!({}));
    }

    #[test]
    fn integral_doubles() {
        let read = |number: f64| struct_to_value(&value_to_struct(&object(json!({ "n": number }))));
        assert_eq!(read(42.0), json!({ "n": 42 }));
        assert_eq!(read(-7.0), json!({ "n": -7 }));
        assert_eq!(read(2.5), json!({ "n": 2.5 }));
        // past 2^53 an f64 no longer holds every integer, so the double stands
        assert_eq!(read(9_007_199_254_740_994.0), json!({ "n": 9_007_199_254_740_994.0 }));
    }
}
