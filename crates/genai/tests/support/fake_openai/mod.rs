//! A scripted OpenAI-compatible chat-completions endpoint served in-process
//! on loopback — what `GENAI_ENDPOINT` points the backend at under test.
//!
//! The fake records every request body and bearer it receives, answers by a
//! [`Script`], and can hold requests at a gate (so a fan-out is seen to be
//! concurrent), park replies until the test releases them (so a completion
//! can be dropped mid-request), or fail every request one way.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

/// What the fake answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Script {
    /// The last user turn's text, back as the answer.
    Echo,
    /// Request `n` gets `replies[n]`; the last reply repeats.
    Replies(Vec<String>),
    /// A conversation's first request gets one call of `name` with `args`;
    /// the request carrying the tool's result gets that result as the
    /// answer — a repairable failure line rephrased as `tool failed:
    /// reason`, the phrasing the shared guests expect.
    ToolCalls { name: String, args: Value },
}

/// How every request fails, when one does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// An error status with an OpenAI-shaped error body; `retry_after`
    /// adds the header, in seconds.
    Status { status: u16, retry_after: Option<u64> },
    /// A 200 whose JSON body ends mid-object.
    TruncatedBody,
    /// Every reply is held until the test releases it.
    Park,
}

/// A fake's script, fault, and gate.
#[derive(Clone, Debug)]
pub struct Config {
    script: Script,
    fault: Option<Fault>,
    gate: Option<usize>,
}

impl Config {
    pub const fn echo() -> Self {
        Self {
            script: Script::Echo,
            fault: None,
            gate: None,
        }
    }

    pub fn replies(replies: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            script: Script::Replies(replies.into_iter().map(Into::into).collect()),
            fault: None,
            gate: None,
        }
    }

    pub fn tool(name: &str, args: Value) -> Self {
        Self {
            script: Script::ToolCalls {
                name: name.to_owned(),
                args,
            },
            fault: None,
            gate: None,
        }
    }

    #[must_use]
    pub const fn fault(mut self, fault: Fault) -> Self {
        self.fault = Some(fault);
        self
    }

    /// Hold every request until `width` are in flight at once, then answer
    /// them all; a fan-out the backend serialised would never fill it.
    #[must_use]
    pub const fn gate(mut self, width: usize) -> Self {
        self.gate = Some(width);
        self
    }
}

/// One request as the fake received it.
#[derive(Clone, Debug)]
pub struct Recorded {
    /// The request path.
    pub path: String,
    /// The JSON body.
    pub body: Value,
    /// The `Authorization: Bearer` token, if any.
    pub bearer: Option<String>,
}

impl Recorded {
    /// The `messages` as `(role, content)` pairs, content flattened to text.
    pub fn messages(&self) -> Vec<(String, String)> {
        self.body["messages"]
            .as_array()
            .map(|messages| {
                messages
                    .iter()
                    .map(|m| {
                        (m["role"].as_str().unwrap_or_default().to_owned(), text_of(&m["content"]))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The content of the last message with `role`.
    pub fn last(&self, role: &str) -> Option<String> {
        self.messages().into_iter().rev().find(|(r, _)| r == role).map(|(_, content)| content)
    }

    /// The names of the tools the request advertises.
    pub fn tools(&self) -> Vec<String> {
        self.body["tools"]
            .as_array()
            .map(|tools| {
                tools
                    .iter()
                    .filter_map(|tool| tool["function"]["name"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }
}

// OpenAI content is a string or an array of typed parts.
fn text_of(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => {
            parts.iter().filter_map(|part| part["text"].as_str()).collect::<Vec<_>>().join("")
        }
        _ => String::new(),
    }
}

struct State {
    config: Config,
    requests: Mutex<Vec<Recorded>>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    abandoned: AtomicUsize,
    parked: Mutex<Vec<oneshot::Sender<()>>>,
    gate: watch::Sender<bool>,
}

/// The fake, serving until dropped.
pub struct FakeOpenAi {
    state: Arc<State>,
    url: String,
    serving: JoinHandle<()>,
}

impl FakeOpenAi {
    /// Serve `config` on an ephemeral loopback port.
    pub async fn serve(config: Config) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind loopback");
        let addr = listener.local_addr().expect("local address");
        let (gate, _) = watch::channel(config.gate.is_none());
        let state = Arc::new(State {
            config,
            requests: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            abandoned: AtomicUsize::new(0),
            parked: Mutex::new(Vec::new()),
            gate,
        });
        let serving = tokio::spawn(accept(listener, Arc::clone(&state)));
        Self {
            state,
            url: format!("http://{addr}/v1/"),
            serving,
        }
    }

    /// The base URL for `ConnectOptions::endpoint`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Every request received so far, in order.
    pub fn requests(&self) -> Vec<Recorded> {
        self.state.requests.lock().expect("requests lock").clone()
    }

    /// The most requests in flight at once.
    pub fn peak_in_flight(&self) -> usize {
        self.state.peak.load(Ordering::SeqCst)
    }

    /// Requests whose connection the client dropped before they were
    /// answered.
    pub fn abandoned(&self) -> usize {
        self.state.abandoned.load(Ordering::SeqCst)
    }

    /// Replies held right now.
    pub fn parked(&self) -> usize {
        self.state.parked.lock().expect("parked lock").len()
    }

    /// Wait until `count` replies are held.
    pub async fn await_parked(&self, count: usize) {
        poll(
            || self.parked() >= count,
            Duration::from_secs(60),
            &format!("{count} parked reply(s)"),
        )
        .await;
    }

    /// Release the oldest held reply; `false` when none is held.
    pub fn release_one(&self) -> bool {
        let mut parked = self.state.parked.lock().expect("parked lock");
        if parked.is_empty() {
            return false;
        }
        let _ = parked.remove(0).send(());
        true
    }

    /// Release every held reply.
    pub fn release_all(&self) {
        for release in self.state.parked.lock().expect("parked lock").drain(..) {
            let _ = release.send(());
        }
    }
}

impl Drop for FakeOpenAi {
    fn drop(&mut self) {
        self.serving.abort();
    }
}

async fn accept(listener: TcpListener, state: Arc<State>) {
    while let Ok((stream, _)) = listener.accept().await {
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let service = service_fn(move |request| handle(Arc::clone(&state), request));
            let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
        });
    }
}

/// One request in flight: counts down on drop, and a drop before the answer
/// went out is the client giving up on it.
struct InFlight {
    state: Arc<State>,
    answered: bool,
}

impl InFlight {
    fn start(state: &Arc<State>) -> Self {
        let now = state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        state.peak.fetch_max(now, Ordering::SeqCst);
        if state.config.gate.is_some_and(|width| now >= width) {
            let _ = state.gate.send(true);
        }
        Self {
            state: Arc::clone(state),
            answered: false,
        }
    }

    fn answer(mut self, response: Response<Full<Bytes>>) -> Response<Full<Bytes>> {
        self.answered = true;
        response
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.state.in_flight.fetch_sub(1, Ordering::SeqCst);
        if !self.answered {
            self.state.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }
}

async fn handle(
    state: Arc<State>, request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let in_flight = InFlight::start(&state);
    let path = request.uri().path().to_owned();
    let bearer = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_owned);
    let body = request.into_body().collect().await?.to_bytes();
    let recorded = Recorded {
        path,
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        bearer,
    };
    let round = {
        let mut requests = state.requests.lock().expect("requests lock");
        requests.push(recorded.clone());
        requests.len() - 1
    };

    if state.config.gate.is_some() {
        let mut open = state.gate.subscribe();
        let opened = tokio::time::timeout(Duration::from_secs(30), open.wait_for(|open| *open));
        if opened.await.is_err() {
            let message = format!(
                "the gate of {} never filled: {} in flight",
                state.config.gate.unwrap_or_default(),
                state.in_flight.load(Ordering::SeqCst)
            );
            return Ok(in_flight.answer(error(StatusCode::INTERNAL_SERVER_ERROR, &message, None)));
        }
    }

    match &state.config.fault {
        Some(Fault::Park) => {
            let (release, released) = oneshot::channel();
            state.parked.lock().expect("parked lock").push(release);
            // `Err` is the fake going away; answer regardless.
            let _ = released.await;
        }
        Some(Fault::Status { status, retry_after }) => {
            let status = StatusCode::from_u16(*status).expect("a valid status");
            return Ok(in_flight.answer(error(status, "scripted failure", *retry_after)));
        }
        Some(Fault::TruncatedBody) => {
            let full = completion(Some("never arrives"), None).to_string();
            let cut = Bytes::from(full[..full.len() / 2].to_owned());
            return Ok(in_flight.answer(json_response(StatusCode::OK, cut)));
        }
        None => {}
    }

    let body = match &state.config.script {
        Script::Echo => completion(Some(&recorded.last("user").unwrap_or_default()), None),
        Script::Replies(replies) => {
            let reply = replies.get(round).or_else(|| replies.last());
            completion(Some(reply.map_or("", String::as_str)), None)
        }
        Script::ToolCalls { name, args } => {
            let call = json!([{
                "id": format!("call_{}", round + 1),
                "type": "function",
                "function": { "name": name, "arguments": args.to_string() },
            }]);
            recorded.last("tool").map_or_else(
                || completion(None, Some(call)),
                |result| completion(Some(&answer_from(&result)), None),
            )
        }
    };
    Ok(in_flight.answer(json_response(StatusCode::OK, Bytes::from(body.to_string()))))
}

// The answer a "model" gives for a tool result: the backend's repairable
// failure line, `tool \`name\` failed: reason`, becomes `tool failed: reason`.
fn answer_from(result: &str) -> String {
    result
        .strip_prefix("tool `")
        .and_then(|rest| rest.split_once("` failed: "))
        .map_or_else(|| result.to_owned(), |(_, reason)| format!("tool failed: {reason}"))
}

/// One OpenAI-shaped chat completion: `content`, or `tool_calls`.
fn completion(content: Option<&str>, tool_calls: Option<Value>) -> Value {
    let mut message = json!({ "role": "assistant", "content": content });
    let finish_reason = tool_calls.map_or("stop", |tool_calls| {
        message["tool_calls"] = tool_calls;
        "tool_calls"
    });
    json!({
        "id": "chatcmpl-fake",
        "object": "chat.completion",
        "model": "gpt-4o-mini",
        "choices": [{ "index": 0, "finish_reason": finish_reason, "message": message }],
    })
}

fn error(status: StatusCode, message: &str, retry_after: Option<u64>) -> Response<Full<Bytes>> {
    let body = json!({ "error": { "message": message, "type": "fake_error" } });
    let mut response = json_response(status, Bytes::from(body.to_string()));
    if let Some(seconds) = retry_after {
        response.headers_mut().insert(RETRY_AFTER, seconds.into());
    }
    response
}

fn json_response(status: StatusCode, body: Bytes) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(body))
        .expect("a well-formed response")
}

/// The key the SDK's `OpenAI` adapter reads per request, when the
/// environment has none. Once per process, before any runtime thread exists.
pub fn dummy_key() {
    static SET: Once = Once::new();
    SET.call_once(|| {
        if std::env::var_os("OPENAI_API_KEY").is_none() {
            // SAFETY: set once, never unset, before the tests spawn threads.
            unsafe { std::env::set_var("OPENAI_API_KEY", "test-key") }
        }
    });
}

/// The bearer the fake should see: whatever `OPENAI_API_KEY` is.
pub fn expected_key() -> String {
    std::env::var("OPENAI_API_KEY").expect("dummy_key() ran")
}

/// Poll `ready` until it holds, failing after `within`.
pub async fn poll(mut ready: impl FnMut() -> bool, within: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + within;
    while !ready() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
