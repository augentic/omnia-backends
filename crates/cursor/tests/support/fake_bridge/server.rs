//! The `sdk.v1` fake itself: bearer-checked routing, the per-process agent
//! registry, parks and hangs at the scripted points, the run streams each
//! script produces, and the `CallCustomTool` POST a tool script makes back
//! to the client's callback endpoint.

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::channel::{Channel, Sender};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message as _;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio::time::sleep;

use super::log::{Kind, Log, Recorder, Rpc};
use super::proto::{
    CallCustomToolRequest, CallCustomToolResponse, struct_to_value, value_to_struct,
};
use super::{Codec, Config, Fault, Point, Releases, Script, Then};

/// Lines the fake writes to stderr before it dies on purpose; a client that
/// leaks the stderr tail into an error message leaks these.
pub const MARKERS: [&str; 2] = [
    "fake-bridge marker: workspace /Users/someone/secret-project",
    "fake-bridge marker: session token sk-fake-0000",
];
/// The exit code of an `ExitOnCreate` fault.
pub const EXIT_ON_CREATE: i32 = 7;

const END_STREAM: u8 = 0x02;
const CALLBACK_PATH: &str = "/sdk.v1.SdkCustomToolCallbackService/CallCustomTool";

type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type Body = BoxBody<Bytes, BoxError>;

/// The callback identity the client started a spawned fake with.
#[derive(Clone, Debug)]
pub struct Callback {
    pub url: String,
    pub token: String,
}

pub struct Server {
    config: Config,
    /// The process number claimed through the log (1-based, in start order).
    process: usize,
    token: String,
    callback: Option<Callback>,
    recorder: Recorder,
    /// The release counts the test publishes, which a park waits on.
    releases: PathBuf,
    state: Mutex<State>,
    shutdown: Notify,
    http: HyperClient<HttpConnector, Body>,
}

#[derive(Default)]
struct State {
    next_agent: usize,
    creates: usize,
    sends: usize,
    agents: HashMap<String, AgentState>,
    /// Live runs by id; firing one ends its stream as cancelled.
    runs: HashMap<String, oneshot::Sender<()>>,
}

struct AgentState {
    api_key: String,
    rounds: usize,
    closed: bool,
}

struct Run {
    id: String,
    agent: String,
    text: String,
    round: usize,
    reset: bool,
}

enum Outcome {
    Finished(String),
    Cancelled,
    Failed(String),
}

impl Server {
    pub fn new(
        config: Config, process: usize, callback: Option<Callback>, recorder: Recorder,
        releases: PathBuf,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            process,
            token: gen_token(),
            callback,
            recorder,
            releases,
            state: Mutex::new(State::default()),
            shutdown: Notify::new(),
            http: HyperClient::builder(TokioExecutor::new()).build_http(),
        })
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub const fn recorder(&self) -> &Recorder {
        &self.recorder
    }

    /// Resolves once a `Shutdown` RPC has been answered.
    pub async fn shutdown_requested(&self) {
        self.shutdown.notified().await;
    }

    /// The faults in effect for this process.
    pub fn faults(&self) -> impl Iterator<Item = &Fault> {
        self.config.faults.iter().filter(|f| f.applies(self.process)).map(|f| &f.fault)
    }

    pub fn has(&self, fault: &Fault) -> bool {
        self.faults().any(|f| f == fault)
    }

    pub fn find<T>(&self, pick: impl Fn(&Fault) -> Option<T>) -> Option<T> {
        self.faults().find_map(pick)
    }

    // A peer's `Send` is recorded before its init event is observed; wait
    // that extra beat so the client notes the run id while we still hold
    // the winner.
    async fn await_peer(&self, rpc: Rpc) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            if Log::read(self.recorder.log_path())
                .events
                .iter()
                .any(|event| event.process != self.process && event.rpc() == Some(rpc))
            {
                sleep(Duration::from_millis(100)).await;
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    // The nth call of some RPC is the one a counted fault targets when a
    // scripted fault, read through `pick`, names that ordinal.
    fn targets(&self, ordinal: usize, pick: impl Fn(&Fault) -> Option<usize>) -> bool {
        self.faults().any(|f| pick(f) == Some(ordinal))
    }

    /// Accept connections until the listener fails or the task is dropped.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                sleep(Duration::from_millis(10)).await;
                continue;
            };
            let server = Arc::clone(&self);
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    let server = Arc::clone(&server);
                    async move { Ok::<_, Infallible>(server.handle(request).await) }
                });
                let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
            });
        }
    }

    async fn handle(self: Arc<Self>, request: Request<Incoming>) -> Response<Body> {
        let (parts, body) = request.into_parts();
        if !self.authorized(&parts.headers) {
            return connect_error(StatusCode::UNAUTHORIZED, "unauthenticated", "bad bearer token");
        }
        let Ok(body) = body.collect().await.map(http_body_util::Collected::to_bytes) else {
            return connect_error(StatusCode::BAD_REQUEST, "invalid_argument", "unreadable body");
        };
        match parts.uri.path() {
            "/sdk.v1.SdkBridgeControlService/Ping" => self.ping().await,
            "/sdk.v1.SdkBridgeControlService/GetVersion" => self.get_version(),
            "/sdk.v1.SdkBridgeControlService/Shutdown" => self.shutdown().await,
            "/sdk.v1.SdkAgentService/CreateAgent" => self.create_agent(&body).await,
            "/sdk.v1.SdkAgentService/Send" => self.send(&body).await,
            "/sdk.v1.SdkAgentService/CancelRun" => self.cancel_run(&body),
            "/sdk.v1.SdkAgentService/CloseAgent" => self.close_agent(&body).await,
            "/sdk.v1.SdkAgentService/DeleteAgent" => self.delete_agent(&body).await,
            other => connect_error(
                StatusCode::NOT_FOUND,
                "unimplemented",
                &format!("unknown procedure {other}"),
            ),
        }
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            == Some(self.token.as_str())
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn record(&self, rpc: Rpc, agent: Option<&str>, arg: Value) {
        self.recorder.record(Kind::Rpc(rpc), agent, arg);
    }

    // Hold here when the script says so: forever for a hang, until released
    // for a park.
    async fn checkpoint(&self, point: Point) {
        if self.has(&Fault::Hang(point)) {
            std::future::pending::<()>().await;
        }
        if self.has(&Fault::Park(point)) {
            self.park(point).await;
        }
    }

    // Take a ticket for `point` and wait until the test has released that
    // many requests there. Cancel-safe: dropping this leaves the ticket
    // taken, which the test's `parked` count documents.
    async fn park(&self, point: Point) {
        let ticket = self.recorder.park(point);
        while Releases::read(&self.releases).count(point) <= ticket {
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn ping(&self) -> Response<Body> {
        self.record(Rpc::Ping, None, Value::Null);
        self.checkpoint(Point::Ping).await;
        empty()
    }

    fn get_version(&self) -> Response<Body> {
        self.record(Rpc::GetVersion, None, Value::Null);
        ok(&json!({ "bridgeVersion": "fake", "protocolVersion": "sdk.v1", "capabilities": [] }))
    }

    async fn shutdown(&self) -> Response<Body> {
        self.record(Rpc::Shutdown, None, Value::Null);
        self.checkpoint(Point::Shutdown).await;
        self.shutdown.notify_one();
        empty()
    }

    async fn create_agent(&self, body: &[u8]) -> Response<Body> {
        let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let options = &request["options"];
        let api_key = options["apiKey"].as_str().unwrap_or_default();
        let arg = json!({
            "cwd": options["local"]["cwd"][0],
            "apiKeyPresent": !api_key.is_empty(),
            "model": options["model"]["id"],
            "customTools": keys(&options["local"]["customTools"]),
            "mcpServers": keys(&options["mcpServers"]),
        });

        let (ordinal, id) = {
            let mut state = self.state();
            state.creates += 1;
            let id = if self.has(&Fault::EmptyId) {
                None
            } else {
                state.next_agent += 1;
                let id = format!("agent-{}", state.next_agent);
                state.agents.insert(
                    id.clone(),
                    AgentState {
                        api_key: api_key.to_owned(),
                        rounds: 0,
                        closed: false,
                    },
                );
                Some(id)
            };
            (state.creates, id)
        };
        if self.targets(ordinal, |f| match f {
            Fault::ExitOnCreate(n) => Some(*n),
            _ => None,
        }) {
            std::process::exit(EXIT_ON_CREATE);
        }
        self.record(Rpc::CreateAgent, id.as_deref(), arg);
        self.checkpoint(Point::CreateAgent).await;

        let Some(id) = id else {
            return ok(&json!({ "agentId": "" }));
        };
        ok(&json!({ "agentId": id, "model": { "id": options["model"]["id"] } }))
    }

    async fn send(self: &Arc<Self>, body: &[u8]) -> Response<Body> {
        // The request rides as one Connect envelope: a 5-byte prefix, then
        // the JSON `SendRequest`.
        let Some(request) =
            body.get(5..).and_then(|json| serde_json::from_slice::<Value>(json).ok())
        else {
            return connect_error(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "malformed SendRequest envelope",
            );
        };
        let agent = request["agentId"].as_str().unwrap_or_default().to_owned();
        let text = request["message"]["text"].as_str().unwrap_or_default().to_owned();

        let (ordinal, round) = {
            let mut state = self.state();
            state.sends += 1;
            let ordinal = state.sends;
            let Some(entry) = state.agents.get_mut(&agent) else {
                return not_found(&agent);
            };
            if entry.closed {
                return connect_error(
                    StatusCode::PRECONDITION_FAILED,
                    "failed_precondition",
                    &format!("Agent {agent} is closed"),
                );
            }
            entry.rounds += 1;
            let round = entry.rounds - 1;
            drop(state);
            (ordinal, round)
        };
        self.record(Rpc::Send, Some(&agent), json!({ "text": text }));

        if self.targets(ordinal, |f| match f {
            Fault::KillOnSend(n) => Some(*n),
            _ => None,
        }) {
            write_markers();
            kill_self();
        }
        self.checkpoint(Point::Send).await;

        let run = Run {
            id: format!("run-{ordinal}"),
            agent,
            text,
            round,
            reset: self.targets(ordinal, |f| match f {
                Fault::ResetStream(n) => Some(*n),
                _ => None,
            }),
        };
        let (sender, stream) = Channel::<Bytes, BoxError>::new(16);
        tokio::spawn(Arc::clone(self).produce(run, sender));
        reply(StatusCode::OK, "application/connect+json", stream.boxed())
    }

    // Produce the run stream: the run id first, then whatever the script
    // says, then the terminal result and the end-of-stream frame.
    async fn produce(self: Arc<Self>, run: Run, mut tx: Sender<Bytes, BoxError>) {
        let init = json!({
            "sdkMessage": { "type": "system", "message": { "subtype": "init", "run_id": run.id } }
        });
        if tx.send_data(envelope(0, &init)).await.is_err() {
            return;
        }
        if run.reset {
            // hyper writes a queued frame out only once the body pends; an
            // abort straight after the send would discard it unflushed, and
            // the reset must land *after* the client has the run id.
            sleep(Duration::from_millis(100)).await;
            tx.abort("stream reset".into());
            return;
        }

        let (cancel_tx, cancel_rx) = oneshot::channel();
        self.state().runs.insert(run.id.clone(), cancel_tx);
        let outcome = self.drive(&run, &mut tx, cancel_rx).await;
        self.state().runs.remove(&run.id);

        let result = match outcome {
            Outcome::Finished(answer) => json!({
                "result": {
                    "runId": run.id,
                    "status": "RUN_LIFECYCLE_STATUS_FINISHED",
                    "result": { "runId": run.id, "result": answer },
                },
                "done": {},
            }),
            Outcome::Cancelled => json!({
                "result": { "runId": run.id, "status": "RUN_LIFECYCLE_STATUS_CANCELLED" },
                "done": {},
            }),
            Outcome::Failed(detail) => json!({
                "result": {
                    "runId": run.id,
                    "status": "RUN_LIFECYCLE_STATUS_ERROR",
                    "errorCode": detail,
                },
                "done": {},
            }),
        };
        if tx.send_data(envelope(0, &result)).await.is_err() {
            return;
        }
        let _ = tx.send_data(envelope(END_STREAM, &json!({}))).await;
    }

    async fn drive(
        &self, run: &Run, tx: &mut Sender<Bytes, BoxError>, mut cancel: oneshot::Receiver<()>,
    ) -> Outcome {
        if self.has(&Fault::Hang(Point::Stream)) {
            std::future::pending::<()>().await;
        }
        if let Some(rpc) = self.find(|fault| match fault {
            Fault::WaitForPeer(rpc) => Some(*rpc),
            _ => None,
        }) {
            self.await_peer(rpc).await;
        }
        if self.has(&Fault::Park(Point::Stream)) {
            tokio::select! {
                () = self.park(Point::Stream) => {}
                _ = &mut cancel => return Outcome::Cancelled,
            }
        }
        match &self.config.script {
            Script::Echo => Outcome::Finished(echo_of(&run.text)),
            Script::Replies(replies) => Outcome::Finished(
                replies.get(run.round).or_else(|| replies.last()).cloned().unwrap_or_default(),
            ),
            Script::Tool { name, args, codec } => self.call_tool(run, name, args, *codec).await,
            Script::Paced {
                every_ms,
                frames,
                then,
            } => {
                for index in 0..*frames {
                    tokio::select! {
                        () = sleep(Duration::from_millis(*every_ms)) => {}
                        _ = &mut cancel => return Outcome::Cancelled,
                    }
                    let tick = json!({
                        "sdkMessage": {
                            "type": "assistant",
                            "message": { "run_id": run.id, "text": format!("tick {index}") },
                        }
                    });
                    if tx.send_data(envelope(0, &tick)).await.is_err() {
                        return Outcome::Cancelled;
                    }
                }
                match then {
                    Then::Finish => Outcome::Finished(echo_of(&run.text)),
                    Then::Hang => {
                        let _ = cancel.await;
                        Outcome::Cancelled
                    }
                }
            }
        }
    }

    async fn call_tool(&self, run: &Run, name: &str, args: &Value, codec: Codec) -> Outcome {
        let Some(callback) = &self.callback else {
            return Outcome::Failed("no --tool-callback-url to call the tool through".to_owned());
        };
        let (content_type, body) = match codec {
            Codec::Json => (
                "application/json",
                full(json!({ "toolName": name, "args": args, "agentId": run.agent }).to_string()),
            ),
            Codec::JsonChunked => (
                "application/json",
                chunked(
                    json!({ "toolName": name, "args": args, "agentId": run.agent }).to_string(),
                ),
            ),
            Codec::Proto => {
                let request = CallCustomToolRequest {
                    tool_name: name.to_owned(),
                    args: args.as_object().map(value_to_struct),
                    tool_call_id: Some(format!("{}-call", run.id)),
                    agent_id: run.agent.clone(),
                };
                ("application/proto", full(request.encode_to_vec()))
            }
        };
        let request = Request::post(format!("{}{CALLBACK_PATH}", callback.url))
            .header(AUTHORIZATION, format!("Bearer {}", callback.token))
            .header(CONTENT_TYPE, content_type)
            .body(body)
            .expect("a well-formed callback request");

        let response = match self.http.request(request).await {
            Ok(response) => response,
            Err(error) => return Outcome::Failed(format!("tool callback failed: {error}")),
        };
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .map(http_body_util::Collected::to_bytes)
            .unwrap_or_default();
        self.recorder.record(
            Kind::Callback,
            Some(&run.agent),
            json!({ "status": status.as_u16(), "token": callback.token, "tool": name }),
        );
        if !status.is_success() {
            let detail: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            return Outcome::Failed(format!(
                "tool call rejected ({status}, {}): {}",
                detail["code"].as_str().unwrap_or("unknown"),
                detail["message"].as_str().unwrap_or_default()
            ));
        }
        let result = match codec {
            Codec::Json | Codec::JsonChunked => {
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null)["result"].clone()
            }
            Codec::Proto => CallCustomToolResponse::decode(bytes)
                .ok()
                .and_then(|response| response.result)
                .map_or(Value::Null, |fields| struct_to_value(&fields)),
        };
        Outcome::Finished(answer_from(&result))
    }

    fn cancel_run(&self, body: &[u8]) -> Response<Body> {
        let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let run_id = request["runId"].as_str().unwrap_or_default().to_owned();
        let agent = request["agentId"].as_str().map(ToOwned::to_owned);
        self.record(Rpc::CancelRun, agent.as_deref(), json!({ "runId": run_id }));
        let cancel = self.state().runs.remove(&run_id);
        if let Some(cancel) = cancel {
            let _ = cancel.send(());
        }
        empty()
    }

    async fn close_agent(&self, body: &[u8]) -> Response<Body> {
        let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let agent = request["agentId"].as_str().unwrap_or_default().to_owned();
        if !self.state().agents.contains_key(&agent) {
            return not_found(&agent);
        }
        self.record(Rpc::CloseAgent, Some(&agent), Value::Null);
        self.checkpoint(Point::CloseAgent).await;
        if self.has(&Fault::CloseFails) {
            return connect_error(StatusCode::INTERNAL_SERVER_ERROR, "internal", "store locked");
        }
        if let Some(entry) = self.state().agents.get_mut(&agent) {
            entry.closed = true;
        }
        empty()
    }

    async fn delete_agent(&self, body: &[u8]) -> Response<Body> {
        let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let agent = request["agentId"].as_str().unwrap_or_default().to_owned();
        let api_key = request["options"]["apiKey"].as_str().unwrap_or_default();
        let Some(api_key_matches_create) =
            self.state().agents.get(&agent).map(|state| state.api_key == api_key)
        else {
            return not_found(&agent);
        };
        self.record(
            Rpc::DeleteAgent,
            Some(&agent),
            json!({
                "cwd": request["options"]["cwd"],
                "apiKeyPresent": !api_key.is_empty(),
                "apiKeyMatchesCreate": api_key_matches_create,
            }),
        );
        self.checkpoint(Point::DeleteAgent).await;
        self.state().agents.remove(&agent);
        empty()
    }
}

/// The last user turn of a rendered prompt: the block before the format
/// instruction the host appends, which is what the echo default answers.
pub fn echo_of(prompt: &str) -> String {
    let blocks: Vec<&str> = prompt.split("\n\n").collect();
    match blocks.len() {
        0 | 1 => prompt.to_owned(),
        n => blocks[n - 2].to_owned(),
    }
}

// Spell a `CallCustomTool` result as the answer text: a repairable `error`
// becomes the model's `tool failed:` line, and the endpoint's `{ "value": v }`
// wrapping of a non-object output is unwrapped back to the output.
fn answer_from(result: &Value) -> String {
    if let Some(error) = result.get("error").and_then(Value::as_str) {
        return format!("tool failed: {error}");
    }
    match (result.as_object(), result.get("value")) {
        (Some(object), Some(Value::String(text))) if object.len() == 1 => text.clone(),
        (Some(object), Some(value)) if object.len() == 1 => value.to_string(),
        _ => result.to_string(),
    }
}

pub fn write_markers() {
    for marker in MARKERS {
        eprintln!("{marker}");
    }
}

// `SIGKILL` ourselves as an OOM killer would: no handler runs, and the
// client sees `signal: 9 (SIGKILL)`. Delivered by a child, since std has no
// `raise`; the signal lands before `kill` has even exited.
fn kill_self() -> ! {
    let pid = std::process::id().to_string();
    let status = std::process::Command::new("kill").args(["-KILL", &pid]).status();
    panic!("kill -KILL {pid} left us running: {status:?}");
}

fn keys(object: &Value) -> Vec<String> {
    object.as_object().map(|map| map.keys().cloned().collect()).unwrap_or_default()
}

fn gen_token() -> String {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).expect("entropy for the fake's token");
    bytes.iter().fold(String::with_capacity(32), |mut hex, byte| {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

/// One Connect envelope: flags, big-endian length, JSON payload.
fn envelope(flags: u8, payload: &Value) -> Bytes {
    let payload = payload.to_string();
    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(flags);
    frame.extend_from_slice(&u32::try_from(payload.len()).expect("frame fits").to_be_bytes());
    frame.extend_from_slice(payload.as_bytes());
    Bytes::from(frame)
}

fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(|never: Infallible| match never {}).boxed()
}

// Frame the same bytes as a chunked body, split ten bytes at a time, the way
// a streaming callback client may.
fn chunked(payload: String) -> Body {
    let payload = payload.into_bytes();
    let (mut tx, body) = Channel::<Bytes, BoxError>::new(payload.len() / 10 + 2);
    for chunk in payload.chunks(10) {
        let _ = tx.try_send(Frame::data(Bytes::copy_from_slice(chunk)));
    }
    drop(tx);
    body.boxed()
}

fn reply(status: StatusCode, content_type: &'static str, body: Body) -> Response<Body> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response.headers_mut().insert(CONTENT_TYPE, http::HeaderValue::from_static(content_type));
    response
}

fn ok(payload: &Value) -> Response<Body> {
    reply(StatusCode::OK, "application/json", full(payload.to_string()))
}

fn empty() -> Response<Body> {
    ok(&json!({}))
}

fn not_found(agent: &str) -> Response<Body> {
    connect_error(StatusCode::NOT_FOUND, "not_found", &format!("Agent {agent} not found"))
}

fn connect_error(status: StatusCode, code: &str, message: &str) -> Response<Body> {
    reply(status, "application/json", full(json!({ "code": code, "message": message }).to_string()))
}
