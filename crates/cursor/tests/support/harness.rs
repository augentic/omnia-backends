//! The guest-driven harness the `model` and `bridge` suites share: a client
//! over the fake on `PATH`, a guest run through `omnia_test::host`, and the
//! wait for every spawned process to be gone that every row ends on.

use std::time::Duration;

use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{Method, Request};
use http_body_util::{BodyExt as _, Full};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use omnia::{Backend as _, ExitStatus};
use omnia_cursor::{Client, ConnectOptions};
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_model::WasiModel;
use serde_json::Value;

use super::fake_bridge::{self, History, Process, Rpc, Spawnable};

/// How long a lease's process may take to be gone once a guest has
/// returned, when the row expects no timeout on the way.
pub const GONE: Duration = Duration::from_secs(8);
/// How long a guest may take to reach its first RPC: its component is
/// compiled on the way, under whatever load the rest of the suite puts on
/// the machine.
pub const STARTUP: Duration = Duration::from_secs(120);
/// A bound that pins "at once": well under every 5s timeout the client
/// pays when a bridge will not answer.
pub const AT_ONCE: Duration = Duration::from_secs(4);

pub fn options(max_agents: usize) -> ConnectOptions {
    ConnectOptions {
        model: "auto".to_owned(),
        timeout_secs: 30,
        inactivity_secs: 10,
        max_agents,
    }
}

/// A client spawning one process of `fake` per lease: the fake is on
/// `PATH` from the moment it is laid out, so this is `connect` with the
/// dependency spelled out.
pub async fn spawning(fake: &Spawnable, max_agents: usize) -> Client {
    let client = connect(options(max_agents)).await;
    assert!(fake.log().process(0).is_some(), "the probe spawned the fake");
    client
}

/// A client over `options`, with the API key the backend requires.
pub async fn connect(options: ConnectOptions) -> Client {
    fake_bridge::dummy_key();
    Client::connect_with(options).await.expect("the fake handshakes")
}

/// Run one guest program over `client`, requiring a clean exit.
pub async fn run_guest(wasm: &str, args: &[&str], client: &Client) {
    let backends = Backends::defaults().await.model(client.clone());
    let status = Deployment::new()
        .guest("guest", wasm)
        .args(args.iter().copied())
        .run_host::<WasiModel, _>(backends)
        .await
        .expect("guest runs");
    assert_eq!(status, ExitStatus::SUCCESS, "guest `{wasm}` failed");
}

/// The `expect_error` guest: the completion fails and its detail carries
/// `needle`; `flags` are its further arguments (`tools`, `without:<text>`).
pub async fn expect_error(needle: &str, flags: &[&str], client: &Client) {
    let mut args = vec![needle];
    args.extend_from_slice(flags);
    run_guest(test_programs::MODEL_EXPECT_ERROR, &args, client).await;
}

/// Wait for every process the client spawned to be gone — reaped, so the
/// client has seen each exit. A slot reopens only once its process is, so
/// this is the pool whole again, and also the wait for each `Shutdown`.
pub async fn await_gone(fake: &Spawnable) {
    await_gone_within(fake, GONE).await;
}

pub async fn await_gone_within(fake: &Spawnable, within: Duration) {
    let alive = || {
        fake.log()
            .workers()
            .into_iter()
            .filter(Process::alive)
            .map(|p| p.number)
            .collect::<Vec<_>>()
    };
    fake_bridge::poll(
        || alive().is_empty(),
        within,
        &format!("every spawned process gone; still up: {:?}", alive()),
    )
    .await;
}

/// Wait for one spawned process to be gone.
pub async fn await_process_gone(process: &Process) {
    fake_bridge::poll(
        || !process.alive(),
        GONE,
        &format!("process {} (pid {}) gone", process.number, process.pid),
    )
    .await;
}

/// Wait for every child a spawned process forked to be gone with it.
pub async fn await_forked_gone(process: &Process) {
    let forked = process.forked();
    assert!(!forked.is_empty(), "process {} forked a child", process.number);
    fake_bridge::poll(
        || forked.iter().all(|pid| !fake_bridge::alive(*pid)),
        GONE,
        &format!("process {}'s forked children {forked:?} gone", process.number),
    )
    .await;
}

/// The one agent a single-completion scenario created, with its RPCs.
pub fn sole_agent(history: &impl History) -> (String, Vec<Rpc>) {
    let agents = history.agents();
    assert_eq!(agents.len(), 1, "one agent was created: {}", history.summary());
    let sequence = history.sequence(&agents[0]);
    (agents[0].clone(), sequence)
}

/// One request to the client's callback endpoint, as a bridge would make
/// it: the status and the JSON reply.
pub async fn callback(
    method: Method, url: &str, token: Option<&str>, body: &Value,
) -> (u16, Value) {
    let client: HyperClient<_, Full<Bytes>> =
        HyperClient::builder(TokioExecutor::new()).build_http();
    let mut request =
        Request::builder().method(method).uri(url).header(CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = request.body(Full::new(Bytes::from(body.to_string()))).expect("a request");
    let response = client.request(request).await.expect("the endpoint answers");
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.expect("a body").to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

/// The `CallCustomTool` path every callback POSTs to.
pub const CALLBACK_PATH: &str = "/sdk.v1.SdkCustomToolCallbackService/CallCustomTool";
