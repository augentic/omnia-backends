//! The guest-driven harness the `model` and `bridge` suites share: a client
//! over one of the fake's mounts, a guest run through `omnia_test::host`,
//! and the pool probe every row ends on.

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

use super::fake_bridge::{self, FakeBridge, History, Rpc, Spawnable};

/// How long a slot may take to reopen once a guest has returned, when the
/// row expects no timeout on the way.
pub const REOPEN: Duration = Duration::from_secs(8);
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
        bridge_bin: "cursor-sdk-bridge".to_owned(),
        bridge_url: None,
        bridge_token: None,
    }
}

/// `options` pointed at the spawnable fake.
pub fn spawn_options(fake: &Spawnable, options: ConnectOptions) -> ConnectOptions {
    ConnectOptions {
        bridge_bin: fake.bin(),
        ..options
    }
}

/// `options` attached to the in-process fake.
pub fn attach_options(fake: &FakeBridge, options: ConnectOptions) -> ConnectOptions {
    ConnectOptions {
        bridge_url: Some(fake.url().to_owned()),
        bridge_token: Some(fake.token().to_owned()),
        ..options
    }
}

/// A client spawning one fake process per lease.
pub async fn spawning(fake: &Spawnable, max_agents: usize) -> Client {
    connect(spawn_options(fake, options(max_agents))).await
}

/// A client attached to the in-process fake.
pub async fn attached(fake: &FakeBridge, max_agents: usize) -> Client {
    connect(attach_options(fake, options(max_agents))).await
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

/// Wait for every slot to reopen: a spawned lease's process must be gone
/// first, so this is also the wait for its `Shutdown`.
pub async fn await_idle(client: &Client, slots: usize) {
    await_idle_within(client, slots, REOPEN).await;
}

pub async fn await_idle_within(client: &Client, slots: usize, within: Duration) {
    fake_bridge::poll(
        || client.idle_slots() == slots,
        within,
        &format!("{slots} idle slot(s), have {}", client.idle_slots()),
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
