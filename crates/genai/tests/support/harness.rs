//! The guest-driven harness the `model` and `provider` suites share: a
//! client pointed at the fake through `ConnectOptions::endpoint`, and a
//! guest run through `omnia_test::host`.

use omnia::{Backend as _, ExitStatus, Mount};
use omnia_genai::{Client, ConnectOptions};
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_model::WasiModel;

use super::fake_openai::{self, FakeOpenAi};

/// A client sending every request to `fake`, with the key the SDK's `OpenAI`
/// adapter reads.
pub async fn client(fake: &FakeOpenAi) -> Client {
    client_with(fake, ConnectOptions::default()).await
}

/// A client sending every request to `fake` under `options`, its `model`
/// and `endpoint` the fake's.
pub async fn client_with(fake: &FakeOpenAi, options: ConnectOptions) -> Client {
    fake_openai::dummy_key();
    Client::connect_with(ConnectOptions {
        // A name the SDK routes to its OpenAI chat-completions adapter.
        model: "gpt-4o-mini".to_owned(),
        endpoint: Some(fake.url().to_owned()),
        ..options
    })
    .await
    .expect("the endpoint is accepted")
}

/// Run one guest program over `client`, requiring a clean exit.
pub async fn run_guest(wasm: &str, args: &[&str], client: &Client) {
    run_guest_over(wasm, args, client, []).await;
}

/// Run one guest program over `client` with `mounts` preopened, requiring a
/// clean exit.
pub async fn run_guest_over(
    wasm: &str, args: &[&str], client: &Client, mounts: impl IntoIterator<Item = Mount>,
) {
    let backends = Backends::defaults().await.model(client.clone());
    let status = Deployment::new()
        .guest("guest", wasm)
        .args(args.iter().copied())
        .mounts(mounts)
        .run_host::<WasiModel, _>(backends)
        .await
        .expect("guest runs");
    assert_eq!(status, ExitStatus::SUCCESS, "guest `{wasm}` failed");
}

/// The `expect_error` guest: the completion fails and its detail carries
/// `needle`; `flags` are its further arguments (`tools`, `tool:<name>`,
/// `invalid`, `without:<text>`).
pub async fn expect_error(needle: &str, flags: &[&str], client: &Client) {
    let mut args = vec![needle];
    args.extend_from_slice(flags);
    run_guest(test_programs::MODEL_EXPECT_ERROR, &args, client).await;
}
