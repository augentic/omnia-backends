//! Provider faults, the cap, and the endpoint seam, guest-driven: each row
//! runs the `expect_error` or `echo_text` guest from `crates/test-programs`
//! over an `omnia_genai::Client` against the fake provider, asserting the
//! failure the guest sees, how many requests the SDK made on the way, and
//! the outcome the backend's `completion` line reports.

mod support;

use std::time::Duration;

use omnia::Backend as _;
use omnia_genai::{Client, ConnectOptions};
use support::capture::Captured;
use support::fake_openai::{self, Config, FakeOpenAi, Fault};
use support::harness::{client, client_with, expect_error, run_guest};

#[tokio::test]
async fn rate_limited() {
    let captured = Captured::install();
    let fake = FakeOpenAi::serve(Config::echo().fault(Fault::Status {
        status: 429,
        retry_after: Some(1),
    }))
    .await;
    let client = client(&fake).await;
    expect_error("status code '429", &[], &client).await;
    // The SDK does not retry a 429, `Retry-After` or not: one request.
    assert_eq!(fake.requests().len(), 1);
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="provider""#), "{completion}");
}

// A reply the cap outlives is given up on: the request is abandoned at the
// fake, nothing further is asked, and the guest sees the time budget spent.
#[tokio::test]
async fn cap_hits() {
    let captured = Captured::install();
    let fake = FakeOpenAi::serve(Config::echo().fault(Fault::Park)).await;
    let client = client_with(
        &fake,
        ConnectOptions {
            timeout_secs: 1,
            ..ConnectOptions::default()
        },
    )
    .await;
    expect_error("timed out after 1s", &["budget"], &client).await;

    fake_openai::poll(
        || fake.abandoned() == 1,
        Duration::from_secs(5),
        &format!("the parked request to be abandoned, have {}", fake.abandoned()),
    )
    .await;
    assert_eq!(fake.requests().len(), 1);
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="timeout""#), "{completion}");
    assert!(completion.contains("attempts=0 "), "{completion}");
    fake.release_all();
}

#[tokio::test]
async fn server_error() {
    let fake = FakeOpenAi::serve(Config::echo().fault(Fault::Status {
        status: 503,
        retry_after: None,
    }))
    .await;
    let client = client(&fake).await;
    // The provider's error body reaches the guest's detail with the status.
    expect_error("status code '503", &[], &client).await;
    expect_error("scripted failure", &[], &client).await;
    assert_eq!(fake.requests().len(), 2, "one request per completion, no retry");
}

#[tokio::test]
async fn truncated_body() {
    let captured = Captured::install();
    let fake = FakeOpenAi::serve(Config::echo().fault(Fault::TruncatedBody)).await;
    let client = client(&fake).await;
    expect_error("Response was invalid json", &[], &client).await;
    assert_eq!(fake.requests().len(), 1);
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="transport""#), "{completion}");
}

#[tokio::test]
async fn endpoint_no_slash() {
    fake_openai::dummy_key();
    let fake = FakeOpenAi::serve(Config::echo()).await;
    let client = Client::connect_with(ConnectOptions {
        model: "gpt-4o-mini".to_owned(),
        endpoint: Some(fake.url().trim_end_matches('/').to_owned()),
        ..ConnectOptions::default()
    })
    .await
    .expect("the endpoint is accepted");
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;

    // The base kept its `/v1` segment when the SDK joined the path.
    let requests = fake.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/chat/completions");
}

#[tokio::test]
async fn invalid_endpoint() {
    for endpoint in ["", "api.openai.com/v1/", "ftp://gateway/v1/"] {
        let error = Client::connect_with(ConnectOptions {
            model: "gpt-4o-mini".to_owned(),
            endpoint: Some(endpoint.to_owned()),
            ..ConnectOptions::default()
        })
        .await
        .expect_err(&format!("accepted `{endpoint}`"));
        assert!(format!("{error:#}").contains("http(s) URL"), "`{endpoint}`: {error:#}");
    }
}

#[tokio::test]
async fn zero_budget() {
    for (what, options) in [
        (
            "max_rounds",
            ConnectOptions {
                max_rounds: 0,
                ..ConnectOptions::default()
            },
        ),
        (
            "max_tool_calls",
            ConnectOptions {
                max_tool_calls: 0,
                ..ConnectOptions::default()
            },
        ),
        (
            "max_candidates",
            ConnectOptions {
                max_candidates: 0,
                ..ConnectOptions::default()
            },
        ),
        (
            "max_repeats",
            ConnectOptions {
                max_repeats: 0,
                ..ConnectOptions::default()
            },
        ),
        (
            "max_tokens",
            ConnectOptions {
                max_tokens: 0,
                ..ConnectOptions::default()
            },
        ),
        (
            "timeout_secs",
            ConnectOptions {
                timeout_secs: 0,
                ..ConnectOptions::default()
            },
        ),
        (
            "shell_timeout_secs",
            ConnectOptions {
                shell_timeout_secs: 0,
                ..ConnectOptions::default()
            },
        ),
    ] {
        let error = Client::connect_with(options).await.expect_err(&format!("accepted {what} 0"));
        assert!(format!("{error:#}").contains(what), "{what}: {error:#}");
    }
}
