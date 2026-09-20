//! Provider faults and the endpoint seam, guest-driven: each row runs the
//! `expect_error` or `echo_text` guest from `crates/test-programs` over an
//! `omnia_genai::Client` against the fake provider, asserting the failure
//! the guest sees and how many requests the SDK made on the way.

mod support;

use omnia::Backend as _;
use omnia_genai::{Client, ConnectOptions};
use support::fake_openai::{self, Config, FakeOpenAi, Fault};
use support::harness::{client, expect_error, run_guest};

#[tokio::test]
async fn rate_limited() {
    let fake = FakeOpenAi::serve(Config::echo().fault(Fault::Status {
        status: 429,
        retry_after: Some(1),
    }))
    .await;
    let client = client(&fake).await;
    expect_error("status code '429", &[], &client).await;
    // The SDK does not retry a 429, `Retry-After` or not: one request.
    assert_eq!(fake.requests().len(), 1);
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
    let fake = FakeOpenAi::serve(Config::echo().fault(Fault::TruncatedBody)).await;
    let client = client(&fake).await;
    expect_error("Response was invalid json", &[], &client).await;
    assert_eq!(fake.requests().len(), 1);
}

#[tokio::test]
async fn endpoint_without_trailing_slash() {
    fake_openai::dummy_key();
    let fake = FakeOpenAi::serve(Config::echo()).await;
    let client = Client::connect_with(ConnectOptions {
        model: "gpt-4o-mini".to_owned(),
        endpoint: Some(fake.url().trim_end_matches('/').to_owned()),
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
async fn connect_rejects_bad_endpoint() {
    for endpoint in ["", "api.openai.com/v1/", "ftp://gateway/v1/"] {
        let error = Client::connect_with(ConnectOptions {
            model: "gpt-4o-mini".to_owned(),
            endpoint: Some(endpoint.to_owned()),
        })
        .await
        .expect_err(&format!("accepted `{endpoint}`"));
        assert!(format!("{error:#}").contains("http(s) URL"), "`{endpoint}`: {error:#}");
    }
}
