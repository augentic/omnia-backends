//! End-to-end tests for the genai backend at the `omnia:model` boundary:
//! every scenario runs a guest component from `crates/test-programs` through
//! the omnia runtime over an `omnia_genai::Client` whose
//! `ConnectOptions::endpoint` is a fake OpenAI-compatible provider served
//! in-process. The guest asserts what it observes and traps on failure; the
//! test asserts what reached the provider.

mod support;

use serde_json::json;
use support::fake_openai::{self, Config, FakeOpenAi, Fault};
use support::harness::{client, expect_error, run_guest};

// Every guest program in `crates/test-programs` must have a matching test
// here; a new program without one fails to compile.
test_programs::foreach_model!();

/// The candidates the `check_*` scenarios' fake proposes, spelled as the
/// backend's schema extraction re-serializes them (sorted keys), so the
/// correction quotes them verbatim.
const PASS: &str = r#"{"findings":[],"verdict":"pass"}"#;
const FAIL: &str = r#"{"findings":["x"],"verdict":"fail"}"#;
/// The backend's bound on provider round-trips per completion.
const MAX_ROUNDS: usize = 8;

#[tokio::test]
async fn model_echo_text() {
    let fake = FakeOpenAi::serve(Config::echo()).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;

    // One chat-completions call, carrying the SDK's own auth and the
    // request's channels; the fake gave no usage, so the guest saw none.
    let requests = fake.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/chat/completions");
    assert_eq!(requests[0].bearer.as_deref(), Some(fake_openai::expected_key().as_str()));
    assert_eq!(
        requests[0].messages(),
        [
            ("system".to_owned(), "be terse".to_owned()),
            ("user".to_owned(), "hi".to_owned()),
            ("user".to_owned(), "second".to_owned()),
        ]
    );
    assert!(requests[0].tools().is_empty(), "no tool is advertised without a workspace");
}

#[tokio::test]
async fn model_check_accepted() {
    let fake = FakeOpenAi::serve(Config::replies([PASS])).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_CHECK_ACCEPTED, &[], &client).await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 1, "an accepted candidate ends the completion");
    assert_eq!(requests[0].last("system").as_deref(), Some("judge"));
    assert_eq!(requests[0].last("user").as_deref(), Some("judge this"));
    assert_eq!(
        requests[0].body["response_format"]["type"], "json_schema",
        "the typed question rides as the provider's response format"
    );
}

#[tokio::test]
async fn model_check_corrected() {
    let fake = FakeOpenAi::serve(Config::replies([FAIL, PASS])).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_CHECK_CORRECTED, &[], &client).await;

    // The second round carries the rejected candidate as the assistant
    // turn and the guest's correction, verbatim, as the user turn after it.
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    let second = requests[1].messages();
    let tail = &second[second.len() - 2..];
    assert_eq!(tail[0], ("assistant".to_owned(), FAIL.to_owned()));
    assert_eq!(tail[1].0, "user");
    let correction = &tail[1].1;
    assert!(correction.starts_with("## Previous answer (rejected)\n\n"), "{correction}");
    assert!(correction.contains(FAIL), "{correction}");
    assert!(correction.contains("## Findings\n\nverdict must be `pass`"), "{correction}");
}

#[tokio::test]
async fn model_check_exhausted() {
    let fake = FakeOpenAi::serve(Config::replies([FAIL])).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_CHECK_EXHAUSTED, &[], &client).await;

    // Every round offered a candidate; each request carries every rejected
    // candidate and correction before it.
    let requests = fake.requests();
    assert_eq!(requests.len(), MAX_ROUNDS);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.messages().len(), 1 + 2 * index, "request {index}");
    }
}

#[tokio::test]
async fn model_tool_roundtrip() {
    let fake = FakeOpenAi::serve(Config::tool("lookup", json!({}))).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_TOOL_ROUNDTRIP, &[], &client).await;

    // The first request advertised the guest's tool; the second carried the
    // guest's result under the call's id, and became the answer.
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].tools(), ["lookup"]);
    assert_eq!(requests[1].last("tool").as_deref(), Some("42"));
    let tool_message = requests[1].body["messages"]
        .as_array()
        .and_then(|messages| messages.iter().find(|m| m["role"] == "tool"))
        .expect("a tool message");
    assert_eq!(tool_message["tool_call_id"], "call_1");
}

#[tokio::test]
async fn model_tool_failure() {
    let fake = FakeOpenAi::serve(Config::tool("lookup", json!({}))).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_TOOL_FAILURE, &[], &client).await;

    // A repairable failure goes back to the model as the tool's result.
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].last("tool").as_deref(), Some("tool `lookup` failed: no data"));
}

#[tokio::test]
async fn model_undeclared_tool() {
    let fake = FakeOpenAi::serve(Config::tool("lookup", json!({}))).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_UNDECLARED_TOOL, &[], &client).await;

    // The host refuses the call and the completion ends: no second round.
    let requests = fake.requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].tools().is_empty());
}

#[tokio::test]
async fn model_fanout() {
    const WIDTH: usize = 4;
    let fake = FakeOpenAi::serve(Config::echo().gate(WIDTH)).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_FANOUT, &[&WIDTH.to_string()], &client).await;

    // Every completion's request was in flight at once, each its own.
    assert_eq!(fake.peak_in_flight(), WIDTH);
    let mut seams: Vec<_> =
        fake.requests().iter().filter_map(|request| request.last("user")).collect();
    seams.sort();
    assert_eq!(seams, ["seam 0", "seam 1", "seam 2", "seam 3"]);
}

#[tokio::test]
async fn model_tool_fanout() {
    const WIDTH: usize = 4;
    let fake = FakeOpenAi::serve(Config::tool("lookup", json!({})).gate(WIDTH)).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_TOOL_FANOUT, &[&WIDTH.to_string()], &client).await;

    // Four tool calls answered with their own seams, each carried back to
    // the provider in its own conversation.
    assert_eq!(fake.peak_in_flight(), WIDTH);
    let requests = fake.requests();
    assert_eq!(requests.len(), 2 * WIDTH);
    let mut results: Vec<_> = requests.iter().filter_map(|request| request.last("tool")).collect();
    results.sort();
    assert_eq!(results, ["seam 0", "seam 1", "seam 2", "seam 3"]);
}

#[tokio::test]
async fn model_fanout_abandon() {
    const WIDTH: usize = 4;
    let fake = FakeOpenAi::serve(Config::echo().fault(Fault::Park)).await;
    let client = client(&fake).await;

    let guest = tokio::spawn({
        let client = client.clone();
        async move {
            run_guest(test_programs::MODEL_FANOUT_ABANDON, &[&WIDTH.to_string()], &client).await;
        }
    });
    // Every completion is mid-request at once; the first released is the
    // winner, and the guest drops the rest.
    fake.await_parked(WIDTH).await;
    assert_eq!(fake.peak_in_flight(), WIDTH);
    assert!(fake.release_one());
    guest.await.expect("the guest task joins");

    // The losers' requests were given up on, not answered: nothing beyond
    // the four issued was asked for.
    fake_openai::poll(
        || fake.abandoned() == WIDTH - 1,
        std::time::Duration::from_secs(5),
        &format!("{} abandoned request(s), have {}", WIDTH - 1, fake.abandoned()),
    )
    .await;
    assert_eq!(fake.requests().len(), WIDTH);
    fake.release_all();
}

#[tokio::test]
async fn model_expect_error() {
    let fake = FakeOpenAi::serve(Config::echo().fault(Fault::Status {
        status: 500,
        retry_after: None,
    }))
    .await;
    let client = client(&fake).await;
    expect_error("status code '500", &[], &client).await;
    assert_eq!(fake.requests().len(), 1);
}
