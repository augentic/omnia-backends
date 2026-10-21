//! End-to-end tests for the genai backend at the `omnia:model` boundary:
//! every scenario runs a guest component from `crates/test-programs` through
//! the omnia runtime over an `omnia_genai::Client` whose
//! `ConnectOptions::endpoint` is a fake OpenAI-compatible provider served
//! in-process. The guest asserts what it observes and traps on failure; the
//! test asserts what reached the provider, and what the backend's own
//! `completion` line reported of the run.

mod support;

use std::path::PathBuf;
use std::time::Duration;

use omnia_genai::ConnectOptions;
use omnia_test::host::{Scratch, scratch};
use serde_json::json;
use support::capture::Captured;
use support::fake_openai::{self, Config, FakeOpenAi, Fault, Round};
use support::harness::{client, client_with, expect_error, run_guest, run_guest_over};

// Every guest program in `crates/test-programs` must have a matching test
// here; a new program without one fails to compile.
test_programs::foreach_model!();

// The candidates the `check_*` scenarios' fake proposes, spelled as the
// backend's schema extraction re-serializes them (sorted keys), so the
// correction quotes them verbatim.
const PASS: &str = r#"{"findings":[],"verdict":"pass"}"#;
const FAIL: &str = r#"{"findings":["x"],"verdict":"fail"}"#;
// The backend's bound on the candidates one completion offers a check.
const MAX_CANDIDATES: usize = 2;

// A scratch with `tree/` to lend, and the shell roots that put it under one.
fn tree() -> (Scratch, Vec<PathBuf>) {
    let scratch = scratch();
    scratch.write("tree/.keep", "");
    let roots = vec![scratch.path().join("tree")];
    (scratch, roots)
}

// The working directory a shell in `tree/` sees: the lend, canonical.
fn cwd_of(scratch: &Scratch) -> String {
    std::fs::canonicalize(scratch.path().join("tree"))
        .expect("the tree exists")
        .to_string_lossy()
        .into_owned()
}

fn shelled(roots: Vec<PathBuf>) -> ConnectOptions {
    ConnectOptions {
        shell_roots: roots,
        ..ConnectOptions::default()
    }
}

// The `worker` guest over `tree/`, with `grant` and the fake's script.
async fn worker(
    config: Config, scratch: &Scratch, options: ConnectOptions, grant: &[&str],
) -> FakeOpenAi {
    let fake = FakeOpenAi::serve(config).await;
    let client = client_with(&fake, options).await;
    let mut args = grant.to_vec();
    args.push("lend=./tree");
    run_guest_over(test_programs::MODEL_WORKER, &args, &client, [scratch.mount(false)]).await;
    fake
}

fn shell(command: &str) -> Config {
    Config::calls([("shell", json!({ "command": command }))]).answering("hi")
}

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

// A lent workspace advertises the host-side `read` and `list` tools over it.
#[tokio::test]
async fn model_lend() {
    let fake = FakeOpenAi::serve(Config::echo()).await;
    let client = client(&fake).await;
    let scratch = scratch();
    scratch.write("tree/.keep", "");
    run_guest_over(test_programs::MODEL_LEND, &["./tree"], &client, [scratch.mount(false)]).await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].tools(), ["read", "list"]);
}

// A lend under a shell root advertises the shell beside `read` and `list`,
// with or without a worker grant.
#[tokio::test]
async fn model_lend_shell() {
    let fake = FakeOpenAi::serve(Config::echo()).await;
    let (scratch, roots) = tree();
    let client = client_with(&fake, shelled(roots)).await;
    run_guest_over(test_programs::MODEL_LEND, &["./tree"], &client, [scratch.mount(false)]).await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].tools(), ["read", "list", "shell"]);
}

// A worker grant over a lend under a shell root opens the shell on it: the
// command runs in the lend, behind the session's proxy, with the grant's
// variable set and the lend as `$WORKSPACE`.
#[tokio::test]
async fn model_worker() {
    let (scratch, roots) = tree();
    let fake = worker(
        shell(r#"printf '%s %s %s %s' "$OMNIA_TEST_TOOLS" "${HTTPS_PROXY%%://*}" "$WORKSPACE" "$(pwd)""#),
        &scratch,
        shelled(roots),
        &["env:OMNIA_TEST_TOOLS=rust", "egress:registry.npmjs.org"],
    )
    .await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].tools(), ["read", "list", "shell"]);
    let cwd = cwd_of(&scratch);
    assert_eq!(requests[1].last("tool"), Some(format!("rust http {cwd} {cwd}")));
}

// Nothing of the host's environment reaches the shell: not a planted
// variable, not the provider key the SDK reads.
#[tokio::test]
async fn model_worker_shell_env() {
    let (scratch, roots) = tree();
    let fake = worker(
        shell(r#"printf '%s|%s|%s' "$OPENAI_API_KEY" "$HOME" "$USER""#),
        &scratch,
        shelled(roots),
        &["env:OMNIA_TEST_TOOLS=rust"],
    )
    .await;

    let result = fake.requests()[1].last("tool").expect("the shell answered");
    let (key, rest) = result.split_once('|').expect("three fields");
    let (home, user) = rest.split_once('|').expect("three fields");
    assert_eq!(key, "", "the provider key stays out: {result}");
    assert_ne!(home, std::env::var("HOME").unwrap_or_default(), "HOME is the session's scratch");
    assert!(!home.is_empty(), "HOME is set: {result}");
    assert_eq!(user, std::env::var("USER").unwrap_or_default(), "USER is inherited");
}

// A worker grant over a lend outside every shell root is the request's fault.
#[tokio::test]
async fn model_worker_read_only() {
    let (scratch, _) = tree();
    let elsewhere = vec![scratch.path().join("elsewhere")];
    let fake = worker(
        Config::echo(),
        &scratch,
        shelled(elsewhere),
        &["env:OMNIA_TEST_TOOLS=rust", "refused:not under a shell root"],
    )
    .await;
    assert!(fake.requests().is_empty(), "refused before any request");
}

// With no shell root configured the sandbox is never set up, so a worker
// grant has no shell to open; a lend without one still reads, as
// `model_lend` has it.
#[tokio::test]
async fn model_worker_unrooted() {
    let (scratch, _) = tree();
    let fake = worker(
        Config::echo(),
        &scratch,
        ConnectOptions::default(),
        &["env:OMNIA_TEST_TOOLS=rust", "refused:no shell root is configured"],
    )
    .await;
    assert!(fake.requests().is_empty(), "refused before any request");
}

// The sandbox's own rules hold the grant: a `~/` root over a credential
// store is denied as `invalid-request`, in the sandbox's words.
#[tokio::test]
async fn model_worker_denied() {
    let (scratch, roots) = tree();
    let fake = worker(
        Config::echo(),
        &scratch,
        shelled(roots),
        &["read:~/.ssh", "refused:worker grant: "],
    )
    .await;
    assert!(fake.requests().is_empty(), "refused before any request");
}

// What a failing command wrote comes back whole, its stderr and its exit
// marked; a clean command that wrote nothing reads `exit 0`.
#[tokio::test]
async fn model_worker_shell_exit() {
    let (scratch, roots) = tree();
    let fake = worker(
        Config::calls([
            ("shell", json!({ "command": "echo out; echo err >&2; exit 3" })),
            ("shell", json!({ "command": "true" })),
            ("shell", json!({ "command": "kill -9 $$" })),
        ])
        .answering("hi"),
        &scratch,
        shelled(roots.clone()),
        &[],
    )
    .await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 4);
    let results: Vec<_> =
        requests[1..].iter().map(|request| request.last("tool").expect("a tool result")).collect();
    assert_eq!(
        results,
        [
            "out\n--- stderr ---\nerr\n--- exit ---\nexit 3",
            "exit 0",
            "--- exit ---\nkilled by signal 9",
        ]
    );
}

// A command past the bound is killed, with what it started, and the model
// told so as the result rather than the completion failing.
#[tokio::test]
async fn model_worker_shell_bound() {
    let (scratch, roots) = tree();
    let fake = worker(
        shell("sleep 30 & echo $! > pid; wait"),
        &scratch,
        ConnectOptions {
            shell_timeout_secs: 1,
            ..shelled(roots)
        },
        &[],
    )
    .await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1].last("tool").as_deref(),
        Some("the command did not exit within 1s and was killed with everything it started")
    );
    await_gone(&scratch).await;
}

// The cap holds a command the shell's own bound would let run: the reply
// is given up on mid-command, the command killed with what it started, and
// the guest sees the time budget spent after the one request.
#[tokio::test]
async fn model_shell_capped() {
    let captured = Captured::install();
    let (scratch, roots) = tree();
    let fake = FakeOpenAi::serve(shell("sleep 30 & echo $! > pid; wait")).await;
    let client = client_with(
        &fake,
        ConnectOptions {
            timeout_secs: 1,
            ..shelled(roots)
        },
    )
    .await;
    run_guest_over(
        test_programs::MODEL_EXPECT_ERROR,
        &["timed out after 1s", "budget", "lend=./tree"],
        &client,
        [scratch.mount(false)],
    )
    .await;

    assert_eq!(fake.requests().len(), 1, "the command's result was never sent");
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="timeout""#), "{completion}");
    await_gone(&scratch).await;
}

// The `sleep` whose pid a shell command wrote to `tree/pid` is gone.
async fn await_gone(scratch: &Scratch) {
    let pid = scratch.read("tree/pid").expect("the sleep's pid was written");
    let pid: u32 = String::from_utf8_lossy(&pid).trim().parse().expect("a pid");
    fake_openai::poll(
        || !alive(pid),
        Duration::from_secs(8),
        &format!("the sleep ({pid}) to be gone"),
    )
    .await;
}

// Every command of one completion runs in one session: the second finds
// what the first left in `$HOME`.
#[tokio::test]
async fn model_worker_shell_shared() {
    let (scratch, roots) = tree();
    let fake = worker(
        Config::calls([
            ("shell", json!({ "command": "echo one > \"$HOME/mark\"" })),
            ("shell", json!({ "command": "cat \"$HOME/mark\"" })),
        ])
        .answering("hi"),
        &scratch,
        shelled(roots),
        &[],
    )
    .await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2].last("tool").as_deref(), Some("one\n"));
}

// A shell call where no shell was advertised is answered as the model's
// slip, not the completion's failure.
#[tokio::test]
async fn model_lend_shell_unadvertised() {
    let fake = FakeOpenAi::serve(shell("pwd")).await;
    let client = client(&fake).await;
    let (scratch, _) = tree();
    run_guest_over(test_programs::MODEL_LEND, &["./tree"], &client, [scratch.mount(false)]).await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].tools(), ["read", "list"]);
    assert_eq!(
        requests[1].last("tool").as_deref(),
        Some("tool `shell` failed: no shell on this workspace")
    );
}

// A guest tool named `shell` would be shadowed where a shell is advertised,
// so the name is the backend's whatever the lend.
#[tokio::test]
async fn model_shell_reserved() {
    let fake = FakeOpenAi::serve(Config::echo()).await;
    let client = client(&fake).await;
    expect_error("tool `shell` is reserved", &["tool:shell", "invalid"], &client).await;
    assert!(fake.requests().is_empty(), "refused before any request");
}

// Whether `pid` is still running, by a signal-0 probe.
#[expect(unsafe_code, reason = "a signal-0 probe is the one portable liveness check for a pid")]
fn alive(pid: u32) -> bool {
    let Ok(signed) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 probes for existence and delivers nothing.
    unsafe { libc::kill(signed, 0) == 0 }
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

// A reply that is not JSON where JSON was asked for is nudged once, with
// what the parser said of it, before any candidate reaches the guest, in a
// closing round that asks for JSON mode in place of the schema with the
// tools still declared; the JSON that follows passes the check first time,
// so the completion is `ok` over two attempts, not corrected.
#[tokio::test]
async fn model_nudged() {
    let captured = Captured::install();
    let fake =
        FakeOpenAi::serve(Config::replies(["Analyzing the claims to produce the verdict.", PASS]))
            .await;
    let client = client(&fake).await;
    let scratch = scratch();
    scratch.write("tree/.keep", "");
    run_guest_over(
        test_programs::MODEL_CHECK_ACCEPTED,
        &["lend=./tree"],
        &client,
        [scratch.mount(false)],
    )
    .await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].tools(), ["read", "list"], "the lend's tools on the first round");
    assert_eq!(requests[0].body["response_format"]["type"], "json_schema");
    let second = requests[1].messages();
    let tail = &second[second.len() - 2..];
    assert_eq!(
        tail[0],
        ("assistant".to_owned(), "Analyzing the claims to produce the verdict.".to_owned())
    );
    assert_eq!(tail[1].0, "user");
    assert!(
        tail[1].1.starts_with("Your last reply is not one well-formed JSON value ("),
        "{}",
        tail[1].1
    );
    assert!(!tail[1].1.contains("No tool"), "the nudge withdraws nothing: {}", tail[1].1);
    assert_eq!(requests[1].tools(), ["read", "list"], "the closing round keeps the tools");
    assert_eq!(requests[1].body["response_format"]["type"], "json_object");
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="ok""#), "{completion}");
    assert!(completion.contains("attempts=2 "), "{completion}");
}

// A reply that is JSON but does not fit the schema is nudged the same way,
// the nudge naming each fault at its path, so a slip of shape never reaches
// the guest as a candidate it cannot read.
#[tokio::test]
async fn model_nudged_off_schema() {
    let captured = Captured::install();
    let fake =
        FakeOpenAi::serve(Config::replies([r#"{"findings":"none","verdict":"pass"}"#, PASS])).await;
    let client = client(&fake).await;
    let scratch = scratch();
    scratch.write("tree/.keep", "");
    run_guest_over(
        test_programs::MODEL_CHECK_ACCEPTED,
        &["lend=./tree"],
        &client,
        [scratch.mount(false)],
    )
    .await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    let second = requests[1].messages();
    let tail = &second[second.len() - 2..];
    assert_eq!(
        tail[0],
        ("assistant".to_owned(), r#"{"findings":"none","verdict":"pass"}"#.to_owned())
    );
    assert_eq!(tail[1].0, "user");
    let nudge = &tail[1].1;
    assert!(
        nudge.starts_with("Your last reply is JSON but not in the shape the prompt asked for ("),
        "{nudge}"
    );
    assert!(nudge.contains("at `/findings`: "), "the fault names its path: {nudge}");
    assert!(nudge.contains("\"array\""), "the fault names the type due: {nudge}");
    assert_eq!(requests[1].tools(), ["read", "list"]);
    assert_eq!(requests[1].body["response_format"]["type"], "json_object");
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="ok""#), "{completion}");
    assert!(completion.contains("attempts=2 "), "{completion}");
}

// A tool call made in a closing round is not run: each is answered with the
// JSON answer as what is due, the next round still under JSON mode with the
// tools declared, so a provider whose JSON mode does not silence the tools
// still ends; the text that follows is the candidate.
#[tokio::test]
async fn model_nudge_keeps_tools() {
    let captured = Captured::install();
    let fake = FakeOpenAi::serve(Config::rounds([
        Round::Text("Let me check the tree first.".to_owned()),
        Round::Call("list".to_owned(), json!({ "path": "." })),
        Round::Text(PASS.to_owned()),
    ]))
    .await;
    let client = client(&fake).await;
    let scratch = scratch();
    scratch.write("tree/.keep", "");
    run_guest_over(
        test_programs::MODEL_CHECK_ACCEPTED,
        &["lend=./tree"],
        &client,
        [scratch.mount(false)],
    )
    .await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 3);
    let formats: Vec<&str> =
        requests.iter().map(|r| r.body["response_format"]["type"].as_str().unwrap()).collect();
    assert_eq!(formats, ["json_schema", "json_object", "json_object"]);
    for request in &requests {
        assert_eq!(request.tools(), ["read", "list"]);
    }
    let refusal = requests[2].last("tool").expect("the refused call is answered as a tool result");
    assert!(refusal.starts_with("not run: your final answer is due now."), "{refusal}");
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="ok""#), "{completion}");
    assert!(completion.contains("attempts=2 "), "{completion}");
}

// The nudge is one per candidate: a correction turn carries the schema
// again, and a second prose reply after it is given a second closing round
// rather than put to the check as it stands; the tools are declared on
// every round.
#[tokio::test]
async fn model_nudged_after_correction() {
    let captured = Captured::install();
    let fake = FakeOpenAi::serve(Config::replies([
        "Let me look at the claims.",
        FAIL,
        "Revising the verdict now.",
        PASS,
    ]))
    .await;
    let client = client(&fake).await;
    let scratch = scratch();
    scratch.write("tree/.keep", "");
    run_guest_over(
        test_programs::MODEL_CHECK_CORRECTED,
        &["lend=./tree"],
        &client,
        [scratch.mount(false)],
    )
    .await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 4);
    let formats: Vec<&str> =
        requests.iter().map(|r| r.body["response_format"]["type"].as_str().unwrap()).collect();
    assert_eq!(formats, ["json_schema", "json_object", "json_schema", "json_object"]);
    for request in &requests {
        assert_eq!(request.tools(), ["read", "list"], "the tools are declared on every round");
    }
    let third = requests[2].messages();
    let tail = &third[third.len() - 2..];
    assert_eq!(tail[0], ("assistant".to_owned(), FAIL.to_owned()));
    assert!(tail[1].1.starts_with("## Previous answer (rejected)\n\n"), "{}", tail[1].1);
    let fourth = requests[3].messages();
    let tail = &fourth[fourth.len() - 2..];
    assert_eq!(tail[0], ("assistant".to_owned(), "Revising the verdict now.".to_owned()));
    assert!(tail[1].1.starts_with("Your last reply is not one well-formed JSON value ("));
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="corrected""#), "{completion}");
    assert!(completion.contains("attempts=4 "), "{completion}");
}

// The nudge is a round like any other: with none left after it, the
// completion is exhausted before a candidate ever reaches the guest.
#[tokio::test]
async fn model_nudge_round() {
    let fake = FakeOpenAi::serve(Config::replies(["Analyzing the claims."])).await;
    let client = client_with(
        &fake,
        ConnectOptions {
            max_rounds: 1,
            ..ConnectOptions::default()
        },
    )
    .await;
    expect_error("no answer after 1 model round-trips", &["nudged", "budget"], &client).await;
    assert_eq!(fake.requests().len(), 1);
}

#[tokio::test]
async fn model_check_corrected() {
    let captured = Captured::install();
    let fake = FakeOpenAi::serve(Config::replies([FAIL, PASS]).billed()).await;
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

    // The completion line reports the correction and bills both rounds.
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="corrected""#), "{completion}");
    assert!(completion.contains("attempts=2 "), "{completion}");
    for billed in [
        "input_tokens=60 ",
        "cache_read_tokens=20 ",
        "output_tokens=14 ",
        "reasoning_tokens=4 ",
        "total_tokens=74 ",
    ] {
        assert!(completion.contains(billed), "{billed}: {completion}");
    }
}

#[tokio::test]
async fn model_check_exhausted() {
    let fake = FakeOpenAi::serve(Config::replies([FAIL])).await;
    let client = client(&fake).await;
    run_guest(test_programs::MODEL_CHECK_EXHAUSTED, &[], &client).await;

    // Every round offered a candidate; each request carries every rejected
    // candidate and correction before it, and the last rejection ends the
    // completion with rounds to spare.
    let requests = fake.requests();
    assert_eq!(requests.len(), MAX_CANDIDATES);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.messages().len(), 1 + 2 * index, "request {index}");
    }
}

// The candidate budget is the deployment's: raised, the check is offered
// that many candidates, each corrected, before the last rejection ends it.
#[tokio::test]
async fn model_check_candidates() {
    let fake = FakeOpenAi::serve(Config::replies([FAIL])).await;
    let client = client_with(
        &fake,
        ConnectOptions {
            max_candidates: 4,
            ..ConnectOptions::default()
        },
    )
    .await;
    run_guest(test_programs::MODEL_CHECK_EXHAUSTED, &[], &client).await;

    let requests = fake.requests();
    assert_eq!(requests.len(), 4);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.messages().len(), 1 + 2 * index, "request {index}");
    }
}

// The round budget bounds a completion that never answers: a model calling
// tools forever is ended as exhausted once the rounds are spent.
#[tokio::test]
async fn model_rounds_exhausted() {
    let captured = Captured::install();
    let fake =
        FakeOpenAi::serve(Config::calls(std::iter::repeat_n(("lookup", json!({})), 8))).await;
    let client = client_with(
        &fake,
        ConnectOptions {
            max_rounds: 3,
            ..ConnectOptions::default()
        },
    )
    .await;
    expect_error("no answer after 3 model round-trips", &["tools", "budget"], &client).await;
    assert_eq!(fake.requests().len(), 3);
    let completion = captured.completion();
    assert!(completion.contains(r#"outcome="exhausted""#), "{completion}");
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
    assert_eq!(requests[0].tools(), Vec::<String>::new());
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
