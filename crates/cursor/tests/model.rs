//! The `omnia:model` contract, one scenario per guest program: the guest
//! (`crates/test-programs/programs/model/`) asserts what it observes over an
//! `omnia_cursor::Client` and traps on failure; the test asserts what reached
//! the fake `cursor-sdk-bridge` and that every spawned process is gone again.

mod support;

use omnia_cursor::ConnectOptions;
use omnia_test::host::scratch;
use serde_json::json;
use support::fake_bridge::{Codec, Config, Fault, History as _, Point, Rpc, Spawnable};
use support::harness::{
    await_gone, connect, options, run_guest, run_guest_over, sole_agent, spawning,
};

// A guest program without a matching test here fails to compile.
test_programs::foreach_model!();

// The candidates the `check_*` fakes propose, spelled as the backend's schema
// extraction re-serializes them (sorted keys), so a correction quotes them
// verbatim.
const PASS: &str = r#"{"findings":[],"verdict":"pass"}"#;
const FAIL: &str = r#"{"findings":["x"],"verdict":"fail"}"#;

#[tokio::test]
async fn model_echo_text() {
    let fake = Spawnable::new(&Config::echo());
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 1, "one process served the completion: {}", log.summary());
    let (agent, sequence) = sole_agent(&workers[0]);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
    assert!(workers[0].ended_with(Rpc::Shutdown), "the lease closed its process");

    // delete is scoped to the create-time cwd and repeats the api key
    let deleted = workers[0].saw(Rpc::DeleteAgent)[0];
    let created = workers[0].saw(Rpc::CreateAgent)[0];
    assert_eq!(created.agent.as_deref(), Some(agent.as_str()));
    assert!(!created.text("cwd").is_empty(), "CreateAgent names a cwd: {}", created.arg);
    assert_eq!(deleted.text("cwd"), created.text("cwd"));
    assert_eq!(created.arg["apiKeyPresent"], true);
    assert_eq!(deleted.arg["apiKeyPresent"], true);
    assert_eq!(deleted.arg["apiKeyMatchesCreate"], true);

    // nothing lent: every built-in tool off, and no sandbox asked for
    assert_eq!(created.arg["tools"], json!([]));
    assert!(created.arg["sandbox"].is_null(), "{}", created.arg);
}

#[tokio::test]
async fn model_lent_workspace() {
    let fake = Spawnable::new(&Config::echo());
    let client = spawning(&fake, 1).await;
    let lent = scratch();
    run_guest_over(test_programs::MODEL_LENT_WORKSPACE, &[], &client, Some(lent.mount(true))).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);

    // the lent tree is the agent's cwd, with the shell among its tools and
    // the sandbox left to the operator's own policy
    let created = log.saw(Rpc::CreateAgent)[0];
    let cwd = std::fs::canonicalize(lent.path()).expect("the scratch root");
    assert_eq!(created.text("cwd"), cwd.to_str().expect("a UTF-8 path"));
    assert_eq!(created.arg["tools"], json!(["glob", "grep", "ls", "mcp", "read", "shell"]));
    assert!(created.arg["sandbox"].is_null(), "{}", created.arg);
}

#[tokio::test]
async fn model_check_accepted() {
    let fake = Spawnable::new(&Config::replies([PASS]));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_CHECK_ACCEPTED, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
    assert!(log.saw(Rpc::Send)[0].text("text").contains("judge this"));
}

#[tokio::test]
async fn model_check_corrected() {
    let fake = Spawnable::new(&Config::replies([FAIL, PASS]));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_CHECK_CORRECTED, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(
        sequence,
        [Rpc::CreateAgent, Rpc::Send, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent],
        "the correction goes on the same agent"
    );

    // the session persists, so the second send is the correction alone
    let sends = log.saw(Rpc::Send);
    let correction = sends[1].text("text");
    assert!(correction.starts_with("## Previous answer (rejected)\n\n"), "{correction}");
    assert!(correction.contains(FAIL), "{correction}");
    assert!(correction.contains("## Findings\n\nverdict must be `pass`"), "{correction}");
}

#[tokio::test]
async fn model_check_nudged() {
    let fake =
        Spawnable::new(&Config::replies(["Analyzing the claims to produce the verdict.", PASS]));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_CHECK_ACCEPTED, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(
        sequence,
        [Rpc::CreateAgent, Rpc::Send, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent],
        "the nudge goes on the same agent"
    );
    let sends = log.saw(Rpc::Send);
    let nudge = sends[1].text("text");
    assert!(
        nudge.starts_with(
            "Your last reply is not one well-formed JSON value (expected value at line 1 column 1)"
        ),
        "{nudge}"
    );
    assert!(!nudge.contains("## Findings"), "no check ran on the narration: {nudge}");
}

// A reply that opens a JSON document and does not close it is nudged as raw
// text, the nudge naming where the parser stopped, rather than checked as a
// fragment of itself.
#[tokio::test]
async fn model_check_malformed() {
    let fake = Spawnable::new(&Config::replies([r#"{"findings":[],"verdict":"pass""#, PASS]));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_CHECK_ACCEPTED, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(
        sequence,
        [Rpc::CreateAgent, Rpc::Send, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent],
        "the nudge goes on the same agent"
    );
    let sends = log.saw(Rpc::Send);
    let nudge = sends[1].text("text");
    assert!(
        nudge.starts_with(
            "Your last reply is not one well-formed JSON value (EOF while parsing an object at \
             line 1 column "
        ),
        "{nudge}"
    );
    assert!(!nudge.contains("## Findings"), "no check ran on the malformed document: {nudge}");
}

#[tokio::test]
async fn model_check_exhausted() {
    let fake = Spawnable::new(&Config::replies([FAIL]));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_CHECK_EXHAUSTED, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(
        sequence,
        [Rpc::CreateAgent, Rpc::Send, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent],
        "the round budget is the opening prompt plus one correction"
    );
}

// One tool-calling completion in `codec`: the fake's `CallCustomTool` reached
// the client's endpoint with the URL and token the process was started with.
async fn tool_roundtrip(codec: Codec) {
    let fake = Spawnable::new(&Config::tool("lookup").codec(codec));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_TOOL_ROUNDTRIP, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 1);
    assert_eq!(
        workers[0].saw(Rpc::CreateAgent)[0].arg["customTools"],
        json!(["lookup"]),
        "CreateAgent advertised the guest's function tool"
    );
    let callbacks = workers[0].callbacks();
    assert_eq!(callbacks.len(), 1, "one CallCustomTool POST: {}", log.summary());
    assert_eq!(callbacks[0].arg["status"], 200, "{}", callbacks[0].arg);
    let (_, token) = workers[0].ready().expect("the process logged its callback identity");
    assert_eq!(callbacks[0].text("token"), token, "the POST carried the process's own token");
    let (_, sequence) = sole_agent(&workers[0]);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
}

#[tokio::test]
async fn model_tool_roundtrip() {
    tool_roundtrip(Codec::Json).await;
}

#[tokio::test]
async fn tool_roundtrip_proto() {
    tool_roundtrip(Codec::Proto).await;
}

#[tokio::test]
async fn tool_roundtrip_chunked() {
    tool_roundtrip(Codec::JsonChunked).await;
}

#[tokio::test]
async fn model_tool_failure() {
    let fake = Spawnable::new(&Config::tool("lookup"));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_TOOL_FAILURE, &[], &client).await;
    await_gone(&fake).await;

    // a repairable failure is a successful callback: the model sees it, not the session
    let log = fake.log();
    let callbacks = log.callbacks();
    assert_eq!(callbacks.len(), 1);
    assert_eq!(callbacks[0].arg["status"], 200, "{}", callbacks[0].arg);
}

#[tokio::test]
async fn model_undeclared_tool() {
    let fake = Spawnable::new(&Config::tool("lookup"));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_UNDECLARED_TOOL, &[], &client).await;
    await_gone(&fake).await;

    // the endpoint refuses the call and aborts the completion; the agent is still torn down
    let log = fake.log();
    let callbacks = log.callbacks();
    assert_eq!(callbacks.len(), 1);
    assert_eq!(callbacks[0].arg["status"], 409, "{}", callbacks[0].arg);
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence.first(), Some(&Rpc::CreateAgent));
    assert_eq!(sequence.last(), Some(&Rpc::DeleteAgent));
    assert!(sequence.contains(&Rpc::CloseAgent), "{sequence:?}");
}

#[tokio::test]
async fn model_fanout() {
    const WIDTH: usize = 4;
    let fake = Spawnable::new(&Config::echo());
    let client = spawning(&fake, WIDTH).await;
    run_guest(test_programs::MODEL_FANOUT, &[&WIDTH.to_string()], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), WIDTH, "one process per completion: {}", log.summary());
    for process in &workers {
        assert_eq!(process.count(Rpc::CreateAgent), 1, "process {}", process.number);
        assert!(process.peak_live() <= 1, "no process held two agents: {}", process.summary());
        assert!(process.ended_with(Rpc::Shutdown), "process {} was closed", process.number);
    }
    assert_eq!(log.count(Rpc::CreateAgent), WIDTH);
}

#[tokio::test]
async fn model_tool_fanout() {
    const WIDTH: usize = 4;
    let fake = Spawnable::new(&Config::tool("lookup"));
    let client = spawning(&fake, WIDTH).await;
    run_guest(test_programs::MODEL_TOOL_FANOUT, &[&WIDTH.to_string()], &client).await;
    await_gone(&fake).await;

    // every process chose the same id; callbacks route by each process's own token
    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), WIDTH);
    let mut tokens = Vec::new();
    for process in &workers {
        assert_eq!(process.agents(), ["agent-1"], "process {}", process.number);
        let callbacks = process.callbacks();
        assert_eq!(callbacks.len(), 1, "process {}: {}", process.number, process.summary());
        assert_eq!(callbacks[0].arg["status"], 200, "{}", callbacks[0].arg);
        tokens.push(callbacks[0].text("token").to_owned());
    }
    tokens.sort();
    tokens.dedup();
    assert_eq!(tokens.len(), WIDTH, "each process called back under its own token");
}

#[tokio::test]
async fn model_fanout_abandon() {
    const WIDTH: usize = 4;
    let fake = Spawnable::new(&Config::echo().fault(Fault::Park(Point::Stream)));
    let client = spawning(&fake, WIDTH).await;

    let guest = tokio::spawn({
        let client = client.clone();
        async move {
            run_guest(test_programs::MODEL_FANOUT_ABANDON, &[&WIDTH.to_string()], &client).await;
        }
    });

    // every completion is mid-run at once; the first released wins
    fake.await_parked(Point::Stream, WIDTH).await;
    assert_eq!(fake.log().peak_live(), WIDTH);
    assert!(fake.release_one(Point::Stream));
    guest.await.expect("the guest task joins");
    await_gone(&fake).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), WIDTH, "{}", log.summary());
    let mut cancelled = 0;
    for process in &workers {
        let (agent, sequence) = sole_agent(process);
        let teardown = &sequence[sequence.len() - 2..];
        assert_eq!(teardown, [Rpc::CloseAgent, Rpc::DeleteAgent], "{agent}: {sequence:?}");
        assert_eq!(sequence.iter().filter(|rpc| **rpc == Rpc::CloseAgent).count(), 1);
        assert_eq!(sequence.iter().filter(|rpc| **rpc == Rpc::DeleteAgent).count(), 1);
        match sequence.as_slice() {
            [Rpc::CreateAgent, Rpc::Send, Rpc::CancelRun, ..] => cancelled += 1,
            [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, ..] => {}
            other => panic!("process {}: unexpected sequence {other:?}", process.number),
        }
        assert!(process.ended_with(Rpc::Shutdown), "process {}", process.number);
    }
    assert_eq!(cancelled, WIDTH - 1, "every loser's run was cancelled: {}", log.summary());
}

#[tokio::test]
async fn model_expect_error() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::Send)));
    let client = connect(ConnectOptions {
        inactivity_secs: 1,
        ..options(1)
    })
    .await;
    run_guest(test_programs::MODEL_EXPECT_ERROR, &["inactivity limit 1s"], &client).await;
    await_gone(&fake).await;

    // no run id arrived, so nothing is cancelled
    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
}
