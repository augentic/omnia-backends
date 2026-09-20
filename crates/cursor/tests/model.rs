//! End-to-end tests for the cursor backend at the `omnia:model` boundary:
//! every scenario runs a guest component from `crates/test-programs` through
//! the omnia runtime over an `omnia_cursor::Client`, against the fake
//! `cursor-sdk-bridge` — spawned per lease, or served in-process and
//! attached. The guest asserts what it observes and traps on failure; the
//! test asserts what reached the bridge and that the pool is whole again.

mod support;

use std::time::SystemTime;

use omnia_cursor::ConnectOptions;
use support::fake_bridge::{Codec, Config, FakeBridge, Fault, History as _, Point, Rpc, Spawnable};
use support::harness::{
    attach_options, attached, await_idle, connect, options, run_guest, sole_agent, spawning,
};

// Every guest program in `crates/test-programs` must have a matching test
// here; a new program without one fails to compile.
test_programs::foreach_model!();

/// The candidates the `check_*` scenarios' fake proposes, spelled as the
/// backend's schema extraction re-serializes them (sorted keys), so the
/// correction quotes them verbatim.
const PASS: &str = r#"{"findings":[],"verdict":"pass"}"#;
const FAIL: &str = r#"{"findings":["x"],"verdict":"fail"}"#;

// ------------------------------------------------------------------------
// Scenarios (one per guest program; guest-side assertions live in
// `crates/test-programs/programs/model/`)
// ------------------------------------------------------------------------

#[tokio::test]
async fn model_echo_text() {
    let fake = Spawnable::new(&Config::echo());
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    let returned = SystemTime::now();
    await_idle(&client, 1).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 1, "one process served the completion: {}", log.summary());
    let (agent, sequence) = sole_agent(&workers[0]);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
    assert!(workers[0].ended_with(Rpc::Shutdown), "the lease closed its process");
    assert!(log.process(0).is_some_and(|probe| probe.ended_with(Rpc::Shutdown)));

    // The answer waits on the teardown.
    let deleted = workers[0].saw(Rpc::DeleteAgent)[0];
    assert!(deleted.at() <= returned, "DeleteAgent landed before the guest returned");

    // Delete is scoped to the create-time cwd and repeats the API key.
    let created = workers[0].saw(Rpc::CreateAgent)[0];
    assert_eq!(created.agent.as_deref(), Some(agent.as_str()));
    assert!(!created.text("cwd").is_empty(), "CreateAgent names a cwd: {}", created.arg);
    assert_eq!(deleted.text("cwd"), created.text("cwd"));
    assert_eq!(deleted.text("apiKey"), created.text("apiKey"));
    assert!(!deleted.text("apiKey").is_empty());
}

#[tokio::test]
async fn model_check_accepted() {
    let fake = Spawnable::new(&Config::replies([PASS]));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_CHECK_ACCEPTED, &[], &client).await;
    await_idle(&client, 1).await;

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
    await_idle(&client, 1).await;

    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(
        sequence,
        [Rpc::CreateAgent, Rpc::Send, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent],
        "the correction goes on the same agent"
    );
    // The agent keeps its session, so the second send is the guest's
    // correction alone, verbatim.
    let sends = log.saw(Rpc::Send);
    let correction = sends[1].text("text");
    assert!(correction.starts_with("## Previous answer (rejected)\n\n"), "{correction}");
    assert!(correction.contains(FAIL), "{correction}");
    assert!(correction.contains("## Findings\n\nverdict must be `pass`"), "{correction}");
}

#[tokio::test]
async fn model_check_exhausted() {
    let fake = Spawnable::new(&Config::replies([FAIL]));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_CHECK_EXHAUSTED, &[], &client).await;
    await_idle(&client, 1).await;

    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(
        sequence,
        [Rpc::CreateAgent, Rpc::Send, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent],
        "the round budget is the opening prompt plus one correction"
    );
}

/// One tool-calling completion in `codec`: the fake's `CallCustomTool`
/// reached the client's own endpoint with the URL and token the process
/// was started with, and was answered.
async fn tool_roundtrip(codec: Codec) {
    let fake = Spawnable::new(&Config::tool("lookup").codec(codec));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_TOOL_ROUNDTRIP, &[], &client).await;
    await_idle(&client, 1).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 1);
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
    await_idle(&client, 1).await;

    // A repairable failure is a successful callback: the model, not the
    // session, sees it.
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
    await_idle(&client, 1).await;

    // The endpoint refuses the call and aborts the completion; the run
    // ends and the agent is still torn down.
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
    await_idle(&client, WIDTH).await;

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
    await_idle(&client, WIDTH).await;

    // Every process chose the same id; the callbacks still reached the
    // right completion, routed by each process's own token.
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
    let fake = FakeBridge::serve(Config::echo().fault(Fault::Park(Point::Stream))).await;
    let client = attached(&fake, WIDTH).await;

    let guest = tokio::spawn({
        let client = client.clone();
        async move {
            run_guest(test_programs::MODEL_FANOUT_ABANDON, &[&WIDTH.to_string()], &client).await;
        }
    });
    // Every completion is mid-run at once; the first released is the winner.
    fake.await_parked(Point::Stream, WIDTH).await;
    assert_eq!(fake.log().peak_live(), WIDTH);
    assert!(fake.release_one(Point::Stream));
    guest.await.expect("the guest task joins");
    await_idle(&client, WIDTH).await;

    let log = fake.log();
    let agents = log.agents();
    assert_eq!(agents.len(), WIDTH);
    let mut cancelled = 0;
    for agent in &agents {
        let sequence = log.sequence(agent);
        let teardown = &sequence[sequence.len() - 2..];
        assert_eq!(teardown, [Rpc::CloseAgent, Rpc::DeleteAgent], "{agent}: {sequence:?}");
        assert_eq!(sequence.iter().filter(|rpc| **rpc == Rpc::CloseAgent).count(), 1);
        assert_eq!(sequence.iter().filter(|rpc| **rpc == Rpc::DeleteAgent).count(), 1);
        match sequence.as_slice() {
            [Rpc::CreateAgent, Rpc::Send, Rpc::CancelRun, ..] => cancelled += 1,
            [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, ..] => {}
            other => panic!("{agent}: unexpected sequence {other:?}"),
        }
    }
    assert_eq!(cancelled, WIDTH - 1, "every loser's run was cancelled: {}", log.summary());
    assert_eq!(log.count(Rpc::Shutdown), 0, "an attached bridge is never shut down");
}

#[tokio::test]
async fn model_expect_error() {
    let fake = FakeBridge::serve(Config::echo().fault(Fault::Hang(Point::Send))).await;
    let client = connect(attach_options(
        &fake,
        ConnectOptions {
            inactivity_secs: 1,
            ..options(1)
        },
    ))
    .await;
    run_guest(test_programs::MODEL_EXPECT_ERROR, &["inactivity limit 1s"], &client).await;
    await_idle(&client, 1).await;

    // No run id ever arrived, so nothing is cancelled; the agent is still
    // closed and deleted.
    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
}
