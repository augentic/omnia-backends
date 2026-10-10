//! Lifecycle and fault matrix for the cursor backend: every row runs a guest
//! from `crates/test-programs` over an `omnia_cursor::Client` against the
//! fake `cursor-sdk-bridge`, then asserts the fake's per-agent RPC sequence
//! and every spawned process gone within a stated bound. No row drives
//! `Client::complete` from the test. The restart rows pin the one retry the
//! client makes — a worker lost before any candidate reached the guest — and
//! that no other loss earns one.

mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use http::Method;
use omnia::Backend as _;
use omnia_cursor::{Client, ConnectOptions};
use omnia_wasi_model::{Limits, WasiModelCtx};
use serde_json::json;
use support::fake_bridge::{
    self, Config, Event, Fault, History as _, Log, Point, Process, Rpc, Spawnable, Then,
};
use support::harness::{
    AT_ONCE, CALLBACK_PATH, GONE, STARTUP, await_forked_gone, await_gone, await_gone_within,
    await_process_gone, callback, connect, expect_error, options, run_guest, sole_agent, spawning,
};
use tokio::task::JoinHandle;
use tracing_subscriber::layer::SubscriberExt as _;

// The inactivity window the deadline rows run with.
const WINDOW: Duration = Duration::from_secs(1);
// `agent.rs`'s bound on one teardown call.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);
// `worker.rs`'s bound on a graceful exit: `Shutdown` and the exit it asks for.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
// `worker.rs`'s bound on binding `sdk.v1` over the ready line.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
// A bound's timer starts a round trip before the fake records the arrival, so a
// span measured between the fake's own timestamps can fall this far short of it.
const TRIP: Duration = Duration::from_millis(50);

fn with_window(window: Duration, max_agents: usize) -> ConnectOptions {
    ConnectOptions {
        inactivity_secs: window.as_secs(),
        ..options(max_agents)
    }
}

// The `fanout_abandon` guest on its own task, so the test can steer the
// fake while it runs.
fn abandon_guest(client: &Client, width: usize) -> JoinHandle<()> {
    let client = client.clone();
    tokio::spawn(async move {
        run_guest(test_programs::MODEL_FANOUT_ABANDON, &[&width.to_string()], &client).await;
    })
}

fn echo_guest(client: &Client) -> JoinHandle<()> {
    let client = client.clone();
    tokio::spawn(async move { run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await })
}

async fn await_rpcs(log: impl Fn() -> Log + Sync, rpc: Rpc, count: usize, within: Duration) {
    fake_bridge::poll(|| log().count(rpc) >= count, within, &format!("{count} {rpc:?}(s)")).await;
}

fn killed(process: &Process) {
    assert!(!process.alive(), "process {} (pid {}) is still up", process.number, process.pid);
    assert_eq!(process.count(Rpc::Shutdown), 0, "process {} was killed, not asked", process.number);
}

// The `Failure::WorkerExited` detail a `SIGKILL`ed worker fails with.
const KILLED: &str = "cursor-sdk-bridge exited (signal: 9 (SIGKILL))";
const ANSWERED: [Rpc; 4] = [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent];
// A completion whose run was still open when it ended — at a deadline,
// dropped, or with its stream lost — is cancelled and billed before teardown.
const CANCELLED: [Rpc; 6] =
    [Rpc::CreateAgent, Rpc::Send, Rpc::CancelRun, Rpc::GetUsage, Rpc::CloseAgent, Rpc::DeleteAgent];
// A completion dropped once its agent existed but before its send: no run
// to cancel or bill, so teardown alone.
const UNSENT: [Rpc; 3] = [Rpc::CreateAgent, Rpc::CloseAgent, Rpc::DeleteAgent];

// The two workers of a two-way abandon: the one that recorded `rpc`, and
// the one that did not.
fn split_by(log: &Log, rpc: Rpc) -> (Process, Process) {
    let mut workers = log.workers().into_iter();
    let (Some(first), Some(second), None) = (workers.next(), workers.next(), workers.next()) else {
        panic!("two workers: {}", log.summary());
    };
    let (with, without) = if first.count(rpc) > 0 { (first, second) } else { (second, first) };
    assert!(with.count(rpc) > 0, "neither worker saw {rpc:?}: {}", log.summary());
    assert_eq!(without.count(rpc), 0, "both workers saw {rpc:?}: {}", log.summary());
    (with, without)
}

// The two workers a restarted completion leaves behind, in start order.
fn restarted(log: &Log) -> (Process, Process) {
    let mut workers = log.workers().into_iter();
    let (first, second) = (workers.next(), workers.next());
    assert!(workers.next().is_none(), "at most one restart per completion: {}", log.summary());
    let first = first.expect("a first attempt");
    let second = second.unwrap_or_else(|| panic!("no second attempt: {}", log.summary()));
    (first, second)
}

// The second attempt began only once the first attempt's process was
// gone: with one slot, the restart waits for the dead lease to be reaped.
fn restarted_after(first: &Process, second: &Process) {
    let first_last = first.events.last().map(Event::at).expect("the first attempt recorded");
    let second_first = second.events.first().map(Event::at).expect("the second attempt recorded");
    assert!(
        second_first >= first_last,
        "process {} started {:?} before process {} was done",
        second.number,
        first_last.duration_since(second_first).unwrap_or_default(),
        first.number
    );
}

// --- Configuration ---

#[tokio::test]
async fn connect_rejects_invalid_options() {
    fake_bridge::dummy_key();
    let fake = Spawnable::new(&Config::echo());
    let good = options(1);
    let bad = [
        (
            ConnectOptions {
                max_agents: 0,
                ..good.clone()
            },
            "max_agents must be greater than 0",
        ),
        (
            ConnectOptions {
                inactivity_secs: 0,
                ..good.clone()
            },
            "inactivity_secs must be greater than 0",
        ),
        (
            ConnectOptions {
                timeout_secs: 0,
                ..good.clone()
            },
            "timeout_secs must be greater than 0",
        ),
        (
            ConnectOptions {
                max_tool_calls: 0,
                ..good.clone()
            },
            "max_tool_calls must be greater than 0",
        ),
    ];
    for (options, needle) in bad {
        let error = Client::connect_with(options.clone())
            .await
            .expect_err(&format!("accepted {options:?}"));
        assert!(format!("{error:#}").contains(needle), "{options:?}: {error:#}");
    }
    assert!(fake.log().events.is_empty(), "a rejected option spawned a process");
}

// The tool-call budget the host enforces on each completion is read from
// the backend through `WasiModelCtx::limits`, so the option is what the
// session counts against; the other bounds stay the host's.
#[tokio::test]
async fn limits_follow_options() {
    fake_bridge::dummy_key();
    let client = Client::connect_with(ConnectOptions {
        max_tool_calls: 5,
        ..options(1)
    })
    .await
    .expect("the options are accepted");
    let limits = WasiModelCtx::limits(&client);
    assert_eq!(limits.max_tool_calls, 5);
    assert_eq!(limits.max_result_bytes, Limits::default().max_result_bytes);
    assert_eq!(limits.tool_timeout, Limits::default().tool_timeout);
}

// A keyless client connects and fails its first completion instead: the key
// is read at connect and required at attempt, before any lease is taken.
#[tokio::test]
#[expect(unsafe_code, reason = "the key is read from the process environment")]
async fn keyless_complete() {
    // SAFETY: nextest runs one test per process, so removing the key races
    // with nothing; every other row sets a dummy key and never unsets it.
    unsafe { std::env::remove_var("CURSOR_API_KEY") };
    let fake = Spawnable::new(&Config::echo());
    let client = Client::connect_with(options(1)).await.expect("connect succeeds without a key");
    expect_error("CURSOR_API_KEY must be set", &[], &client).await;
    await_gone(&fake).await;
    assert!(fake.log().events.is_empty(), "a keyless completion spawned a process");
}

// --- Abandon: the guest drops a completion at every point it can be waiting ---

// Process 2 takes `Ping` and never answers: the loser is dropped with its
// ready line seen but no RPC bound.
#[tokio::test]
async fn abandon_during_handshake() {
    let fake = Spawnable::new(&Config::echo().fault_on(2, Fault::Hang(Point::Ping)));
    let client = spawning(&fake, 2).await;
    run_guest(test_programs::MODEL_FANOUT_ABANDON, &["2"], &client).await;

    // nothing is bound to ask an unbound worker: killed at once, well under the graceful bound
    await_gone_within(&fake, AT_ONCE).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 2, "{}", log.summary());
    let (winner, loser) = (&workers[0], &workers[1]);
    let (_, sequence) = sole_agent(winner);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
    assert!(winner.ended_with(Rpc::Shutdown));
    assert_eq!(loser.last_rpc(), Some(Rpc::Ping), "{}", loser.summary());
    assert_eq!(loser.count(Rpc::CreateAgent), 0);
    killed(loser);
}

// Process 2 forks a child, then never prints its ready line: the loser is
// dropped inside the stderr scan.
#[tokio::test]
async fn abandon_before_ready() {
    let fake = Spawnable::new(
        &Config::echo().fault_on(2, Fault::Grandchild).fault_on(2, Fault::NeverReady),
    );
    let client = spawning(&fake, 2).await;
    run_guest(test_programs::MODEL_FANOUT_ABANDON, &["2"], &client).await;
    await_gone_within(&fake, AT_ONCE).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 2, "{}", log.summary());
    let loser = &workers[1];
    assert!(loser.ready().is_none(), "process 2 never got to its ready line");
    assert_eq!(loser.last_rpc(), None);
    assert_eq!(log.count(Rpc::CreateAgent), 1, "only the winner made an agent");
    killed(loser);

    // the kill is the group's, so the forked child goes with it
    await_forked_gone(loser).await;
}

// The loser's create is held until the row releases it, inside the window
// the client waits for an id, so the window is a wide one.
#[tokio::test]
async fn abandon_during_create() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Park(Point::CreateAgent)));
    let client = connect(with_window(Duration::from_secs(30), 2)).await;

    let guest = abandon_guest(&client, 2);
    fake.await_parked(Point::CreateAgent, 2).await;
    assert!(fake.release_one(Point::CreateAgent));
    guest.await.expect("the guest task joins");

    // the held create owns its slot until its late id is deleted; the winner's process goes
    assert_eq!(fake.parked(Point::CreateAgent), 1);
    let (winner, loser) = split_by(&fake.log(), Rpc::Send);
    await_process_gone(&winner).await;
    assert!(loser.alive(), "the abandoned create holds its slot");
    assert_eq!(fake.log().count(Rpc::DeleteAgent), 1, "only the winner is torn down yet");

    fake.release_all();
    await_rpcs(|| fake.log(), Rpc::DeleteAgent, 2, GONE).await;
    await_gone(&fake).await;
    let log = fake.log();
    let loser = log.process(loser.number).expect("the loser's history");
    let (_, sequence) = sole_agent(&loser);
    assert_eq!(sequence, UNSENT);
    let created = loser.saw(Rpc::CreateAgent)[0];
    let deleted = loser.saw(Rpc::DeleteAgent)[0];
    assert_eq!(
        deleted.text("cwd"),
        created.text("cwd"),
        "the late id is deleted where it was made"
    );
    assert_eq!(deleted.arg["apiKeyPresent"], true);
    assert_eq!(deleted.arg["apiKeyMatchesCreate"], true);
    assert!(loser.ended_with(Rpc::Shutdown));

    // both slots are back
    let fresh = echo_guest(&client);
    fake.await_parked(Point::CreateAgent, 1).await;
    fake.release_all();
    fresh.await.expect("the fresh guest joins");
    await_gone(&fake).await;
    let log = fake.log();
    assert_eq!(log.workers().len(), 3, "{}", log.summary());
    assert_eq!(log.count(Rpc::CreateAgent), 3);
    assert_eq!(log.count(Rpc::DeleteAgent), 3);
}

#[tokio::test]
async fn abandon_before_stream() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Park(Point::Send)));
    let client = spawning(&fake, 2).await;

    let guest = abandon_guest(&client, 2);
    fake.await_parked(Point::Send, 2).await;
    assert!(fake.release_one(Point::Send));
    guest.await.expect("the guest task joins");
    await_gone(&fake).await;

    // no stream opened for the loser, so nothing to cancel
    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 2, "{}", log.summary());
    for process in &workers {
        let (_, sequence) = sole_agent(process);
        assert_eq!(sequence, ANSWERED, "process {}", process.number);
        assert!(process.ended_with(Rpc::Shutdown), "process {}", process.number);
    }
    assert_eq!(log.count(Rpc::CancelRun), 0);
}

#[tokio::test]
async fn abandon_during_teardown() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Park(Point::CloseAgent)));
    let client = spawning(&fake, 2).await;

    let guest = abandon_guest(&client, 2);
    fake.await_parked(Point::CloseAgent, 2).await;
    guest.await.expect("the guest task joins");

    // both teardowns run on past the answer; the one released goes, the one
    // still parked keeps its process
    assert!(fake.release_one(Point::CloseAgent));
    await_rpcs(|| fake.log(), Rpc::DeleteAgent, 1, GONE).await;
    assert_eq!(fake.parked(Point::CloseAgent), 1);
    let (released, parked) = split_by(&fake.log(), Rpc::DeleteAgent);
    await_process_gone(&released).await;
    assert!(parked.alive(), "the teardown in flight holds its slot");

    fake.release_all();
    await_rpcs(|| fake.log(), Rpc::DeleteAgent, 2, GONE).await;
    await_gone(&fake).await;
    let log = fake.log();
    for process in log.workers() {
        // the winner answered before its teardown, so the loser may have been
        // dropped mid-run, or before its send
        let (_, sequence) = sole_agent(&process);
        assert!(
            sequence == ANSWERED || sequence == CANCELLED || sequence == UNSENT,
            "process {}: {sequence:?}",
            process.number
        );
        assert!(process.ended_with(Rpc::Shutdown), "process {}", process.number);
    }
}

// Three completions on two slots: process 1 answers once process 2 has
// opened a run, process 2 hangs mid-run, and the third is still queued when
// the guest drops it. `Send` is recorded before the stream's first event, so
// a winner answering at once could drop the loser with no run id to cancel;
// lingering after `Shutdown` keeps the third completion queued.
#[tokio::test]
async fn abandon_while_queued() {
    let fake = Spawnable::new(
        &Config::echo()
            .fault_on(1, Fault::WaitForPeer(Rpc::Send))
            .fault_on(1, Fault::LingerOnShutdown(1500))
            .fault_on(2, Fault::Hang(Point::Stream)),
    );
    let client = spawning(&fake, 2).await;
    run_guest(test_programs::MODEL_FANOUT_ABANDON, &["3"], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 2, "the queued completion never got a process: {}", log.summary());
    assert_eq!(log.count(Rpc::CreateAgent), 2);
    let (_, winner) = sole_agent(&workers[0]);
    assert_eq!(winner, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
    let (_, loser) = sole_agent(&workers[1]);
    assert_eq!(loser, CANCELLED, "the mid-run loser is cancelled and torn down");
    for process in &workers {
        assert!(process.ended_with(Rpc::Shutdown), "process {}", process.number);
    }
}

// --- Pooling ---

#[tokio::test]
async fn lease_waits() {
    let fake = Spawnable::new(&Config::echo());
    let client = spawning(&fake, 2).await;
    run_guest(test_programs::MODEL_FANOUT, &["3"], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 3, "{}", log.summary());
    assert!(log.peak_live() <= 2, "no more agents than slots: {}", log.summary());

    // the third agent waited for a slot, which reopened only once a process was gone
    let creates = log.saw(Rpc::CreateAgent);
    let first_close = log.saw(Rpc::CloseAgent)[0].at();
    let first_shutdown = workers
        .iter()
        .filter_map(|process| process.saw(Rpc::Shutdown).first().map(|e| e.at()))
        .min()
        .expect("a worker was shut down");
    assert!(creates[2].at() > first_close, "the third agent waited for a slot");
    assert!(creates[2].at() > first_shutdown, "the slot reopened only once its process was gone");
    for process in &workers {
        assert!(process.ended_with(Rpc::Shutdown), "process {}", process.number);
    }
}

// `CreateAgent` never answered, one slot: the completion gives up after one
// window and its worker goes with nothing to tear down; the next completion
// gets a worker of its own.
#[tokio::test]
async fn hang_on_create() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::CreateAgent)));
    let client = connect(with_window(WINDOW, 1)).await;
    expect_error("unanswered after 1s", &[], &client).await;
    expect_error("unanswered after 1s", &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 2, "{}", log.summary());
    assert_eq!(log.count(Rpc::CloseAgent), 0, "no id arrived to close");
    assert_eq!(log.count(Rpc::DeleteAgent), 0);
    for process in &workers {
        assert_eq!(process.count(Rpc::CreateAgent), 1, "process {}", process.number);
        assert!(process.ended_with(Rpc::Shutdown), "process {}", process.number);
    }
}

#[tokio::test]
async fn create_returns_empty_id() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::EmptyId));
    let client = spawning(&fake, 1).await;
    expect_error("returned an empty agent id", &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    assert_eq!(log.count(Rpc::CreateAgent), 1);
    assert!(log.agents().is_empty(), "no id was handed out: {}", log.summary());
    assert_eq!(log.count(Rpc::CloseAgent), 0, "nothing to tear down");
    assert_eq!(log.count(Rpc::DeleteAgent), 0);
    assert!(log.workers()[0].ended_with(Rpc::Shutdown));
}

// --- Deadlines ---

#[tokio::test]
async fn hang_on_send() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::Send)));
    let client = connect(with_window(WINDOW, 1)).await;
    expect_error("inactive for 1s", &[], &client).await;
    await_gone(&fake).await;

    // the stream never opened, so nothing to cancel
    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
    assert!(log.workers()[0].ended_with(Rpc::Shutdown));
}

// Activity every 200ms keeps the inactivity window rearmed; the 1s cap ends
// the run anyway, as the guest's time budget rather than a backend failure,
// and the run it noted is cancelled.
#[tokio::test]
async fn cap_hits() {
    let fake = Spawnable::new(&Config::paced(200, 1000, Then::Hang));
    let client = connect(ConnectOptions {
        timeout_secs: 1,
        ..options(1)
    })
    .await;
    expect_error(
        "timed out after 1s (absolute cap exceeded while still active)",
        &["budget"],
        &client,
    )
    .await;
    await_gone(&fake).await;

    let (_, sequence) = sole_agent(&fake.log());
    assert_eq!(sequence, CANCELLED);
}

// The bridge's shell started and never ended: the window stands down for
// the command the bridge itself bounds, and the 2s cap ends the run as the
// guest's time budget.
#[tokio::test]
async fn shell_stalls() {
    let fake = Spawnable::new(&Config::started("shell", json!({ "command": "cargo test" })));
    let client = connect(ConnectOptions {
        timeout_secs: 2,
        ..with_window(WINDOW, 1)
    })
    .await;
    expect_error("timed out after 2s", &["budget"], &client).await;
    await_gone(&fake).await;

    let (_, sequence) = sole_agent(&fake.log());
    assert_eq!(sequence, CANCELLED);
}

// A guest tool the agent named `shell` rides under the bridge's `mcp`, a
// tool like any other: a stall on it is the window's to end, not the cap's.
#[tokio::test]
async fn custom_shell_stalls() {
    let custom = json!({ "providerIdentifier": "custom-user-tools", "toolName": "shell",
        "args": { "command": "cargo test" } });
    let fake = Spawnable::new(&Config::started("mcp", custom));
    let client = connect(ConnectOptions {
        timeout_secs: 30,
        ..with_window(WINDOW, 1)
    })
    .await;
    expect_error("inactive for 1s waiting on tool `shell`", &[], &client).await;
    await_gone(&fake).await;

    let (_, sequence) = sole_agent(&fake.log());
    assert_eq!(sequence, CANCELLED);
}

// Five frames 400ms apart outlast the 1s window several times over: the
// model's frames stand it down, and the cap is nowhere near.
#[tokio::test]
async fn slow_stream_completes() {
    let fake = Spawnable::new(&Config::paced(400, 5, Then::Finish));
    let client = connect(with_window(WINDOW, 1)).await;
    let started = Instant::now();
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    assert!(started.elapsed() >= Duration::from_secs(2), "the run streamed for 2s");
    await_gone(&fake).await;

    let (_, sequence) = sole_agent(&fake.log());
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
}

// --- Process death ---

// Process 1 dies as the opening `Send` begins: no candidate has been offered,
// so the prompt goes once more, on process 2, and the guest gets its answer.
#[tokio::test]
async fn worker_killed_on_send_restarts() {
    let fake = Spawnable::new(&Config::echo().fault_on(1, Fault::KillOnSend(1)));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (first, second) = restarted(&log);
    let (_, sequence) = sole_agent(&first);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send], "{}", first.summary());
    killed(&first);
    let (_, sequence) = sole_agent(&second);
    assert_eq!(sequence, ANSWERED, "{}", second.summary());
    assert!(second.ended_with(Rpc::Shutdown));
    restarted_after(&first, &second);
}

#[tokio::test]
async fn worker_exited_on_create_restarts() {
    let fake = Spawnable::new(&Config::echo().fault_on(1, Fault::ExitOnCreate(1)));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (first, second) = restarted(&log);

    // the fake exits before recording the create
    assert_eq!(first.count(Rpc::CreateAgent), 0, "{}", first.summary());
    killed(&first);
    let (_, sequence) = sole_agent(&second);
    assert_eq!(sequence, ANSWERED, "{}", second.summary());
    assert!(second.ended_with(Rpc::Shutdown));
    restarted_after(&first, &second);
}

// Each process forks a child that outlives it: process 1 dies on its `Send`
// with the child still up and holding the stderr pipe, process 2 exits on
// `Shutdown` the same way. Neither child is anyone's to reap; both go with
// their process as its exit is seen.
#[tokio::test]
async fn grandchildren_swept() {
    let fake =
        Spawnable::new(&Config::echo().fault(Fault::Grandchild).fault_on(1, Fault::KillOnSend(1)));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (first, second) = restarted(&log);
    killed(&first);
    assert!(second.ended_with(Rpc::Shutdown), "{}", second.summary());
    for process in [&first, &second] {
        await_forked_gone(process).await;
    }
}

// Every process dies on its opening `Send`: one restart, then the second exit
// stands. The socket resets as each process dies; the typed exit wins over
// the transport error, and what the processes wrote to stderr stays out.
#[tokio::test]
async fn worker_killed_twice_fails() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::KillOnSend(1)));
    let client = spawning(&fake, 1).await;
    expect_error(KILLED, &["without:fake-bridge marker"], &client).await;

    // teardown is skipped on a dead worker: no timeout is paid
    await_gone_within(&fake, AT_ONCE).await;

    let log = fake.log();
    let (first, second) = restarted(&log);
    for process in [&first, &second] {
        let (_, sequence) = sole_agent(process);
        assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send], "{}", process.summary());
        killed(process);
    }
}

// The first candidate reached the guest's check and was rejected; the
// process dies on the correction's `Send`. The guest has seen this agent, so
// the prompt is not offered again.
#[tokio::test]
async fn killed_after_candidate_fails() {
    let fake =
        Spawnable::new(&Config::replies(["alpha", "beta"]).fault_on(1, Fault::KillOnSend(2)));
    let client = spawning(&fake, 1).await;
    expect_error(KILLED, &["check"], &client).await;
    await_gone_within(&fake, AT_ONCE).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 1, "no restart after a candidate: {}", log.summary());
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::Send]);
    let sends = log.saw(Rpc::Send);
    assert!(
        sends[1].text("text").contains("rejects every candidate"),
        "the second send carried the guest's correction: {}",
        sends[1].arg
    );
    killed(&workers[0]);
}

// The opening reply was narration, not JSON, so the nudge went on the same
// session and the process died on its `Send`. No candidate has reached the
// guest, but the opening prompt has been answered — whatever tools it ran
// have run — so the prompt is not offered again.
#[tokio::test]
async fn killed_on_nudge_fails() {
    let fake = Spawnable::new(
        &Config::replies(["Analyzing the claims.", "{}"]).fault_on(1, Fault::KillOnSend(2)),
    );
    let client = spawning(&fake, 1).await;
    expect_error(KILLED, &["nudged"], &client).await;
    await_gone_within(&fake, AT_ONCE).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 1, "no restart after the opening answer: {}", log.summary());
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::Send]);
    let sends = log.saw(Rpc::Send);
    assert!(
        sends[1].text("text").starts_with("Your last reply is not one well-formed JSON value"),
        "the second send was the nudge: {}",
        sends[1].arg
    );
    killed(&workers[0]);
}

// A worker up and silent once its stream has opened is not a lost worker,
// and the silence is the model's: the cap ends the run, which is cancelled,
// and the failure stands.
#[tokio::test]
async fn silent_run_not_restarted() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::Stream)));
    let client = connect(ConnectOptions {
        timeout_secs: 1,
        ..with_window(WINDOW, 1)
    })
    .await;
    expect_error("timed out after 1s", &["budget"], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 1, "no restart on a stall: {}", log.summary());
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, CANCELLED);
    assert!(workers[0].ended_with(Rpc::Shutdown));
}

#[tokio::test]
async fn exit_before_ready() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::ExitBeforeReady(3)));
    let client = spawning(&fake, 1).await;
    expect_error(
        "handshake failed: cursor-sdk-bridge exited (exit status: 3)",
        &["without:fake-bridge marker"],
        &client,
    )
    .await;
    await_gone(&fake).await;

    let log = fake.log();
    let worker = &log.workers()[0];
    assert!(worker.ready().is_none(), "no ready line was printed");
    assert_eq!(log.count(Rpc::CreateAgent), 0);
    assert!(!worker.alive());
}

#[tokio::test]
async fn ready_then_refused() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::ReadyThenRefused));
    let client = spawning(&fake, 1).await;
    expect_error("handshake failed", &[], &client).await;
    await_gone(&fake).await;

    // nothing answered at the ready line's url: the process is not left behind
    let log = fake.log();
    let worker = &log.workers()[0];
    assert!(worker.ready().is_some());
    assert_eq!(worker.count(Rpc::Ping), 0);
    killed(worker);
}

#[tokio::test]
async fn ready_line_not_loopback() {
    let fake =
        Spawnable::new(&Config::echo().fault(Fault::ReadyUrl("http://192.0.2.1:9".to_owned())));
    let client = spawning(&fake, 1).await;
    expect_error("loopback", &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let worker = &log.workers()[0];
    assert_eq!(worker.count(Rpc::Ping), 0, "nothing went on the wire");
    killed(worker);
}

// --- Silence ---

#[tokio::test]
async fn hang_on_teardown() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::CloseAgent)));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    let returned = SystemTime::now();
    await_gone(&fake).await;

    // the hung close is bounded, not fatal: delete still follows, and the answer never waited on it
    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
    let closed = log.saw(Rpc::CloseAgent)[0].at();
    let waited = returned.duration_since(closed).unwrap_or_default();
    assert!(waited < TEARDOWN_TIMEOUT, "the answer arrived {waited:?} after CloseAgent");
    let deleted = log.saw(Rpc::DeleteAgent)[0].at();
    let held = deleted.duration_since(closed).unwrap_or_default();
    let bound = TEARDOWN_TIMEOUT.saturating_sub(TRIP);
    assert!(held >= bound, "DeleteAgent followed {held:?} after CloseAgent");
}

#[tokio::test]
async fn hang_on_shutdown() {
    let fake = Spawnable::new(
        &Config::echo().fault(Fault::Hang(Point::Shutdown)).fault(Fault::Grandchild),
    );
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    assert!(fake.log().workers()[0].alive(), "the process is still up");

    // shutdown unanswered for one bound, then the group is killed, forked child included
    await_gone_within(&fake, SHUTDOWN_TIMEOUT + GONE).await;
    let gone = SystemTime::now();

    let worker = &fake.log().workers()[0];
    assert_eq!(worker.last_rpc(), Some(Rpc::Shutdown));
    let asked = worker.saw(Rpc::Shutdown)[0].at();
    let held = gone.duration_since(asked).unwrap_or_default();
    let bound = SHUTDOWN_TIMEOUT.saturating_sub(TRIP);
    assert!(held >= bound, "the process was gone {held:?} after Shutdown");
    await_forked_gone(worker).await;
}

#[tokio::test]
async fn handshake_hangs() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::Ping)));
    let client = spawning(&fake, 1).await;
    let started = Instant::now();
    expect_error(
        &format!("no answer to the sdk.v1 handshake within {}s", CONNECT_TIMEOUT.as_secs()),
        &[],
        &client,
    )
    .await;
    assert!(started.elapsed() >= CONNECT_TIMEOUT);
    await_gone(&fake).await;

    let worker = &fake.log().workers()[0];
    assert_eq!(worker.last_rpc(), Some(Rpc::Ping));
    killed(worker);
}

// --- Transport ---

#[tokio::test]
async fn close_500() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::CloseFails));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    // a failed close is logged; delete still follows
    let (_, sequence) = sole_agent(&fake.log());
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
}

// Process 1 resets its opening run's stream after its first frame and stays
// up: the socket failure alone is the lost worker. The run id that frame
// carried is cancelled, the agent torn down and the process asked to go
// before the restart takes the slot.
#[tokio::test]
async fn stream_reset_restarts() {
    let fake = Spawnable::new(&Config::echo().fault_on(1, Fault::ResetStream(1)));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (first, second) = restarted(&log);
    let (_, sequence) = sole_agent(&first);
    assert_eq!(sequence, CANCELLED, "{}", first.summary());
    assert_eq!(first.saw(Rpc::CancelRun)[0].text("runId"), "run-1");
    assert!(first.ended_with(Rpc::Shutdown), "{}", first.summary());
    let (_, sequence) = sole_agent(&second);
    assert_eq!(sequence, ANSWERED, "{}", second.summary());
    assert!(second.ended_with(Rpc::Shutdown));
    restarted_after(&first, &second);
}

// Every process resets its opening stream: the second transport failure is
// the guest's, typed below Connect.
#[tokio::test]
async fn stream_reset_twice_fails() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::ResetStream(1)));
    let client = spawning(&fake, 1).await;
    expect_error(
        "sdk.v1 RPC `SdkAgentService/Send` transport failed reading the stream",
        &[],
        &client,
    )
    .await;
    await_gone(&fake).await;

    let log = fake.log();
    let (first, second) = restarted(&log);
    for process in [&first, &second] {
        let (_, sequence) = sole_agent(process);
        assert_eq!(sequence, CANCELLED, "{}", process.summary());
        assert!(process.ended_with(Rpc::Shutdown), "{}", process.summary());
    }
}

// --- Callback endpoint ---

// A run that streams for a few seconds keeps its agent live while the test
// knocks on the endpoint the way a misbehaving worker would.
#[tokio::test]
async fn callback_rejections() {
    let fake = Spawnable::new(&Config::paced(200, 20, Then::Finish));
    let client = spawning(&fake, 1).await;
    let guest = echo_guest(&client);

    // the guest's component is compiled on the way to its send
    await_rpcs(|| fake.log(), Rpc::Send, 1, STARTUP).await;
    let worker = &fake.log().workers()[0];
    let (base, token) = worker.ready().expect("the process logged its callback identity");
    let url = format!("{base}{CALLBACK_PATH}");
    let agent = worker.agents()[0].clone();
    let body = json!({ "toolName": "lookup", "args": {}, "agentId": agent });

    let (status, reply) = callback(Method::POST, &url, Some("wrong"), &body).await;
    assert_eq!((status, reply["code"].as_str()), (401, Some("unauthenticated")), "{reply}");

    // a body in flight when the head is rejected must be drained, or the reply is a tcp reset
    let padded =
        json!({ "toolName": "lookup", "args": { "pad": "x".repeat(1 << 20) }, "agentId": agent });
    let (status, reply) = callback(Method::POST, &url, Some("wrong"), &padded).await;
    assert_eq!((status, reply["code"].as_str()), (401, Some("unauthenticated")), "{reply}");
    let (status, reply) =
        callback(Method::POST, &format!("{base}/elsewhere"), Some(&token), &body).await;
    assert_eq!((status, reply["code"].as_str()), (404, Some("not_found")), "{reply}");
    assert!(reply["message"].as_str().unwrap_or_default().contains("unknown callback path"));
    let (status, reply) = callback(Method::GET, &url, Some(&token), &body).await;
    assert_eq!((status, reply["code"].as_str()), (405, Some("unimplemented")), "{reply}");
    let ghost = json!({ "toolName": "lookup", "args": {}, "agentId": "ghost" });
    let (status, reply) = callback(Method::POST, &url, Some(&token), &ghost).await;
    assert_eq!((status, reply["code"].as_str()), (404, Some("not_found")), "{reply}");
    assert!(reply["message"].as_str().unwrap_or_default().contains("no live completion"));

    guest.await.expect("the guest task joins");
    await_gone(&fake).await;
    assert_eq!(fake.log().callbacks().len(), 0, "the fake itself never called back");
}

// The process lingers after `Shutdown`, so there is a moment when its
// completion is over but its token is still the live one.
#[tokio::test]
async fn callback_token_revoked_after_exit() {
    let fake = Spawnable::new(&Config::tool("lookup").fault(Fault::LingerOnShutdown(3000)));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_TOOL_ROUNDTRIP, &[], &client).await;

    let worker = &fake.log().workers()[0];
    let callbacks = worker.callbacks();
    assert_eq!(callbacks.len(), 1);
    assert_eq!(callbacks[0].arg["status"], 200, "the token routed while the completion lived");
    let (base, token) = worker.ready().expect("callback identity");
    assert_eq!(callbacks[0].text("token"), token);
    let url = format!("{base}{CALLBACK_PATH}");
    let body = json!({ "toolName": "lookup", "args": {}, "agentId": worker.agents()[0] });

    // asked to go, still up: the finished completion no longer routes
    fake_bridge::poll(|| fake.log().workers()[0].count(Rpc::Shutdown) == 1, GONE, "Shutdown").await;
    assert!(worker.alive(), "the process lingers");
    let (status, reply) = callback(Method::POST, &url, Some(&token), &body).await;
    assert_eq!((status, reply["code"].as_str()), (404, Some("not_found")), "{reply}");

    // gone, and its token with it a beat after the client sees the exit
    await_gone(&fake).await;
    let deadline = Instant::now() + GONE;
    loop {
        let (status, reply) = callback(Method::POST, &url, Some(&token), &body).await;
        if status == 401 {
            assert_eq!(reply["code"].as_str(), Some("unauthenticated"), "{reply}");
            break;
        }
        assert!(Instant::now() < deadline, "the token still routes: {status} {reply}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// --- Logging ---

// Every event's fields, flattened to text.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Captured {
    fn on_event(
        &self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut text = String::new();
        event.record(&mut Flatten(&mut text));
        self.0.lock().expect("captured lock").push(text);
    }
}

struct Flatten<'a>(&'a mut String);

impl tracing::field::Visit for Flatten<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        let _ = write!(self.0, "{}={value:?} ", field.name());
    }
}

#[tokio::test]
async fn ready_line_never_logged() {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(captured.clone()))
        .expect("this test owns the process's subscriber");

    // the older ready-line form carries the bearer token inline
    let fake = Spawnable::new(&Config::echo().fault(Fault::InlineToken));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    let token = fake.log().workers()[0].token().expect("the process logged its token");
    assert_ne!(token, "");
    let events = captured.0.lock().expect("captured lock").clone();
    assert!(
        events.iter().any(|event| event.contains("cursor-sdk-bridge spawned")),
        "the capture saw the client's own events: {events:#?}"
    );
    for event in &events {
        assert!(!event.contains(&token), "the bearer token reached a log event: {event}");
        assert!(!event.contains("cursor-sdk-bridge ready"), "the ready line was logged: {event}");
    }
}

// The one `completion` finish line the capture saw: one attempt, one line.
fn completion_event(captured: &Captured) -> String {
    let events = captured.0.lock().expect("captured lock").clone();
    let mut completions =
        events.iter().filter(|event| event.contains("message=completion outcome="));
    let (Some(completion), None) = (completions.next(), completions.next()) else {
        panic!("one completion event: {events:#?}");
    };
    completion.clone()
}

// A narration reply is nudged, and the JSON that follows passes the guest's
// check first time: two sends, no correction. `corrected` is the check's
// verdict, not the send count.
#[tokio::test]
async fn nudged_completion_is_ok() {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(captured.clone()))
        .expect("this test owns the process's subscriber");

    let fake = Spawnable::new(&Config::replies([
        "Analyzing the claims to produce the verdict.",
        r#"{"findings":[],"verdict":"pass"}"#,
    ]));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_CHECK_ACCEPTED, &[], &client).await;
    await_gone(&fake).await;

    assert_eq!(fake.log().count(Rpc::Send), 2, "{}", fake.log().summary());
    let completion = completion_event(&captured);
    assert!(completion.contains(r#"outcome="ok""#), "{completion}");
    assert!(completion.contains("attempts=2 "), "{completion}");
}

#[tokio::test]
async fn corrected_completion_is_corrected() {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(captured.clone()))
        .expect("this test owns the process's subscriber");

    let fake = Spawnable::new(&Config::replies([
        r#"{"findings":["x"],"verdict":"fail"}"#,
        r#"{"findings":[],"verdict":"pass"}"#,
    ]));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_CHECK_CORRECTED, &[], &client).await;
    await_gone(&fake).await;

    let completion = completion_event(&captured);
    assert!(completion.contains(r#"outcome="corrected""#), "{completion}");
    assert!(completion.contains("attempts=2 "), "{completion}");
}
