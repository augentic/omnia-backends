//! Lifecycle and fault matrix for the cursor backend, guest-driven: every
//! row runs a guest component from `crates/test-programs` over an
//! `omnia_cursor::Client` against the fake `cursor-sdk-bridge` — `expect_error`
//! with the needle when the completion fails, `echo_text`/`fanout` when it
//! succeeds, `fanout_abandon` when the guest must drop a completion — then
//! asserts the fake's per-agent RPC sequence and every spawned process
//! gone again within a stated bound (a slot reopens only once its process
//! is). No row drives `Client::complete` from the test.
//!
//! The restart rows (under "Process death" and "Transport") pin the one
//! retry the client makes: a bridge lost before any candidate reached the
//! guest is given up, and the prompt goes once more to a fresh lease; a
//! second loss, a loss after a candidate, or a run that merely stalls is
//! the failure as it stands.

mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use http::Method;
use omnia::Backend as _;
use omnia_cursor::{Client, ConnectOptions};
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

/// The inactivity window the deadline rows run with.
const WINDOW: Duration = Duration::from_secs(1);
/// `agent.rs`'s bound on one teardown call.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// `bridge.rs`'s bound on a graceful exit: the `Shutdown` RPC and the exit
/// it asks for, together.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// `rpc.rs`'s bound on the `Ping`/`GetVersion` handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

fn with_window(window: Duration, max_agents: usize) -> ConnectOptions {
    ConnectOptions {
        inactivity_secs: window.as_secs(),
        ..options(max_agents)
    }
}

/// The `fanout_abandon` guest on its own task, so the test can steer the
/// fake while it runs.
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

/// Wait until the fake has recorded `count` `rpc`s.
async fn await_rpcs(log: impl Fn() -> Log + Sync, rpc: Rpc, count: usize, within: Duration) {
    fake_bridge::poll(|| log().count(rpc) >= count, within, &format!("{count} {rpc:?}(s)")).await;
}

fn without_cancel(sequence: &[Rpc]) -> Vec<Rpc> {
    sequence.iter().copied().filter(|rpc| *rpc != Rpc::CancelRun).collect()
}

fn killed(process: &Process) {
    assert!(!process.alive(), "process {} (pid {}) is still up", process.number, process.pid);
    assert_eq!(process.count(Rpc::Shutdown), 0, "process {} was killed, not asked", process.number);
}

/// The `Failure::BridgeExited` detail a `SIGKILL`ed bridge fails with.
const KILLED: &str = "cursor-sdk-bridge exited (signal: 9 (SIGKILL)) during the run";
/// The full sequence of a completion that answered.
const ANSWERED: [Rpc; 4] = [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent];

/// The two workers of a two-way abandon: the one that recorded `rpc`, and
/// the one that did not.
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

/// The two workers a restarted completion leaves behind, in start order.
fn restarted(log: &Log) -> (Process, Process) {
    let mut workers = log.workers().into_iter();
    let (first, second) = (workers.next(), workers.next());
    assert!(workers.next().is_none(), "at most one restart per completion: {}", log.summary());
    let first = first.expect("a first attempt");
    let second = second.unwrap_or_else(|| panic!("no second attempt: {}", log.summary()));
    (first, second)
}

/// The second attempt began only once the first attempt's process was
/// gone: with one slot, the restart waits for the dead lease to be reaped.
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

// ------------------------------------------------------------------------
// Configuration
// ------------------------------------------------------------------------

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
    ];
    for (options, needle) in bad {
        let error = Client::connect_with(options.clone())
            .await
            .expect_err(&format!("accepted {options:?}"));
        assert!(format!("{error:#}").contains(needle), "{options:?}: {error:#}");
    }
    assert!(fake.log().events.is_empty(), "a rejected option spawned a process");
}

// ------------------------------------------------------------------------
// Abandon: the guest drops a completion at every point it can be waiting
// ------------------------------------------------------------------------

#[tokio::test]
async fn abandon_during_handshake() {
    // Process 2 takes `Ping` and never answers: the loser is dropped with
    // its ready line seen but no RPC bound.
    let fake = Spawnable::new(&Config::echo().fault_on(2, Fault::Hang(Point::Ping)));
    let client = spawning(&fake, 2).await;
    run_guest(test_programs::MODEL_FANOUT_ABANDON, &["2"], &client).await;
    // Nothing to ask an unbound bridge: it is killed at once, well under
    // the graceful `SHUTDOWN_TIMEOUT`.
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

#[tokio::test]
async fn abandon_before_ready() {
    // Process 2 forks a child, then never prints its ready line: the loser
    // is dropped inside the stderr scan.
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
    // Nothing was bound to ask, so the kill is the group's from the start:
    // the forked child goes with it.
    await_forked_gone(loser).await;
}

#[tokio::test]
async fn abandon_during_create() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Park(Point::CreateAgent)));
    let client = spawning(&fake, 2).await;

    let guest = abandon_guest(&client, 2);
    fake.await_parked(Point::CreateAgent, 2).await;
    assert!(fake.release_one(Point::CreateAgent));
    guest.await.expect("the guest task joins");

    // The loser's `CreateAgent` is still held: its task owns the slot — and
    // so the process — until the id it will get is torn down, while the
    // winner's process is asked to go.
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
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::CloseAgent, Rpc::DeleteAgent]);
    let created = loser.saw(Rpc::CreateAgent)[0];
    let deleted = loser.saw(Rpc::DeleteAgent)[0];
    assert_eq!(
        deleted.text("cwd"),
        created.text("cwd"),
        "the late id is deleted where it was made"
    );
    assert!(!deleted.text("apiKey").is_empty());
    assert!(loser.ended_with(Rpc::Shutdown));

    // Both slots are back: a fresh run gets one and completes.
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

    // No stream opened for the loser, so there is no run to cancel; the
    // agent is still closed and deleted.
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
    // Both runs are over; both teardowns are held at `CloseAgent`.
    fake.await_parked(Point::CloseAgent, 2).await;
    assert!(fake.release_one(Point::CloseAgent));
    guest.await.expect("the guest task joins");

    // The loser was dropped while waiting on its teardown, which runs on
    // and keeps its process; the winner's is asked to go.
    assert_eq!(fake.parked(Point::CloseAgent), 1);
    let (winner, loser) = split_by(&fake.log(), Rpc::DeleteAgent);
    await_process_gone(&winner).await;
    assert!(loser.alive(), "the teardown in flight holds its slot");

    fake.release_all();
    await_rpcs(|| fake.log(), Rpc::DeleteAgent, 2, GONE).await;
    await_gone(&fake).await;
    let log = fake.log();
    for process in log.workers() {
        let (_, sequence) = sole_agent(&process);
        assert_eq!(sequence, ANSWERED, "process {}", process.number);
        assert!(process.ended_with(Rpc::Shutdown), "process {}", process.number);
    }
}

#[tokio::test]
async fn abandon_while_queued() {
    // Three completions on two slots: process 1 answers once process 2
    // has opened a run, process 2 hangs mid-run, and the third is still
    // waiting for a permit when the guest drops it. `Send` is recorded
    // before the stream's first event is observed, so a winner that
    // answers at once can drop the loser with no run id to cancel.
    // Lingering after `Shutdown` keeps the third completion queued.
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
    assert_eq!(
        loser,
        [Rpc::CreateAgent, Rpc::Send, Rpc::CancelRun, Rpc::CloseAgent, Rpc::DeleteAgent],
        "the mid-run loser is cancelled and torn down"
    );
    for process in &workers {
        assert!(process.ended_with(Rpc::Shutdown), "process {}", process.number);
    }
}

// ------------------------------------------------------------------------
// Pooling
// ------------------------------------------------------------------------

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
    // The third completion waited for a slot, which reopened only once a
    // first process had closed its agent and been shut down.
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

/// `CreateAgent` never answered, one slot: the completion gives up after
/// one window; its task holds the slot for one more, then reopens it with
/// nothing to tear down, and the next completion reaches its own
/// `CreateAgent`.
#[tokio::test]
async fn hang_on_create() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::CreateAgent)));
    let client = connect(with_window(WINDOW, 1)).await;
    expect_error("unanswered after 1s", &[], &client).await;
    expect_error("unanswered after 1s", &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let creates = log.saw(Rpc::CreateAgent);
    assert_eq!(creates.len(), 2, "{}", log.summary());
    let gap = creates[1].at().duration_since(creates[0].at()).unwrap_or_default();
    // The task's timer starts a round trip before the fake records the
    // arrival, hence the slack.
    let reopen = (2 * WINDOW).saturating_sub(Duration::from_millis(50));
    assert!(gap >= reopen, "the slot reopened after {gap:?}");
    assert_eq!(log.count(Rpc::CloseAgent), 0, "no id arrived to close");
    assert_eq!(log.count(Rpc::DeleteAgent), 0);
    for process in log.workers() {
        assert!(process.ended_with(Rpc::Shutdown), "process {}", process.number);
    }
}

#[tokio::test]
async fn create_reaps_late_agent() {
    // The task holds the slot for the late id for one more window after the
    // completion gives up; the id must arrive inside it, and the guest's
    // exit sits between the two under load, so the window is a wide one.
    const HOLD: Duration = Duration::from_secs(5);
    let fake = Spawnable::new(&Config::echo().fault(Fault::Park(Point::CreateAgent)));
    let client = connect(with_window(HOLD, 1)).await;
    expect_error("unanswered after 5s", &[], &client).await;
    assert_eq!(fake.parked(Point::CreateAgent), 1, "the create is still in flight");
    assert_eq!(fake.log().count(Rpc::CloseAgent), 0, "nothing to reap before the id arrives");

    // The id nobody is waiting for is closed and deleted by the task that
    // asked for it, against the create-time cwd.
    fake.release_all();
    await_rpcs(|| fake.log(), Rpc::DeleteAgent, 1, GONE).await;
    await_gone(&fake).await;
    let log = fake.log();
    let (agent, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::CloseAgent, Rpc::DeleteAgent], "{agent}");
    let created = log.saw(Rpc::CreateAgent)[0];
    let deleted = log.saw(Rpc::DeleteAgent)[0];
    assert_eq!(deleted.agent.as_deref(), Some(agent.as_str()));
    assert_eq!(deleted.text("cwd"), created.text("cwd"));
    assert!(!deleted.text("apiKey").is_empty());
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

// ------------------------------------------------------------------------
// Deadlines
// ------------------------------------------------------------------------

#[tokio::test]
async fn hang_on_send() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::Send)));
    let client = connect(with_window(WINDOW, 1)).await;
    expect_error("inactive for 1s", &[], &client).await;
    await_gone(&fake).await;

    // The stream never opened, so there is no run id to cancel.
    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
    assert!(log.workers()[0].ended_with(Rpc::Shutdown));
}

#[tokio::test]
async fn cap_hits() {
    // Activity every 200ms keeps the inactivity window rearmed; the 1s cap
    // ends the run anyway, and the run it noted is cancelled.
    let fake = Spawnable::new(&Config::paced(200, 1000, Then::Hang));
    let client = connect(ConnectOptions {
        timeout_secs: 1,
        ..options(1)
    })
    .await;
    expect_error("timed out after 1s (absolute cap exceeded while still active)", &[], &client)
        .await;
    await_gone(&fake).await;

    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(log.count(Rpc::CancelRun), 1, "{sequence:?}");
    // `CancelRun` goes out on its own task, racing the teardown's calls.
    assert_eq!(
        without_cancel(&sequence),
        [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]
    );
}

#[tokio::test]
async fn slow_stream_rearms() {
    // Five frames 400ms apart outlast the 1s window several times over;
    // each one rearms it.
    let fake = Spawnable::new(&Config::paced(400, 5, Then::Finish));
    let client = connect(with_window(WINDOW, 1)).await;
    let started = Instant::now();
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    assert!(started.elapsed() >= Duration::from_secs(2), "the run streamed for 2s");
    await_gone(&fake).await;

    let (_, sequence) = sole_agent(&fake.log());
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
}

// ------------------------------------------------------------------------
// Process death
// ------------------------------------------------------------------------

#[tokio::test]
async fn bridge_killed_on_send_restarts() {
    // Process 1 dies as the opening `Send` begins: no candidate has been
    // offered, so the prompt goes once more, on process 2, and the guest
    // gets its answer.
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
async fn bridge_exited_on_create_restarts() {
    let fake = Spawnable::new(&Config::echo().fault_on(1, Fault::ExitOnCreate(1)));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (first, second) = restarted(&log);
    // The fake exits before it records the create.
    assert_eq!(first.count(Rpc::CreateAgent), 0, "{}", first.summary());
    killed(&first);
    let (_, sequence) = sole_agent(&second);
    assert_eq!(sequence, ANSWERED, "{}", second.summary());
    assert!(second.ended_with(Rpc::Shutdown));
    restarted_after(&first, &second);
}

#[tokio::test]
async fn grandchildren_swept() {
    // Each process forks a child that outlives it: process 1 dies on its
    // `Send` with the child still up and holding the stderr pipe, process
    // 2 exits on `Shutdown` the same way. Neither child is anyone's to
    // reap; both go with their process as its exit is seen.
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

#[tokio::test]
async fn bridge_killed_twice_fails() {
    // Every process dies on its opening `Send`: one restart, then the
    // second exit stands. The socket resets as each process dies; the typed
    // exit wins over the transport error, and what the processes wrote to
    // stderr stays out.
    let fake = Spawnable::new(&Config::echo().fault(Fault::KillOnSend(1)));
    let client = spawning(&fake, 1).await;
    expect_error(KILLED, &["without:fake-bridge marker"], &client).await;
    // Teardown is skipped on a dead bridge: no `TEARDOWN_TIMEOUT` is paid.
    await_gone_within(&fake, AT_ONCE).await;

    let log = fake.log();
    let (first, second) = restarted(&log);
    for process in [&first, &second] {
        let (_, sequence) = sole_agent(process);
        assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send], "{}", process.summary());
        killed(process);
    }
}

#[tokio::test]
async fn killed_after_candidate_fails() {
    // The first candidate reached the guest's check and was rejected; the
    // process dies on the correction's `Send`. The guest has seen this
    // agent, so the prompt is not offered again.
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

#[tokio::test]
async fn inactive_run_not_restarted() {
    // A bridge that stays up and silent is not a lost bridge: the run is
    // cancelled at the inactivity bound and the failure stands.
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::Stream)));
    let client = connect(with_window(WINDOW, 1)).await;
    expect_error("inactive for 1s", &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let workers = log.workers();
    assert_eq!(workers.len(), 1, "no restart on a stall: {}", log.summary());
    let (_, sequence) = sole_agent(&log);
    assert_eq!(log.count(Rpc::CancelRun), 1, "{sequence:?}");
    assert_eq!(without_cancel(&sequence), ANSWERED);
    assert!(workers[0].ended_with(Rpc::Shutdown));
}

#[tokio::test]
async fn exit_before_ready() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::ExitBeforeReady(3)));
    let client = spawning(&fake, 1).await;
    expect_error(
        "exited (exit status: 3) during the handshake",
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
    expect_error("did not complete the handshake", &[], &client).await;
    await_gone(&fake).await;

    // The ready line was read but nothing answered at its URL: the process
    // is not left behind.
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

// ------------------------------------------------------------------------
// Silence
// ------------------------------------------------------------------------

#[tokio::test]
async fn hang_on_teardown() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::CloseAgent)));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    let returned = SystemTime::now();
    await_gone(&fake).await;

    // The answer waits on the teardown, and the hung call is bounded, not
    // fatal: `DeleteAgent` still follows it.
    let log = fake.log();
    let (_, sequence) = sole_agent(&log);
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
    let closed = log.saw(Rpc::CloseAgent)[0].at();
    let waited = returned.duration_since(closed).unwrap_or_default();
    assert!(waited >= TEARDOWN_TIMEOUT, "the answer arrived {waited:?} after CloseAgent");
}

#[tokio::test]
async fn hang_on_shutdown() {
    let fake = Spawnable::new(
        &Config::echo().fault(Fault::Hang(Point::Shutdown)).fault(Fault::Grandchild),
    );
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    assert!(fake.log().workers()[0].alive(), "the process is still up");
    // `Shutdown` unanswered for the one bound, then the kill — of the
    // group, so the child the process forked goes with it.
    await_gone_within(&fake, SHUTDOWN_TIMEOUT + GONE).await;
    let gone = SystemTime::now();

    let worker = &fake.log().workers()[0];
    assert_eq!(worker.last_rpc(), Some(Rpc::Shutdown));
    let asked = worker.saw(Rpc::Shutdown)[0].at();
    let held = gone.duration_since(asked).unwrap_or_default();
    // The bound's timer starts a round trip before the fake records the
    // arrival, hence the slack.
    let bound = SHUTDOWN_TIMEOUT.saturating_sub(Duration::from_millis(50));
    assert!(held >= bound, "the process was gone {held:?} after Shutdown");
    await_forked_gone(worker).await;
}

#[tokio::test]
async fn handshake_hangs() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::Hang(Point::Ping)));
    let client = spawning(&fake, 1).await;
    let started = Instant::now();
    expect_error(
        &format!("did not answer the handshake within {}s", CONNECT_TIMEOUT.as_secs()),
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

// ------------------------------------------------------------------------
// Transport
// ------------------------------------------------------------------------

#[tokio::test]
async fn close_500() {
    let fake = Spawnable::new(&Config::echo().fault(Fault::CloseFails));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    // A failed close is logged, and the delete still follows.
    let (_, sequence) = sole_agent(&fake.log());
    assert_eq!(sequence, [Rpc::CreateAgent, Rpc::Send, Rpc::CloseAgent, Rpc::DeleteAgent]);
}

/// The first attempt's stream was reset after its first frame: the run id
/// that frame carried is cancelled before the agent is torn down.
const RESET: [Rpc; 5] =
    [Rpc::CreateAgent, Rpc::Send, Rpc::CancelRun, Rpc::CloseAgent, Rpc::DeleteAgent];

#[tokio::test]
async fn stream_reset_restarts() {
    // Process 1 resets its opening run's stream and stays up: the socket
    // failure alone is the lost bridge. Its agent is torn down and the
    // process asked to go before the restart takes the slot.
    let fake = Spawnable::new(&Config::echo().fault_on(1, Fault::ResetStream(1)));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    let log = fake.log();
    let (first, second) = restarted(&log);
    let (_, sequence) = sole_agent(&first);
    assert_eq!(sequence, RESET, "{}", first.summary());
    assert_eq!(first.saw(Rpc::CancelRun)[0].text("runId"), "run-1");
    assert!(first.ended_with(Rpc::Shutdown), "{}", first.summary());
    let (_, sequence) = sole_agent(&second);
    assert_eq!(sequence, ANSWERED, "{}", second.summary());
    assert!(second.ended_with(Rpc::Shutdown));
    restarted_after(&first, &second);
}

#[tokio::test]
async fn stream_reset_twice_fails() {
    // Every process resets its opening stream: the second transport failure
    // is the guest's, typed below Connect.
    let fake = Spawnable::new(&Config::echo().fault(Fault::ResetStream(1)));
    let client = spawning(&fake, 1).await;
    expect_error(
        "bridge RPC `SdkAgentService/Send` transport failed reading the stream",
        &[],
        &client,
    )
    .await;
    await_gone(&fake).await;

    let log = fake.log();
    let (first, second) = restarted(&log);
    for process in [&first, &second] {
        let (_, sequence) = sole_agent(process);
        assert_eq!(sequence, RESET, "{}", process.summary());
        assert!(process.ended_with(Rpc::Shutdown), "{}", process.summary());
    }
}

// ------------------------------------------------------------------------
// Callback endpoint
// ------------------------------------------------------------------------

#[tokio::test]
async fn callback_rejections() {
    // A run that streams for a few seconds keeps its agent live while the
    // test knocks on the endpoint the way a misbehaving bridge would.
    let fake = Spawnable::new(&Config::paced(200, 20, Then::Finish));
    let client = spawning(&fake, 1).await;
    let guest = echo_guest(&client);
    // The guest's component is compiled on the way to its `Send`.
    await_rpcs(|| fake.log(), Rpc::Send, 1, STARTUP).await;
    let worker = &fake.log().workers()[0];
    let (base, token) = worker.ready().expect("the process logged its callback identity");
    let url = format!("{base}{CALLBACK_PATH}");
    let agent = worker.agents()[0].clone();
    let body = json!({ "toolName": "lookup", "args": {}, "agentId": agent });

    let (status, reply) = callback(Method::POST, &url, Some("wrong"), &body).await;
    assert_eq!((status, reply["code"].as_str()), (401, Some("unauthenticated")), "{reply}");
    // A body still in flight when the head is rejected must be taken
    // before the close, or the reply is lost to a TCP reset.
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

#[tokio::test]
async fn callback_token_revoked_after_exit() {
    // The process lingers after `Shutdown`, so there is a moment when its
    // completion is over but its token is still the live one.
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

    // Asked to go, still up: the finished completion no longer routes.
    fake_bridge::poll(|| fake.log().workers()[0].count(Rpc::Shutdown) == 1, GONE, "Shutdown").await;
    assert!(worker.alive(), "the process lingers");
    let (status, reply) = callback(Method::POST, &url, Some(&token), &body).await;
    assert_eq!((status, reply["code"].as_str()), (404, Some("not_found")), "{reply}");

    // Gone, and its token with it: the client revokes it once it has seen
    // the exit, a beat after the process is reaped.
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

// ------------------------------------------------------------------------
// Logging
// ------------------------------------------------------------------------

/// Every event's fields, flattened to text.
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

    // The older ready-line form carries the bearer token inline.
    let fake = Spawnable::new(&Config::echo().fault(Fault::InlineToken));
    let client = spawning(&fake, 1).await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(&fake).await;

    let token = fake.log().workers()[0].bridge_token().expect("the process logged its token");
    assert!(!token.is_empty());
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
