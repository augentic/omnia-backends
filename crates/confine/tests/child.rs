//! The supervised process over `/bin/sh`: what a child's death sweeps, what
//! its exit publishes, how a stop and a future under its life see it, and
//! that it has no terminal.

mod support;

use std::os::unix::process::ExitStatusExt as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use omnia_confine::{Exit, Options, Substrate};
use support::{alive, await_gone, boundary, connect, pid_of, sh, spawn_sh};
use tokio::time::Instant;
use tracing_subscriber::layer::SubscriberExt as _;

const READY: &str = "ready ";
const BOUND: Duration = Duration::from_secs(5);

fn code(exit: &Exit) -> Option<i32> {
    exit.status.and_then(|status| status.code())
}

fn signal(exit: &Exit) -> Option<i32> {
    exit.status.and_then(|status| status.signal())
}

#[expect(unsafe_code, reason = "the ask of a graceful stop is one signal to one pid")]
fn terminate(pid: u32) {
    let pid = i32::try_from(pid).expect("a pid fits");
    // SAFETY: `kill` with a pid this test spawned and read back delivers one signal.
    let _ = unsafe { libc::kill(pid, libc::SIGTERM) };
}

// --- Death ---

#[tokio::test]
async fn grandchild_swept_on_exit() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let mut child = spawn_sh(boundary, "sleep 600 & echo \"ready $!\" >&2; exit 3", Some(READY));

    let grandchild = pid_of(&child.ready().await.expect("the ready line names the fork"));
    assert!(alive(grandchild));

    let exit = child.exited().await;
    assert_eq!(code(&exit), Some(3), "{exit}");
    assert!(!child.is_running());
    await_gone(grandchild).await;
    await_gone(child.pid()).await;
}

#[tokio::test]
async fn grandchild_swept_on_drop() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let mut child = spawn_sh(boundary, "sleep 600 & echo \"ready $!\" >&2; wait", Some(READY));

    let grandchild = pid_of(&child.ready().await.expect("the ready line names the fork"));
    let pid = child.pid();
    let exited = child.exited();
    drop(child);

    let exit = exited.await;
    assert_eq!(signal(&exit), Some(libc::SIGKILL), "{exit}");
    await_gone(grandchild).await;
    await_gone(pid).await;
}

// --- Stop ---

#[tokio::test]
async fn stop_answered() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let script = "trap 'exit 0' TERM; echo \"ready $$\" >&2; while :; do sleep 1; done";
    let mut child = spawn_sh(boundary, script, Some(READY));

    let pid = pid_of(&child.ready().await.expect("the ready line names the shell"));
    assert_eq!(pid, child.pid());
    let exited = child.exited();
    child.stop(async move { terminate(pid) }, BOUND);
    drop(child);

    let exit = exited.await;
    assert_eq!(code(&exit), Some(0), "the ask was answered, not killed: {exit}");
    await_gone(pid).await;
}

#[tokio::test]
async fn stop_unanswered() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let mut child = spawn_sh(boundary, "echo \"ready $$\" >&2; exec sleep 600", Some(READY));
    child.ready().await.expect("the ready line");

    let asked = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&asked);
    let wait = Duration::from_millis(300);
    let exited = child.exited();
    let stopped = Instant::now();
    child.stop(async move { flag.store(true, Ordering::SeqCst) }, wait);
    drop(child);

    let exit = exited.await;
    assert!(asked.load(Ordering::SeqCst), "the ask ran");
    assert_eq!(signal(&exit), Some(libc::SIGKILL), "{exit}");
    assert!(stopped.elapsed() >= wait, "the kill waited out the ask: {:?}", stopped.elapsed());
}

// --- Under its life ---

#[tokio::test]
async fn fail_on_exit_sees_the_exit() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let child = spawn_sh(boundary, "exit 7", None);

    let exit = child
        .fail_on_exit(std::future::pending::<Result<(), ()>>())
        .await
        .expect_err("the exit ends the wait");
    assert_eq!(code(&exit), Some(7), "{exit}");
}

// The future fails first; the exit lands within the grace and wins.
#[tokio::test]
async fn fail_on_exit_outruns_a_failure() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let child = spawn_sh(boundary, "sleep 0.1; exit 7", None);

    let exit = child
        .fail_on_exit(async { Err::<(), _>("connection reset") })
        .await
        .expect_err("the exit behind the failure is reported");
    assert_eq!(code(&exit), Some(7), "{exit}");
}

#[tokio::test]
async fn fail_on_exit_keeps_a_failure() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let child = spawn_sh(boundary, "exec sleep 600", None);

    let outcome = child
        .fail_on_exit(async { Err::<(), _>("connection reset") })
        .await
        .expect("the process is still running");
    assert_eq!(outcome, Err("connection reset"));
    assert!(child.is_running());
}

#[tokio::test]
async fn fail_on_exit_passes_a_value() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let child = spawn_sh(boundary, "exec sleep 600", None);

    let outcome = child.fail_on_exit(async { Ok::<_, ()>(5) }).await.expect("running");
    assert_eq!(outcome, Ok(5));
}

// --- Terminal ---

// The session is the boundary's own, read through `ps`, which the kernel
// substrate never runs (it is setuid on macOS).
#[tokio::test]
async fn tty_opens_nothing() {
    let confine = connect(Options {
        substrate: Substrate::Unconfined,
        ..Options::default()
    })
    .await;
    let boundary = boundary(&confine, None).await;
    // in a subshell, since dash exits on a special builtin's redirection failing
    let command = sh(
        &boundary,
        "if ( : </dev/tty ) 2>/dev/null; then echo open; else echo none; fi; ps -o tty= -p $$",
    );
    let output = boundary.output(command, BOUND).await.expect("sh answers");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines().map(str::trim);
    assert_eq!(lines.next(), Some("none"), "/dev/tty opened: {stdout}");
    assert!(matches!(lines.next(), Some("?" | "??" | "-")), "a controlling terminal: {stdout}");
}

// --- One shot ---

#[tokio::test]
async fn output_captured() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let command = sh(&boundary, "echo out; echo err >&2; exit 4");
    let output = boundary.output(command, BOUND).await.expect("sh answers");

    assert_eq!(output.status.code(), Some(4));
    assert_eq!(output.stdout, b"out\n");
    assert_eq!(output.stderr, b"err\n");
}

#[tokio::test]
async fn output_killed_past_the_bound() {
    let lend = tempfile::tempdir().expect("a lend");
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, Some(lend.path())).await;
    let command = sh(&boundary, "sleep 600 & echo $! > pid; wait");
    let error = boundary
        .output(command, Duration::from_millis(500))
        .await
        .expect_err("the shell waits on its fork");

    assert!(error.to_string().contains("did not exit within"), "{error:#}");
    let grandchild = pid_of(&std::fs::read_to_string(lend.path().join("pid")).expect("the pid"));
    await_gone(grandchild).await;
}

// --- Output ---

// Both pipes are read to EOF whatever the bytes: a line in anything but
// UTF-8 ahead of the ready line neither hides it nor closes stdout under
// the write behind it.
#[tokio::test]
async fn ready_behind_binary_output() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let script = "printf '\\377\\376 binary\\n' >&2; printf '\\377\\376 binary\\n'; \
                  echo 'ready token' >&2; echo after; exit 0";
    let mut child = spawn_sh(boundary, script, Some(READY));

    assert_eq!(child.ready().await.as_deref(), Some("token"));
    let exit = child.exited().await;
    assert_eq!(code(&exit), Some(0), "{exit}");
}

// --- Logging ---

// Every event's fields, flattened to text.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<String>>>);

impl Captured {
    fn events(&self) -> Vec<String> {
        self.0.lock().expect("captured lock").clone()
    }
}

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

// An exit nobody asked for is a crash: warned with its status, its tail
// behind it at DEBUG, and the ready line in neither.
#[tokio::test]
async fn crash_logged_with_tail() {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(captured.clone()))
        .expect("this test owns the process's subscriber");

    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;
    let script = "echo 'ready secret-token' >&2; echo 'line one' >&2; echo 'line two' >&2; exit 2";
    let mut child = spawn_sh(boundary, script, Some(READY));
    assert_eq!(child.ready().await.as_deref(), Some("secret-token"));
    let exit = child.exited().await;
    assert_eq!(code(&exit), Some(2), "{exit}");

    let events = captured.events();
    let exited = events
        .iter()
        .find(|event| event.contains("message=child exited"))
        .unwrap_or_else(|| panic!("the exit was logged: {events:#?}"));
    assert!(exited.contains("status=exit status: 2"), "{exited}");
    assert!(exited.contains("program=sh"), "{exited}");
    let tail = events
        .iter()
        .find(|event| event.contains("message=child tail"))
        .unwrap_or_else(|| panic!("the tail was logged: {events:#?}"));
    assert!(tail.contains("line one\n  line two"), "{tail}");
    for event in &events {
        assert!(!event.contains("secret-token"), "the ready line reached a log event: {event}");
    }
}
