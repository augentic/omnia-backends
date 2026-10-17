//! The supervised process over `/bin/sh`:
//!
//! - what a child's death sweeps
//! - what its exit publishes
//! - how a stop and a future under its life see it
//! - that it has no terminal
//! - what a one-shot captures, and what it sweeps

mod support;

use std::os::unix::process::ExitStatusExt as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use omnia_sandbox::{Exit, Options};
use support::{BOUND, Captured, alive, await_gone, connect, pid_of, session, sh, spawn_sh};
use tokio::time::Instant;
use tracing_subscriber::layer::SubscriberExt as _;

const READY: &str = "ready ";

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
async fn grandchild_on_exit() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let mut child = spawn_sh(session, "sleep 600 & echo \"ready $!\" >&2; exit 3", Some(READY));

    let grandchild = pid_of(&child.ready().await.expect("the ready line names the fork"));
    assert!(alive(grandchild));

    let exit = child.exited().await;
    assert_eq!(code(&exit), Some(3), "{exit}");
    assert!(!child.is_running());
    await_gone(grandchild).await;
    await_gone(child.pid()).await;
}

#[tokio::test]
async fn grandchild_on_drop() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let mut child = spawn_sh(session, "sleep 600 & echo \"ready $!\" >&2; wait", Some(READY));

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
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let script = "trap 'exit 0' TERM; echo \"ready $$\" >&2; while :; do sleep 1; done";
    let mut child = spawn_sh(session, script, Some(READY));

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
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let mut child = spawn_sh(session, "echo \"ready $$\" >&2; exec sleep 600", Some(READY));
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
async fn fail_on_exit() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let child = spawn_sh(session, "exit 7", None);

    let exit = child
        .fail_on_exit(std::future::pending::<Result<(), ()>>())
        .await
        .expect_err("the exit ends the wait");
    assert_eq!(code(&exit), Some(7), "{exit}");
}

// The future fails first. The exit lands within the grace and wins.
#[tokio::test]
async fn fail_on_exit_grace() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let child = spawn_sh(session, "sleep 0.1; exit 7", None);

    let exit = child
        .fail_on_exit(async { Err::<(), _>("connection reset") })
        .await
        .expect_err("the exit behind the failure is reported");
    assert_eq!(code(&exit), Some(7), "{exit}");
}

#[tokio::test]
async fn fail_on_exit_err() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let child = spawn_sh(session, "exec sleep 600", None);

    let outcome = child
        .fail_on_exit(async { Err::<(), _>("connection reset") })
        .await
        .expect("the process is still running");
    assert_eq!(outcome, Err("connection reset"));
    assert!(child.is_running());
}

#[tokio::test]
async fn fail_on_exit_ok() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let child = spawn_sh(session, "exec sleep 600", None);

    let outcome = child.fail_on_exit(async { Ok::<_, ()>(5) }).await.expect("running");
    assert_eq!(outcome, Ok(5));
}

// --- Terminal ---

// The process group is the session's own: `/dev/tty` opens nothing inside,
// and `ps` shows the child no controlling terminal. `ps` runs from the test
// process, since it is setuid on macOS and the kernel boundary never runs it.
#[tokio::test]
async fn controlling_tty() {
    let sandbox = connect(Options::default()).await;
    let session = support::session(&sandbox, None).await;
    // in a subshell, since dash exits on a special builtin's redirection failing
    let script = "if ( : </dev/tty ) 2>/dev/null; then echo open; else echo none; fi";
    let output = sh(session, script).output(BOUND).await.expect("sh answers");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "none", "/dev/tty opened");

    let session = support::session(&sandbox, None).await;
    let child = spawn_sh(session, "exec sleep 600", None);
    let ps = std::process::Command::new("ps")
        .args(["-o", "tty=", "-p", &child.pid().to_string()])
        .output()
        .expect("ps runs on the host");
    let tty = String::from_utf8_lossy(&ps.stdout).trim().to_owned();
    assert!(matches!(tty.as_str(), "?" | "??" | "-"), "a controlling terminal: {tty:?}");
}

// --- One shot ---

#[tokio::test]
async fn output_captured() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let command = sh(session, "echo out; echo err >&2; exit 4");
    let output = command.output(BOUND).await.expect("sh answers");

    assert_eq!(output.status.code(), Some(4));
    assert_eq!(output.stdout, b"out\n");
    assert_eq!(output.stderr, b"err\n");
}

#[tokio::test]
async fn output_timeout() {
    let workspace = tempfile::tempdir().expect("a workspace");
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, Some(workspace.path())).await;
    let command = sh(session, "sleep 600 & echo $! > pid; wait");
    let error =
        command.output(Duration::from_millis(500)).await.expect_err("the shell waits on its fork");

    assert!(error.to_string().contains("did not exit within"), "{error:#}");
    let grandchild =
        pid_of(&std::fs::read_to_string(workspace.path().join("pid")).expect("the pid"));
    await_gone(grandchild).await;
}

// A fork left holding the pipes neither delays the exit nor outlives it.
#[tokio::test]
async fn output_with_fork() {
    let workspace = tempfile::tempdir().expect("a workspace");
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, Some(workspace.path())).await;
    let started = Instant::now();
    let command = sh(session, "sleep 600 & echo $! > pid; echo done; exit 5");
    let output = command.output(BOUND).await.expect("the shell answers");

    assert_eq!(output.status.code(), Some(5));
    assert_eq!(output.stdout, b"done\n");
    assert!(started.elapsed() < BOUND, "the exit waited on the fork: {:?}", started.elapsed());
    let grandchild =
        pid_of(&std::fs::read_to_string(workspace.path().join("pid")).expect("the pid"));
    await_gone(grandchild).await;
}

// A stream is kept to its cap and the rest drained, so a process writing
// past it neither fills the host's memory nor blocks on a full pipe.
#[tokio::test]
async fn output_capped() {
    const CAP: usize = 16 * 1024 * 1024;
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let script = format!("head -c {} /dev/zero; echo tail >&2", CAP + 1024 * 1024);
    let output = sh(session, &script).output(BOUND).await.expect("the shell answers");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stdout.len(), CAP);
    assert_eq!(output.stderr, b"tail\n");
}

// --- Output ---

// Both pipes are read to EOF whatever the bytes. A line in anything but
// UTF-8 ahead of the ready line neither hides it nor closes stdout under
// the write behind it.
#[tokio::test]
async fn binary_before_ready() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let script = "printf '\\377\\376 binary\\n' >&2; printf '\\377\\376 binary\\n'; \
                  echo 'ready token' >&2; echo after; exit 0";
    let mut child = spawn_sh(session, script, Some(READY));

    assert_eq!(child.ready().await.as_deref(), Some("token"));
    let exit = child.exited().await;
    assert_eq!(code(&exit), Some(0), "{exit}");
}

// --- Logging ---

// An exit nobody asked for is a crash: warned with its status, its tail
// behind it at DEBUG, and the ready line in neither.
#[tokio::test]
async fn crash_log() {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(captured.clone()))
        .expect("this test owns the process's subscriber");

    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let script = "echo 'ready secret-token' >&2; echo 'line one' >&2; echo 'line two' >&2; exit 2";
    let mut child = spawn_sh(session, script, Some(READY));
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
