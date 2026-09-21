//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! A bridge is spawned with the tool-callback registration flags and a
//! client-owned state root, handshaken by scanning stderr for the
//! `cursor-sdk-bridge ready ` line, and watched until it exits — on request
//! through [`Bridge::close`] or the last [`Bridge`] dropping, or on its own,
//! which [`Bridge::died`] reports and the watcher logs at WARN with the pid,
//! the uptime, the exit status, and whether a run was in flight and for how
//! long its stream had been silent. The last lines it wrote to stderr are
//! untrusted subprocess output: they are logged at DEBUG for operators and
//! never carried into WARN events or the error messages callers see.

mod discovery;
mod messages;
mod rpc;

use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow};
use discovery::{Discovery, Tail};
pub use messages::{
    AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig, ModelSelection,
    RunStatus, RunStreamResult, SdkMessage, ToolList,
};
pub use rpc::{Rpc, TransportError};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::endpoint::Registration;

/// The bridge executable, resolved on `PATH`.
pub const BIN: &str = "cursor-sdk-bridge";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the stderr pipe is given to hand over its last lines after the
/// process exits. A grandchild that inherited the pipe can hold it open, so
/// this is a bound, not a wait for EOF.
pub const EXIT_GRACE: Duration = Duration::from_millis(250);
/// How long a socket failure waits to observe [`Bridge::died`]. The watcher
/// may spend a full [`EXIT_GRACE`] draining stderr before it publishes, so
/// this is that window plus its own observe budget.
pub const EXIT_WAIT: Duration = EXIT_GRACE.saturating_mul(2);
const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// One `sdk.v1` bridge: a spawned `cursor-sdk-bridge` process this client
/// watches.
#[derive(Debug)]
pub struct Bridge {
    shared: Arc<Shared>,
    // `None` while the process runs.
    exit: watch::Receiver<Option<Exit>>,
    // Sent on by `close`, or closed by the last handle dropping: either
    // wakes the watcher into a graceful shutdown.
    shutdown: watch::Sender<()>,
}

/// One process as its handle, its handshake, its stderr reader, and its
/// watcher all see it.
#[derive(Debug)]
struct Shared {
    // `Child::id` is `None` once the process has been reaped, so the pid is
    // kept from the spawn for the exit report.
    pid: Option<u32>,
    started: Instant,
    // Bound once the handshake completes.
    rpc: OnceLock<Rpc>,
    // The run in flight, for the watcher's exit report.
    run: RunWatch,
    // The last lines of stderr, for the DEBUG report.
    tail: Tail,
}

/// The activity clock of the run in flight on a bridge: the agent's `Send`
/// hands over a receiver whose sender lives as long as the run does.
#[derive(Debug, Default)]
struct RunWatch(Mutex<Option<watch::Receiver<tokio::time::Instant>>>);

impl RunWatch {
    fn set(&self, activity: watch::Receiver<tokio::time::Instant>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(activity);
    }

    /// Whether a run is in flight, and how long its stream has been silent.
    fn snapshot(&self) -> (bool, Option<u64>) {
        let guard = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match guard.as_ref() {
            // A closed channel is a `Send` that has returned.
            Some(activity) if activity.has_changed().is_ok() => {
                (true, Some(millis(activity.borrow().elapsed())))
            }
            _ => (false, None),
        }
    }
}

/// A spawned process whose ready-line handshake has not finished.
///
/// The [`Bridge`] is already watched, so the caller can occupy the agent
/// slot before [`PendingHandshake::complete`] returns.
pub struct Started {
    pub bridge: Bridge,
    pub handshake: PendingHandshake,
}

/// Ready-line scan and RPC connect for a [`Started`] process.
pub struct PendingHandshake {
    // The stderr reader's verdict on the ready line.
    ready: oneshot::Receiver<Result<Discovery>>,
    shared: Arc<Shared>,
    exit: watch::Receiver<Option<Exit>>,
}

/// How a spawned bridge ended.
#[derive(Clone, Copy, Debug, Default)]
pub struct Exit {
    /// The exit status, when the wait reported one.
    pub status: Option<ExitStatus>,
    /// The process id, when the spawn reported one.
    pub pid: Option<u32>,
}

impl PendingHandshake {
    /// Finish the ready-line scan and bind `sdk.v1`. The process is already
    /// watched; the caller holds the slot across this wait.
    ///
    /// # Errors
    ///
    /// Returns an error when the ready line never arrives or the RPC
    /// handshake fails.
    pub async fn complete(mut self) -> Result<()> {
        let scanned = (&mut self.ready)
            .await
            .unwrap_or_else(|_gone| Err(anyhow!("the stderr reader ended without a ready line")));
        let discovery = self.or_exit(scanned).await?;
        let connected = discovery.into_rpc().await;
        let rpc = self.or_exit(connected).await?;
        let _ = self.shared.rpc.set(rpc);
        tracing::info!(
            pid = self.shared.pid,
            histogram.cursor_bridge_spawn_ms = elapsed_ms(self.shared.started),
            "cursor-sdk-bridge spawned"
        );
        Ok(())
    }

    // Attach the bridge's exit status to a failed handshake step, once the
    // watcher has published one. What it wrote to stderr goes to DEBUG only,
    // never into the error.
    async fn or_exit<T>(&mut self, step: Result<T>) -> Result<T> {
        let error = match step {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let status =
            timeout(EXIT_WAIT, wait_exit(&mut self.exit)).await.ok().and_then(|exit| exit.status);
        log_stderr(&self.shared.tail);
        Err(status.map_or_else(
            || anyhow!("`{BIN}` did not complete the handshake ({error:#})"),
            |status| anyhow!("`{BIN}` exited ({status}) during the handshake ({error:#})"),
        ))
    }
}

impl Bridge {
    /// Spawn a bridge registered against `callback` and handshake it.
    pub async fn spawn(callback: &Registration) -> Result<Self> {
        let Started { bridge, handshake } = Self::start(callback)?;
        match handshake.complete().await {
            Ok(()) => Ok(bridge),
            Err(error) => {
                bridge.close().await;
                Err(error)
            }
        }
    }

    /// Start [`BIN`] calling back as `callback`, and watch it. The ready-line
    /// handshake is left on the returned [`Started`] so a pool lease can
    /// occupy the slot first.
    ///
    /// Synchronous past `Command::spawn`, so no cancellation can leave a
    /// process without a watcher.
    ///
    /// # Errors
    ///
    /// Returns an error when the state root cannot be created or the
    /// executable cannot be spawned.
    pub fn start(callback: &Registration) -> Result<Started> {
        let started = Instant::now();
        let state_root = tempfile::Builder::new()
            .prefix("omnia-cursor-")
            .tempdir()
            .context("creating state root")?;

        // The rest of the environment is inherited — `CURSOR_SDK_BRIDGE_LOG`
        // included, so an operator can turn the bridge's own logging up
        // from outside — bar the git identity, which would point the agent
        // at the host's repository rather than its cwd.
        let mut command = Command::new(BIN);
        command
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("CURSOR_SDK_CLIENT_LANGUAGE", "rust")
            .arg("--state-root")
            .arg(state_root.path())
            .args(["--tool-callback-url", callback.url()])
            .args(["--tool-callback-auth-token", callback.token()]);
        for var in GIT_IDENTITY {
            command.env_remove(var);
        }

        let mut child = command.spawn().with_context(|| format!("issue spawning `{BIN}`"))?;
        let shared = Arc::new(Shared {
            pid: child.id(),
            started,
            rpc: OnceLock::new(),
            run: RunWatch::default(),
            tail: Tail::default(),
        });
        drain_stdout(child.stdout.take().expect("stdout is piped"));
        let (ready, stderr) =
            read_stderr(child.stderr.take().expect("stderr is piped"), Arc::clone(&shared));

        let (shutdown, shutdown_rx) = watch::channel(());
        let (exit_tx, exit) = watch::channel(None);
        tokio::spawn(watch_child(
            Spawned {
                child,
                state_root,
                stderr,
                shared: Arc::clone(&shared),
            },
            shutdown_rx,
            exit_tx,
        ));

        Ok(Started {
            bridge: Self {
                shared: Arc::clone(&shared),
                exit: exit.clone(),
                shutdown,
            },
            handshake: PendingHandshake { ready, shared, exit },
        })
    }

    /// The bound `sdk.v1` client.
    ///
    /// # Panics
    ///
    /// Before [`PendingHandshake::complete`] has returned `Ok`.
    pub fn rpc(&self) -> &Rpc {
        self.shared.rpc.get().expect("the bridge handshake has completed")
    }

    /// Hand the watcher the activity clock of the run about to go over this
    /// bridge, so an uninvited exit reports whether a run was in flight and
    /// how long its stream had been silent. The sender's life is the run's.
    pub fn watch_run(&self, activity: watch::Receiver<tokio::time::Instant>) {
        self.shared.run.set(activity);
    }

    /// Whether the bridge has exited.
    pub fn is_dead(&self) -> bool {
        self.exit.borrow().is_some()
    }

    /// Resolves once the bridge exits.
    pub fn died(&self) -> impl Future<Output = Exit> + Send + 'static {
        let mut exit = self.exit.clone();
        async move { wait_exit(&mut exit).await }
    }

    /// Shut the bridge down and wait for it to exit.
    pub async fn close(&self) {
        let _ = self.shutdown.send(());
        self.died().await;
    }
}

/// A spawned process and what it leaves behind.
struct Spawned {
    child: Child,
    state_root: TempDir,
    // The stderr reader, which ends at the pipe's EOF.
    stderr: JoinHandle<()>,
    shared: Arc<Shared>,
}

// Wait for a shutdown request or the process's own exit, publish how it
// ended either way, then release the state root.
async fn watch_child(
    mut spawned: Spawned, mut shutdown: watch::Receiver<()>, exit_tx: watch::Sender<Option<Exit>>,
) {
    let (status, crashed) = tokio::select! {
        // an exit in the same tick as a close is still an exit
        biased;
        waited = spawned.child.wait() => (waited.ok(), true),
        // `close`, or the last `Bridge` handle dropped.
        _ = shutdown.changed() => (shut_down(&mut spawned).await, false),
    };
    // Taken as the exit is seen, before the drain below: the agent's
    // `Send` is still pending on the socket, so this is the run's state at
    // the moment the process went.
    let (run_in_flight, silent_ms) = spawned.shared.run.snapshot();
    let uptime_ms = elapsed_ms(spawned.shared.started);

    // The pipe may still hold the bridge's last lines; a grandchild that
    // inherited it can also hold it open, so do not wait for EOF.
    let _ = timeout(EXIT_GRACE, spawned.stderr).await;
    if crashed {
        tracing::warn!(
            pid = spawned.shared.pid,
            uptime_ms,
            status_text = %status_text(status),
            run_in_flight,
            silent_ms,
            monotonic_counter.cursor_bridge_exits = 1_u64,
            "cursor-sdk-bridge exited"
        );
        log_stderr(&spawned.shared.tail);
    }
    let _ = exit_tx.send(Some(Exit {
        status,
        pid: spawned.shared.pid,
    }));
    drop(spawned.state_root);
}

// Ask the process to go — `Shutdown` once RPC is bound, a kill when the
// handshake never got there — and wait, killing at the bound.
async fn shut_down(spawned: &mut Spawned) -> Option<ExitStatus> {
    if let Some(rpc) = spawned.shared.rpc.get() {
        let _ = timeout(SHUTDOWN_TIMEOUT, rpc.shutdown()).await;
    } else {
        let _ = spawned.child.start_kill();
    }
    match timeout(SHUTDOWN_TIMEOUT, spawned.child.wait()).await {
        Ok(waited) => waited.ok(),
        Err(_elapsed) => {
            let _ = spawned.child.start_kill();
            spawned.child.wait().await.ok()
        }
    }
}

// Resolves once the watcher publishes the exit. A closed channel is the
// watcher gone, and the process with it (`kill_on_drop`).
async fn wait_exit(exit: &mut watch::Receiver<Option<Exit>>) -> Exit {
    exit.wait_for(Option::is_some).await.ok().and_then(|published| *published).unwrap_or_default()
}

// Read stderr for the process's life: the ready line's verdict goes to the
// handshake, every other line to DEBUG and the tail. The reader holds the
// pipe open to EOF, so a handshake given up early never closes it under a
// process still writing.
fn read_stderr(
    stderr: ChildStderr, shared: Arc<Shared>,
) -> (oneshot::Receiver<Result<Discovery>>, JoinHandle<()>) {
    let (ready_tx, ready_rx) = oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        let scanned = discovery::from_stderr(&mut lines, &shared.tail).await;
        // Nobody waiting is a handshake already abandoned; the drain goes on.
        let _ = ready_tx.send(scanned);
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = "stderr", "bridge output");
            shared.tail.push(line);
        }
    });
    (ready_rx, reader)
}

// Drain stdout so the process never blocks on a full pipe.
fn drain_stdout(stdout: ChildStdout) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = "stdout", "bridge output");
        }
    });
}

// The bridge's stderr is untrusted and may carry workspace paths, provider
// error context, or session fragments: DEBUG is the only sink it reaches.
fn log_stderr(tail: &Tail) {
    let stderr = tail.to_string();
    if !stderr.is_empty() {
        tracing::debug!(%stderr, "cursor-sdk-bridge stderr tail");
    }
}

/// An exit status for a log line or message: `status unknown` when the wait
/// itself failed.
pub fn status_text(status: Option<ExitStatus>) -> String {
    status.map_or_else(|| "status unknown".to_owned(), |status| status.to_string())
}

pub fn elapsed_ms(since: Instant) -> u64 {
    millis(since.elapsed())
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt as _;
    use std::process::ExitStatus;
    use std::time::Duration;

    use tokio::sync::watch;

    use super::{RunWatch, status_text};

    // The crash WARN and `Failure::BridgeExited` both lean on std naming
    // the signal, so a bare number here would be a regression.
    #[test]
    fn status_text_names_the_signal() {
        assert_eq!(status_text(Some(ExitStatus::from_raw(9))), "signal: 9 (SIGKILL)");
        assert_eq!(status_text(Some(ExitStatus::from_raw(3 << 8))), "exit status: 3");
        assert_eq!(status_text(None), "status unknown");
    }

    #[tokio::test(start_paused = true)]
    async fn run_watch_follows_the_send() {
        let run = RunWatch::default();
        assert_eq!(run.snapshot(), (false, None), "nothing has been sent yet");

        let (activity, rx) = watch::channel(tokio::time::Instant::now());
        run.set(rx);
        tokio::time::advance(Duration::from_millis(1500)).await;
        assert_eq!(run.snapshot(), (true, Some(1500)), "a run in flight, silent since it began");

        activity.send_replace(tokio::time::Instant::now());
        tokio::time::advance(Duration::from_millis(200)).await;
        assert_eq!(run.snapshot(), (true, Some(200)), "an event rearms the silence");

        drop(activity);
        assert_eq!(run.snapshot(), (false, None), "the `Send` has returned");
    }
}
