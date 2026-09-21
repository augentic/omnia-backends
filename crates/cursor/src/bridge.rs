//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! A bridge is spawned with the tool-callback registration flags and a
//! client-owned state root, handshaken by scanning stderr for the
//! `cursor-sdk-bridge ready ` line, and watched until it exits — on request
//! through [`Bridge::close`], or on its own, which [`Bridge::died`] reports
//! and the watcher logs at WARN with the pid, the uptime, the exit status,
//! and whether a run was in flight and for how long its stream had been
//! silent. The last lines it wrote to stderr are untrusted subprocess
//! output: they are logged at DEBUG for operators and never carried into
//! WARN events or the error messages callers see.

mod discovery;
mod messages;
mod rpc;

use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use discovery::Tail;
pub use messages::{
    AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig, ModelSelection,
    RunStatus, RunStreamResult, SdkMessage, ToolList,
};
pub use rpc::{Rpc, TransportError};
use tempfile::TempDir;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStderr, Command};
use tokio::sync::{Notify, watch};
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
pub const EXIT_OBSERVE: Duration = EXIT_GRACE.saturating_mul(2);
const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// One `sdk.v1` bridge: a spawned `cursor-sdk-bridge` process this client
/// watches.
#[derive(Debug)]
pub struct Bridge {
    rpc: Arc<OnceLock<Rpc>>,
    // `None` while the process runs.
    exit: watch::Receiver<Option<Exit>>,
    // Wakes the watcher into a graceful shutdown.
    shutdown: Arc<Notify>,
    // The run in flight on this bridge, for the watcher's exit report.
    run: Arc<RunWatch>,
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
                let silent = activity.borrow().elapsed().as_millis();
                (true, Some(u64::try_from(silent).unwrap_or(u64::MAX)))
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
    pub pid: Option<u32>,
    pub at: Instant,
}

/// Stderr scan and RPC connect for a [`Started`] process.
pub struct PendingHandshake {
    lines: Lines<BufReader<ChildStderr>>,
    tail: Arc<Tail>,
    io: Arc<HandshakeIo>,
    exit: watch::Receiver<Option<Exit>>,
}

/// Shared with the watcher: drain and RPC are filled in after the ready line.
struct HandshakeIo {
    drained: Mutex<Option<JoinHandle<()>>>,
    rpc: Arc<OnceLock<Rpc>>,
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
        let discovery = match discovery::from_stderr(&mut self.lines, &self.tail).await {
            Ok(discovery) => discovery,
            Err(error) => return Err(handshake_failure(error, self.exit, &self.tail).await),
        };
        let drained = drain_stderr(self.lines, Arc::clone(&self.tail));
        *self.io.drained.lock().unwrap_or_else(PoisonError::into_inner) = Some(drained);

        let rpc = match discovery.into_rpc().await {
            Ok(rpc) => rpc,
            Err(error) => return Err(handshake_failure(error, self.exit, &self.tail).await),
        };
        let _ = self.io.rpc.set(rpc);
        Ok(())
    }
}

impl Bridge {
    /// Spawn a bridge registered against `callback` and handshake it.
    pub async fn spawn(callback: &Registration) -> Result<Self> {
        let Started {
            bridge,
            handshake,
            pid,
            at,
        } = Self::start(callback)?;
        match handshake.complete().await {
            Ok(()) => {
                tracing::info!(
                    pid,
                    histogram.cursor_bridge_spawn_ms = elapsed_ms(at),
                    "cursor-sdk-bridge spawned"
                );
                Ok(bridge)
            }
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
    /// There is no `.await` after `Command::spawn`, so cancelling this
    /// function cannot leave a process without a watcher.
    ///
    /// # Errors
    ///
    /// Returns an error when the state root cannot be created or the
    /// executable cannot be spawned.
    pub fn start(callback: &Registration) -> Result<Started> {
        let at = Instant::now();
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
        let pid = child.id();

        let stdout = child.stdout.take().expect("stdout");
        drain(BufReader::new(stdout).lines(), "stdout");

        let stderr = child.stderr.take().expect("stderr");
        let lines = BufReader::new(stderr).lines();
        let tail = Arc::new(Tail::default());

        let rpc = Arc::new(OnceLock::new());
        let io = Arc::new(HandshakeIo {
            drained: Mutex::new(None),
            rpc: Arc::clone(&rpc),
        });
        let run = Arc::new(RunWatch::default());
        let (exit_tx, exit) = watch::channel(None);
        let shutdown = Arc::new(Notify::new());
        tokio::spawn(watch_child(
            Spawned {
                child,
                pid,
                started: at,
                tail: Arc::clone(&tail),
                state_root,
                io: Arc::clone(&io),
                run: Arc::clone(&run),
            },
            Arc::clone(&shutdown),
            exit_tx,
        ));

        Ok(Started {
            bridge: Self {
                rpc,
                exit: exit.clone(),
                shutdown,
                run,
            },
            handshake: PendingHandshake {
                lines,
                tail,
                io,
                exit,
            },
            pid,
            at,
        })
    }

    pub fn rpc(&self) -> &Rpc {
        self.rpc.get().expect("the bridge handshake has completed")
    }

    /// Hand the watcher the activity clock of the run about to go over this
    /// bridge, so an uninvited exit reports whether a run was in flight and
    /// how long its stream had been silent. The sender's life is the run's.
    pub fn watch_run(&self, activity: watch::Receiver<tokio::time::Instant>) {
        self.run.set(activity);
    }

    /// Whether the bridge has exited.
    pub fn is_dead(&self) -> bool {
        self.exit.borrow().is_some()
    }

    /// Resolves once the bridge exits.
    pub fn died(&self) -> impl Future<Output = Exit> + Send + 'static {
        let mut exit = self.exit.clone();
        async move {
            loop {
                let state = *exit.borrow_and_update();
                if let Some(exit) = state {
                    return exit;
                }
                if exit.changed().await.is_err() {
                    // The watcher is gone, and the process with it (`kill_on_drop`).
                    return Exit::default();
                }
            }
        }
    }

    /// Shut the bridge down and wait for it to exit.
    pub async fn close(&self) {
        self.shutdown.notify_one();
        self.died().await;
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

// A spawned process and what it leaves behind.
struct Spawned {
    child: Child,
    // `Child::id` is `None` once the process has been reaped, so the pid is
    // kept from the spawn for the exit report.
    pid: Option<u32>,
    started: Instant,
    tail: Arc<Tail>,
    state_root: TempDir,
    io: Arc<HandshakeIo>,
    run: Arc<RunWatch>,
}

// Wait for a shutdown request or the process's own exit, publish how it
// ended either way, then release the state root.
async fn watch_child(
    mut spawned: Spawned, shutdown: Arc<Notify>, exit_tx: watch::Sender<Option<Exit>>,
) {
    let (status, crashed) = tokio::select! {
        // an exit in the same tick as a close is still an exit
        biased;
        waited = spawned.child.wait() => (waited.ok(), true),
        () = shutdown.notified() => {
            if let Some(rpc) = spawned.io.rpc.get() {
                let _ = timeout(SHUTDOWN_TIMEOUT, rpc.shutdown()).await;
            } else {
                // Handshake never bound RPC: nothing to ask, so kill now.
                let _ = spawned.child.start_kill();
            }
            let status = match timeout(SHUTDOWN_TIMEOUT, spawned.child.wait()).await {
                Ok(waited) => waited.ok(),
                Err(_elapsed) => {
                    let _ = spawned.child.start_kill();
                    spawned.child.wait().await.ok()
                }
            };
            (status, false)
        }
    };
    // Taken as the exit is seen, before the drain below: the agent's
    // `Send` is still pending on the socket, so this is the run's state at
    // the moment the process went.
    let (run_in_flight, silent_ms) = spawned.run.snapshot();
    let uptime_ms = elapsed_ms(spawned.started);

    // The pipe may still hold the bridge's last lines; a grandchild that
    // inherited it can also hold it open, so do not wait for EOF.
    let drained = spawned.io.drained.lock().unwrap_or_else(PoisonError::into_inner).take();
    if let Some(drained) = drained {
        let _ = timeout(EXIT_GRACE, drained).await;
    }
    let exit = Exit {
        status,
        pid: spawned.pid,
    };
    if crashed {
        tracing::warn!(
            pid = spawned.pid,
            uptime_ms,
            status_text = %status_text(exit.status),
            run_in_flight,
            silent_ms,
            monotonic_counter.cursor_bridge_exits = 1_u64,
            "cursor-sdk-bridge exited"
        );
        log_stderr(&spawned.tail);
    }
    let _ = exit_tx.send(Some(exit));
    drop(spawned.state_root);
}

// The handshake error, with the bridge's exit status when the watcher has
// already published one. What it wrote to stderr goes to DEBUG only, never
// into the error.
async fn handshake_failure(
    error: anyhow::Error, mut exit: watch::Receiver<Option<Exit>>, tail: &Tail,
) -> anyhow::Error {
    let status = timeout(EXIT_OBSERVE, async {
        loop {
            let state = *exit.borrow_and_update();
            if let Some(ended) = state {
                return ended;
            }
            if exit.changed().await.is_err() {
                return Exit::default();
            }
        }
    })
    .await
    .ok()
    .and_then(|ended| ended.status);
    log_stderr(tail);
    status.map_or_else(
        || anyhow::anyhow!("`{BIN}` did not complete the handshake ({error:#})"),
        |status| anyhow::anyhow!("`{BIN}` exited ({status}) during the handshake ({error:#})"),
    )
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
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

// Drain stdout so the child process never blocks.
fn drain(mut lines: Lines<impl AsyncBufRead + Unpin + Send + 'static>, label: &'static str) {
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = label, "bridge output");
        }
    });
}

// Drain stderr likewise, keeping the last lines for an exit report.
fn drain_stderr(
    mut lines: Lines<impl AsyncBufRead + Unpin + Send + 'static>, tail: Arc<Tail>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = "stderr", "bridge output");
            tail.push(line);
        }
    })
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
