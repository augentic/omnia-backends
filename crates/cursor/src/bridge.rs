//! Spawn and manage a `cursor-sdk-bridge` process.

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
pub use rpc::{Rpc, RunStream, TransportError};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant as TokioInstant, timeout};

use crate::endpoint::Registration;

/// How long a socket failure waits to observe [`Bridge::died`].
///
/// Covers one [`EXIT_GRACE`] stderr drain plus the caller's own observe budget.
pub const EXIT_WAIT: Duration = EXIT_GRACE.saturating_mul(2);

const BIN: &str = "cursor-sdk-bridge";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const EXIT_GRACE: Duration = Duration::from_millis(250);
const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// One `sdk.v1` bridge: a spawned `cursor-sdk-bridge` process this client
/// watches.
#[derive(Debug)]
pub struct Bridge {
    process: Arc<Process>,
    exit: watch::Receiver<Option<Exit>>,
    shutdown: watch::Sender<()>,
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
        let spawned_at = Instant::now();
        let state_root = tempfile::Builder::new()
            .prefix("omnia-cursor-")
            .tempdir()
            .context("creating state root")?;

        // inherit the environment (`CURSOR_SDK_BRIDGE_LOG` included) but drop
        // git identity, which would point the agent at the host repository
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
        let process = Arc::new(Process {
            pid: child.id(),
            started: spawned_at,
            rpc: OnceLock::new(),
            clock: Activity::default(),
            tail: Tail::default(),
        });
        drain_stdout(child.stdout.take().expect("stdout is piped"));
        let (discovery, stderr) =
            read_stderr(child.stderr.take().expect("stderr is piped"), Arc::clone(&process));

        let (shutdown, shutdown_rx) = watch::channel(());
        let (exit_tx, exit) = watch::channel(None);
        tokio::spawn(supervise(
            Supervisor {
                child,
                state_root,
                stderr,
                process: Arc::clone(&process),
            },
            shutdown_rx,
            exit_tx,
        ));

        Ok(Started {
            bridge: Self {
                process: Arc::clone(&process),
                exit: exit.clone(),
                shutdown,
            },
            handshake: Handshake {
                discovery,
                process,
                exit,
            },
        })
    }

    /// The bound `sdk.v1` client.
    ///
    /// # Panics
    ///
    /// Before [`Handshake::complete`] has returned `Ok`.
    pub fn rpc(&self) -> &Rpc {
        self.process.rpc.get().expect("the bridge handshake has completed")
    }

    /// Record the run's activity clock for an uninvited exit.
    ///
    /// The sender lives as long as the run.
    pub fn watch_run(&self, clock: watch::Receiver<tokio::time::Instant>) {
        self.process.clock.set(clock);
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

/// A spawned process whose ready-line handshake has not finished.
///
/// The [`Bridge`] is already watched, so the caller can occupy the agent
/// slot before [`Handshake::complete`] returns.
pub struct Started {
    pub bridge: Bridge,
    pub handshake: Handshake,
}

/// Ready-line scan and RPC connect for a [`Started`] process.
pub struct Handshake {
    discovery: oneshot::Receiver<Result<Discovery>>,
    process: Arc<Process>,
    exit: watch::Receiver<Option<Exit>>,
}

impl Handshake {
    /// Finish the ready-line scan and bind `sdk.v1`.
    ///
    /// # Errors
    ///
    /// Returns an error when the ready line never arrives or the RPC handshake fails.
    pub async fn complete(mut self) -> Result<()> {
        let scanned = (&mut self.discovery)
            .await
            .unwrap_or_else(|_gone| Err(anyhow!("the stderr reader ended without a ready line")));
        let discovery = self.or_exit(scanned).await?;
        let connected = discovery.into_rpc().await;
        let rpc = self.or_exit(connected).await?;
        let _ = self.process.rpc.set(rpc);

        tracing::info!(
            pid = self.process.pid,
            histogram.cursor_bridge_spawn_ms = elapsed_ms(self.process.started),
            "cursor-sdk-bridge spawned"
        );
        
        Ok(())
    }

    // attach the published exit status; stderr stays at DEBUG, never in the error
    async fn or_exit<T>(&mut self, step: Result<T>) -> Result<T> {
        let error = match step {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let status =
            timeout(EXIT_WAIT, wait_exit(&mut self.exit)).await.ok().and_then(|exit| exit.status);
        log_stderr(&self.process.tail);
        Err(status.map_or_else(
            || anyhow!("`{BIN}` did not complete the handshake ({error:#})"),
            |status| anyhow!("`{BIN}` exited ({status}) during the handshake ({error:#})"),
        ))
    }
}

/// How a spawned bridge ended.
#[derive(Clone, Copy, Debug, Default)]
pub struct Exit {
    /// The exit status, when the wait reported one.
    pub status: Option<ExitStatus>,
    /// The process id, when the spawn reported one.
    pub pid: Option<u32>,
}

#[derive(Debug)]
struct Process {
    pid: Option<u32>,
    started: Instant,
    rpc: OnceLock<Rpc>,
    clock: Activity,
    tail: Tail,
}

// activity clock of the in-flight run; the sender lives as long as the run
#[derive(Debug, Default)]
struct Activity(Mutex<Option<watch::Receiver<TokioInstant>>>);

impl Activity {
    fn set(&self, clock: watch::Receiver<TokioInstant>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(clock);
    }

    // whether a run is in flight, and how long its stream has been silent
    fn snapshot(&self) -> (bool, Option<u64>) {
        let guard = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match guard.as_ref() {
            // a closed channel is a `Send` that has returned
            Some(clock) if clock.has_changed().is_ok() => {
                (true, Some(millis(clock.borrow().elapsed())))
            }
            _ => (false, None),
        }
    }
}

struct Supervisor {
    child: Child,
    state_root: TempDir,
    stderr: JoinHandle<()>,
    process: Arc<Process>,
}

// shutdown request or the process's own exit
async fn supervise(
    mut supervisor: Supervisor, mut stop: watch::Receiver<()>, exit_tx: watch::Sender<Option<Exit>>,
) {
    let (status, self_exited) = tokio::select! {
        // an exit in the same tick as a close is still an exit
        biased;
        waited = supervisor.child.wait() => (waited.ok(), true),
        // `close`, or the last `Bridge` dropped
        _ = stop.changed() => (shutdown(&mut supervisor).await, false),
    };
    // before the drain: `Send` is still pending, so this is the run at exit
    let (run_in_flight, silent_ms) = supervisor.process.clock.snapshot();
    let uptime_ms = elapsed_ms(supervisor.process.started);

    // bound the drain: a grandchild can hold the pipe open past the last lines
    let _ = timeout(EXIT_GRACE, supervisor.stderr).await;
    if self_exited {
        tracing::warn!(
            pid = supervisor.process.pid,
            uptime_ms,
            status_text = %status_text(status),
            run_in_flight,
            silent_ms,
            monotonic_counter.cursor_bridge_exits = 1_u64,
            "cursor-sdk-bridge exited"
        );
        log_stderr(&supervisor.process.tail);
    }
    let _ = exit_tx.send(Some(Exit {
        status,
        pid: supervisor.process.pid,
    }));
    drop(supervisor.state_root);
}

// `Shutdown` once RPC is bound, otherwise kill
async fn shutdown(supervisor: &mut Supervisor) -> Option<ExitStatus> {
    if let Some(rpc) = supervisor.process.rpc.get() {
        let _ = timeout(SHUTDOWN_TIMEOUT, rpc.shutdown()).await;
    } else {
        let _ = supervisor.child.start_kill();
    }
    match timeout(SHUTDOWN_TIMEOUT, supervisor.child.wait()).await {
        Ok(waited) => waited.ok(),
        Err(_elapsed) => {
            let _ = supervisor.child.start_kill();
            supervisor.child.wait().await.ok()
        }
    }
}

/// An exit status for a log line: `status unknown` when the wait itself failed.
pub fn status_text(status: Option<ExitStatus>) -> String {
    status.map_or_else(|| "status unknown".to_owned(), |status| status.to_string())
}

pub fn elapsed_ms(since: Instant) -> u64 {
    millis(since.elapsed())
}

// a closed channel is the watcher gone, and the process with it (`kill_on_drop`)
async fn wait_exit(exit: &mut watch::Receiver<Option<Exit>>) -> Exit {
    exit.wait_for(Option::is_some).await.ok().and_then(|published| *published).unwrap_or_default()
}

// hold the pipe to EOF so an abandoned handshake never closes it under a live writer
fn read_stderr(
    stderr: ChildStderr, process: Arc<Process>,
) -> (oneshot::Receiver<Result<Discovery>>, JoinHandle<()>) {
    let (discovery_tx, discovery_rx) = oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        let scanned = discovery::from_stderr(&mut lines, &process.tail).await;
        // a dropped receiver is an abandoned handshake; keep draining
        let _ = discovery_tx.send(scanned);
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = "stderr", "bridge output");
            process.tail.push(line);
        }
    });
    (discovery_rx, reader)
}

// drain stdout so a full pipe never blocks the process
fn drain_stdout(stdout: ChildStdout) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = "stdout", "bridge output");
        }
    });
}

// untrusted (paths, provider context, session fragments): DEBUG only
fn log_stderr(tail: &Tail) {
    let stderr = tail.to_string();
    if !stderr.is_empty() {
        tracing::debug!(%stderr, "cursor-sdk-bridge stderr tail");
    }
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

    use super::{Activity, status_text};

    // crash WARN and `Failure::BridgeExited` both depend on std naming the signal
    #[test]
    fn status_text_names_the_signal() {
        assert_eq!(status_text(Some(ExitStatus::from_raw(9))), "signal: 9 (SIGKILL)");
        assert_eq!(status_text(Some(ExitStatus::from_raw(3 << 8))), "exit status: 3");
        assert_eq!(status_text(None), "status unknown");
    }

    #[tokio::test(start_paused = true)]
    async fn activity_follows_the_send() {
        let clock = Activity::default();
        assert_eq!(clock.snapshot(), (false, None), "nothing has been sent yet");

        let (tx, rx) = watch::channel(tokio::time::Instant::now());
        clock.set(rx);
        tokio::time::advance(Duration::from_millis(1500)).await;
        assert_eq!(clock.snapshot(), (true, Some(1500)), "a run in flight, silent since it began");

        tx.send_replace(tokio::time::Instant::now());
        tokio::time::advance(Duration::from_millis(200)).await;
        assert_eq!(clock.snapshot(), (true, Some(200)), "an event rearms the silence");

        drop(tx);
        assert_eq!(clock.snapshot(), (false, None), "the `Send` has returned");
    }
}
