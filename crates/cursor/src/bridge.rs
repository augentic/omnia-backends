//! Spawn and manage a `cursor-sdk-bridge` process.

mod discovery;
mod messages;
mod rpc;

use std::fmt;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use discovery::{Discovery, Tail};
pub use messages::{
    AgentOperationOptions, AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig,
    ModelSelection, RunStatus, RunStreamResult, SdkMessage, ToolList,
};
pub use rpc::{Rpc, RunStream, TransportError};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

use crate::endpoint::Registration;
use crate::{elapsed_ms, lock};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const EXIT_GRACE: Duration = Duration::from_millis(250);
const EXIT_WAIT: Duration = EXIT_GRACE.saturating_mul(2);

// `None` until the supervisor publishes the exit
type ExitWatch = watch::Receiver<Option<Exit>>;

/// A spawned `cursor-sdk-bridge` process this client watches.
#[derive(Debug)]
pub struct Bridge {
    state: Arc<State>,
    exit: ExitWatch,
    shutdown: watch::Sender<()>,
}

impl Bridge {
    /// Spawn `cursor-sdk-bridge` calling back as `callback`, and watch it.
    /// The ready-line handshake is left on the returned [`Handshake`] so a
    /// pool lease can occupy the slot first.
    ///
    /// # Errors
    ///
    /// Returns an error when the state root cannot be created or the
    /// executable cannot be spawned.
    pub fn spawn(callback: &Registration) -> Result<(Self, Handshake)> {
        let started_at = Instant::now();
        let state_root = tempfile::Builder::new()
            .prefix("omnia-cursor-")
            .tempdir()
            .context("creating state root")?;

        let mut command = Command::new("cursor-sdk-bridge");
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

        // drop git identity, so agent does not point at the host repository
        for var in &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"] {
            command.env_remove(var);
        }

        let mut child = command.spawn().context("issue spawning `cursor-sdk-bridge`")?;
        let pid = child.id().context("the spawned bridge reported no pid")?;
        let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
            bail!("the spawned bridge has no piped stdout and stderr");
        };

        let state = Arc::new(State {
            pid,
            started_at,
            clock: Activity::default(),
            tail: Tail::default(),
        });

        drain_stdout(stdout);
        let (discovery, stderr) = read_stderr(stderr, Arc::clone(&state));

        let (shutdown, stop) = watch::channel(());
        let (exit_tx, exit) = watch::channel(None);
        let (rpc_tx, rpc_rx) = oneshot::channel();
        tokio::spawn(
            Supervisor {
                child,
                state_root,
                stderr,
                state: Arc::clone(&state),
                stop,
                exit: exit_tx,
                rpc: rpc_rx,
            }
            .run(),
        );

        let bridge = Self {
            state: Arc::clone(&state),
            exit: exit.clone(),
            shutdown,
        };
        let handshake = Handshake {
            discovery,
            state,
            exit,
            rpc: rpc_tx,
        };
        Ok((bridge, handshake))
    }

    /// Whether the bridge has yet to exit.
    pub fn is_running(&self) -> bool {
        self.exit.borrow().is_none()
    }

    /// Record the run's activity clock for an uninvited exit.
    ///
    /// The sender lives as long as the run.
    pub fn watch_run(&self, clock: watch::Receiver<Instant>) {
        self.state.clock.set(clock);
    }

    /// Resolves once the bridge exits.
    pub fn died(&self) -> impl Future<Output = Exit> + Send + 'static {
        let mut exit = self.exit.clone();
        let pid = self.state.pid;
        async move { wait_exit(&mut exit, pid).await }
    }

    /// The exit a socket failure usually runs ahead of, waited for briefly;
    /// `None` when the process is still running past that window.
    pub async fn recent_exit(&self) -> Option<Exit> {
        timeout(EXIT_WAIT, self.died()).await.ok()
    }

    /// Shut the bridge down and wait for it to exit.
    pub async fn close(&self) {
        let _ = self.shutdown.send(());
        self.died().await;
    }
}

/// Ready-line scan and RPC connect for a process [`Bridge::spawn`] spawned.
///
/// The [`Bridge`] is already watched, so the caller can occupy the agent
/// slot before [`Handshake::complete`] returns.
pub struct Handshake {
    discovery: oneshot::Receiver<Result<Discovery>>,
    state: Arc<State>,
    exit: ExitWatch,
    rpc: oneshot::Sender<Rpc>,
}

impl Handshake {
    /// Finish the ready-line scan and bind `sdk.v1`, returning the client.
    ///
    /// # Errors
    ///
    /// Returns an error when the ready line never arrives or the RPC handshake fails.
    pub async fn complete(self) -> Result<Rpc> {
        let Self {
            discovery,
            state,
            mut exit,
            rpc: rpc_tx,
        } = self;

        let scanned = discovery
            .await
            .unwrap_or_else(|_gone| Err(anyhow!("the stderr reader ended without a ready line")));
        let discovery = or_exit(&mut exit, &state, scanned).await?;
        let connected = discovery.into_rpc().await;
        let rpc = or_exit(&mut exit, &state, connected).await?;

        // a supervisor already gone has no process left to shut down gracefully
        let _ = rpc_tx.send(rpc.clone());

        tracing::info!(
            pid = state.pid,
            histogram.cursor_bridge_spawn_ms = elapsed_ms(state.started_at),
            "cursor-sdk-bridge spawned"
        );

        Ok(rpc)
    }
}

// attach the published exit status; stderr stays at DEBUG, never in the error
async fn or_exit<T>(exit: &mut ExitWatch, state: &State, step: Result<T>) -> Result<T> {
    let error = match step {
        Ok(value) => return Ok(value),
        Err(error) => error,
    };
    let status =
        timeout(EXIT_WAIT, wait_exit(exit, state.pid)).await.ok().and_then(|exit| exit.status);
    log_stderr(&state.tail);
    Err(status.map_or_else(
        || anyhow!("cursor sdk did not complete the handshake ({error:#})"),
        |status| anyhow!("cursor sdk exited ({status}) during the handshake ({error:#})"),
    ))
}

/// How a spawned bridge ended.
#[derive(Clone, Copy, Debug)]
pub struct Exit {
    /// The exit status, when the wait reported one.
    pub status: Option<ExitStatus>,
    /// The process id.
    pub pid: u32,
}

impl fmt::Display for Exit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.status {
            Some(status) => status.fmt(f),
            None => f.write_str("status unknown"),
        }
    }
}

// what the bridge, its handshake and its supervisor all see of one process
#[derive(Debug)]
struct State {
    pid: u32,
    started_at: Instant,
    clock: Activity,
    tail: Tail,
}

// activity clock of the in-flight run; the sender lives as long as the run
#[derive(Debug, Default)]
struct Activity(Mutex<Option<watch::Receiver<Instant>>>);

impl Activity {
    fn set(&self, clock: watch::Receiver<Instant>) {
        *lock(&self.0) = Some(clock);
    }

    // how long the in-flight run's stream has been silent; `None` without a run
    fn silent_ms(&self) -> Option<u64> {
        match lock(&self.0).as_ref() {
            // a closed channel is a `Send` that has returned
            Some(clock) if clock.has_changed().is_ok() => Some(elapsed_ms(*clock.borrow())),
            _ => None,
        }
    }
}

// waits on the process, tears it down on request, and publishes its exit
struct Supervisor {
    child: Child,
    state_root: TempDir,
    stderr: JoinHandle<()>,
    state: Arc<State>,
    stop: watch::Receiver<()>,
    exit: watch::Sender<Option<Exit>>,
    // the bound client, once the handshake has one
    rpc: oneshot::Receiver<Rpc>,
}

impl Supervisor {
    async fn run(mut self) {
        let (status, self_exited) = tokio::select! {
            // an exit in the same tick as a close is still an exit
            biased;
            waited = self.child.wait() => (waited.ok(), true),
            // `close`, or the `Bridge` dropped
            _ = self.stop.changed() => (self.shutdown().await, false),
        };
        // before the drain: `Send` is still pending, so this is the run at exit
        let silent_ms = self.state.clock.silent_ms();
        let uptime_ms = elapsed_ms(self.state.started_at);

        // the process is gone, so a grandchild holding the pipe open past the
        // last lines has nothing worth waiting for
        if timeout(EXIT_GRACE, &mut self.stderr).await.is_err() {
            self.stderr.abort();
        }
        let exit = Exit {
            status,
            pid: self.state.pid,
        };
        if self_exited {
            tracing::warn!(
                pid = exit.pid,
                uptime_ms,
                status = %exit,
                run_in_flight = silent_ms.is_some(),
                silent_ms,
                monotonic_counter.cursor_bridge_exits = 1_u64,
                "cursor-sdk-bridge exited"
            );
            log_stderr(&self.state.tail);
        }
        let _ = self.exit.send(Some(exit));
        drop(self.state_root);
    }

    // `Shutdown` once RPC is bound, otherwise kill
    async fn shutdown(&mut self) -> Option<ExitStatus> {
        if let Ok(rpc) = self.rpc.try_recv() {
            let _ = timeout(SHUTDOWN_TIMEOUT, rpc.shutdown()).await;
        } else {
            let _ = self.child.start_kill();
        }
        match timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await {
            Ok(waited) => waited.ok(),
            Err(_elapsed) => {
                let _ = self.child.start_kill();
                self.child.wait().await.ok()
            }
        }
    }
}

// a closed channel is the supervisor gone, and the process with it
// (`kill_on_drop`), so the exit is real even if its status never arrives
async fn wait_exit(exit: &mut ExitWatch, pid: u32) -> Exit {
    exit.wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|published| *published)
        .unwrap_or(Exit { status: None, pid })
}

// hold the pipe to EOF so an abandoned handshake never closes it under a live writer
fn read_stderr(
    stderr: ChildStderr, state: Arc<State>,
) -> (oneshot::Receiver<Result<Discovery>>, JoinHandle<()>) {
    let (discovery_tx, discovery_rx) = oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        let scanned = discovery::from_stderr(&mut lines, &state.tail).await;
        // a dropped receiver is an abandoned handshake; keep draining
        let _ = discovery_tx.send(scanned);
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = "stderr", "bridge output");
            state.tail.push(line);
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

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt as _;
    use std::process::ExitStatus;
    use std::time::Duration;

    use tokio::sync::watch;

    use super::{Activity, Exit};

    // the crash WARN and `Failure::BridgeExited` both read this text
    #[test]
    fn exit_display() {
        let exit = |status| Exit { status, pid: 1 }.to_string();
        assert_eq!(exit(Some(ExitStatus::from_raw(9))), "signal: 9 (SIGKILL)");
        assert_eq!(exit(Some(ExitStatus::from_raw(3 << 8))), "exit status: 3");
        assert_eq!(exit(None), "status unknown");
    }

    #[tokio::test(start_paused = true)]
    async fn activity_follows_the_send() {
        let clock = Activity::default();
        assert_eq!(clock.silent_ms(), None, "nothing has been sent yet");

        let (tx, rx) = watch::channel(tokio::time::Instant::now());
        clock.set(rx);
        tokio::time::advance(Duration::from_millis(1500)).await;
        assert_eq!(clock.silent_ms(), Some(1500), "a run in flight, silent since it began");

        tx.send_replace(tokio::time::Instant::now());
        tokio::time::advance(Duration::from_millis(200)).await;
        assert_eq!(clock.silent_ms(), Some(200), "an event rearms the silence");

        drop(tx);
        assert_eq!(clock.silent_ms(), None, "the `Send` has returned");
    }
}
