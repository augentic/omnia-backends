//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! The bridge leads a process group of its own: a kill reaches the agent
//! processes it forks, and whatever a bridge left in its group when it
//! exited is swept as the exit is seen, so nothing of a slot's process
//! outlives it.

mod discovery;
mod messages;
mod rpc;

use std::fmt;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use discovery::{Discovery, Tail};
pub use messages::{
    AgentOperationOptions, AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig,
    ModelSelection, RunStatus, RunStreamMessage, RunStreamResult, SdkMessage, ToolList,
};
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop, ProcessGroup};
pub use rpc::{Rpc, RunStream, TransportError};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{ChildStderr, ChildStdout, Command};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

use crate::endpoint::Registration;
use crate::{Failure, elapsed_ms, lock};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const EXIT_GRACE: Duration = Duration::from_millis(250);
// how long a failure is given for the exit it usually runs ahead of
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

        let mut command = CommandWrap::from(command);
        command.wrap(KillOnDrop);
        command.wrap(ProcessGroup::leader());

        let mut child = command.spawn().context("issue spawning `cursor-sdk-bridge`")?;
        let pid = child.id().context("no pid for spawned bridge")?;
        let (Some(stdout), Some(stderr)) = (child.stdout().take(), child.stderr().take()) else {
            bail!("no piped stdout and stderr for spawned bridge");
        };

        let state = Arc::new(State {
            pid,
            started_at,
            clock: Activity::default(),
            tail: Tail::default(),
            rpc: OnceLock::new(),
        });

        drain_stdout(stdout);
        let (discovery, stderr) = read_stderr(stderr, Arc::clone(&state));

        // wait for whichever comes first: the process exiting, or a stop request
        let (shutdown, stop) = watch::channel(());
        let (exit_tx, exit) = watch::channel(None);
        tokio::spawn(
            Supervisor {
                child,
                state_root,
                stderr,
                state: Arc::clone(&state),
                stop,
                exit: exit_tx,
            }
            .run(),
        );

        Ok((
            Self {
                state,
                exit,
                shutdown,
            },
            Handshake(discovery),
        ))
    }

    /// Whether the bridge has yet to exit.
    pub fn is_running(&self) -> bool {
        self.exit.borrow().is_none()
    }

    /// Record the run's activity clock for an uninvited exit.
    ///
    /// The sender lives as long as the run.
    pub fn watch(&self, clock: watch::Receiver<Instant>) {
        self.state.clock.set(clock);
    }

    /// Resolves once the bridge exits.
    pub fn exited(&self) -> impl Future<Output = Exit> + Send + 'static {
        let mut exit = self.exit.clone();
        let pid = self.state.pid;
        async move { wait_exit(&mut exit, pid).await }
    }

    /// `error`, unless the bridge reports the exit it usually runs ahead of.
    pub async fn exit_or(&self, error: anyhow::Error) -> anyhow::Error {
        match timeout(EXIT_WAIT, self.exited()).await {
            Ok(exit) => Failure::BridgeExited(exit).into(),
            Err(_elapsed) => error,
        }
    }

    /// Shut the bridge down and wait for it to exit.
    pub async fn close(&self) {
        let _ = self.shutdown.send(());
        self.exited().await;
    }

    // a failed handshake step; stderr stays at DEBUG, never in the error
    async fn step<T>(&self, step: Result<T>) -> Result<T> {
        match step {
            Ok(value) => Ok(value),
            Err(error) => {
                log_stderr(&self.state.tail);
                Err(self.exit_or(error).await.context("cursor sdk handshake failed"))
            }
        }
    }
}

/// Ready-line scan and RPC connect for a process [`Bridge::spawn`] spawned.
///
/// The [`Bridge`] is already watched, so the caller can occupy the agent
/// slot before [`Handshake::complete`] returns.
pub struct Handshake(oneshot::Receiver<Result<Discovery>>);

impl Handshake {
    /// Finish the ready-line scan and bind `sdk.v1` on `bridge`, returning
    /// the client.
    ///
    /// # Errors
    ///
    /// Returns an error when the ready line never arrives or the RPC handshake fails.
    pub async fn complete(self, bridge: &Bridge) -> Result<Rpc> {
        let scanned = self
            .0
            .await
            .unwrap_or_else(|_gone| Err(anyhow!("the stderr reader ended without a ready line")));
        let discovery = bridge.step(scanned).await?;
        let rpc = bridge.step(discovery.into_rpc().await).await?;

        // from here a close is asked over the client; until now it is a kill
        let _ = bridge.state.rpc.set(rpc.clone());

        tracing::info!(
            pid = bridge.state.pid,
            histogram.cursor_bridge_spawn_ms = elapsed_ms(bridge.state.started_at),
            "cursor-sdk-bridge spawned"
        );

        Ok(rpc)
    }
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
    // the bound client, once the handshake has one
    rpc: OnceLock<Rpc>,
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
    child: Box<dyn ChildWrapper>,
    state_root: TempDir,
    stderr: JoinHandle<()>,
    state: Arc<State>,
    stop: watch::Receiver<()>,
    exit: watch::Sender<Option<Exit>>,
}

impl Supervisor {
    async fn run(mut self) {
        let self_exited = tokio::select! {
            // an exit in the same tick as a close is still an exit
            biased;
            _ = self.child.wait() => true,
            // `close`, or the `Bridge` dropped
            _ = self.stop.changed() => {
                self.ask().await;
                false
            }
        };
        // The one kill, of the group: the leader if it is still up — asked
        // and not gone, or never bound to be asked — and whatever it forked
        // and left behind, so nothing of the slot outlives it. A leader
        // already reaped answers the wait at once with the status it kept.
        let _ = self.child.start_kill();
        let status = self.child.wait().await.ok();
        // before the drain: `Send` is still pending, so this is the run at exit
        let silent_ms = self.state.clock.silent_ms();
        let uptime_ms = elapsed_ms(self.state.started_at);

        // the group is gone, so a pipe still open is held by a process that
        // left it: not worth waiting for
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

    // ask over the bound client and wait for the exit it brings, under one
    // bound together; unbound, there is nothing to ask
    async fn ask(&mut self) {
        let Self { child, state, .. } = self;
        let Some(rpc) = state.rpc.get() else {
            return;
        };
        let asked = async {
            let _ = rpc.shutdown().await;
            let _ = child.wait().await;
        };
        let _ = timeout(SHUTDOWN_TIMEOUT, asked).await;
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
