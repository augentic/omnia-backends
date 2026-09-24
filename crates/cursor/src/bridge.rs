//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! The bridge leads a process group of its own: a kill reaches the agent
//! processes it forks, and whatever a bridge left in its group when it
//! exited is swept as the exit is seen, so nothing of a slot's process
//! outlives it.

mod discovery;
mod messages;
mod rpc;

use std::collections::VecDeque;
use std::fmt;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use discovery::Discovery;
pub use messages::{
    AgentOperationOptions, AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig,
    ModelSelection, RunStatus, RunStreamMessage, RunStreamResult, SdkMessage, TokenUsage, ToolList,
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

// The handshake's two bounds: the ready line on stderr, then `sdk.v1`
// bound over it (token read, `Ping`, `GetVersion`).
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
// The grace `Shutdown` asks the bridge for, and the bound on the whole ask —
// grace, reply and exit — before the kill; the grace must sit well inside.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(1);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const EXIT_GRACE: Duration = Duration::from_millis(250);
const EXIT_WAIT: Duration = EXIT_GRACE.saturating_mul(2);
const TAIL_LINES: usize = 20;
// What precedes the discovery payload on the ready line; always the upstream
// name, whatever the executable is called locally.
const READY_PREFIX: &str = "cursor-sdk-bridge ready ";

/// A spawned `cursor-sdk-bridge` process this client watches, with `sdk.v1`
/// bound on it. Dropping it asks the bridge to go; [`Bridge::close`] also
/// waits for it to.
#[derive(Debug)]
pub struct Bridge {
    watched: Watched,
    rpc: Rpc,
}

impl Bridge {
    /// Spawn `cursor-sdk-bridge` calling back as `callback`, and watch it.
    /// The ready-line handshake is left to [`Spawned::handshake`] so a pool
    /// lease can occupy the slot first.
    ///
    /// # Errors
    ///
    /// Returns an error when the state root cannot be created or the
    /// executable cannot be spawned.
    pub fn spawn(callback: &Registration) -> Result<Spawned> {
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

        // create process group
        let mut command = CommandWrap::from(command);
        command.wrap(KillOnDrop);
        command.wrap(ProcessGroup::leader());

        // spawn the bridge
        let child = command.spawn().context("issue spawning `cursor-sdk-bridge`")?;
        Supervisor::spawn(child, state_root)
    }

    /// The bound `sdk.v1` client.
    pub const fn rpc(&self) -> &Rpc {
        &self.rpc
    }

    /// The bound `sdk.v1` client, while the bridge is still running.
    pub fn live_rpc(&self) -> Option<&Rpc> {
        self.watched.is_running().then_some(&self.rpc)
    }

    /// `future`, failing as the bridge's exit when it exits under it.
    pub async fn fail_on_exit<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        self.watched.fail_on_exit(future).await
    }

    /// Ask the bridge to go, and wait for it to exit.
    pub async fn close(&self) {
        self.ask();
        self.watched.exited().await;
    }

    // Hand the client to the supervisor to ask over. A `Watched` that drops
    // without one is killed outright.
    fn ask(&self) {
        let _ = self.watched.shutdown.send(Some(self.rpc.clone()));
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.ask();
    }
}

// waits on the process, tears it down on request, and publishes its exit
struct Supervisor {
    child: Box<dyn ChildWrapper>,
    state_root: TempDir,
    stderr: JoinHandle<()>,
    state: Arc<State>,
    stop: watch::Receiver<Option<Rpc>>,
    exit: watch::Sender<Option<Exit>>,
}

impl Supervisor {
    // Take the process under supervision: wire its pipes, spawn the
    // supervisor as a task, and hand back the client's end of it.
    fn spawn(mut child: Box<dyn ChildWrapper>, state_root: TempDir) -> Result<Spawned> {
        let started_at = Instant::now();

        let pid = child.id().context("no pid for spawned bridge")?;
        let (Some(stdout), Some(stderr)) = (child.stdout().take(), child.stderr().take()) else {
            bail!("no piped stdout and stderr for spawned bridge");
        };

        let state = Arc::new(State {
            pid,
            started_at,
            tail: Tail::default(),
        });

        drain_stdout(stdout);
        let (discovery, stderr) = read_stderr(stderr, Arc::clone(&state));

        let (shutdown, stop) = watch::channel(None);
        let (exit_tx, exit) = watch::channel(None);
        tokio::spawn(
            Self {
                child,
                state_root,
                stderr,
                state: Arc::clone(&state),
                stop,
                exit: exit_tx,
            }
            .run(),
        );

        Ok(Spawned {
            watched: Watched {
                state,
                exit,
                shutdown,
            },
            discovery,
        })
    }

    async fn run(mut self) {
        let self_exited = tokio::select! {
            // an exit in the same tick as a close is still an exit
            biased;
            _ = self.child.wait() => true,
            // `close`, or the `Bridge` or `Spawned` dropped
            _ = self.stop.changed() => {
                // a `Bridge` hands its client over to be asked; a `Spawned`
                // has none to hand over, and is killed outright
                let rpc = self.stop.borrow().clone();
                if let Some(rpc) = rpc {
                    self.ask(&rpc).await;
                }
                false
            }
        };

        // the one kill of the group: nothing of the slot outlives it
        let _ = self.child.start_kill();
        let status = self.child.wait().await.ok();
        let uptime_ms = elapsed_ms(self.state.started_at);

        // kill the stderr if it's not done yet
        if timeout(EXIT_GRACE, &mut self.stderr).await.is_err() {
            self.stderr.abort();
        }

        let exit = Exit {
            status,
            pid: self.state.pid,
        };

        // an exit nobody asked for is a crash
        if self_exited {
            tracing::warn!(
                pid = exit.pid,
                uptime_ms,
                status = %exit,
                monotonic_counter.cursor_bridge_exits = 1_u64,
                "cursor-sdk-bridge exited"
            );
            self.state.trace_err();
        }

        let _ = self.exit.send(Some(exit));
        drop(self.state_root);
    }

    // Ask over `rpc` and wait for the exit it brings, both under one bound.
    async fn ask(&mut self, rpc: &Rpc) {
        let asked = async {
            let _ = rpc.shutdown(SHUTDOWN_GRACE).await;
            let _ = self.child.wait().await;
        };
        let _ = timeout(SHUTDOWN_TIMEOUT, asked).await;
    }
}

/// A process [`Bridge::spawn`] spawned, watched and killable, with its
/// ready-line handshake still to run. Dropped, it is killed: nothing is
/// bound to ask over.
pub struct Spawned {
    watched: Watched,
    discovery: oneshot::Receiver<Result<Discovery>>,
}

impl Spawned {
    /// Resolves once the process exits, however the handshake goes.
    pub fn exited(&self) -> impl Future<Output = Exit> + Send + 'static {
        self.watched.exited()
    }

    /// Wait for the ready line and bind `sdk.v1` over it.
    ///
    /// # Errors
    ///
    /// Returns an error when the ready line never arrives or the RPC
    /// handshake fails; the process is killed with it.
    pub async fn handshake(self) -> Result<Bridge> {
        let Self { watched, discovery } = self;
        let scanned = async {
            discovery.await.unwrap_or_else(|_gone| {
                Err(anyhow!("the stderr reader ended without a ready line"))
            })
        };
        let discovery = watched.step("no ready line", READY_TIMEOUT, scanned).await?;

        let bound = async {
            let base_url = discovery.base_url()?;
            let token = discovery.token().await?;
            Rpc::connect(&base_url, &token).await
        };
        let rpc = watched.step("no answer to the sdk.v1 handshake", CONNECT_TIMEOUT, bound).await?;

        tracing::info!(
            pid = watched.state.pid,
            histogram.cursor_bridge_spawn_ms = elapsed_ms(watched.state.started_at),
            "cursor-sdk-bridge spawned"
        );

        Ok(Bridge { watched, rpc })
    }
}

// The client's end of one watched process: the facts shared with its
// supervisor, the exit the supervisor publishes, and the stop that asks it
// to end the process — over the client the stop carries, or by a kill when
// the sender drops without one.
#[derive(Debug)]
struct Watched {
    state: Arc<State>,
    exit: watch::Receiver<Option<Exit>>,
    shutdown: watch::Sender<Option<Rpc>>,
}

impl Watched {
    fn is_running(&self) -> bool {
        self.exit.borrow().is_none()
    }

    fn exited(&self) -> impl Future<Output = Exit> + Send + 'static {
        let mut exit = self.exit.clone();
        let pid = self.state.pid;
        async move { wait_exit(&mut exit, pid).await }
    }

    async fn fail_on_exit<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        let exited = self.exited();
        tokio::pin!(exited);

        // wait for the future or the process to exit
        let error = tokio::select! {
            // branch 1: the future completed
            outcome = future => match outcome {
                Ok(value) => return Ok(value),
                Err(error) => error,
            },
            // branch 2: the process exited before the future completed
            exit = &mut exited => return Err(Failure::BridgeExited(exit).into()),
        };

        // wait for the exit that explains the failure
        match timeout(EXIT_WAIT, exited).await {
            Ok(exit) => Err(Failure::BridgeExited(exit).into()),
            Err(_elapsed) => Err(error),
        }
    }

    // Run one handshake step within `bound` and under the process's exit;
    // past the bound it fails as `missing`. The stderr tail behind a failure
    // is logged at DEBUG; it never reaches the error.
    async fn step<T>(
        &self, missing: &str, bound: Duration, step: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let bounded = async {
            timeout(bound, step)
                .await
                .unwrap_or_else(|_elapsed| Err(anyhow!("{missing} within {}s", bound.as_secs())))
        };
        self.fail_on_exit(bounded)
            .await
            .inspect_err(|_error| self.state.trace_err())
            .context("cursor sdk handshake failed")
    }
}

// what the bridge, its handshake and its supervisor all see of one process
#[derive(Debug)]
struct State {
    pid: u32,
    started_at: Instant,
    tail: Tail,
}

impl State {
    // The tail is untrusted subprocess output (paths, provider context,
    // session fragments), so it is logged at DEBUG only.
    fn trace_err(&self) {
        let tail = self.tail.to_string();
        if !tail.is_empty() {
            tracing::debug!(%tail, "cursor-sdk-bridge tail");
        }
    }
}

// The last few lines the bridge wrote to stderr (the ready line aside),
// shared between the reader and whoever reports how the process ended.
#[derive(Debug, Default)]
struct Tail(Mutex<VecDeque<String>>);

impl Tail {
    fn push(&self, line: String) {
        let mut lines = lock(&self.0);
        if lines.len() == TAIL_LINES {
            lines.pop_front();
        }
        lines.push_back(line);
    }
}

impl fmt::Display for Tail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, line) in lock(&self.0).iter().enumerate() {
            if index > 0 {
                f.write_str("\n")?;
            }
            write!(f, "  {line}")?;
        }
        Ok(())
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

// A closed channel means the supervisor is gone, and the process with it
// (`kill_on_drop`). The exit is real even if its status never arrives.
async fn wait_exit(exit: &mut watch::Receiver<Option<Exit>>, pid: u32) -> Exit {
    exit.wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|published| *published)
        .unwrap_or(Exit { status: None, pid })
}

// Read stderr to EOF, handing the ready line to the handshake and keeping
// the rest in the tail. The pipe is held however the handshake goes, so an
// abandoned one never closes it under a live writer; an EOF before the
// ready line reaches the handshake as the dropped sender.
fn read_stderr(
    stderr: ChildStderr, state: Arc<State>,
) -> (oneshot::Receiver<Result<Discovery>>, JoinHandle<()>) {
    let (discovery_tx, discovery_rx) = oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        let mut ready = Some(discovery_tx);
        while let Ok(Some(line)) = lines.next_line().await {
            // The ready line is the handshake's alone: it may carry the
            // bearer token, so it is neither logged nor kept. A dropped
            // receiver is an abandoned handshake; keep draining.
            if let Some(payload) = line.strip_prefix(READY_PREFIX) {
                if let Some(tx) = ready.take() {
                    let _ = tx.send(payload.parse());
                }
            } else {
                tracing::debug!(%line, stream = "stderr", "bridge output");
                state.tail.push(line);
            }
        }
    });
    (discovery_rx, reader)
}

// Drain stdout so a full pipe never blocks the process.
fn drain_stdout(stdout: ChildStdout) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = "stdout", "bridge output");
        }
    });
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt as _;
    use std::process::ExitStatus;

    use super::{Exit, TAIL_LINES, Tail};

    // The crash WARN and `Failure::BridgeExited` both read this text.
    #[test]
    fn exit_display() {
        let exit = |status| Exit { status, pid: 1 }.to_string();
        assert_eq!(exit(Some(ExitStatus::from_raw(9))), "signal: 9 (SIGKILL)");
        assert_eq!(exit(Some(ExitStatus::from_raw(3 << 8))), "exit status: 3");
        assert_eq!(exit(None), "status unknown");
    }

    #[test]
    fn tail_bounded() {
        let tail = Tail::default();
        assert!(tail.to_string().is_empty());
        for index in 0..TAIL_LINES + 5 {
            tail.push(format!("line {index}"));
        }
        let text = tail.to_string();
        assert_eq!(text.lines().count(), TAIL_LINES);
        assert!(text.starts_with("  line 5\n"), "the oldest lines are dropped: {text}");
        assert!(text.ends_with(&format!("  line {}", TAIL_LINES + 4)), "{text}");
    }
}
