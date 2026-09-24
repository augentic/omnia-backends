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
use std::sync::{Arc, OnceLock};
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
use crate::{Failure, elapsed_ms};

// for the exit a `Shutdown` RPC asks for
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
// for the stderr pipe once the group is gone
const EXIT_GRACE: Duration = Duration::from_millis(250);
// how long a failure is given for the exit it usually runs ahead of
const EXIT_WAIT: Duration = EXIT_GRACE.saturating_mul(2);

/// A spawned `cursor-sdk-bridge` process this client watches.
#[derive(Debug)]
pub struct Bridge {
    state: Arc<State>,
    exit: watch::Receiver<Option<Exit>>,
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

        // its own process group: one kill reaches whatever it forks
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
            tail: Tail::default(),
            rpc: OnceLock::new(),
        });

        drain_stdout(stdout);
        let (discovery, stderr) = read_stderr(stderr, Arc::clone(&state));

        // the supervisor owns the child from here to its exit
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

    /// Resolves once the bridge exits.
    pub fn exited(&self) -> impl Future<Output = Exit> + Send + 'static {
        let mut exit = self.exit.clone();
        let pid = self.state.pid;
        async move { wait_exit(&mut exit, pid).await }
    }

    /// `future`, failing as the bridge's exit when it exits under it.
    pub async fn fail_on_exit<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        let exited = self.exited();
        tokio::pin!(exited);

        let error = tokio::select! {
            outcome = future => match outcome {
                Ok(value) => return Ok(value),
                Err(error) => error,
            },
            exit = &mut exited => return Err(Failure::BridgeExited(exit).into()),
        };

        // the socket fails ahead of the exit the supervisor publishes, so
        // the failure waits a moment for the exit that explains it
        match timeout(EXIT_WAIT, exited).await {
            Ok(exit) => Err(Failure::BridgeExited(exit).into()),
            Err(_elapsed) => Err(error),
        }
    }

    /// Shut the bridge down and wait for it to exit.
    pub async fn close(&self) {
        let _ = self.shutdown.send(());
        self.exited().await;
    }

    // one handshake step; stderr stays at DEBUG, never in the error
    async fn step<T>(&self, step: impl Future<Output = Result<T>>) -> Result<T> {
        self.fail_on_exit(step)
            .await
            .inspect_err(|_error| self.state.trace_err())
            .context("cursor sdk handshake failed")
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
        let scanned = async {
            self.0.await.unwrap_or_else(|_gone| {
                Err(anyhow!("the stderr reader ended without a ready line"))
            })
        };
        let discovery = bridge.step(scanned).await?;
        let rpc = bridge.step(discovery.into_rpc()).await?;

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
    tail: Tail,
    rpc: OnceLock<Rpc>,
}

impl State {
    // untrusted (paths, provider context, session fragments): DEBUG only
    fn trace_err(&self) {
        let tail = self.tail.to_string();
        if !tail.is_empty() {
            tracing::debug!(%tail, "cursor-sdk-bridge tail");
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
        
        // the one kill, of the group: nothing of the slot outlives it
        let _ = self.child.start_kill();
        let status = self.child.wait().await.ok();
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

        // an exit nobody asked for is a crash: WARN, with the stderr behind it
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

    // Ask over the bound client and wait for the exit it brings, under one
    // bound together; unbound, there is nothing to ask
    async fn ask(&mut self) {
        let Some(rpc) = self.state.rpc.get() else {
            return;
        };

        let asked = async {
            let _ = rpc.shutdown().await;
            let _ = self.child.wait().await;
        };
        let _ = timeout(SHUTDOWN_TIMEOUT, asked).await;
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

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt as _;
    use std::process::ExitStatus;

    use super::Exit;

    // the crash WARN and `Failure::BridgeExited` both read this text
    #[test]
    fn exit_display() {
        let exit = |status| Exit { status, pid: 1 }.to_string();
        assert_eq!(exit(Some(ExitStatus::from_raw(9))), "signal: 9 (SIGKILL)");
        assert_eq!(exit(Some(ExitStatus::from_raw(3 << 8))), "exit status: 3");
        assert_eq!(exit(None), "status unknown");
    }
}
