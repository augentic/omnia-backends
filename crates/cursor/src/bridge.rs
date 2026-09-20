//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! A bridge is spawned with the tool-callback registration flags and a
//! client-owned state root, handshaken by scanning stderr for the
//! `cursor-sdk-bridge ready ` line, and watched until it exits — on request
//! through [`Bridge::close`], or on its own, which [`Bridge::died`] reports.
//! The last lines it wrote to stderr are untrusted subprocess output: they
//! are logged at DEBUG for operators and never carried into WARN events or
//! the error messages callers see. [`Bridge::attach`] joins a bridge some
//! other process manages instead.

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
pub use rpc::Rpc;
use tempfile::TempDir;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStderr, Command};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::endpoint::Registration;

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
/// watches, or an attached one it does not own.
#[derive(Debug)]
pub struct Bridge {
    rpc: Arc<OnceLock<Rpc>>,
    // `None` while the process runs; an attached bridge stays there.
    exit: watch::Receiver<Option<Exit>>,
    // Wakes the watcher into a graceful shutdown; `None` for an attached bridge.
    shutdown: Option<Arc<Notify>>,
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
    bin: String,
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

/// How a spawned bridge ended: the exit status, when the wait reported one.
#[derive(Clone, Copy, Debug, Default)]
pub struct Exit {
    pub status: Option<ExitStatus>,
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
            Err(error) => {
                return Err(handshake_failure(&self.bin, error, self.exit, &self.tail).await);
            }
        };
        let drained = drain_stderr(self.lines, Arc::clone(&self.tail));
        *self.io.drained.lock().unwrap_or_else(PoisonError::into_inner) = Some(drained);

        let rpc = match discovery.into_rpc().await {
            Ok(rpc) => rpc,
            Err(error) => {
                return Err(handshake_failure(&self.bin, error, self.exit, &self.tail).await);
            }
        };
        let _ = self.io.rpc.set(rpc);
        Ok(())
    }
}

impl Bridge {
    /// Spawn `bin` registered against `callback` and handshake it.
    pub async fn spawn(bin: &str, callback: &Registration) -> Result<Self> {
        let Started {
            bridge,
            handshake,
            pid,
            at,
        } = Self::start(bin, callback)?;
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

    /// Start `bin` calling back as `callback`, and watch it. The ready-line
    /// handshake is left on the returned [`Started`] so a pool lease can
    /// occupy the slot first.
    ///
    /// There is no `.await` after `Command::spawn`, so cancelling this
    /// function cannot leave a process without a watcher.
    ///
    /// # Errors
    ///
    /// Returns an error when the state root cannot be created or `bin`
    /// cannot be spawned.
    pub fn start(bin: &str, callback: &Registration) -> Result<Started> {
        let at = Instant::now();
        let state_root = tempfile::Builder::new()
            .prefix("omnia-cursor-")
            .tempdir()
            .context("creating state root")?;

        let mut command = Command::new(bin);
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

        let mut child = command.spawn().with_context(|| format!("issue spawning `{bin}`"))?;
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
        let (exit_tx, exit) = watch::channel(None);
        let shutdown = Arc::new(Notify::new());
        tokio::spawn(watch_child(
            Spawned {
                child,
                tail: Arc::clone(&tail),
                state_root,
                io: Arc::clone(&io),
            },
            Arc::clone(&shutdown),
            exit_tx,
        ));

        Ok(Started {
            bridge: Self {
                rpc,
                exit: exit.clone(),
                shutdown: Some(shutdown),
            },
            handshake: PendingHandshake {
                bin: bin.to_owned(),
                lines,
                tail,
                io,
                exit,
            },
            pid,
            at,
        })
    }

    /// Join a loopback bridge already listening at `base`, owned and shut
    /// down by whoever started it.
    pub async fn attach(base: String, token: &str) -> Result<Self> {
        // No watcher publishes for it: the sender is gone, so the bridge is
        // never seen to die and `died` pends.
        let (_, exit) = watch::channel(None);
        let rpc = Arc::new(OnceLock::new());
        let _ = rpc.set(Rpc::connect(base, token).await?);
        Ok(Self {
            rpc,
            exit,
            shutdown: None,
        })
    }

    pub fn rpc(&self) -> &Rpc {
        self.rpc.get().expect("the bridge handshake has completed")
    }

    /// Whether this client spawned the process and shuts it down.
    pub const fn is_owned(&self) -> bool {
        self.shutdown.is_some()
    }

    /// Whether a spawned bridge has exited.
    pub fn is_dead(&self) -> bool {
        self.exit.borrow().is_some()
    }

    /// Resolves once a spawned bridge exits; an attached bridge is never
    /// observed to.
    pub fn died(&self) -> impl Future<Output = Exit> + Send + 'static {
        let mut exit = self.exit.clone();
        let owned = self.is_owned();
        async move {
            loop {
                let state = *exit.borrow_and_update();
                if let Some(exit) = state {
                    return exit;
                }
                if exit.changed().await.is_err() {
                    // The watcher is gone. An owned process went with it
                    // (`kill_on_drop`); an attached one is nobody's to see.
                    if owned {
                        return Exit::default();
                    }
                    std::future::pending::<()>().await;
                }
            }
        }
    }

    /// Shut a spawned bridge down and wait for it to exit; an attached
    /// bridge is left running.
    pub async fn close(&self) {
        let Some(shutdown) = &self.shutdown else {
            return;
        };
        shutdown.notify_one();
        self.died().await;
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        if let Some(shutdown) = &self.shutdown {
            shutdown.notify_one();
        }
    }
}

// A spawned process and what it leaves behind.
struct Spawned {
    child: Child,
    tail: Arc<Tail>,
    state_root: TempDir,
    io: Arc<HandshakeIo>,
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

    // The pipe may still hold the bridge's last lines; a grandchild that
    // inherited it can also hold it open, so do not wait for EOF.
    let drained = spawned.io.drained.lock().unwrap_or_else(PoisonError::into_inner).take();
    if let Some(drained) = drained {
        let _ = timeout(EXIT_GRACE, drained).await;
    }
    let exit = Exit { status };
    if crashed {
        tracing::warn!(
            status = %status_text(exit.status),
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
    bin: &str, error: anyhow::Error, mut exit: watch::Receiver<Option<Exit>>, tail: &Tail,
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
        || anyhow::anyhow!("`{bin}` did not complete the handshake ({error:#})"),
        |status| anyhow::anyhow!("`{bin}` exited ({status}) during the handshake ({error:#})"),
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
