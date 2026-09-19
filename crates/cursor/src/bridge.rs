//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! A bridge is spawned with the tool-callback registration flags and a
//! client-owned state root, handshaken by scanning stderr for the
//! `cursor-sdk-bridge ready ` line, and watched until it exits — on request
//! through [`Bridge::close`], or on its own, which [`Bridge::died`] reports
//! along with the last lines it wrote to stderr. [`Bridge::attach`] joins a
//! bridge some other process manages instead.

mod discovery;
mod messages;
mod rpc;

use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
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
use tokio::process::{Child, Command};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::endpoint::Endpoint;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a bridge whose socket just failed is given to be seen exiting,
/// so the failure is reported as the exit rather than a transport error —
/// and how long its stderr pipe is given to hand over its last lines.
pub const EXIT_GRACE: Duration = Duration::from_millis(250);
const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// One `sdk.v1` bridge: a spawned `cursor-sdk-bridge` process this client
/// watches, or an attached one it does not own.
#[derive(Debug)]
pub struct Bridge {
    rpc: Rpc,
    // `None` while the process runs; an attached bridge stays there.
    exit: watch::Receiver<Option<Exit>>,
    // Wakes the watcher into a graceful shutdown; `None` for an attached bridge.
    shutdown: Option<Arc<Notify>>,
}

/// How a spawned bridge ended: the exit status, when the wait reported one,
/// and the last lines it wrote to stderr.
#[derive(Clone, Debug, Default)]
pub struct Exit {
    pub status: Option<ExitStatus>,
    pub stderr: String,
}

impl Bridge {
    /// Spawn `bin` registered against `callback` and handshake it.
    pub async fn spawn(bin: &str, callback: &Endpoint) -> Result<Self> {
        let started = Instant::now();
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

        // drain stdout
        let stdout = child.stdout.take().expect("stdout");
        drain(BufReader::new(stdout).lines(), "stdout");

        // scan stderr for the discovery line, keeping whatever else it says
        let stderr = child.stderr.take().expect("stderr");
        let mut lines = BufReader::new(stderr).lines();
        let tail = Arc::new(Tail::default());
        let discovery = match discovery::from_stderr(&mut lines, &tail).await {
            Ok(discovery) => discovery,
            Err(error) => return Err(handshake_failure(bin, error, &mut child, &tail).await),
        };
        let drained = drain_stderr(lines, Arc::clone(&tail));

        let rpc = match discovery.into_rpc().await {
            Ok(rpc) => rpc,
            Err(error) => return Err(handshake_failure(bin, error, &mut child, &tail).await),
        };

        let (exit_tx, exit) = watch::channel(None);
        let shutdown = Arc::new(Notify::new());
        let spawned = Spawned {
            child,
            drained,
            tail,
            state_root,
        };
        tokio::spawn(watch_child(spawned, rpc.clone(), Arc::clone(&shutdown), exit_tx));

        tracing::info!(
            pid,
            histogram.cursor_bridge_spawn_ms = elapsed_ms(started),
            "cursor-sdk-bridge spawned"
        );
        Ok(Self {
            rpc,
            exit,
            shutdown: Some(shutdown),
        })
    }

    /// Join a bridge already listening at `base`, owned and shut down by
    /// whoever started it.
    pub async fn attach(base: String, token: &str) -> Result<Self> {
        // No watcher publishes for it: the sender is gone, so the bridge is
        // never seen to die and `died` pends.
        let (_, exit) = watch::channel(None);
        Ok(Self {
            rpc: Rpc::connect(base, token).await?,
            exit,
            shutdown: None,
        })
    }

    pub const fn rpc(&self) -> &Rpc {
        &self.rpc
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
                let state = exit.borrow_and_update().clone();
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
    drained: JoinHandle<()>,
    tail: Arc<Tail>,
    state_root: TempDir,
}

// Wait for a shutdown request or the process's own exit, publish how it
// ended either way, then release the state root.
async fn watch_child(
    mut spawned: Spawned, rpc: Rpc, shutdown: Arc<Notify>, exit_tx: watch::Sender<Option<Exit>>,
) {
    let (status, crashed) = tokio::select! {
        // an exit in the same tick as a close is still an exit
        biased;
        waited = spawned.child.wait() => (waited.ok(), true),
        () = shutdown.notified() => {
            let _ = timeout(SHUTDOWN_TIMEOUT, rpc.shutdown()).await;
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
    let _ = timeout(EXIT_GRACE, &mut spawned.drained).await;
    let exit = Exit {
        status,
        stderr: spawned.tail.to_string(),
    };
    if crashed {
        tracing::warn!(
            status = %status_text(exit.status),
            stderr = %exit.stderr,
            monotonic_counter.cursor_bridge_exits = 1_u64,
            "cursor-sdk-bridge exited"
        );
    }
    let _ = exit_tx.send(Some(exit));
    drop(spawned.state_root);
}

// The handshake error, with the bridge's exit status when it already exited
// and what it wrote to stderr — one message, so the stderr tail comes last.
async fn handshake_failure(
    bin: &str, error: anyhow::Error, child: &mut Child, tail: &Tail,
) -> anyhow::Error {
    let status = timeout(EXIT_GRACE, child.wait()).await.ok().and_then(Result::ok);
    let lead = status.map_or_else(
        || format!("`{bin}` did not complete the handshake ({error:#})"),
        |status| format!("`{bin}` exited ({status}) during the handshake ({error:#})"),
    );
    anyhow::anyhow!(with_stderr(lead, &tail.to_string()))
}

/// An exit status for a log line or message: `status unknown` when the wait
/// itself failed.
pub fn status_text(status: Option<ExitStatus>) -> String {
    status.map_or_else(|| "status unknown".to_owned(), |status| status.to_string())
}

/// `lead`, then the stderr tail on its own lines when there is one.
pub fn with_stderr(mut lead: String, stderr: &str) -> String {
    if !stderr.is_empty() {
        lead.push_str("; stderr:\n");
        lead.push_str(stderr);
    }
    lead
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
