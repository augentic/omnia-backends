//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! A bridge is spawned with the tool-callback registration flags and a
//! client-owned state root, handshaken by scanning stderr for the
//! `cursor-sdk-bridge ready ` line, and watched until it exits — on request
//! through [`Bridge::close`], or on its own, which [`Bridge::died`] reports.
//! [`Bridge::attach`] joins a bridge some other process manages instead.

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

use crate::endpoint::Endpoint;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a bridge whose socket just failed is given to be seen exiting,
/// so the failure is reported as the exit rather than a transport error.
pub const EXIT_GRACE: Duration = Duration::from_millis(250);
const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// One `sdk.v1` bridge: a spawned `cursor-sdk-bridge` process this client
/// watches, or an attached one it does not own.
#[derive(Debug)]
pub struct Bridge {
    rpc: Rpc,
    exit: watch::Receiver<Exit>,
    // Wakes the watcher into a graceful shutdown; `None` for an attached bridge.
    shutdown: Option<Arc<Notify>>,
}

#[derive(Clone, Copy, Debug)]
enum Exit {
    Running,
    Exited(Option<ExitStatus>),
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

        // scan stderr for the discovery line
        let stderr = child.stderr.take().expect("stderr");
        let mut lines = BufReader::new(stderr).lines();
        let mut tail = Tail::default();
        let discovery = match discovery::from_stderr(&mut lines, &mut tail).await {
            Ok(discovery) => discovery,
            Err(error) => return Err(handshake_failure(bin, error, &mut child, &tail).await),
        };

        // drain stderr
        drain(lines, "stderr");

        let rpc = discovery.into_rpc().await?;

        let (exit_tx, exit) = watch::channel(Exit::Running);
        let shutdown = Arc::new(Notify::new());
        tokio::spawn(watch_child(child, rpc.clone(), Arc::clone(&shutdown), exit_tx, state_root));

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
        let (_, exit) = watch::channel(Exit::Running);
        Ok(Self {
            rpc: Rpc::connect(base, token).await?,
            exit,
            shutdown: None,
        })
    }

    pub const fn rpc(&self) -> &Rpc {
        &self.rpc
    }

    /// Whether a spawned bridge has exited.
    pub fn is_dead(&self) -> bool {
        matches!(*self.exit.borrow(), Exit::Exited(_))
    }

    /// Resolves with the exit status once a spawned bridge exits; an
    /// attached bridge is never observed to.
    pub fn died(&self) -> impl Future<Output = Option<ExitStatus>> + Send + 'static {
        let mut exit = self.exit.clone();
        async move {
            loop {
                let state = *exit.borrow_and_update();
                if let Exit::Exited(status) = state {
                    return status;
                }
                if exit.changed().await.is_err() {
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

// Wait for a shutdown request or the process's own exit, publish the exit
// status either way, then release the state root.
async fn watch_child(
    mut child: Child, rpc: Rpc, shutdown: Arc<Notify>, exit: watch::Sender<Exit>,
    state_root: TempDir,
) {
    let status = tokio::select! {
        () = shutdown.notified() => {
            let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, rpc.shutdown()).await;
            match tokio::time::timeout(SHUTDOWN_TIMEOUT, child.wait()).await {
                Ok(waited) => waited.ok(),
                Err(_elapsed) => {
                    let _ = child.start_kill();
                    child.wait().await.ok()
                }
            }
        }
        waited = child.wait() => {
            let status = waited.ok();
            tracing::warn!(
                status = %status_text(status),
                monotonic_counter.cursor_bridge_exits = 1_u64,
                "cursor-sdk-bridge exited"
            );
            status
        }
    };
    let _ = exit.send(Exit::Exited(status));
    drop(state_root);
}

// The handshake error, with the bridge's exit status when it already exited
// and what it wrote to stderr — one message, so the stderr tail comes last.
async fn handshake_failure(
    bin: &str, error: anyhow::Error, child: &mut Child, tail: &Tail,
) -> anyhow::Error {
    let status = tokio::time::timeout(EXIT_GRACE, child.wait()).await.ok().and_then(Result::ok);
    let mut detail = status.map_or_else(
        || format!("`{bin}` did not become ready ({error:#})"),
        |status| format!("`{bin}` exited ({status}) before its ready line ({error:#})"),
    );
    if !tail.is_empty() {
        detail.push_str("; stderr:\n");
        detail.push_str(&tail.to_string());
    }
    anyhow::anyhow!(detail)
}

/// An exit status for a log line or message: `status unknown` when the wait
/// itself failed.
pub fn status_text(status: Option<ExitStatus>) -> String {
    status.map_or_else(|| "status unknown".to_owned(), |status| status.to_string())
}

pub fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

// Drain stdout/stderr so the child process never blocks.
fn drain(mut lines: Lines<impl AsyncBufRead + Unpin + Send + 'static>, label: &'static str) {
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = label, "bridge output");
        }
    });
}
