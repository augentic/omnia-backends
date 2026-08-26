//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! The bridge is located in the `PATH`, spawned
//! with the tool-callback registration flags and a client-owned state root,
//! and handshaken by scanning stderr for the `cursor-sdk-bridge ready ` line.

mod discovery;
mod messages;
mod rpc;

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use discovery::BRIDGE_BIN;
pub use messages::{
    AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig, ModelSelection,
    RunStatus, RunStreamMessage, RunStreamResult, SdkMessage, TokenUsage, ToolList,
};
use omnia_wasi_model::ToolHost;
pub use rpc::Rpc;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};

use crate::endpoint::{Attached, Endpoint};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// One spawned bridge process plus the `sdk.v1` client speaking to it.
#[derive(Debug)]
pub struct Bridge {
    rpc: Rpc,
    _shutdown: oneshot::Sender<()>,
    endpoint: Endpoint,
}

impl Bridge {
    // Bind the tool-callback endpoint and spawn the bridge against it.
    pub async fn spawn() -> Result<Self> {
        let endpoint = Endpoint::bind().await?;
        let state_root = tempfile::Builder::new()
            .prefix("omnia-cursor-")
            .tempdir()
            .context("creating state root")?;

        let mut command = Command::new(BRIDGE_BIN);
        command
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("CURSOR_SDK_CLIENT_LANGUAGE", "rust")
            .arg("--state-root")
            .arg(state_root.path())
            .args(["--tool-callback-url", endpoint.url()])
            .args(["--tool-callback-auth-token", endpoint.token()]);
        for var in GIT_IDENTITY {
            command.env_remove(var);
        }

        let mut child = command.spawn().context("issue spawning cursor-sdk-bridge")?;

        // drain stdout
        let stdout = child.stdout.take().expect("stdout");
        drain(BufReader::new(stdout).lines(), "stdout");

        // scan stderr for the discovery line
        let stderr = child.stderr.take().expect("stderr");
        let mut lines = BufReader::new(stderr).lines();
        let discovery = discovery::from_stderr(&mut lines).await?;

        // drain stderr
        drain(lines, "stderr");

        let rpc = discovery.into_rpc().await?;

        // shutdown handler
        let (tx, rx) = oneshot::channel();
        tokio::spawn({
            let rpc = rpc.clone();
            async move {
                // wait for shutdown or process exit
                tokio::select! {
                    _ = rx => {}
                    status = child.wait() => {
                        tracing::warn!(?status, "cursor-sdk-bridge exited");
                        return;
                    }
                }

                // shutdown the bridge
                let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, rpc.shutdown()).await;
                if tokio::time::timeout(SHUTDOWN_TIMEOUT, child.wait()).await.is_err() {
                    let _ = child.start_kill();
                }
                drop(state_root);
            }
        });

        Ok(Self {
            rpc,
            _shutdown: tx,
            endpoint,
        })
    }

    pub const fn rpc(&self) -> &Rpc {
        &self.rpc
    }

    // Route callbacks for `agent_id` into `tool_host` until the returned guard
    // drops.
    pub fn attach(
        &self, agent_id: String, tool_host: Arc<dyn ToolHost>, abort: mpsc::UnboundedSender<String>,
    ) -> Attached {
        self.endpoint.attach(agent_id, tool_host, abort)
    }
}

// Drain stdout/stderr so the child process never blocks.
fn drain(mut lines: Lines<impl AsyncBufRead + Unpin + Send + 'static>, label: &'static str) {
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(%line, stream = label, "bridge output");
        }
    });
}
