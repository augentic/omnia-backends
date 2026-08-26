//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! The bridge is located in the `PATH`, spawned
//! with the tool-callback registration flags and a client-owned state root,
//! and handshaken by scanning stderr for the `cursor-sdk-bridge ready ` line.

mod discovery;
pub mod transport;
pub mod types;

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
use discovery::{BRIDGE_BIN, Discovery};
use omnia_wasi_model::ToolHost;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use transport::Transport;
use types::{Empty, GetVersionResponse, ShutdownRequest};

use crate::endpoint::{Attached, Endpoint};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// One spawned bridge process plus the Connect transport speaking to it.
#[derive(Debug)]
pub struct Bridge {
    transport: Transport,
    _shutdown: oneshot::Sender<()>,
    endpoint: Endpoint,
}

impl Bridge {
    /// Bind the tool-callback endpoint, spawn the bridge against it, complete
    /// the ready-line handshake, and verify it with `Ping` + `GetVersion`.
    ///
    /// # Errors
    ///
    /// Returns an error when the loopback bind fails, the executable is
    /// missing, the process exits or stays silent through the startup
    /// timeout, the discovery line is malformed, or the endpoint does not
    /// speak `sdk.v1`.
    pub async fn spawn() -> Result<Self> {
        let endpoint = Endpoint::bind().await?;
        let state_root = tempfile::Builder::new().prefix("omnia-cursor-").tempdir()?;

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
        let stdout = child.stdout.take().expect("stdout");
        drain_lines(BufReader::new(stdout).lines(), "stdout");

        let stderr = child.stderr.take().expect("stderr");
        let mut lines = BufReader::new(stderr).lines();
        let discovery = Discovery::scan(&mut lines).await?;
        drain_lines(lines, "stderr");

        let transport = discovery.into_transport().await?;

        transport.unary::<_, Empty>("SdkBridgeControlService/Ping", &Empty {}).await?;
        let version: GetVersionResponse =
            transport.unary("SdkBridgeControlService/GetVersion", &Empty {}).await?;
        ensure!(
            version.protocol_version == "sdk.v1",
            "bridge speaks `{}`, this backend requires `sdk.v1`",
            version.protocol_version
        );
        tracing::info!(
            bridge_version = %version.bridge_version,
            capabilities = version.capabilities.len(),
            "cursor-sdk-bridge ready"
        );

        let (shutdown, rx) = oneshot::channel();
        supervise(child, transport.clone(), state_root, rx);
        Ok(Self {
            transport,
            _shutdown: shutdown,
            endpoint,
        })
    }

    pub const fn transport(&self) -> &Transport {
        &self.transport
    }

    /// Route callbacks for `agent_id` into `tool_host` until the returned
    /// guard drops.
    pub fn attach(
        &self, agent_id: String, tool_host: Arc<dyn ToolHost>, abort: mpsc::UnboundedSender<String>,
    ) -> Attached {
        self.endpoint.attach(agent_id, tool_host, abort)
    }
}

/// Own the child and its state root until `shutdown` fires or the process exits.
fn supervise(
    mut child: Child, transport: Transport, state_root: tempfile::TempDir,
    shutdown: oneshot::Receiver<()>,
) {
    tokio::spawn(async move {
        let already_exited = tokio::select! {
            _ = shutdown => false,
            status = child.wait() => {
                tracing::warn!(?status, "cursor-sdk-bridge exited");
                true
            }
        };
        if !already_exited {
            let _ = tokio::time::timeout(
                SHUTDOWN_TIMEOUT,
                transport.unary::<_, Empty>(
                    "SdkBridgeControlService/Shutdown",
                    &ShutdownRequest { grace_seconds: 1 },
                ),
            )
            .await;
            if tokio::time::timeout(SHUTDOWN_TIMEOUT, child.wait()).await.is_err() {
                let _ = child.start_kill();
            }
        }
        drop(child);
        drop(state_root);
    });
}

// Log lines forever so the child can never block on a full pipe.
fn drain_lines(mut lines: Lines<impl AsyncBufRead + Unpin + Send + 'static>, label: &'static str) {
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(line = %line, "{label}");
        }
    });
}
