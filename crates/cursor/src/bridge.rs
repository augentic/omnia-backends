//! Spawn and manage one `cursor-sdk-bridge` child process.
//!
//! The bridge is located via `CURSOR_SDK_BRIDGE_BIN` (else `PATH`), spawned
//! with the tool-callback registration flags and a client-owned state root,
//! and handshaken by scanning stderr for the `cursor-sdk-bridge ready ` line.
//! The discovery line can carry an inline auth token on older bridges, so it
//! is never logged; the bearer token is read from `authTokenFile`. Shutdown is
//! best-effort graceful (`Shutdown` RPC, then kill).

pub mod transport;
pub mod types;

use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncRead, BufReader, Lines};
use tokio::process::{Child, Command};
use transport::Transport;
use types::{GetVersionResponse, ShutdownRequest};

/// Environment override for the bridge executable; else `PATH` is searched.
const BRIDGE_BIN_ENV: &str = "CURSOR_SDK_BRIDGE_BIN";
const BRIDGE_BIN: &str = "cursor-sdk-bridge";
/// Literal stderr prefix (trailing space included) of the discovery line.
const READY_PREFIX: &str = "cursor-sdk-bridge ready ";
/// The reference adapters allow the bridge 30s to print the ready line.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long shutdown waits after the `Shutdown` RPC before killing.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Host git-identity vars stripped from the bridge spawn so nothing under it
/// can mistake the host checkout for the agent workspace.
const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// One spawned bridge process plus the Connect transport speaking to it.
#[derive(Debug)]
pub struct Bridge {
    transport: Transport,
    child: Mutex<Option<Child>>,
    /// Durable local agent state lives here instead of `~/.cursor`, and is
    /// removed with the client.
    _state_root: tempfile::TempDir,
}

impl Bridge {
    /// Spawn the bridge, complete the ready-line handshake, and verify the
    /// endpoint with `Ping` + `GetVersion`.
    ///
    /// # Errors
    ///
    /// Returns an error when the executable is missing, the process exits or
    /// stays silent through the startup timeout, the discovery line is
    /// malformed, or the endpoint does not speak `sdk.v1`.
    pub async fn spawn(callback_url: &str, callback_token: &str) -> Result<Self> {
        let bin = std::env::var(BRIDGE_BIN_ENV).unwrap_or_else(|_| BRIDGE_BIN.to_owned());
        let state_root = tempfile::Builder::new()
            .prefix("omnia-cursor-state-")
            .tempdir()
            .context("creating the bridge state root")?;

        let mut command = Command::new(&bin);
        command
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("CURSOR_SDK_CLIENT_LANGUAGE", "rust")
            .arg("--state-root")
            .arg(state_root.path())
            .args(["--tool-callback-url", callback_url])
            .args(["--tool-callback-auth-token", callback_token]);
        for var in GIT_IDENTITY {
            command.env_remove(var);
        }

        let mut child = command.spawn().with_context(|| {
            format!("spawning `{bin}` (install cursor-sdk-bridge or set {BRIDGE_BIN_ENV})")
        })?;
        let stdout = child.stdout.take().context("bridge stdout is piped")?;
        let stderr = child.stderr.take().context("bridge stderr is piped")?;
        drain(stdout, "bridge stdout");

        let mut lines = BufReader::new(stderr).lines();
        let discovery = tokio::time::timeout(READY_TIMEOUT, ready_line(&mut lines))
            .await
            .map_err(|_elapsed| anyhow::anyhow!("bridge printed no ready line within 30s"))??;
        // Keep draining stderr forever — a full pipe would block the bridge.
        drain_lines(lines, "bridge stderr");

        let url = discovery.endpoint()?;
        let token = discovery.token()?;
        let transport = Transport::new(url, &token);

        let _: Value = transport.unary("SdkBridgeControlService/Ping", &json!({})).await?;
        let version: GetVersionResponse =
            transport.unary("SdkBridgeControlService/GetVersion", &json!({})).await?;
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

        Ok(Self {
            transport,
            child: Mutex::new(Some(child)),
            _state_root: state_root,
        })
    }

    pub const fn transport(&self) -> &Transport {
        &self.transport
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        let Some(mut child) = self.child.lock().ok().and_then(|mut slot| slot.take()) else {
            return;
        };
        // Prefer a graceful stop; the kill_on_drop backstop covers the rest
        // (including the no-runtime path, where dropping the child kills it).
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let transport = self.transport.clone();
        handle.spawn(async move {
            let shutdown = transport.unary::<_, Value>(
                "SdkBridgeControlService/Shutdown",
                &ShutdownRequest { grace_seconds: 1 },
            );
            let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, shutdown).await;
            if tokio::time::timeout(SHUTDOWN_TIMEOUT, child.wait()).await.is_err() {
                let _ = child.start_kill();
            }
        });
    }
}

/// Scan stderr for the discovery line, logging ordinary diagnostics.
async fn ready_line(lines: &mut Lines<impl AsyncBufRead + Unpin>) -> Result<Discovery> {
    let mut diagnostics = Vec::new();
    while let Some(line) = lines.next_line().await.context("reading bridge stderr")? {
        if let Some(json) = line.strip_prefix(READY_PREFIX) {
            return Discovery::parse(json);
        }
        tracing::debug!(line = %line, "bridge stderr");
        diagnostics.push(line);
    }
    bail!("bridge exited before its ready line: {}", diagnostics.join("\n"))
}

/// The JSON payload of the ready line. Unknown fields are forward-compatible
/// additions and ignored; the whole line is never logged (older bridges
/// inline `authToken`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Discovery {
    schema_version: u32,
    transport: String,
    protocol: String,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    auth_token_file: Option<String>,
    #[serde(default)]
    auth_token: Option<String>,
}

impl Discovery {
    fn parse(json: &str) -> Result<Self> {
        let discovery: Self =
            serde_json::from_str(json).context("parsing the bridge discovery payload")?;
        ensure!(
            discovery.schema_version == 1,
            "unsupported discovery schema version {}",
            discovery.schema_version
        );
        ensure!(discovery.transport == "tcp", "unsupported transport `{}`", discovery.transport);
        ensure!(discovery.protocol == "connect", "unsupported protocol `{}`", discovery.protocol);
        Ok(discovery)
    }

    /// Prefer `url`; fall back to `host` + `port` (bracketing `IPv6` hosts).
    fn endpoint(&self) -> Result<String> {
        if let Some(url) = &self.url {
            return Ok(url.trim_end_matches('/').to_owned());
        }
        let (Some(host), Some(port)) = (&self.host, self.port) else {
            bail!("discovery payload carries neither a url nor host and port");
        };
        let host = if host.contains(':') { format!("[{host}]") } else { host.clone() };
        Ok(format!("http://{host}:{port}"))
    }

    /// Prefer an inline token when present; else read `authTokenFile`.
    fn token(&self) -> Result<String> {
        if let Some(token) = &self.auth_token {
            return Ok(token.clone());
        }
        let path = self
            .auth_token_file
            .as_deref()
            .context("discovery payload carries no auth token or token file")?;
        let token =
            std::fs::read_to_string(path).with_context(|| format!("reading token file {path}"))?;
        Ok(token.trim().to_owned())
    }
}

/// Log a pipe line by line forever so the child can never block on it.
fn drain(pipe: impl AsyncRead + Unpin + Send + 'static, label: &'static str) {
    drain_lines(BufReader::new(pipe).lines(), label);
}

fn drain_lines(mut lines: Lines<impl AsyncBufRead + Unpin + Send + 'static>, label: &'static str) {
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(line = %line, "{label}");
        }
    });
}

// Deliberate unit tests: pure discovery-line parsing (CI floor);
// `tests/live.rs` proves the spawn-and-handshake path against a real bridge.
#[cfg(test)]
mod tests {
    use super::Discovery;

    const READY: &str = r#"{"schemaVersion":1,"serverVersion":"1.0.0","pid":12345,"transport":"tcp","protocol":"connect","host":"127.0.0.1","port":49152,"url":"http://127.0.0.1:49152","authTokenFile":"/tmp/auth-token","workspaceRef":"/home/me/project","stateRoot":"/home/me/.cursor/sdk-agent-store/abc"}"#;

    #[test]
    fn discovery_parses_the_documented_payload() {
        let discovery = Discovery::parse(READY).expect("the documented ready payload parses");
        assert_eq!(discovery.endpoint().expect("url"), "http://127.0.0.1:49152");
        assert_eq!(discovery.auth_token_file.as_deref(), Some("/tmp/auth-token"));
    }

    #[test]
    fn discovery_tolerates_unknown_fields() {
        let discovery = Discovery::parse(
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","url":"http://127.0.0.1:1","authToken":"inline","futureField":{"nested":true}}"#,
        )
        .expect("unknown fields are forward-compatible additions");
        assert_eq!(discovery.token().expect("inline token"), "inline");
    }

    #[test]
    fn discovery_rejects_other_schemas_and_transports() {
        for payload in [
            r#"{"schemaVersion":2,"transport":"tcp","protocol":"connect"}"#,
            r#"{"schemaVersion":1,"transport":"unix","protocol":"connect"}"#,
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"grpc"}"#,
        ] {
            assert!(Discovery::parse(payload).is_err(), "unsupported payload accepted: {payload}");
        }
    }

    #[test]
    fn endpoint_falls_back_to_host_and_port() {
        let discovery = Discovery::parse(
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","host":"::1","port":9}"#,
        )
        .expect("host/port payload parses");
        assert_eq!(discovery.endpoint().expect("endpoint"), "http://[::1]:9");
    }
}
