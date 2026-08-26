//! Spawn and manage a `cursor-sdk-bridge` process.
//!
//! The bridge is located in the `PATH`, spawned
//! with the tool-callback registration flags and a client-owned state root,
//! and handshaken by scanning stderr for the `cursor-sdk-bridge ready ` line.

pub mod transport;
pub mod types;

use std::net::IpAddr;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use serde::Deserialize;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncRead, BufReader, Lines};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use transport::Transport;
use types::{Empty, GetVersionResponse, ShutdownRequest};

const BRIDGE_BIN: &str = "cursor-sdk-bridge";
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// One spawned bridge process plus the Connect transport speaking to it.
#[derive(Debug)]
pub struct Bridge {
    transport: Transport,
    _shutdown: oneshot::Sender<()>,
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
        // let bin = std::env::var_os(BRIDGE_BIN_ENV).unwrap_or_else(|| OsString::from(BRIDGE_BIN));
        let state_root = tempfile::Builder::new()
            .prefix("omnia-cursor-state-")
            .tempdir()
            .context("creating the bridge state root")?;

        let mut command = Command::new(BRIDGE_BIN);
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

        let mut child = command.spawn().context("issue spawning cursor-sdk-bridge")?;
        let stdout = child.stdout.take().context("bridge stdout is piped")?;
        let stderr = child.stderr.take().context("bridge stderr is piped")?;
        drain(stdout, "bridge stdout");

        let mut lines = BufReader::new(stderr).lines();
        let discovery = tokio::time::timeout(READY_TIMEOUT, ready_line(&mut lines))
            .await
            .map_err(|_elapsed| {
                anyhow::anyhow!("bridge printed no ready line within {}s", READY_TIMEOUT.as_secs())
            })??;
        // Keep draining stderr forever — a full pipe would block the bridge.
        drain_lines(lines, "bridge stderr");

        let (url, token) = discovery.into_connect().await?;
        let transport = Transport::new(url, &token);

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
        })
    }

    pub const fn transport(&self) -> &Transport {
        &self.transport
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

/// Scan stderr for the discovery line, logging ordinary diagnostics.
async fn ready_line(lines: &mut Lines<impl AsyncBufRead + Unpin>) -> Result<Discovery> {
    let mut diagnostics = Vec::new();
    while let Some(line) = lines.next_line().await.context("reading bridge stderr")? {
        if let Some(json) = line.strip_prefix(&format!("{BRIDGE_BIN} ready ")) {
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
    #[serde(rename = "transport")]
    _transport: DiscoveryTransport,
    #[serde(rename = "protocol")]
    _protocol: DiscoveryProtocol,
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

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum DiscoveryTransport {
    Tcp,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum DiscoveryProtocol {
    Connect,
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
        Ok(discovery)
    }

    async fn into_connect(self) -> Result<(String, String)> {
        let endpoint = self.endpoint()?;
        let token = self.into_token().await?;
        Ok((endpoint, token))
    }

    /// Prefer `url`; fall back to `host` + `port` (bracketing `IPv6` hosts).
    fn endpoint(&self) -> Result<String> {
        if let Some(url) = &self.url {
            return Ok(url.trim_end_matches('/').to_owned());
        }
        let (Some(host), Some(port)) = (&self.host, self.port) else {
            bail!("discovery payload carries neither a url nor host and port");
        };
        Ok(match host.parse::<IpAddr>() {
            Ok(IpAddr::V6(ip)) => format!("http://[{ip}]:{port}"),
            Ok(ip) => format!("http://{ip}:{port}"),
            Err(_) if host.contains(':') => format!("http://[{host}]:{port}"),
            Err(_) => format!("http://{host}:{port}"),
        })
    }

    /// Prefer an inline token when present; else read `authTokenFile`.
    async fn into_token(self) -> Result<String> {
        if let Some(token) = self.auth_token {
            return Ok(token);
        }
        let path = self
            .auth_token_file
            .context("discovery payload carries no auth token or token file")?;
        let token = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("reading token file {path}"))?;
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

    #[tokio::test]
    async fn discovery_tolerates_unknown_fields() {
        let discovery = Discovery::parse(
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","url":"http://127.0.0.1:1","authToken":"inline","futureField":{"nested":true}}"#,
        )
        .expect("unknown fields are forward-compatible additions");
        let (_, token) = discovery.into_connect().await.expect("inline token");
        assert_eq!(token, "inline");
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

    #[tokio::test]
    async fn token_is_read_from_the_auth_token_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("token");
        std::fs::write(&path, " file-token \n").expect("write token");
        let payload = serde_json::json!({
            "schemaVersion": 1,
            "transport": "tcp",
            "protocol": "connect",
            "url": "http://127.0.0.1:1",
            "authTokenFile": path,
        });
        let discovery =
            Discovery::parse(&payload.to_string()).expect("payload with a token file parses");
        let (_, token) = discovery.into_connect().await.expect("token file");
        assert_eq!(token, "file-token");
    }
}
