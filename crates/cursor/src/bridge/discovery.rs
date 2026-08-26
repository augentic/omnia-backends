//! Ready-line handshake with `cursor-sdk-bridge`.
//!
//! Scans stderr for the `cursor-sdk-bridge ready ` JSON payload. Unknown
//! fields are forward-compatible additions and ignored; the whole line is
//! never logged (older bridges inline `authToken`).

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail, ensure};
use serde::Deserialize;
use tokio::io::{AsyncBufRead, Lines};

use super::transport::Transport;

pub const BRIDGE_BIN: &str = "cursor-sdk-bridge";
const READY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Discovery {
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
    /// Scan stderr for the ready line and parse its JSON payload.
    pub async fn scan(lines: &mut Lines<impl AsyncBufRead + Unpin>) -> Result<Self> {
        let ready_prefix = format!("{BRIDGE_BIN} ready ");
        tokio::time::timeout(READY_TIMEOUT, async {
            let mut diagnostics = Vec::new();
            while let Some(line) = lines.next_line().await.context("reading bridge stderr")? {
                if let Some(json) = line.strip_prefix(&ready_prefix) {
                    return Self::parse(json);
                }
                tracing::debug!(line = %line, "bridge stderr");
                diagnostics.push(line);
            }
            bail!("bridge exited before its ready line: {}", diagnostics.join("\n"))
        })
        .await
        .map_err(|_elapsed| anyhow!("no ready line within {}s", READY_TIMEOUT.as_secs()))?
    }

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

    pub async fn into_transport(self) -> Result<Transport> {
        let base_url = self.base_url()?;
        let token = self.token().await?;
        Ok(Transport::new(base_url, &token))
    }

    /// Prefer `url`; fall back to `host` + `port` (bracketing `IPv6` hosts).
    fn base_url(&self) -> Result<String> {
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
    async fn token(self) -> Result<String> {
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

// Deliberate unit tests: pure discovery-line parsing (CI floor);
// `tests/live.rs` proves the spawn-and-handshake path against a real bridge.
#[cfg(test)]
mod tests {
    use super::Discovery;

    const READY: &str = r#"{"schemaVersion":1,"serverVersion":"1.0.0","pid":12345,"transport":"tcp","protocol":"connect","host":"127.0.0.1","port":49152,"url":"http://127.0.0.1:49152","authTokenFile":"/tmp/auth-token","workspaceRef":"/home/me/project","stateRoot":"/home/me/.cursor/sdk-agent-store/abc"}"#;

    #[test]
    fn discovery_parses_the_documented_payload() {
        let discovery = Discovery::parse(READY).expect("the documented ready payload parses");
        assert_eq!(discovery.base_url().expect("url"), "http://127.0.0.1:49152");
        assert_eq!(discovery.auth_token_file.as_deref(), Some("/tmp/auth-token"));
    }

    #[tokio::test]
    async fn discovery_tolerates_unknown_fields() {
        let discovery = Discovery::parse(
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","url":"http://127.0.0.1:1","authToken":"inline","futureField":{"nested":true}}"#,
        )
        .expect("unknown fields are forward-compatible additions");
        let token = discovery.token().await.expect("inline token");
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
    fn base_url_falls_back_to_host_and_port() {
        let discovery = Discovery::parse(
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","host":"::1","port":9}"#,
        )
        .expect("host/port payload parses");
        assert_eq!(discovery.base_url().expect("base url"), "http://[::1]:9");
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
        let token = discovery.token().await.expect("token file");
        assert_eq!(token, "file-token");
    }
}
