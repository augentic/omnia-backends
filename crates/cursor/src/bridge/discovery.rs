//! Ready-line handshake with `cursor-sdk-bridge`.
//!
//! Scans stderr for the `cursor-sdk-bridge ready ` JSON payload. Unknown
//! fields are forward-compatible additions and ignored; the whole line is
//! never logged (older bridges inline `authToken`).

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use serde::Deserialize;
use serde_repr::Deserialize_repr;
use tokio::io::{AsyncBufRead, Lines};

use super::rpc::Rpc;

pub const BRIDGE_BIN: &str = "cursor-sdk-bridge";
const TIMEOUT: Duration = Duration::from_secs(30);

// Scan stderr for the ready line and parse its JSON payload.
pub async fn from_stderr(lines: &mut Lines<impl AsyncBufRead + Unpin>) -> Result<Discovery> {
    let ready_prefix = format!("{BRIDGE_BIN} ready ");

    tokio::time::timeout(TIMEOUT, async {
        while let Some(line) = lines.next_line().await.context("reading stderr")? {
            // look for "ready" line
            let Some(json) = line.strip_prefix(&ready_prefix) else {
                tracing::debug!(line = %line, "stderr");
                continue;
            };

            let discovery: Discovery = serde_json::from_str(json)
                .with_context(|| format!("parsing discovery payload: {json}"))?;
            return Ok(discovery);
        }

        bail!("no ready line found")
    })
    .await
    .map_err(|_elapsed| anyhow!("no ready line within {}s", TIMEOUT.as_secs()))?
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Discovery {
    schema_version: Version,
    url: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    auth_token: Option<String>,
    auth_token_file: Option<String>,
    transport: Transport,
    protocol: Protocol,
}

#[derive(Default, Deserialize_repr)]
#[repr(u32)]
enum Version {
    #[default]
    V1 = 1,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Transport {
    #[default]
    Tcp,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Protocol {
    #[default]
    Connect,
}

impl Discovery {
    pub async fn into_rpc(self) -> Result<Rpc> {
        let base_url = self.base_url()?;
        let token = self.token().await?;
        Rpc::connect(base_url, &token).await
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

    #[test]
    fn discovery_parsed() {
        let discovery: Discovery = serde_json::from_str(
            r#"{"schemaVersion":1,"serverVersion":"1.0.0","pid":12345,"transport":"tcp","protocol":"connect","host":"127.0.0.1","port":49152,"url":"http://127.0.0.1:49152","authTokenFile":"/tmp/auth-token","workspaceRef":"/home/me/project","stateRoot":"/home/me/.cursor/sdk-agent-store/abc"}"#
        ).expect("should parse");
        assert_eq!(discovery.base_url().expect("url"), "http://127.0.0.1:49152");
        assert_eq!(discovery.auth_token_file.as_deref(), Some("/tmp/auth-token"));
    }

    #[tokio::test]
    async fn unknown_fields() {
        let discovery: Discovery = serde_json::from_str(
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","url":"http://127.0.0.1:1","authToken":"inline","futureField":{"nested":true}}"#,
        ).expect("should parse");
        let token = discovery.token().await.expect("inline token");
        assert_eq!(token, "inline");
    }

    #[test]
    fn discovery_rejected() {
        for payload in [
            r#"{"schemaVersion":2,"transport":"tcp","protocol":"connect"}"#,
            r#"{"schemaVersion":1,"transport":"unix","protocol":"connect"}"#,
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"grpc"}"#,
        ] {
            assert!(
                serde_json::from_str::<Discovery>(payload).is_err(),
                "unsupported payload accepted: {payload}"
            );
        }
    }

    #[test]
    fn base_url_fallback() {
        let discovery: Discovery = serde_json::from_str(
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","host":"::1","port":9}"#,
        )
        .expect("host/port payload parses");
        assert_eq!(discovery.base_url().expect("base url"), "http://[::1]:9");
    }

    #[tokio::test]
    async fn token_read() {
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
        let discovery = serde_json::from_str::<Discovery>(&payload.to_string())
            .expect("payload with a token file parses");
        let token = discovery.token().await.expect("token file");
        assert_eq!(token, "file-token");
    }
}
