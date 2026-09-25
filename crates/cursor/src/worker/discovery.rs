//! The payload of the ready line `cursor-sdk-bridge` writes to stderr.
//!
//! Unknown fields are forward-compatible additions and ignored; the whole
//! line is never logged (older `cursor-sdk-bridge` releases inline
//! `authToken`).

use std::net::IpAddr;
use std::str::FromStr;

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use serde_repr::Deserialize_repr;

/// The ready line's payload: where the worker listens, and how to
/// authenticate to it.
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

impl FromStr for Discovery {
    type Err = anyhow::Error;

    fn from_str(payload: &str) -> Result<Self> {
        serde_json::from_str(payload).context("parsing discovery payload")
    }
}

impl Discovery {
    /// Prefer `url`; fall back to `host` + `port` (bracketing `IPv6` hosts).
    pub fn base_url(&self) -> Result<String> {
        if let Some(url) = &self.url {
            return Ok(url.trim_end_matches('/').to_owned());
        }
        let (Some(host), Some(port)) = (&self.host, self.port) else {
            bail!("discovery payload carries neither a url nor host and port");
        };

        let host = host.strip_prefix('[').and_then(|host| host.strip_suffix(']')).unwrap_or(host);
        let url = match host.parse::<IpAddr>() {
            Ok(IpAddr::V6(ip)) => format!("http://[{ip}]:{port}"),
            Ok(ip) => format!("http://{ip}:{port}"),
            Err(_) if host.contains(':') => format!("http://[{host}]:{port}"),
            Err(_) => format!("http://{host}:{port}"),
        };

        Ok(url)
    }

    /// Prefer an inline token when present; else read `authTokenFile`.
    pub async fn token(self) -> Result<String> {
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

// Deliberate unit tests: pure payload parsing (CI floor); `tests/worker.rs`
// proves the line reaches the handshake, `tests/live.rs` the real
// `cursor-sdk-bridge`.
#[cfg(test)]
mod tests {
    use super::Discovery;

    #[test]
    fn discovery_parsed() {
        let discovery: Discovery = r#"{"schemaVersion":1,"serverVersion":"1.0.0","pid":12345,"transport":"tcp","protocol":"connect","host":"127.0.0.1","port":49152,"url":"http://127.0.0.1:49152","authTokenFile":"/tmp/auth-token","workspaceRef":"/home/me/project","stateRoot":"/home/me/.cursor/sdk-agent-store/abc"}"#
            .parse()
            .expect("should parse");
        assert_eq!(discovery.base_url().expect("url"), "http://127.0.0.1:49152");
        assert_eq!(discovery.auth_token_file.as_deref(), Some("/tmp/auth-token"));
    }

    #[test]
    fn discovery_malformed() {
        assert!("{not json".parse::<Discovery>().is_err());
    }

    #[tokio::test]
    async fn unknown_fields() {
        let discovery: Discovery = r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","url":"http://127.0.0.1:1","authToken":"inline","futureField":{"nested":true}}"#
            .parse()
            .expect("should parse");
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
                payload.parse::<Discovery>().is_err(),
                "unsupported payload accepted: {payload}"
            );
        }
    }

    #[test]
    fn base_url_fallback() {
        let discovery: Discovery =
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","host":"::1","port":9}"#
                .parse()
                .expect("host/port payload parses");
        assert_eq!(discovery.base_url().expect("base url"), "http://[::1]:9");
    }

    #[test]
    fn base_url_ipv6() {
        let discovery: Discovery =
            r#"{"schemaVersion":1,"transport":"tcp","protocol":"connect","host":"[::1]","port":9}"#
                .parse()
                .expect("pre-bracketed host/port payload parses");
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
        let discovery: Discovery =
            payload.to_string().parse().expect("payload with a token file parses");
        let token = discovery.token().await.expect("token file");
        assert_eq!(token, "file-token");
    }
}
