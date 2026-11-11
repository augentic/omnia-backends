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

impl Discovery {
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

impl FromStr for Discovery {
    type Err = anyhow::Error;

    fn from_str(payload: &str) -> Result<Self> {
        serde_json::from_str(payload).context("parsing discovery payload")
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
