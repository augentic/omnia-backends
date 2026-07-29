#![doc = include_str!("../README.md")]
#![allow(clippy::multiple_crate_versions)]

mod mcp;
mod model;

use std::fmt::Debug;
use std::time::Duration;

use anyhow::{Context, Result};
use omnia::Backend;
use tracing::instrument;

/// Spawned, filesystem-capable `cursor-agent` model backend.
#[derive(Clone)]
pub struct Client {
    timeout: Duration,
    /// Default model id when a request leaves `model` unset.
    model: Option<String>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        model::check_cursor().await?;

        Ok(Self {
            timeout: Duration::from_secs(options.timeout_secs),
            model: options.model.filter(|id| !id.trim().is_empty()),
        })
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the `cursor-agent` backend.
    ///
    /// The working tree is lent per completion through the guest's
    /// `grants.workspace`, which the host resolves to a node-local path on the
    /// tool host.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Wall-clock bound in seconds on one `cursor-agent` spawn; orphaned
        /// processes are killed on timeout.
        #[env(from = "CURSOR_TIMEOUT_SECS", default = "600")]
        pub timeout_secs: u64,
        /// Default model id when a request leaves `model` unset; omitted means
        /// `cursor-agent` chooses.
        #[env(from = "CURSOR_MODEL")]
        pub model: Option<String>,
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn from_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
