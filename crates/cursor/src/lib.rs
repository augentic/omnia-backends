#![doc = include_str!("../README.md")]
#![allow(clippy::multiple_crate_versions)]

mod bridge;
mod endpoint;
mod model;

use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use omnia::Backend;
use tracing::instrument;

use crate::bridge::Bridge;
use crate::model::Deadlines;

/// Cursor model backend
#[derive(Clone)]
pub struct Client {
    deadlines: Deadlines,
    model: String,
    bridge: Arc<Bridge>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("deadlines", &self.deadlines)
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        ensure!(env::var("CURSOR_API_KEY").is_ok(), "CURSOR_API_KEY must be set");
        ensure!(options.timeout_secs > 0, "timeout_secs must be greater than 0");
        ensure!(options.inactivity_secs > 0, "inactivity_secs must be greater than 0");

        Ok(Self {
            deadlines: Deadlines {
                inactivity: Duration::from_secs(options.inactivity_secs),
                cap: Duration::from_secs(options.timeout_secs),
            },
            model: options.model,
            bridge: Arc::new(Bridge::spawn().await?),
        })
    }
}

// A named module solely to scope the allow: the `FromEnv` derive expands to
// an undocumented public builder that `missing_docs` would otherwise flag.
#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the cursor backend.
    ///
    /// The working tree is lent per completion through the guest's
    /// `grants.workspace`, which the host resolves to a node-local path on
    /// the tool host; without one, a completion runs tool-only in a private
    /// empty directory.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Default model id when a request leaves `model` unset; omitted
        /// means Cursor's server-side selection (`auto`).
        #[env(from = "CURSOR_MODEL", default = "auto")]
        pub model: String,
        /// Absolute wall-clock cap in seconds on one agent run (the opening
        /// prompt, or a format-repair); timed-out runs are cancelled. A
        /// completion that repairs gets a fresh cap on the second send.
        #[env(from = "CURSOR_TIMEOUT_SECS", default = "600")]
        pub timeout_secs: u64,
        /// Inactivity bound in seconds: a run is cancelled after this long
        /// with no stream events, so a stalled agent dies fast while one
        /// that is still streaming survives up to the absolute cap.
        #[env(from = "CURSOR_INACTIVITY_SECS", default = "120")]
        pub inactivity_secs: u64,
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
