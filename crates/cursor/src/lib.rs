#![doc = include_str!("../README.md")]
#![allow(clippy::multiple_crate_versions)]

mod bridge;
mod endpoint;
mod model;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use omnia::Backend;
use tracing::instrument;

use crate::bridge::Bridge;

#[derive(Clone, Copy, Debug)]
struct Deadlines {
    inactivity: Duration,
    cap: Duration,
}

/// Cursor model backend
#[derive(Clone, Debug)]
pub struct Client {
    deadlines: Deadlines,
    model: Option<String>,
    bridge: Arc<Bridge>,
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        ensure!(
            std::env::var_os("CURSOR_API_KEY").is_some_and(|key| !key.is_empty()),
            "CURSOR_API_KEY must be set for the cursor backend"
        );

        Ok(Self {
            deadlines: Deadlines {
                inactivity: Duration::from_secs(options.inactivity_secs),
                cap: Duration::from_secs(options.timeout_secs),
            },
            model: options.model.filter(|id| !id.trim().is_empty()),
            bridge: Arc::new(Bridge::spawn().await?),
        })
    }
}

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
        #[env(from = "CURSOR_MODEL")]
        pub model: Option<String>,
        /// Absolute wall-clock cap in seconds on one agent run; timed-out
        /// runs are cancelled.
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
