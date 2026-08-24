#![doc = include_str!("../README.md")]
#![allow(clippy::multiple_crate_versions)]

mod bridge;
mod callback;
mod model;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use omnia::Backend;
use tracing::instrument;

use crate::bridge::Bridge;
use crate::callback::{CallbackServer, Registry};

#[derive(Clone, Copy, Debug)]
struct Deadlines {
    inactivity: Duration,
    cap: Duration,
}

/// Cursor model backend driving completions through a spawned
/// `cursor-sdk-bridge` process.
#[derive(Clone, Debug)]
pub struct Client {
    deadlines: Deadlines,
    model: Option<String>,
    shared: Arc<Shared>,
}

/// Connect-scoped state shared by every completion: the bridge process and
/// its transport, the loopback callback server, and the agent registry that
/// routes `CallCustomTool` callbacks into live sessions.
#[derive(Debug)]
struct Shared {
    bridge: Bridge,
    registry: Arc<Registry>,
    /// Held for its lifetime: dropping it stops the callback server.
    _callback: CallbackServer,
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        // The bridge protocol wants the key set explicitly per agent; fail
        // fast here rather than on the first completion.
        ensure!(
            std::env::var_os("CURSOR_API_KEY").is_some_and(|key| !key.is_empty()),
            "CURSOR_API_KEY must be set for the cursor backend"
        );

        let registry = Arc::new(Registry::default());
        let callback = CallbackServer::spawn(Arc::clone(&registry)).await?;
        let bridge = Bridge::spawn(callback.url(), callback.token()).await?;

        Ok(Self {
            deadlines: Deadlines {
                inactivity: Duration::from_secs(options.inactivity_secs),
                cap: Duration::from_secs(options.timeout_secs),
            },
            model: options.model.filter(|id| !id.trim().is_empty()),
            shared: Arc::new(Shared {
                bridge,
                registry,
                _callback: callback,
            }),
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
