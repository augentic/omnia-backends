#![doc = include_str!("../README.md")]

mod bridge;
mod endpoint;
mod failure;
mod model;
mod pool;

use std::env;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
pub use bridge::{Exit, RunStatus, TransportError};
pub use failure::Failure;
use omnia::Backend;
use tokio::time::Instant;
use tracing::instrument;

use crate::model::Deadlines;
use crate::pool::Pool;

/// Cursor model backend
#[derive(Clone)]
pub struct Client {
    deadlines: Deadlines,
    model: String,
    // read once at connect; every `CreateAgent` and `DeleteAgent` carries it
    api_key: String,
    pool: Arc<Pool>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("deadlines", &self.deadlines)
            .field("model", &self.model)
            .field("max_agents", &self.pool.max_agents())
            .finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let api_key =
            env::var("CURSOR_API_KEY").map_err(|_unset| anyhow!("CURSOR_API_KEY must be set"))?;
        ensure!(options.timeout_secs > 0, "timeout_secs must be greater than 0");
        ensure!(options.inactivity_secs > 0, "inactivity_secs must be greater than 0");
        ensure!(options.max_agents > 0, "max_agents must be greater than 0");

        let pool = Pool::connect(options.max_agents).await?;
        Ok(Self {
            deadlines: Deadlines {
                inactivity: Duration::from_secs(options.inactivity_secs),
                cap: Duration::from_secs(options.timeout_secs),
            },
            model: options.model,
            api_key,
            pool: Arc::new(pool),
        })
    }
}

// Every mutex in this crate guards data no panic can leave half-written, so
// a poisoned lock is still worth reading.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Milliseconds since `since`, saturating.
fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
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
        /// prompt, or a check's correction); timed-out runs are cancelled. A
        /// completion that is corrected gets a fresh cap on the second send.
        #[env(from = "CURSOR_TIMEOUT_SECS", default = "600")]
        pub timeout_secs: u64,
        /// Inactivity bound in seconds: a run is cancelled after this long
        /// with no stream events, so a stalled agent dies fast while one
        /// that is still streaming survives up to the absolute cap.
        #[env(from = "CURSOR_INACTIVITY_SECS", default = "120")]
        pub inactivity_secs: u64,
        /// Agents live at once; a further completion waits for a slot. Each
        /// live agent runs in its own `cursor-sdk-bridge` process, resolved
        /// on `PATH`.
        #[env(from = "CURSOR_MAX_AGENTS", default = "4")]
        pub max_agents: usize,
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
