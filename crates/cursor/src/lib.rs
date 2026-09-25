#![doc = include_str!("../README.md")]

mod endpoint;
mod failure;
mod model;
mod pool;
mod protocol;
mod worker;

use std::env;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
pub use failure::Failure;
use omnia::Backend;
pub use protocol::{RpcError, RunStatus};
use tokio::time::Instant;
use tracing::instrument;
pub use worker::Exit;

use crate::model::Deadlines;
use crate::pool::Pool;

/// Cursor model backend
#[derive(Clone)]
pub struct Client {
    deadlines: Deadlines,
    model: String,
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the cursor backend.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Default model id.
        #[env(from = "CURSOR_MODEL", default = "auto")]
        pub model: String,
        /// Absolute cap in seconds on one agent run. A completion that is
        /// corrected gets a fresh cap on the second send.
        #[env(from = "CURSOR_TIMEOUT_SECS", default = "600")]
        pub timeout_secs: u64,
        /// The period of time without events after which a run is cancelled.
        #[env(from = "CURSOR_INACTIVITY_SECS", default = "120")]
        pub inactivity_secs: u64,
        /// The maximum number of agents that can be live at once.
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
