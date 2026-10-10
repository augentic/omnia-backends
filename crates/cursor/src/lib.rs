#![doc = include_str!("../README.md")]

mod endpoint;
mod failure;
mod git;
mod model;
mod pool;
mod protocol;
mod worker;

use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
pub use failure::Failure;
use omnia::Backend;
pub use protocol::{RpcError, RunStatus};
use tokio::time::Instant;
use tracing::instrument;
pub use worker::Exit;

use crate::model::Deadlines;
use crate::pool::Pool;

/// Cursor model backend.
#[derive(Clone)]
pub struct Client {
    deadlines: Deadlines,
    model: String,
    api_key: Option<String>,
    pool: Arc<Pool>,
    max_tool_calls: u32,
    shell_roots: Vec<PathBuf>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("deadlines", &self.deadlines)
            .field("model", &self.model)
            .field("max_agents", &self.pool.max_agents())
            .field("max_tool_calls", &self.max_tool_calls)
            .field("shell_roots", &self.shell_roots)
            .finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let api_key = env::var("CURSOR_API_KEY").ok();
        ensure!(options.timeout_secs > 0, "timeout_secs must be greater than 0");
        ensure!(options.inactivity_secs > 0, "inactivity_secs must be greater than 0");
        ensure!(options.max_agents > 0, "max_agents must be greater than 0");
        ensure!(options.max_tool_calls > 0, "max_tool_calls must be greater than 0");
        for root in &options.shell_roots {
            ensure!(root.is_absolute(), "shell_roots entry {} is not absolute", root.display());
        }
        if !options.shell_roots.is_empty() {
            let vars = env::vars_os().filter_map(|(name, value)| {
                Some((name.into_string().ok()?, value.into_string().ok()?))
            });
            worker::Worker::check_proxies(vars)?;
            git::check_version().await?;
        }

        let pool = Pool::connect(options.max_agents).await?;
        Ok(Self {
            deadlines: Deadlines {
                inactivity: Duration::from_secs(options.inactivity_secs),
                cap: Duration::from_secs(options.timeout_secs),
            },
            model: options.model,
            api_key,
            pool: Arc::new(pool),
            max_tool_calls: options.max_tool_calls,
            shell_roots: options.shell_roots,
        })
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use std::path::PathBuf;

    use fromenv::{FromEnv, ParseResult};

    /// Connection options for the cursor backend.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Default model id.
        #[env(from = "CURSOR_MODEL", default = "composer-2.5")]
        pub model: String,
        /// Absolute cap in seconds on one agent run; a correction's send gets
        /// a fresh cap.
        #[env(from = "CURSOR_TIMEOUT_SECS", default = "600")]
        pub timeout_secs: u64,
        /// Seconds without stream events before a run waiting on the bridge
        /// (its opening frame, a tool call) is cancelled; a model composing
        /// its answer is bounded by `timeout_secs` alone.
        #[env(from = "CURSOR_INACTIVITY_SECS", default = "120")]
        pub inactivity_secs: u64,
        /// Maximum agents live at once.
        #[env(from = "CURSOR_MAX_AGENTS", default = "8")]
        pub max_agents: usize,
        /// Guest tool calls one completion may make before it is ended as
        /// `budget-exhausted`; the host's default is 32.
        #[env(from = "CURSOR_MAX_TOOL_CALLS", default = "128")]
        pub max_tool_calls: u32,
        /// Host directories whose lends grant the agent a shell, run under
        /// the bridge's sandbox: writes confined to the tree, the network
        /// closed. Absolute paths separated as `PATH` is; a lend elsewhere
        /// keeps the read-only tools.
        #[env(from = "CURSOR_SHELL_ROOTS", default = "", with = paths)]
        pub shell_roots: Vec<PathBuf>,
    }

    #[expect(clippy::unnecessary_wraps, reason = "the signature a `FromEnv` parser takes")]
    fn paths(value: &str) -> ParseResult<Vec<PathBuf>> {
        Ok(std::env::split_paths(value).filter(|path| !path.as_os_str().is_empty()).collect())
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
