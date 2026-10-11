#![doc = include_str!("../README.md")]

mod endpoint;
mod failure;
mod model;
mod pool;
mod protocol;
mod relay;
mod worker;

use std::env;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
pub use failure::Failure;
use omnia::Backend;
pub use omnia_confine::Exit;
use omnia_confine::{Confine, Options};
pub use protocol::{RpcError, RunStatus};
use tokio::time::Instant;
use tracing::instrument;

use crate::model::Deadlines;
use crate::pool::Pool;
use crate::worker::Worker;

/// Cursor model backend.
#[derive(Clone)]
pub struct Client {
    deadlines: Deadlines,
    model: String,
    api_key: Option<String>,
    pool: Arc<Pool>,
    confine: Arc<Confine>,
    max_tool_calls: u32,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("deadlines", &self.deadlines)
            .field("model", &self.model)
            .field("max_agents", &self.pool.max_agents())
            .field("max_tool_calls", &self.max_tool_calls)
            .field("confine", &self.confine.options())
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

        let confine = Confine::connect(options.confine()).await?;
        let pool = Pool::connect(options.max_agents, &options.backend_url).await?;
        Ok(Self {
            deadlines: Deadlines {
                inactivity: Duration::from_secs(options.inactivity_secs),
                cap: Duration::from_secs(options.timeout_secs),
            },
            model: options.model,
            api_key,
            pool: Arc::new(pool),
            confine: Arc::new(confine),
            max_tool_calls: options.max_tool_calls,
        })
    }
}

impl ConnectOptions {
    // What a worker may reach: the operator's roots and hosts, the bridge's
    // own settings and the operator's `worker_env` as the environment it
    // reads of the host, and never the client's own credential or the
    // backend address, which would point the bridge past the relay.
    fn confine(&self) -> Options {
        let mut passthrough: Vec<String> =
            Worker::PASSTHROUGH.iter().map(ToString::to_string).collect();
        passthrough.extend(self.worker_env.iter().cloned());
        Options {
            shell_roots: self.shell_roots.clone(),
            read_roots: self.read_roots.clone(),
            egress: self.egress.clone(),
            passthrough,
            refused: Worker::REFUSED.iter().map(ToString::to_string).collect(),
            substrate: self.confine,
        }
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
    use omnia_confine::Substrate;

    /// Connection options for the cursor backend.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Default model id.
        #[env(from = "CURSOR_MODEL", default = "composer-2.5")]
        pub model: String,
        /// The Cursor backend the client's relay forwards the bridge's API
        /// calls to, an `http` or `https` URL; the bridge itself is pointed
        /// at the relay.
        #[env(from = "CURSOR_BACKEND_URL", default = "https://api2.cursor.sh")]
        pub backend_url: String,
        /// Absolute cap in seconds on one agent run; a correction's send gets
        /// a fresh cap.
        #[env(from = "CURSOR_TIMEOUT_SECS", default = "600")]
        pub timeout_secs: u64,
        /// Seconds without stream events before a run waiting on the bridge
        /// (its opening frame, a tool call) is cancelled; a model composing
        /// its answer, or a shell command the bridge bounds itself, is
        /// bounded by `timeout_secs` alone.
        #[env(from = "CURSOR_INACTIVITY_SECS", default = "120")]
        pub inactivity_secs: u64,
        /// Maximum agents live at once.
        #[env(from = "CURSOR_MAX_AGENTS", default = "8")]
        pub max_agents: usize,
        /// Guest tool calls one completion may make before it is ended as
        /// `budget-exhausted`; the host's default is 32.
        #[env(from = "CURSOR_MAX_TOOL_CALLS", default = "128")]
        pub max_tool_calls: u32,
        /// Trees a lent workspace at or beneath which runs with a shell,
        /// `:`-separated and relative to the invocation directory; a
        /// workspace lent elsewhere is read-only.
        #[env(from = "CURSOR_SHELL_ROOTS", default = "", with = paths)]
        pub shell_roots: Vec<PathBuf>,
        /// Trees a worker may read beside the system's own and its
        /// workspace, `:`-separated; each must exist.
        #[env(from = "CURSOR_READ_ROOTS", default = "", with = paths)]
        pub read_roots: Vec<PathBuf>,
        /// Hosts a worker's commands may reach through the egress proxy,
        /// comma-separated; `*.example.com` names every host under a
        /// domain. The bridge's own API goes through the relay, so none is
        /// needed for it.
        #[env(from = "CURSOR_EGRESS", default = "", with = split)]
        pub egress: Vec<String>,
        /// Host environment variables passed to a worker by name,
        /// comma-separated, beside the bridge's own settings and the
        /// trust-store variables it honours; `CURSOR_API_KEY`,
        /// `CURSOR_BACKEND_URL`, and the proxy variables, which name the
        /// relay and the egress proxy, are refused.
        #[env(from = "CURSOR_WORKER_ENV", default = "", with = split)]
        pub worker_env: Vec<String>,
        /// The mechanism that holds the boundary around a worker. Set in
        /// code alone: the environment never turns the boundary off.
        pub confine: Substrate,
    }

    #[expect(clippy::unnecessary_wraps, reason = "a `FromEnv` `with =` hook returns `ParseResult`")]
    fn split(s: &str) -> ParseResult<Vec<String>> {
        Ok(s.split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(ToOwned::to_owned)
            .collect())
    }

    #[expect(clippy::unnecessary_wraps, reason = "a `FromEnv` `with =` hook returns `ParseResult`")]
    fn paths(s: &str) -> ParseResult<Vec<PathBuf>> {
        Ok(std::env::split_paths(s).filter(|path| !path.as_os_str().is_empty()).collect())
    }
}
pub use config::ConnectOptions;
pub use omnia_confine::Substrate;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env()
            .confine(Substrate::default())
            .finalize()
            .context("issue loading connection options")
    }
}
