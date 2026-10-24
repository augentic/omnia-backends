#![doc = include_str!("../README.md")]

mod conversation;
mod model;
mod observe;
mod request;
mod shell;
mod tools;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use genai::ServiceTarget;
use genai::resolver::{Endpoint, ServiceTargetResolver};
use omnia::Backend;
use omnia_sandbox::{Options, Sandbox};
use tokio::time::Instant;
use tracing::instrument;

use crate::conversation::Budgets;

/// Multi-provider generative-AI model backend.
#[derive(Clone)]
pub struct Client {
    model: String,
    inner: genai::Client,
    sandbox: Option<Arc<Sandbox>>,
    budgets: Budgets,
    max_tool_calls: u32,
    max_tokens: u32,
    shell_timeout: Duration,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("model", &self.model)
            .field("sandbox", &self.sandbox)
            .field("budgets", &self.budgets)
            .field("max_tool_calls", &self.max_tool_calls)
            .field("max_tokens", &self.max_tokens)
            .field("shell_timeout", &self.shell_timeout)
            .finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        ensure!(options.max_rounds > 0, "max_rounds must be greater than 0");
        ensure!(options.max_tool_calls > 0, "max_tool_calls must be greater than 0");
        ensure!(options.max_candidates > 0, "max_candidates must be greater than 0");
        ensure!(options.max_repeats > 0, "max_repeats must be greater than 0");
        ensure!(options.max_tokens > 0, "max_tokens must be greater than 0");
        ensure!(options.timeout_secs > 0, "timeout_secs must be greater than 0");
        ensure!(options.shell_timeout_secs > 0, "shell_timeout_secs must be greater than 0");
        let inner = options.build_client()?;

        let sandbox = if options.shell_roots.is_empty() {
            None
        } else {
            let sandbox = Sandbox::connect(Options {
                shell_roots: options.shell_roots,
                inherit: shell::INHERIT,
                refused: shell::REFUSED,
            })
            .await?;
            Some(Arc::new(sandbox))
        };

        Ok(Self {
            model: options.model,
            inner,
            sandbox,
            budgets: Budgets {
                max_rounds: options.max_rounds,
                max_candidates: options.max_candidates,
                max_repeats: options.max_repeats,
                timeout: Duration::from_secs(options.timeout_secs),
            },
            max_tool_calls: options.max_tool_calls,
            max_tokens: options.max_tokens,
            shell_timeout: Duration::from_secs(options.shell_timeout_secs),
        })
    }
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use std::path::PathBuf;

    use fromenv::{FromEnv, ParseResult};

    /// Connection options for the genai backend.
    ///
    /// Provider API keys are read from the ambient environment per request,
    /// routed by the model ID's prefix.
    #[derive(Debug, Clone, PartialEq, Eq, FromEnv)]
    pub struct ConnectOptions {
        /// Default model ID when a request leaves `model` unset.
        #[env(from = "GENAI_MODEL", default = "gpt-5.5")]
        pub model: String,
        /// Base URL override for a self-hosted gateway or proxy.
        ///
        /// Requests keep the wire format and API key of the provider selected
        /// by the model ID.
        #[env(from = "GENAI_ENDPOINT")]
        pub endpoint: Option<String>,
        /// Colon-separated directories under which a lent workspace gets a
        /// sandboxed `shell` tool.
        ///
        /// Workspaces outside these roots are read-only. When empty, no
        /// sandbox is initialized and worker grants are refused.
        #[env(from = "GENAI_SHELL_ROOTS", default = "", with = paths)]
        pub shell_roots: Vec<PathBuf>,
        /// Maximum provider round-trips per completion before failing with
        /// `budget-exhausted`.
        #[env(from = "GENAI_MAX_ROUNDS", default = "128")]
        pub max_rounds: u32,
        /// Maximum guest tool calls per completion before failing with
        /// `budget-exhausted`.
        #[env(from = "GENAI_MAX_TOOL_CALLS", default = "128")]
        pub max_tool_calls: u32,
        /// Maximum candidate answers offered to the guest's `check`.
        ///
        /// Rejecting the final candidate fails the completion with
        /// `budget-exhausted` carrying the guest's correction.
        #[env(from = "GENAI_MAX_CANDIDATES", default = "2")]
        pub max_candidates: u32,
        /// Maximum identical tool-call failures before failing with
        /// `budget-exhausted`.
        #[env(from = "GENAI_MAX_REPEATS", default = "5")]
        pub max_repeats: u32,
        /// Default per-round `max_tokens` when the request leaves it unset.
        #[env(from = "GENAI_MAX_TOKENS", default = "32768")]
        pub max_tokens: u32,
        /// Timeout in seconds for producing each candidate reply, including
        /// tool rounds.
        #[env(from = "GENAI_TIMEOUT_SECS", default = "600")]
        pub timeout_secs: u64,
        /// Timeout in seconds for a single `shell` command.
        #[env(from = "GENAI_SHELL_TIMEOUT_SECS", default = "600")]
        pub shell_timeout_secs: u64,
    }

    #[expect(clippy::unnecessary_wraps, reason = "a `FromEnv` `with =` hook returns `ParseResult`")]
    fn paths(s: &str) -> ParseResult<Vec<PathBuf>> {
        Ok(std::env::split_paths(s).filter(|path| !path.as_os_str().is_empty()).collect())
    }
}
pub use config::ConnectOptions;

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            model: "gpt-5.5".to_owned(),
            endpoint: None,
            shell_roots: Vec::new(),
            max_rounds: 128,
            max_tool_calls: 128,
            max_candidates: 2,
            max_repeats: 5,
            max_tokens: 32_768,
            timeout_secs: 600,
            shell_timeout_secs: 600,
        }
    }
}

// Stay under uvicorn's 5s keep-alive timeout to avoid racing server-side close.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

impl ConnectOptions {
    fn build_client(&self) -> Result<genai::Client> {
        let builder = genai::Client::builder().with_reqwest(
            genai::WebConfig::default()
                .apply_to_builder(reqwest::Client::builder())
                .pool_idle_timeout(POOL_IDLE_TIMEOUT)
                .build()
                .context("failed to create the web client")?,
        );
        let Some(endpoint) = &self.endpoint else {
            return builder.build().context("failed to create genai client");
        };
        ensure!(
            endpoint.starts_with("http://") || endpoint.starts_with("https://"),
            "endpoint must be an http(s) URL, got `{endpoint}`"
        );
        // Ensure a trailing slash so URL joining preserves the final path segment.
        let base: Arc<str> = if endpoint.ends_with('/') {
            endpoint.as_str().into()
        } else {
            format!("{endpoint}/").into()
        };
        let resolver = ServiceTargetResolver::from_resolver_fn(
            move |mut target: ServiceTarget| -> genai::resolver::Result<ServiceTarget> {
                target.endpoint = Endpoint::from_owned(Arc::clone(&base));
                Ok(target)
            },
        );
        builder
            .with_service_target_resolver(resolver)
            .build()
            .context("failed to create genai client with custom endpoint")
    }
}

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
