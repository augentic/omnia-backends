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
    // `None` where no shell root is configured: the kernel is never probed,
    // and a worker grant is refused.
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
    /// Provider API keys are never carried here: the genai SDK reads them
    /// from the ambient environment per request, routed by the model id's
    /// prefix. Each completion runs within the `max_rounds` provider budget
    /// alongside the `max_tool_calls` guest-tool budget, under the
    /// `timeout_secs` cap on each reply of the model, each round decoding
    /// at most `max_tokens` where the request names no bound, offers the
    /// guest's `check` at most `max_candidates` candidates — the rejection
    /// of the last ends the completion as `budget-exhausted` with the
    /// correction — ends the same way once one tool call has failed
    /// `max_repeats` times with the same arguments, and asks for the answer
    /// once one has returned the same result that many times.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Default model id when a request leaves `model` unset.
        #[env(from = "GENAI_MODEL", default = "gpt-5.5")]
        pub model: String,
        /// Base URL every request goes to in place of the provider's own —
        /// a self-hosted gateway or proxy speaking the provider's API. The
        /// request keeps the shape of the provider the model id routes to,
        /// and auth is still that provider's key from the environment.
        #[env(from = "GENAI_ENDPOINT")]
        pub endpoint: Option<String>,
        /// Trees a lent workspace at or beneath which runs with a `shell`
        /// tool, `:`-separated and relative to the invocation directory; a
        /// workspace lent elsewhere is read-only. What a shell reaches
        /// beyond its workspace — hosts, `~/` roots, variables — is the
        /// completion's `grants.worker`, held to the sandbox's own rules
        /// and to nothing of the deployment's. With none, no kernel
        /// boundary is set up and a worker grant is refused.
        #[env(from = "GENAI_SHELL_ROOTS", default = "", with = paths)]
        pub shell_roots: Vec<PathBuf>,
        /// Provider round-trips one completion may make — a tool round, a
        /// candidate, or a correction each one — before it is ended as
        /// `budget-exhausted`.
        #[env(from = "GENAI_MAX_ROUNDS", default = "128")]
        pub max_rounds: u32,
        /// Guest tool calls one completion may make before it is ended as
        /// `budget-exhausted`; the host's default is 32.
        #[env(from = "GENAI_MAX_TOOL_CALLS", default = "128")]
        pub max_tool_calls: u32,
        /// Candidates one completion may offer the guest's `check`: a
        /// rejection of the last is `budget-exhausted` carrying the
        /// correction, so every candidate but the last is corrected once.
        /// The default is the cursor backend's figure, so a correction
        /// counts the same under either.
        #[env(from = "GENAI_MAX_CANDIDATES", default = "2")]
        pub max_candidates: u32,
        /// Failures one tool call may earn with the same arguments — one
        /// tool, the arguments as sent, each answered a failure the model
        /// was to repair — before the completion is ended as
        /// `budget-exhausted` naming the call; the model is told the count
        /// from the third, and a success of the call clears it. The same
        /// count of identical results from one succeeding call makes the
        /// next round the model's to answer in, its tool calls refused.
        #[env(from = "GENAI_MAX_REPEATS", default = "5")]
        pub max_repeats: u32,
        /// Tokens one reply of the model may decode in one round where the
        /// request names no `max_tokens` of its own, so a round that runs
        /// on is cut short of the context window.
        #[env(from = "GENAI_MAX_TOKENS", default = "32768")]
        pub max_tokens: u32,
        /// Absolute cap in seconds on one reply of the model: the provider
        /// rounds and tool calls up to its next text, a nudge or a
        /// correction starting a fresh cap. Past it the completion is ended
        /// as `budget-exhausted`, a shell command still running killed with
        /// everything it started.
        #[env(from = "GENAI_TIMEOUT_SECS", default = "600")]
        pub timeout_secs: u64,
        /// Seconds one `shell` command may run before it is killed with
        /// everything it started and the model told so.
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

/// How long a pooled connection may sit idle before the client drops it
/// rather than reuse it. Uvicorn, which serves vLLM, closes an idle
/// keep-alive connection after 5 s by default, and a request written onto
/// one at the same moment fails at the client before the server sees it
/// (`error sending request`) — a tool call of four or five seconds between
/// two rounds runs exactly that race. Dropping the connection well inside
/// the server's timeout keeps reuse for the quick rounds and never hands a
/// request to a socket the server is about to close.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

impl ConnectOptions {
    // A client sending every request to `endpoint` when set, in place of the
    // provider's own base URL. Auth is untouched: the SDK still reads the key
    // the model id's provider expects from the environment.
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
        // The SDK joins `chat/completions` onto the base with URL semantics, so a
        // base without its trailing slash would lose its last path segment.
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
