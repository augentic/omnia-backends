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
use tracing::instrument;

/// Multi-provider generative-AI model backend.
#[derive(Clone)]
pub struct Client {
    model: String,
    inner: genai::Client,
    // `None` where no shell root is configured: the kernel is never probed,
    // and a worker grant is refused.
    sandbox: Option<Arc<Sandbox>>,
    max_rounds: usize,
    max_tool_calls: u32,
    max_candidates: u32,
    cap: Duration,
    shell_timeout: Duration,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("model", &self.model)
            .field("sandbox", &self.sandbox)
            .field("max_rounds", &self.max_rounds)
            .field("max_tool_calls", &self.max_tool_calls)
            .field("max_candidates", &self.max_candidates)
            .field("cap", &self.cap)
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
            max_rounds: options.max_rounds,
            max_tool_calls: options.max_tool_calls,
            max_candidates: options.max_candidates,
            cap: Duration::from_secs(options.timeout_secs),
            shell_timeout: Duration::from_secs(options.shell_timeout_secs),
        })
    }
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
    /// `timeout_secs` cap on each reply of the model, and offers the
    /// guest's `check` at most `max_candidates` candidates: the rejection
    /// of the last ends the completion as `budget-exhausted` with the
    /// correction.
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
        pub max_rounds: usize,
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
            timeout_secs: 600,
            shell_timeout_secs: 600,
        }
    }
}

impl ConnectOptions {
    // A client sending every request to `endpoint` when set, in place of the
    // provider's own base URL. Auth is untouched: the SDK still reads the key
    // the model id's provider expects from the environment.
    fn build_client(&self) -> Result<genai::Client> {
        let Some(endpoint) = &self.endpoint else {
            return Ok(genai::Client::default());
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
        Ok(genai::Client::builder().with_service_target_resolver(resolver).build())
    }
}

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
