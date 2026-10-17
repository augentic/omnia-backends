#![doc = include_str!("../README.md")]

mod model;

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use genai::ServiceTarget;
use genai::resolver::{Endpoint, ServiceTargetResolver};
use omnia::Backend;
use tracing::instrument;

/// Multi-provider generative-AI model backend.
#[derive(Clone)]
pub struct Client {
    model: String,
    inner: genai::Client,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").field("model", &self.model).finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let inner = options.build_client()?;
        Ok(Self {
            model: options.model,
            inner,
        })
    }
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the genai backend.
    ///
    /// Provider API keys are never carried here: the genai SDK reads them
    /// from the ambient environment per request, routed by the model id's
    /// prefix. Each completion runs within an eight-round provider budget
    /// alongside the host's tool-call limit.
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
    }
}
pub use config::ConnectOptions;

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            model: "gpt-5.5".to_owned(),
            endpoint: None,
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
