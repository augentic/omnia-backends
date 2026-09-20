//! A multi-provider generative-AI model backend.
//!
//! The genai SDK's dependency tree pulls duplicate transitive crates (e.g.
//! `schemars`, `indexmap`); these are outside this crate's control and cannot
//! be unified without patching upstream, so silence the workspace `cargo` lint
//! here.

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
        let inner = match &options.endpoint {
            Some(endpoint) => genai::Client::builder()
                .with_service_target_resolver(endpoint_resolver(endpoint)?)
                .build(),
            None => genai::Client::default(),
        };
        Ok(Self {
            model: options.model,
            inner,
        })
    }
}

/// A resolver sending every request to `endpoint` in place of the provider's
/// own base URL. Auth is untouched: the SDK still reads the key the model
/// id's provider expects from the environment.
fn endpoint_resolver(endpoint: &str) -> Result<ServiceTargetResolver> {
    ensure!(
        endpoint.starts_with("http://") || endpoint.starts_with("https://"),
        "endpoint must be an http(s) URL, got `{endpoint}`"
    );
    // The SDK joins `chat/completions` onto the base with URL semantics, so a
    // base without its trailing slash would lose its last path segment.
    let base: Arc<str> =
        if endpoint.ends_with('/') { endpoint.into() } else { format!("{endpoint}/").into() };
    Ok(ServiceTargetResolver::from_resolver_fn(
        move |mut target: ServiceTarget| -> genai::resolver::Result<ServiceTarget> {
            target.endpoint = Endpoint::from_owned(Arc::clone(&base));
            Ok(target)
        },
    ))
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the genai backend.
    ///
    /// Provider API keys are never carried here: the genai SDK reads them
    /// from the ambient environment per request, routed by the model id's
    /// prefix.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Default model id when a request leaves `model` unset
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

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
