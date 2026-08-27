#![doc = include_str!("../README.md")]
// The genai SDK's dependency tree pulls duplicate transitive crates (e.g.
// `schemars`, `indexmap`); these are outside this crate's control and cannot be
// unified without patching upstream, so silence the workspace `cargo` lint here.
#![allow(clippy::multiple_crate_versions)]

mod model;

use anyhow::{Context, Result};
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
        Ok(Self {
            model: options.model,
            inner: genai::Client::default(),
        })
    }
}

// A named module solely to scope the allow: the `FromEnv` derive expands to
// an undocumented public builder that `missing_docs` would otherwise flag.
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
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
