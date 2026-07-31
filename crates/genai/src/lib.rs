#![doc = include_str!("../README.md")]
// The genai SDK's dependency tree pulls duplicate transitive crates (e.g.
// `schemars`, `indexmap`); these are outside this crate's control and cannot be
// unified without patching upstream, so silence the workspace `cargo` lint here.
#![allow(clippy::multiple_crate_versions)]

mod model;

use std::fmt::Debug;

use anyhow::Result;
use omnia::Backend;
use tracing::instrument;

/// Multi-provider generative-AI model backend.
#[derive(Clone)]
pub struct Client {
    inner: genai::Client,
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Backend for Client {
    // The model id is carried per-request, so nothing is read from the
    // environment.
    type ConnectOptions = omnia::NoOptions;

    #[instrument(name = "GenAi::connect_with", skip_all)]
    async fn connect_with(_options: Self::ConnectOptions) -> Result<Self> {
        // The genai client reads provider API keys from the ambient environment
        // (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, …) on demand. Keys are never
        // captured here, logged, or recorded into fixtures (§7.5).
        let inner = genai::Client::default();
        tracing::info!("configured genai backend");
        Ok(Self { inner })
    }
}
