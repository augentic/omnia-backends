#![doc = include_str!("../README.md")]

mod identity;

use std::fmt::Debug;

use anyhow::Result;
use omnia::Backend;
use tracing::instrument;

/// Azure Identity backend client, authenticating via Managed Identity.
#[derive(Clone)]
pub struct Client;

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzIdentiyClient").finish()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    // `#[instrument]` records `options` in the span, which uses the binding.
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        Ok(Self)
    }
}

/// Connection options for the Azure Identity backend.
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Ok(Self)
    }
}
