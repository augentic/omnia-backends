#![doc = include_str!("../README.md")]

mod blobstore;
mod keyvalue;
mod messaging;

use std::sync::Arc;

use anyhow::{Context, Result};
use async_nats::AuthError;
use omnia::Backend;
use tracing::instrument;

/// NATS backend client for messaging, key-value, and blobstore.
#[derive(Debug, Clone)]
pub struct Client {
    inner: async_nats::Client,
    topics: Option<Vec<String>>,
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let mut nats_opts = async_nats::ConnectOptions::new();

        if let Some(jwt) = &options.jwt
            && let Some(seed) = &options.seed
        {
            let key_pair = nkeys::KeyPair::from_seed(seed).context("creating KeyPair")?;
            let key_pair = Arc::new(key_pair);
            nats_opts = nats_opts.jwt(jwt.clone(), move |nonce| {
                let key_pair = Arc::clone(&key_pair);
                async move { key_pair.sign(&nonce).map_err(AuthError::new) }
            });
        }

        let client = nats_opts.connect(&options.address).await.context("connecting to NATS")?;

        Ok(Self {
            inner: client,
            topics: options.topics,
        })
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::{FromEnv, ParseResult};

    /// Connection options for the NATS backend.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// NATS server address.
        #[env(from = "NATS_ADDR", default = "demo.nats.io")]
        pub address: String,
        /// Optional topics for subscription mode.
        #[env(from = "NATS_TOPICS", with = split)]
        pub topics: Option<Vec<String>>,
        /// Optional JWT used for NATS authentication.
        #[env(from = "NATS_JWT")]
        pub jwt: Option<String>,
        /// Optional `NKey` seed used to sign server nonce challenges.
        #[env(from = "NATS_SEED")]
        pub seed: Option<String>,
    }

    // The `FromEnv` `with =` hook requires a `ParseResult` return type.
    #[allow(clippy::unnecessary_wraps)]
    fn split(s: &str) -> ParseResult<Vec<String>> {
        Ok(s.split(',').map(ToOwned::to_owned).collect())
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
