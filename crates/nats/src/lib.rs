#![doc = include_str!("../README.md")]

mod blobstore;
mod keyvalue;
mod messaging;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use async_nats::{AuthError, jetstream};
use omnia_backend::Connect;
use tracing::instrument;

/// NATS backend client for messaging, key-value, and blobstore.
#[derive(Clone)]
pub struct Client {
    inner: async_nats::Client,
    jetstream: jetstream::Context,
    topics: Option<Vec<String>>,
    kv_max_age: Option<Duration>,
    expiries: Arc<Mutex<HashMap<String, Instant>>>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("topics", &self.topics)
            .field("kv_max_age", &self.kv_max_age)
            .finish_non_exhaustive()
    }
}

impl Connect for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        ensure!(
            options.jwt.is_some() == options.seed.is_some(),
            "NATS_JWT and NATS_SEED must be set together"
        );
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
        let jetstream = jetstream::new(client.clone());

        Ok(Self {
            inner: client,
            jetstream,
            topics: options.topics,
            kv_max_age: options.kv_max_age_secs.map(Duration::from_secs),
            expiries: Arc::default(),
        })
    }
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use std::fmt;

    use fromenv::{FromEnv, ParseResult};

    /// Connection options for the NATS backend.
    #[derive(Clone, FromEnv)]
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
        /// Optional bucket-level max age in seconds for newly created JetStream KV buckets.
        #[env(from = "NATS_KV_MAX_AGE_SECS")]
        pub kv_max_age_secs: Option<u64>,
    }

    impl fmt::Debug for ConnectOptions {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ConnectOptions")
                .field("address", &self.address)
                .field("topics", &self.topics)
                .field("jwt", &self.jwt.as_ref().map(|_| "<redacted>"))
                .field("seed", &self.seed.as_ref().map(|_| "<redacted>"))
                .field("kv_max_age_secs", &self.kv_max_age_secs)
                .finish()
        }
    }

    #[expect(clippy::unnecessary_wraps, reason = "a `FromEnv` `with =` hook returns `ParseResult`")]
    fn split(s: &str) -> ParseResult<Vec<String>> {
        Ok(s.split(',')
            .map(str::trim)
            .filter(|topic| !topic.is_empty())
            .map(ToOwned::to_owned)
            .collect())
    }
}
pub use config::ConnectOptions;

impl omnia_backend::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
