#![doc = include_str!("../README.md")]

mod keyvalue;

use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use omnia::{Connect, FromEnv};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use tracing::instrument;

/// Redis key-value backend client.
#[derive(Clone)]
pub struct Client {
    conn: ConnectionManager,
    buckets: Arc<HashSet<String>>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").field("buckets", &self.buckets).finish_non_exhaustive()
    }
}

impl Connect for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "Redis::connect_with", skip(options))]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let buckets = keyvalue::declare(options.buckets)?;
        let client =
            redis::Client::open(options.url.clone()).context("failed to create redis client")?;
        let config = ConnectionManagerConfig::new()
            .set_number_of_retries(options.max_retries)
            .set_max_delay(Duration::from_millis(options.max_delay));

        let conn = client
            .get_connection_manager_with_config(config)
            .await
            .context("issue getting redis connection")?;

        tracing::info!("connected to redis");
        Ok(Self {
            conn,
            buckets: Arc::new(buckets),
        })
    }
}

/// Connection options for the Redis backend.
#[derive(Clone, FromEnv)]
#[env(crate = omnia)]
pub struct ConnectOptions {
    /// Redis connection URL.
    #[env(from = "REDIS_URL", default = "redis://localhost:6379")]
    pub url: String,
    /// Maximum number of reconnection retries.
    #[env(from = "REDIS_MAX_RETRIES", default = "3")]
    pub max_retries: usize,
    /// Maximum backoff delay in milliseconds.
    #[env(from = "REDIS_MAX_DELAY", default = "1000")]
    pub max_delay: u64,
    /// Buckets a guest may open, comma-separated.
    #[env(from = "KEYVALUE_BUCKETS", default = "default", with = omnia::env::list)]
    pub buckets: Vec<String>,
}

impl Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("url", &"<redacted>")
            .field("max_retries", &self.max_retries)
            .field("max_delay", &self.max_delay)
            .field("buckets", &self.buckets)
            .finish()
    }
}
