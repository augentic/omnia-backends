#![doc = include_str!("../README.md")]

mod keyvalue;

use std::fmt::Debug;
use std::time::Duration;

use anyhow::{Context, Result};
use omnia::Backend;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use tracing::instrument;

/// Redis key-value backend client.
#[derive(Clone)]
pub struct Client(ConnectionManager);

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "Redis::connect_with")]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
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
        Ok(Self(conn))
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the Redis backend.
    #[derive(Debug, Clone, FromEnv)]
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
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
