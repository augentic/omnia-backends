#![doc = include_str!("../README.md")]

mod blobstore;

use anyhow::{Context, Result};
use omnia::Backend;
use tracing::instrument;

/// MongoDB blobstore backend client.
#[derive(Debug, Clone)]
pub struct Client(mongodb::Client);

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "MongoDb::connect")]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let client = mongodb::Client::with_uri_str(&options.uri)
            .await
            .context("failed to connect to mongo")?;
        tracing::info!("connected to mongo");

        Ok(Self(client))
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the MongoDB backend.
    #[derive(Clone, Debug, FromEnv)]
    pub struct ConnectOptions {
        /// MongoDB connection URI (must include a default database).
        #[env(from = "MONGODB_URL")]
        pub uri: String,
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
