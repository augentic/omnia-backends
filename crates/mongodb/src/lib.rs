#![doc = include_str!("../README.md")]

mod blobstore;

use std::fmt::Debug;

use anyhow::{Context, Result};
use omnia::Backend;
use tracing::instrument;

/// MongoDB blobstore backend client.
#[derive(Clone)]
pub struct Client(mongodb::Client);

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "MongoDb::connect", skip(options))]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let client = mongodb::Client::with_uri_str(&options.uri)
            .await
            .context("failed to connect to mongo")?;
        tracing::info!("connected to mongo");

        Ok(Self(client))
    }
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use std::fmt;

    use fromenv::FromEnv;

    /// Connection options for the MongoDB backend.
    #[derive(Clone, FromEnv)]
    pub struct ConnectOptions {
        /// MongoDB connection URI (must include a default database).
        #[env(from = "MONGODB_URL")]
        pub uri: String,
    }

    impl fmt::Debug for ConnectOptions {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ConnectOptions").field("uri", &"<redacted>").finish()
        }
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
