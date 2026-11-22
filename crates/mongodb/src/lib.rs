#![doc = include_str!("../README.md")]

mod blobstore;

use std::fmt::{self, Debug};

use anyhow::{Context, Result};
use omnia::{Connect, FromEnv};
use tracing::instrument;

/// MongoDB blobstore backend client.
#[derive(Clone)]
pub struct Client(mongodb::Client);

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Connect for Client {
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

/// Connection options for the MongoDB backend.
#[derive(Clone, FromEnv)]
#[env(crate = omnia)]
pub struct ConnectOptions {
    /// MongoDB connection URI (must include a default database).
    #[env(from = "MONGODB_URL")]
    pub uri: String,
}

impl Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectOptions").field("uri", &"<redacted>").finish()
    }
}
