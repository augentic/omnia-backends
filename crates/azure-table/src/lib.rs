#![doc = include_str!("../README.md")]

pub mod store;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use base64ct::{Base64, Encoding};
use omnia::Backend;

/// Default HTTP request timeout for Azure Table operations.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Backend client for Azure Table storage.
#[derive(Clone)]
pub struct Client {
    options: Arc<ConnectOptions>,
    http: reqwest::Client,
    base_url: Arc<str>,
    /// Pre-decoded HMAC signing key (from base64 account key).
    hmac_key: Arc<[u8]>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzTableClient").finish()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[tracing::instrument(skip(options))]
    async fn connect_with(options: Self::ConnectOptions) -> anyhow::Result<Self> {
        let base_url: Arc<str> = options.base_url().into();
        let hmac_key: Arc<[u8]> = Base64::decode_vec(&options.key)
            .context("decoding storage account key from base64")?
            .into();
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            options: Arc::new(options),
            http,
            base_url,
            hmac_key,
        })
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Azure Table connection options.
    #[derive(Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Storage account name.
        #[env(from = "AZURE_STORAGE_ACCOUNT")]
        pub name: String,

        /// Storage account access key.
        #[env(from = "AZURE_STORAGE_KEY")]
        pub key: String,

        /// Table service endpoint URL. When empty (the default), the Azure
        /// public cloud URL `https://{name}.table.core.windows.net` is used.
        /// Set to `http://127.0.0.1:10002/{name}` for Azurite, or to a
        /// sovereign-cloud / Azure Stack endpoint as needed.
        #[env(from = "AZURE_TABLE_ENDPOINT", default = "")]
        pub endpoint: String,
    }

    impl std::fmt::Debug for ConnectOptions {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ConnectOptions")
                .field("name", &self.name)
                .field("endpoint", &self.endpoint)
                .field("key", &"[REDACTED]")
                .finish()
        }
    }

    impl ConnectOptions {
        /// Resolved base URL for the table service (no trailing slash).
        #[must_use]
        pub fn base_url(&self) -> String {
            if self.endpoint.is_empty() {
                format!("https://{}.table.core.windows.net", self.name)
            } else {
                self.endpoint.trim_end_matches('/').to_owned()
            }
        }
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> anyhow::Result<Self> {
        Self::from_env().finalize().context("issue loading azure table connection options")
    }
}
