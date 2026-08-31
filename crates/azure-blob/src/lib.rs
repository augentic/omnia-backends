#![doc = include_str!("../README.md")]

mod blobstore;
mod plugin;

use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Context, Result};
use azure_core::credentials::TokenCredential;
use azure_core::http::Url;
use azure_identity::{ClientSecretCredential, DeveloperToolsCredential};
use azure_storage_blob::BlobServiceClient;
use omnia::Backend;
use tracing::instrument;

/// Azure Blob Storage backend client.
#[derive(Clone)]
pub struct Client {
    service: Arc<BlobServiceClient>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzBlobClient").finish()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let credential: Arc<dyn TokenCredential> = if let Some(cred) = &options.credential {
            ClientSecretCredential::new(
                &cred.tenant_id,
                cred.client_id.clone(),
                azure_core::credentials::Secret::new(cred.client_secret.clone()),
                None,
            )?
        } else {
            DeveloperToolsCredential::new(None).context("could not create credential")?
        };

        let endpoint: Url = options.endpoint.parse().context("invalid blob endpoint URL")?;
        let service = BlobServiceClient::new(endpoint, Some(credential), None)
            .context("failed to create blob service client")?;
        tracing::info!("connected to azure blob storage");

        Ok(Self {
            service: Arc::new(service),
        })
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the Azure Blob Storage backend.
    #[derive(Clone, Debug, FromEnv)]
    pub struct ConnectOptions {
        /// Storage account endpoint URL.
        #[env(from = "AZURE_BLOB_ENDPOINT")]
        pub endpoint: String,

        /// Optional service principal credentials (falls back to developer tools).
        #[env(nested)]
        pub credential: Option<CredentialOptions>,
    }

    /// Azure service principal credential fields.
    #[derive(Debug, Clone, FromEnv)]
    pub struct CredentialOptions {
        /// Azure AD tenant identifier.
        #[env(from = "AZURE_TENANT_ID")]
        pub tenant_id: String,
        /// Azure AD application (client) identifier.
        #[env(from = "AZURE_CLIENT_ID")]
        pub client_id: String,
        /// Azure AD application secret.
        #[env(from = "AZURE_CLIENT_SECRET")]
        pub client_secret: String,
    }
}
pub use config::{ConnectOptions, CredentialOptions};

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading azure blob connection options")
    }
}
