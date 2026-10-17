#![doc = include_str!("../README.md")]

mod key_vault;

use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Context, Result};
use azure_core::credentials::{Secret, TokenCredential};
use azure_identity::{ClientSecretCredential, DeveloperToolsCredential};
use azure_security_keyvault_secrets::SecretClient;
use omnia::Backend;
use tracing::instrument;

/// Azure Key Vault secrets backend client.
#[derive(Clone)]
pub struct Client {
    key_vault: Option<Arc<SecretClient>>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
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
                Secret::new(cred.client_secret.clone()),
                None,
            )?
        } else {
            DeveloperToolsCredential::new(None).context("could not create credential")?
        };
        tracing::info!("connected to azure api");

        let Some(url) = &options.keyvault_url else {
            tracing::info!("no azure keyvault url provided");
            return Ok(Self { key_vault: None });
        };

        let client = SecretClient::new(url, Arc::clone(&credential), None)
            .context("failed to connect to azure keyvault")?;
        tracing::info!("connected to azure keyvault");

        Ok(Self {
            key_vault: Some(Arc::new(client)),
        })
    }
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use std::fmt;

    use fromenv::FromEnv;

    /// Connection options for the Azure Key Vault backend.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Optional service principal credentials (falls back to developer tools).
        #[env(nested)]
        pub credential: Option<CredentialOptions>,
        /// Key Vault URL. When absent, the vault is disabled.
        #[env(from = "AZURE_KEYVAULT_URL")]
        pub keyvault_url: Option<String>,
    }

    /// Azure service principal credential fields.
    #[derive(Clone, FromEnv)]
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

    impl fmt::Debug for CredentialOptions {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("CredentialOptions")
                .field("tenant_id", &self.tenant_id)
                .field("client_id", &self.client_id)
                .field("client_secret", &"<redacted>")
                .finish()
        }
    }
}
pub use config::{ConnectOptions, CredentialOptions};

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
