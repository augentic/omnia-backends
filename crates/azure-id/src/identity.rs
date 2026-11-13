use std::fmt::Debug;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use azure_core::credentials::TokenCredential;
use azure_identity::ManagedIdentityCredential;
use futures::future::FutureExt;
use omnia::FutureResult;
use omnia_wasi_identity::{AccessToken, Error, Identity, IdentityClient};

use crate::Client;

impl IdentityClient for Client {
    fn get_identity(&self, name: String) -> FutureResult<Arc<dyn Identity>> {
        tracing::trace!("opening identity: {name}");
        let credential = self.credentials.get(&name).map(Arc::clone);

        async move {
            let Some(credential) = credential else {
                anyhow::bail!(Error::NoSuchIdentity);
            };
            let identity = AzIdentity { name, credential };
            Ok(Arc::new(identity) as Arc<dyn Identity>)
        }
        .boxed()
    }
}

struct AzIdentity {
    name: String,
    credential: Arc<ManagedIdentityCredential>,
}

impl Debug for AzIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzIdentity").field("name", &self.name).finish_non_exhaustive()
    }
}

impl Identity for AzIdentity {
    fn get_token(&self, scopes: Vec<String>) -> FutureResult<AccessToken> {
        tracing::debug!("getting token for identity: {}", self.name);
        let credential = Arc::clone(&self.credential);

        async move {
            let scope = scopes.iter().map(AsRef::as_ref).collect::<Vec<_>>();
            let access_token = credential.get_token(&scope, None).await?;

            let now_ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let expires_ts =
                u64::try_from(access_token.expires_on.unix_timestamp()).unwrap_or_default();
            let expires_in = expires_ts.saturating_sub(now_ts);

            Ok(AccessToken {
                token: access_token.token.secret().into(),
                expires_in,
            })
        }
        .boxed()
    }
}
