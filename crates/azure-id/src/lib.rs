#![doc = include_str!("../README.md")]

mod identity;

use std::collections::BTreeMap;
use std::env::{self, VarError};
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Context, Result};
use azure_identity::{ManagedIdentityCredential, ManagedIdentityCredentialOptions, UserAssignedId};
use omnia::Connect;
use tracing::instrument;

/// Azure Identity backend client, authenticating via Managed Identity.
///
/// It serves the identities its [`ConnectOptions`] declare and refuses every
/// other name. Each identity keeps one credential, which caches the tokens it
/// issues. Connecting fails on a host whose managed-identity source
/// [`azure_identity`] does not support, such as Azure Arc or Cloud Shell.
#[derive(Clone)]
pub struct Client {
    credentials: Arc<BTreeMap<String, Arc<ManagedIdentityCredential>>>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Connect for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let mut credentials = BTreeMap::new();
        for (name, client_id) in options.identities {
            let credential_options = ManagedIdentityCredentialOptions {
                user_assigned_id: client_id.map(UserAssignedId::ClientId),
                ..Default::default()
            };
            let credential = ManagedIdentityCredential::new(Some(credential_options))
                .with_context(|| format!("could not create a credential for identity `{name}`"))?;
            credentials.insert(name, credential);
        }

        Ok(Self {
            credentials: Arc::new(credentials),
        })
    }
}

const DEFAULT: &str = "default";

/// The managed identities a [`Client`] serves, by the name a guest asks for.
///
/// [`FromEnv`](omnia::FromEnv) reads them from the environment:
///
/// - `IDENTITY_NAMES` lists the names, comma-separated, and is `default` alone
///   when unset.
/// - `IDENTITY_CLIENT_ID` is the client id of the user-assigned identity
///   behind `default`. Unset, `default` is the system-assigned identity.
/// - `IDENTITY_CLIENT_ID__<NAME>` is the client id behind each other listed
///   name, `<NAME>` the name in ASCII uppercase. Every such name needs one.
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// The client id of the user-assigned identity behind each name, or `None`
    /// for the system-assigned identity.
    pub identities: BTreeMap<String, Option<String>>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            identities: BTreeMap::from([(DEFAULT.to_owned(), None)]),
        }
    }
}

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        let names = var("IDENTITY_NAMES")?.unwrap_or_else(|| DEFAULT.to_owned());

        let mut identities = BTreeMap::new();
        for name in names.split(',').map(str::trim).filter(|name| !name.is_empty()) {
            let client_id = if name == DEFAULT {
                var("IDENTITY_CLIENT_ID")?
            } else {
                let key = format!("IDENTITY_CLIENT_ID__{}", name.to_ascii_uppercase());
                let client_id =
                    var(&key)?.with_context(|| format!("missing `{key}` for identity `{name}`"))?;
                Some(client_id)
            };
            identities.insert(name.to_owned(), client_id);
        }

        Ok(Self { identities })
    }
}

fn var(key: &str) -> Result<Option<String>> {
    match env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("reading `{key}`")),
    }
}
