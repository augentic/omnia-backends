//! Live token acquisition for the Azure Identity backend, driven through the
//! `omnia:identity` contract (`IdentityClient`).
//!
//! `#[ignore]`d so it never touches the network in CI. Run in an environment
//! with an ambient Azure managed-identity credential:
//! `cargo nextest run -p omnia-azure-id --run-ignored all`.

use anyhow::Result;
use omnia::Connect;
use omnia_azure_id::Client;
use omnia_wasi_identity::{Identity, IdentityClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an ambient Azure credential; run with --run-ignored"]
async fn get_token() -> Result<()> {
    let client = <Client as Connect>::connect().await?;
    let identity: std::sync::Arc<dyn Identity> =
        client.get_identity("omnia-live".to_owned()).await?;

    let token =
        identity.get_token(vec!["https://management.azure.com/.default".to_owned()]).await?;
    assert!(!token.token.is_empty(), "a non-empty access token is issued");
    Ok(())
}
