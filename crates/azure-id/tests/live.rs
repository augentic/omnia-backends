//! Live token acquisition for the Azure Identity backend, driven through the
//! `omnia:identity` contract (`IdentityClient`).
//!
//! `#[ignore]`d so it never touches the network in CI. Run on an Azure host
//! with a managed identity, setting `IDENTITY_CLIENT_ID` when it is
//! user-assigned: `cargo nextest run -p omnia-azure-id --run-ignored all`.

use anyhow::Result;
use omnia::Connect;
use omnia_azure_id::Client;
use omnia_wasi_identity::{Identity, IdentityClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an Azure host with a managed identity, and IDENTITY_CLIENT_ID for a user-assigned one; run with --run-ignored"]
async fn get_token() -> Result<()> {
    let client = <Client as Connect>::connect().await?;
    let identity: std::sync::Arc<dyn Identity> = client.get_identity("default".to_owned()).await?;

    let token =
        identity.get_token(vec!["https://management.azure.com/.default".to_owned()]).await?;
    assert!(!token.token.is_empty(), "a non-empty access token is issued");
    Ok(())
}
