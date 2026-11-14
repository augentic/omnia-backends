//! Live secret round-trip for the Azure Key Vault backend, driven through the
//! `omnia:vault` contract (`VaultClient`).
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a real vault
//! (`AZURE_KEYVAULT_URL` plus service-principal credentials):
//! `cargo nextest run -p omnia-azure-vault --run-ignored all`.

use anyhow::Result;
use omnia::Connect;
use omnia_azure_vault::Client;
use omnia_wasi_vault::{Locker, VaultClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an Azure Key Vault (AZURE_KEYVAULT_URL); run with --run-ignored"]
async fn set_get_delete() -> Result<()> {
    let client = <Client as Connect>::connect().await?;
    let locker: std::sync::Arc<dyn Locker> = client.open_locker("omnia-live".to_owned()).await?;

    // Key Vault secret names are alphanumeric + dashes.
    let secret = format!("omnia-live-{}", std::process::id());
    locker.set(secret.clone(), b"s3cr3t".to_vec()).await?;
    assert_eq!(locker.get(secret.clone()).await?.as_deref(), Some(b"s3cr3t".as_slice()));
    assert!(locker.list_ids().await?.contains(&secret));

    let other: std::sync::Arc<dyn Locker> = client.open_locker("omnia".to_owned()).await?;
    assert_eq!(other.get(secret.clone()).await?, None, "prefix locker does not see secret");

    locker.delete(secret.clone()).await?;
    locker.delete(secret).await?;
    Ok(())
}
