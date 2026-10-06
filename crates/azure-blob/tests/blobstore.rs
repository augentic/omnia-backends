//! Service-free rows of the `omnia:blobstore` host boundary (`WasiBlobstoreCtx`).
//!
//! Connecting with service-principal options reaches no network, so what the
//! view refuses before any request is asserted here; the round-trips the
//! service must answer live in `tests/live.rs`.

use anyhow::Result;
use omnia::Backend;
use omnia_azure_blob::{Client, ConnectOptions, CredentialOptions};
use omnia_wasi_blobstore::WasiBlobstoreCtx;

async fn connect() -> Result<Client> {
    Client::connect_with(ConnectOptions {
        endpoint: "https://account.blob.core.windows.net/".to_owned(),
        credential: Some(CredentialOptions {
            tenant_id: "00000000-0000-0000-0000-000000000000".to_owned(),
            client_id: "client".to_owned(),
            client_secret: "secret".to_owned(),
        }),
    })
    .await
}

// The package store's container is refused by name on every entry point, so
// no guest handle on it exists to plant, replace, or delete a release.
#[tokio::test]
async fn reserved_container() -> Result<()> {
    let client = connect().await?;
    let reserved = || "omnia-plugins".to_owned();

    let refused = |err: anyhow::Error| {
        assert!(
            err.to_string().contains("reserved for the package store"),
            "refusal names the store: {err}"
        );
    };
    refused(client.create_container(reserved()).await.expect_err("create is refused"));
    refused(client.get_container(reserved()).await.expect_err("get is refused"));
    refused(client.delete_container(reserved()).await.expect_err("delete is refused"));
    refused(client.container_exists(reserved()).await.expect_err("exists is refused"));

    // a guest container of a neighbouring name is opened as any other
    client.get_container("plugins".to_owned()).await?;
    Ok(())
}
