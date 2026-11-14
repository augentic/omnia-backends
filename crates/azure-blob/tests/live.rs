//! Live `wasi:blobstore` for the Azure Blob Storage backend: the
//! `omnia_test::contract::blob` rows from a real guest, and the ranged reads
//! and package store no guest row reaches.
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a real
//! storage account (`AZURE_BLOB_ENDPOINT` plus credentials, or Azurite):
//! `cargo nextest run -p omnia-azure-blob --all-features --run-ignored all`.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use omnia::{Connect, ExitStatus};
use omnia_azure_blob::Client;
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_blobstore::{BlobStoreClient, Container, WasiBlobStore};

// Every blobstore program in `crates/test-programs` must have a matching
// test here; a new program without one fails to compile.
test_programs::foreach_blobstore!();

// Azure holds a deleted container's name for a while, so each run scopes the
// rows' containers by its process id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an Azure Blob endpoint (AZURE_BLOB_ENDPOINT); run with --run-ignored"]
async fn blobstore_blob_store() -> Result<()> {
    let client = <Client as Connect>::connect().await?;
    let scope = format!("omnia-live-{}", std::process::id());
    let backends = Backends::defaults().await.blobstore(client.clone());

    let status = Deployment::new()
        .guest("guest", test_programs::BLOBSTORE_BLOB_STORE)
        .args([scope.as_str()])
        .run_host::<WasiBlobStore, _>(backends.clone())
        .await?;
    assert_eq!(status, ExitStatus::SUCCESS, "guest failed");

    // the guest's bytes are stored as they are
    let host = format!("{scope}-host");
    assert_eq!(backends.object(&host, "raw").await, Some(b"raw".to_vec()));
    client.delete_container(host).await
}

// Live counterpart of the `range_options` unit cases: the service must honor
// the HTTP `Range` each (start, end) pair translates to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an Azure Blob endpoint (AZURE_BLOB_ENDPOINT); run with --run-ignored"]
async fn ranged_reads() -> Result<()> {
    let client = <Client as Connect>::connect().await?;

    let container = format!("omnia-live-range-{}", std::process::id());
    let store: std::sync::Arc<dyn Container> = client.create_container(container.clone()).await?;

    let object = "ranged".to_owned();
    let payload = b"0123456789abcdefghij"; // 20 bytes
    store.write_data(object.clone(), payload.to_vec().into()).await?;

    // (start, end, expected slice); end is inclusive, 0/u64::MAX mean unbounded.
    let cases: &[(u64, u64, &[u8])] = &[
        (0, 0, payload),               // full read
        (0, u64::MAX, payload),        // full read
        (10, 14, b"abcde"),            // bounded, inclusive end
        (5, 5, b"5"),                  // single byte
        (10, 0, b"abcdefghij"),        // offset with unbounded end
        (10, u64::MAX, b"abcdefghij"), // offset with unbounded end
        (15, 1_000, b"fghij"),         // end past EOF clamps to object size
    ];
    for (start, end, expected) in cases {
        let data = store.get_data(object.clone(), *start, *end).await?;
        assert_eq!(
            data.as_deref(),
            Some(*expected),
            "get_data({start}, {end}) returns the ranged slice"
        );
    }

    let err = store
        .get_data(object.clone(), 10, 5)
        .await
        .expect_err("inverted range is rejected before hitting the service");
    assert!(err.to_string().contains("end (5) < start (10)"), "rejection surfaces: {err}");

    store.delete_object(object).await?;
    client.delete_container(container).await?;
    Ok(())
}

// Package-store round-trip over the dedicated `omnia-plugins` container: a
// release by reference, a second write leaving the first in place, an absent
// reference answering none, and disjointness from a guest container named
// `plugins`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an Azure Blob endpoint (AZURE_BLOB_ENDPOINT); run with --run-ignored"]
async fn package_store() -> Result<()> {
    use omnia_plugin::{PackageStore, Reference};

    let client = <Client as Connect>::connect().await?;

    // a reference of this run's own, so a rerun never meets a stale blob
    let reference: Reference = format!("live:component-{}@1.2.3", std::process::id()).parse()?;
    let bytes = format!("component-{}", std::process::id()).into_bytes();

    assert_eq!(client.get(&reference).await?, None, "nothing stored yet");
    client.put(&reference, &bytes).await?;
    assert_eq!(client.get(&reference).await?.as_deref(), Some(bytes.as_slice()));

    // A stored release is final: a second write leaves the first.
    client.put(&reference, b"other bytes").await?;
    assert_eq!(client.get(&reference).await?.as_deref(), Some(bytes.as_slice()));
    assert!(client.describe(&reference).contains(&reference.file_name()));

    // A guest container named `plugins` shares nothing with the store's
    // own `omnia-plugins` container.
    let guest: std::sync::Arc<dyn Container> =
        client.create_container("plugins".to_string()).await?;
    let names = guest.list_objects().await?;
    assert!(
        !names.iter().any(|name| name == &reference.file_name()),
        "guest container must not see stored packages: {names:?}"
    );
    client.delete_container("plugins".to_string()).await?;

    delete_stored(&reference).await
}

// The blobstore view refuses the store's container (`tests/blobstore.rs`),
// so cleanup reaches the blob through the SDK under the same connection
// options the client connected with.
async fn delete_stored(reference: &omnia_plugin::Reference) -> Result<()> {
    use std::sync::Arc;

    use azure_core::credentials::{Secret, TokenCredential};
    use azure_identity::{ClientSecretCredential, DeveloperToolsCredential};
    use azure_storage_blob::BlobServiceClient;
    use omnia::FromEnv as _;
    use omnia_azure_blob::ConnectOptions;

    let options = ConnectOptions::load_env()?;
    let credential: Arc<dyn TokenCredential> = match &options.credential {
        Some(cred) => ClientSecretCredential::new(
            &cred.tenant_id,
            cred.client_id.clone(),
            Secret::new(cred.client_secret.clone()),
            None,
        )?,
        None => DeveloperToolsCredential::new(None)?,
    };
    let service = BlobServiceClient::new(options.endpoint.parse()?, Some(credential), None)?;
    service.blob_client("omnia-plugins", &reference.file_name()).delete(None).await?;
    Ok(())
}
