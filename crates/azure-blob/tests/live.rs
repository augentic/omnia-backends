//! Live blob round-trip for the Azure Blob Storage backend, driven through the
//! `omnia:blobstore` host boundary (`WasiBlobstoreCtx`).
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a real
//! storage account (`AZURE_BLOB_ENDPOINT` plus credentials, or Azurite):
//! `cargo nextest run -p omnia-azure-blob --run-ignored all`.

use anyhow::Result;
use omnia::Backend;
use omnia_azure_blob::Client;
use omnia_wasi_blobstore::{Container, WasiBlobstoreCtx};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an Azure Blob endpoint (AZURE_BLOB_ENDPOINT); run with --run-ignored"]
async fn write_read_delete() -> Result<()> {
    let client = <Client as Backend>::connect().await?;

    // Container names are lowercase alphanumeric + dashes, 3-63 chars.
    let container = format!("omnia-live-{}", std::process::id());
    let store: std::sync::Arc<dyn Container> = client.create_container(container.clone()).await?;

    let object = "greeting".to_owned();
    store.write_data(object.clone(), b"payload".to_vec().into()).await?;

    // (0, 0) is a full read (see `range_options_full_read_zero_zero`).
    assert_eq!(store.get_data(object.clone(), 0, 0).await?.as_deref(), Some(b"payload".as_slice()));
    assert!(store.has_object(object.clone()).await?, "object exists after write");

    // Exercise the real list/metadata mappings against the service (these
    // replace the deleted unit tests that asserted against reimplemented helpers).
    let names = store.list_objects().await?;
    assert!(names.contains(&object), "written object appears in listing: {names:?}");
    let info = store.object_info(object.clone()).await?;
    assert_eq!(info.name, object, "object name maps through get-properties");
    assert_eq!(info.size, b"payload".len() as u64, "content length maps through get-properties");

    store.delete_object(object).await?;
    client.delete_container(container).await?;
    Ok(())
}

/// Live counterpart of the `range_options` unit cases: the service must honor
/// the HTTP `Range` each (start, end) pair translates to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an Azure Blob endpoint (AZURE_BLOB_ENDPOINT); run with --run-ignored"]
async fn ranged_reads() -> Result<()> {
    let client = <Client as Backend>::connect().await?;

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

/// Plugin-store round-trip over the dedicated `omnia-plugins` container:
/// content by digest, release records per registry, and disjointness from a
/// guest container named `plugins`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an Azure Blob endpoint (AZURE_BLOB_ENDPOINT); run with --run-ignored"]
async fn plugin_store_round_trip() -> Result<()> {
    use omnia_plugin::{ContentStore, ReleaseStore, sha256_digest};

    let client = <Client as Backend>::connect().await?;

    let bytes = format!("component-{}", std::process::id()).into_bytes();
    let digest = sha256_digest(&bytes);

    // Content round-trips by digest.
    client.put_content(&digest, &bytes).await?;
    assert_eq!(client.content(&digest).await?.as_deref(), Some(bytes.as_slice()));

    // A mismatched write is refused before it reaches the service.
    let err = client.put_content(&digest, b"other bytes").await.expect_err("must refuse");
    assert!(err.to_string().contains("refusing to persist"), "unexpected error: {err}");

    // Release records are scoped per registry.
    client.put_release("omnia.host", "emery:intent", "1.2.3", &digest).await?;
    assert_eq!(client.release("omnia.host", "emery:intent", "1.2.3").await?, Some(digest.clone()));
    assert_eq!(client.release("registry.example", "emery:intent", "1.2.3").await?, None);

    // A guest container named `plugins` shares nothing with the store's
    // own `omnia-plugins` container.
    let guest: std::sync::Arc<dyn Container> =
        client.create_container("plugins".to_string()).await?;
    let names = guest.list_objects().await?;
    assert!(
        !names.iter().any(|name| name.starts_with("content/")),
        "guest container must not see plugin store blobs: {names:?}"
    );
    client.delete_container("plugins".to_string()).await?;
    Ok(())
}
