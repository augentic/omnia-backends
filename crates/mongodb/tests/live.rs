//! Live `wasi:blobstore` for the MongoDB backend, where containers map to
//! collections: the `omnia_test::contract::blob` rows from a real guest, and
//! the wire ranges no guest row reaches.
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a reachable
//! MongoDB (`MONGODB_URL`, including a default database):
//! `cargo nextest run -p omnia-mongodb --all-features --run-ignored all`.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use omnia::{Connect, ExitStatus};
use omnia_mongodb::Client;
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_blobstore::{BlobStoreClient, Container, WasiBlobStore};

// Every blobstore program in `crates/test-programs` must have a matching
// test here; a new program without one fails to compile.
test_programs::foreach_blobstore!();

// A failed run leaves its containers behind, so each run scopes the rows'
// containers by its process id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable MongoDB (MONGODB_URL); run with --run-ignored"]
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

// The wire range a guest's `BlobStore` never sends: an end of 0 for
// unbounded, and an inverted range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable MongoDB (MONGODB_URL); run with --run-ignored"]
async fn ranged_reads() -> Result<()> {
    let client = <Client as Connect>::connect().await?;

    let container = format!("omnia-live-range-{}", std::process::id());
    let store: std::sync::Arc<dyn Container> = client.create_container(container.clone()).await?;

    let object = "greeting".to_owned();
    store.write_data(object.clone(), b"payload".to_vec().into()).await?;

    assert_eq!(store.get_data(object.clone(), 0, 0).await?.as_deref(), Some(b"payload".as_slice()));
    assert_eq!(store.get_data(object.clone(), 0, 2).await?.as_deref(), Some(b"pay".as_slice()));
    assert_eq!(store.get_data(object.clone(), 3, 0).await?.as_deref(), Some(b"load".as_slice()));
    assert!(store.get_data(object, 5, 2).await.is_err(), "inverted range is rejected");

    client.delete_container(container).await
}
