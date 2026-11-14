//! The filesystem backend at the `omnia:blobstore` host boundary, against a
//! temporary directory. The `omnia_test::contract::blob` rows run from a
//! real guest. The rest is what no guest reaches: a write through a handle
//! whose container was deleted since, and the names the backend refuses.
//!
//! No service, so it runs in CI.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use bytes::Bytes;
use omnia::ExitStatus;
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_blobstore::{BlobStoreClient, Container, Error, WasiBlobStore};
use tempfile::TempDir;

// Every blobstore program in `crates/test-programs` must have a matching
// test here; a new program without one fails to compile.
test_programs::foreach_blobstore!();

fn client(root: &TempDir) -> omnia_filesystem::Client {
    omnia_filesystem::Client::open(root.path()).expect("open")
}

async fn snapshots(client: &omnia_filesystem::Client) -> Arc<dyn Container> {
    client.create_container("snapshots".to_string()).await.expect("create container")
}

#[tokio::test]
async fn blobstore_blob_store() {
    let root = TempDir::new().expect("tempdir");
    let backends = Backends::defaults().await.blobstore(client(&root));

    let status = Deployment::new()
        .guest("guest", test_programs::BLOBSTORE_BLOB_STORE)
        .run_host::<WasiBlobStore, _>(backends.clone())
        .await
        .expect("guest runs");
    assert_eq!(status, ExitStatus::SUCCESS, "guest failed");

    // the guest's bytes are stored as they are
    assert_eq!(backends.object("contract-host", "raw").await, Some(b"raw".to_vec()));
}

#[tokio::test]
async fn stale_handle() {
    let root = TempDir::new().expect("tempdir");
    let client = client(&root);
    let container = snapshots(&client).await;

    client.delete_container("snapshots".to_string()).await.expect("delete");

    let write = container.write_data("obj".to_string(), Bytes::from_static(b"x")).await;
    let err = write.expect_err("a write through a stale handle");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::NotFound(_))), "{err}");
    assert!(!client.container_exists("snapshots".to_string()).await.expect("exists"));
}

#[tokio::test]
async fn invalid_names() {
    let root = TempDir::new().expect("tempdir");
    let client = client(&root);
    let container = snapshots(&client).await;

    for name in ["", ".", "..", "../escape", "/abs", "a//b", "a/../b", "a\\b"] {
        let err = container
            .write_data(name.to_string(), Bytes::from_static(b"x"))
            .await
            .expect_err("must refuse");
        assert!(err.to_string().contains("container-relative"), "`{name}`: {err}");
    }
    for name in ["", "..", "a/b", "/abs"] {
        let err = client.create_container(name.to_string()).await.expect_err("must refuse");
        assert!(err.to_string().contains("plain directory name"), "`{name}`: {err}");
    }
}
