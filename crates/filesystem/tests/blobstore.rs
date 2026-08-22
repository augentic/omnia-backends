//! Filesystem blobstore contract: container lifecycle, atomic
//! write-once visibility, nested object names, range reads, and name
//! sanitization.

use std::sync::Arc;

use omnia_wasi_blobstore::{Bytes, Container, WasiBlobstoreCtx};
use tempfile::TempDir;

fn client(root: &TempDir) -> omnia_filesystem::Client {
    omnia_filesystem::Client::open(root.path()).expect("open")
}

async fn snapshots(client: &omnia_filesystem::Client) -> Arc<dyn Container> {
    client.create_container("snapshots".to_string()).await.expect("create container")
}

#[tokio::test]
async fn container_lifecycle() {
    let root = TempDir::new().expect("tempdir");
    let client = client(&root);

    assert!(!client.container_exists("snapshots".to_string()).await.expect("exists"));
    let container = snapshots(&client).await;
    assert_eq!(container.name().expect("name"), "snapshots");
    assert!(client.container_exists("snapshots".to_string()).await.expect("exists"));

    // Idempotent creation preserves contents.
    container.write_data("obj".to_string(), Bytes::from_static(b"x")).await.expect("write");
    let again = snapshots(&client).await;
    assert!(again.has_object("obj".to_string()).await.expect("has"));

    // Get refuses an absent container; delete is idempotent.
    client.get_container("snapshots".to_string()).await.expect("get");
    client.delete_container("snapshots".to_string()).await.expect("delete");
    client.delete_container("snapshots".to_string()).await.expect("delete again");
    assert!(!client.container_exists("snapshots".to_string()).await.expect("exists"));
    let err = client.get_container("snapshots".to_string()).await.expect_err("must refuse");
    assert!(err.to_string().contains("not found"), "unexpected error: {err}");
}

#[tokio::test]
async fn write_read_round_trip() {
    let root = TempDir::new().expect("tempdir");
    let client = client(&root);
    let container = snapshots(&client).await;

    let payload = Bytes::from_static(&[0_u8, 159, 146, 150, 255]);
    container.write_data("ab/cdef".to_string(), payload.clone()).await.expect("write");

    // Nested names shard into subdirectories and read back whole.
    let data = container.get_data("ab/cdef".to_string(), 0, u64::MAX).await.expect("get");
    assert_eq!(data, Some(payload.clone()));

    // Overwrite replaces atomically (last rename wins).
    container
        .write_data("ab/cdef".to_string(), Bytes::from_static(b"second"))
        .await
        .expect("rewrite");
    let data = container.get_data("ab/cdef".to_string(), 0, u64::MAX).await.expect("get");
    assert_eq!(data, Some(Bytes::from_static(b"second")));

    let info = container.object_info("ab/cdef".to_string()).await.expect("info");
    assert_eq!(info.size, 6);
    assert_eq!(info.container, "snapshots");

    // Absent objects read as `None`; info refuses them.
    assert_eq!(container.get_data("missing".to_string(), 0, 0).await.expect("get"), None);
    let err = container.object_info("missing".to_string()).await.expect_err("must refuse");
    assert!(err.to_string().contains("not found"), "unexpected error: {err}");
}

#[tokio::test]
async fn range_reads() {
    let root = TempDir::new().expect("tempdir");
    let client = client(&root);
    let container = snapshots(&client).await;
    container
        .write_data("obj".to_string(), Bytes::from_static(b"0123456789"))
        .await
        .expect("write");

    // `end` is inclusive; 0 and u64::MAX read to the end; bounds clamp.
    let slice = container.get_data("obj".to_string(), 2, 4).await.expect("get");
    assert_eq!(slice, Some(Bytes::from_static(b"234")));
    let tail = container.get_data("obj".to_string(), 8, u64::MAX).await.expect("get");
    assert_eq!(tail, Some(Bytes::from_static(b"89")));
    let all = container.get_data("obj".to_string(), 0, 0).await.expect("get");
    assert_eq!(all, Some(Bytes::from_static(b"0123456789")));
    let clamped = container.get_data("obj".to_string(), 4, 400).await.expect("get");
    assert_eq!(clamped, Some(Bytes::from_static(b"456789")));
    let err = container.get_data("obj".to_string(), 4, 2).await.expect_err("must refuse");
    assert!(err.to_string().contains("invalid byte range"), "unexpected error: {err}");
}

#[tokio::test]
async fn list_and_delete() {
    let root = TempDir::new().expect("tempdir");
    let client = client(&root);
    let container = snapshots(&client).await;

    for name in ["a", "b/c", "b/d/e"] {
        container.write_data(name.to_string(), Bytes::from_static(b"x")).await.expect("write");
    }
    let mut names = container.list_objects().await.expect("list");
    names.sort();
    assert_eq!(names, ["a", "b/c", "b/d/e"]);

    container.delete_object("b/c".to_string()).await.expect("delete");
    container.delete_object("b/c".to_string()).await.expect("delete again");
    assert!(!container.has_object("b/c".to_string()).await.expect("has"));
    assert!(container.has_object("b/d/e".to_string()).await.expect("has"));
}

#[tokio::test]
async fn names_sanitized() {
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
