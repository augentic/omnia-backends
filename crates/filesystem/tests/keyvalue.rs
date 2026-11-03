//! The filesystem backend at the `omnia:keyvalue` contract boundary
//! (`KeyValueClient`): round trips, CAS under contention, counters and
//! their encoding, and a bucket's tree kept apart from a same-named
//! container's, against a temporary directory. No service, so it runs in
//! CI.

use std::sync::Arc;

use bytes::Bytes;
use omnia_backend::blobstore::BlobStoreClient;
use omnia_backend::keyvalue::{Bucket, KeyValueClient};
use omnia_filesystem::Client;
use tempfile::TempDir;

fn client(root: &TempDir) -> Client {
    Client::open(root.path()).expect("open")
}

async fn state(client: &Client) -> Arc<dyn Bucket> {
    client.open_bucket("state".to_string()).await.expect("open bucket")
}

#[tokio::test]
async fn round_trip() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;

    assert_eq!(bucket.get("missing".to_string()).await.expect("get"), None);
    bucket.set("a".to_string(), b"one".to_vec(), None).await.expect("set");
    bucket.set("b/c".to_string(), b"two".to_vec(), None).await.expect("set");
    assert_eq!(bucket.get("a".to_string()).await.expect("get").as_deref(), Some(b"one".as_slice()));
    assert!(bucket.exists("b/c".to_string()).await.expect("exists"));

    let mut keys = bucket.keys().await.expect("keys");
    keys.sort();
    assert_eq!(keys, ["a", "b/c"]);

    bucket.delete("a".to_string()).await.expect("delete");
    bucket.delete("a".to_string()).await.expect("delete again");
    assert!(!bucket.exists("a".to_string()).await.expect("exists"));

    drop(bucket);
    drop(store);
    let bucket = state(&client(&root)).await;
    assert_eq!(
        bucket.get("b/c".to_string()).await.expect("get").as_deref(),
        Some(b"two".as_slice())
    );
}

#[tokio::test]
async fn same_named_disjoint() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;
    let container = store.create_container("state".to_string()).await.expect("create container");

    bucket.set("entry".to_string(), b"kv".to_vec(), None).await.expect("set");
    container.write_data("entry".to_string(), Bytes::from_static(b"blob")).await.expect("write");

    assert_eq!(
        bucket.get("entry".to_string()).await.expect("get").as_deref(),
        Some(b"kv".as_slice())
    );
    assert_eq!(
        container.get_data("entry".to_string(), 0, 0).await.expect("get"),
        Some(Bytes::from_static(b"blob"))
    );
    assert_eq!(bucket.keys().await.expect("keys"), ["entry"]);
    assert_eq!(container.list_objects().await.expect("list"), ["entry"]);
}

#[tokio::test]
async fn names_sanitized() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;

    for key in ["", ".", "..", "../escape", "/abs", "a//b", "a/../b", "a\\b"] {
        let err = bucket.set(key.to_string(), b"x".to_vec(), None).await.expect_err("must refuse");
        assert!(err.to_string().contains("bucket-relative"), "`{key}`: {err}");
    }
    for name in ["", "..", "a/b", "/abs"] {
        let err = store.open_bucket(name.to_string()).await.expect_err("must refuse");
        assert!(err.to_string().contains("plain directory name"), "`{name}`: {err}");
    }
}

#[tokio::test]
async fn cas_absent_expected() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;

    bucket
        .cas("pointer".to_string(), None, b"first".to_vec())
        .await
        .expect("cas")
        .expect("must commit");
    assert_eq!(
        bucket.get("pointer".to_string()).await.expect("get").as_deref(),
        Some(b"first".as_slice())
    );

    // A repeated absent-expected CAS returns the current value.
    let observed = bucket
        .cas("pointer".to_string(), None, b"second".to_vec())
        .await
        .expect("cas")
        .expect_err("must mismatch");
    assert_eq!(observed.as_deref(), Some(b"first".as_slice()));
    assert_eq!(
        bucket.get("pointer".to_string()).await.expect("get").as_deref(),
        Some(b"first".as_slice())
    );
}

#[tokio::test]
async fn cas_stale_expected() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;

    bucket.set("pointer".to_string(), b"v1".to_vec(), None).await.expect("seed");
    bucket.set("pointer".to_string(), b"v2".to_vec(), None).await.expect("interfere");

    let observed = bucket
        .cas("pointer".to_string(), Some(b"v1".to_vec()), b"lost".to_vec())
        .await
        .expect("cas")
        .expect_err("must mismatch");
    assert_eq!(observed.as_deref(), Some(b"v2".as_slice()), "returns observed value");
    assert_eq!(
        bucket.get("pointer".to_string()).await.expect("get").as_deref(),
        Some(b"v2".as_slice()),
        "stale swap must not overwrite"
    );

    bucket
        .cas("pointer".to_string(), observed, b"v3".to_vec())
        .await
        .expect("cas")
        .expect("retry commits");
    assert_eq!(
        bucket.get("pointer".to_string()).await.expect("get").as_deref(),
        Some(b"v3".as_slice())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cas_contention() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;
    bucket.set("pointer".to_string(), b"seed".to_vec(), None).await.expect("seed");

    // Exactly one contender may swap from the shared snapshot.
    let mut attempts = tokio::task::JoinSet::new();
    for i in 0..16_u8 {
        let bucket = Arc::clone(&bucket);
        attempts.spawn(async move {
            let won = bucket
                .cas("pointer".to_string(), Some(b"seed".to_vec()), vec![i])
                .await
                .expect("cas")
                .is_ok();
            (i, won)
        });
    }
    let results = attempts.join_all().await;

    let winners: Vec<u8> = results.iter().filter(|(_, won)| *won).map(|(i, _)| *i).collect();
    assert_eq!(winners.len(), 1, "exactly one swap wins: {results:?}");
    assert_eq!(
        bucket.get("pointer".to_string()).await.expect("get"),
        Some(vec![winners[0]]),
        "final value is the winner's"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn increment_contention() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let bucket = Arc::clone(&bucket);
        tasks.spawn(
            async move { bucket.increment("counter".to_string(), 1).await.expect("increment") },
        );
    }
    let mut observed = tasks.join_all().await;
    observed.sort_unstable();
    assert_eq!(observed, (1..=32).collect::<Vec<i64>>(), "every increment lands exactly once");

    let stored = bucket.get("counter".to_string()).await.expect("get").expect("counter exists");
    assert_eq!(stored, 32_i64.to_be_bytes());
}

#[tokio::test]
async fn increment_encoding() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;

    assert_eq!(bucket.increment("counter".to_string(), 5).await.expect("increment"), 5);
    assert_eq!(bucket.increment("counter".to_string(), -2).await.expect("increment"), 3);

    bucket.set("text".to_string(), b"not a number".to_vec(), None).await.expect("set");
    let err = bucket.increment("text".to_string(), 1).await.expect_err("must refuse");
    assert!(err.to_string().contains("big-endian"), "unexpected error: {err}");
}
