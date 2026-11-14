//! The filesystem backend at the `omnia:keyvalue` host boundary, against a
//! temporary directory. The `omnia_test::contract::state` rows run from a
//! real guest. The rest is what no guest reaches:
//!
//! - deadlines surviving a reopen;
//! - a bucket's tree kept apart from a same-named container's;
//! - CAS and counters under contention;
//! - the kinds a bad key, a bad counter, or a bad declaration fails with.
//!
//! No service, so it runs in CI.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use omnia::{Connect as _, ExitStatus};
use omnia_filesystem::{Client, ConnectOptions};
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_blobstore::{BlobStoreClient, Bytes};
use omnia_wasi_keyvalue::{Bucket, Error, KeyValueClient, WasiKeyValue};
use tempfile::TempDir;

// Every keyvalue program in `crates/test-programs` must have a matching
// test here; a new program without one fails to compile.
test_programs::foreach_keyvalue!();

async fn connect(root: &TempDir, buckets: &[&str]) -> Result<Client> {
    Client::connect_with(ConnectOptions {
        root: root.path().to_string_lossy().into_owned(),
        buckets: buckets.iter().map(|&bucket| bucket.to_owned()).collect(),
    })
    .await
}

fn open(root: &TempDir) -> Client {
    Client::open(root.path()).expect("open")
}

async fn default(client: &Client) -> Arc<dyn Bucket> {
    client.open_bucket("default".to_owned()).await.expect("open bucket")
}

#[tokio::test]
async fn keyvalue_state_store() {
    let root = TempDir::new().expect("tempdir");
    let client = connect(&root, &["default", "second"]).await.expect("connect");
    let backends = Backends::defaults().await.keyvalue(client);

    let status = Deployment::new()
        .guest("guest", test_programs::KEYVALUE_STATE_STORE)
        .run_host::<WasiKeyValue, _>(backends.clone())
        .await
        .expect("guest runs");
    assert_eq!(status, ExitStatus::SUCCESS, "guest failed");

    // the guest's bytes are stored as they are, and the backend expires them
    assert_eq!(backends.state("host.raw").await, Some(b"raw".to_vec()));
    assert_eq!(backends.state("host.persist").await, Some(b"kept".to_vec()));
    assert_eq!(backends.state("host.expiring").await, None);
}

#[tokio::test]
async fn reopen() {
    let root = TempDir::new().expect("tempdir");
    let bucket = default(&open(&root)).await;
    bucket.set("nested/key".to_owned(), b"kept".to_vec(), None).await.expect("set");
    let long = Some(Duration::from_hours(1));
    bucket.set("long".to_owned(), b"long".to_vec(), long).await.expect("set");
    let short = Some(Duration::from_millis(200));
    bucket.set("short".to_owned(), b"short".to_vec(), short).await.expect("set");
    drop(bucket);

    let bucket = default(&open(&root)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut keys = bucket.keys().await.expect("keys");
    keys.sort();
    assert_eq!(keys, ["long", "nested/key"]);
    assert_eq!(bucket.get("nested/key".to_owned()).await.expect("get"), Some(b"kept".to_vec()));
    assert_eq!(bucket.get("long".to_owned()).await.expect("get"), Some(b"long".to_vec()));
    assert_eq!(bucket.get("short".to_owned()).await.expect("get"), None);
}

#[tokio::test]
async fn shared_name() {
    let root = TempDir::new().expect("tempdir");
    let store = open(&root);
    let bucket = default(&store).await;
    let container = store.create_container("default".to_owned()).await.expect("create container");

    let ttl = Some(Duration::from_hours(1));
    bucket.set("entry".to_owned(), b"kv".to_vec(), ttl).await.expect("set");
    container.write_data("entry".to_owned(), Bytes::from_static(b"blob")).await.expect("write");

    assert_eq!(bucket.get("entry".to_owned()).await.expect("get"), Some(b"kv".to_vec()));
    assert_eq!(
        container.get_data("entry".to_owned(), 0, 0).await.expect("get"),
        Some(Bytes::from_static(b"blob"))
    );
    assert_eq!(bucket.keys().await.expect("keys"), ["entry"]);
    assert_eq!(container.list_objects().await.expect("list"), ["entry"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cas_contention() {
    let root = TempDir::new().expect("tempdir");
    let bucket = default(&open(&root)).await;
    bucket.set("pointer".to_owned(), b"seed".to_vec(), None).await.expect("seed");

    let mut attempts = tokio::task::JoinSet::new();
    for i in 0..16_u8 {
        let bucket = Arc::clone(&bucket);
        attempts.spawn(async move {
            let seed = Some(b"seed".to_vec());
            let swapped = bucket.compare_and_swap("pointer".to_owned(), seed, vec![i], None).await;
            (i, swapped.expect("compare_and_swap").is_ok())
        });
    }
    let results = attempts.join_all().await;

    let winners: Vec<u8> = results.iter().filter(|(_, won)| *won).map(|(i, _)| *i).collect();
    assert_eq!(winners.len(), 1, "exactly one swap wins: {results:?}");
    assert_eq!(bucket.get("pointer".to_owned()).await.expect("get"), Some(vec![winners[0]]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn increment_contention() {
    let root = TempDir::new().expect("tempdir");
    let bucket = default(&open(&root)).await;

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let bucket = Arc::clone(&bucket);
        tasks.spawn(async move {
            bucket.increment("counter".to_owned(), 1, None).await.expect("increment")
        });
    }
    let mut observed = tasks.join_all().await;
    observed.sort_unstable();
    assert_eq!(observed, (1..=32).collect::<Vec<i64>>(), "every increment lands exactly once");

    let stored = bucket.get("counter".to_owned()).await.expect("get");
    assert_eq!(stored, Some(32_i64.to_be_bytes().to_vec()));
}

#[tokio::test]
async fn invalid() {
    let root = TempDir::new().expect("tempdir");
    let bucket = default(&open(&root)).await;

    for key in ["", ".", "..", "../escape", "/abs", "a//b", "a/../b", "a\\b", "a:b"] {
        let err = bucket.set(key.to_owned(), b"x".to_vec(), None).await.expect_err("must refuse");
        assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "`{key}`: {err}");
    }

    bucket.set("text".to_owned(), b"not a number".to_vec(), None).await.expect("set");
    let err = bucket.increment("text".to_owned(), 1, None).await.expect_err("must refuse");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "{err}");

    bucket.increment("max".to_owned(), i64::MAX, None).await.expect("increment");
    let err = bucket.increment("max".to_owned(), 1, None).await.expect_err("must overflow");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "{err}");
}

#[tokio::test]
async fn declared() {
    let root = TempDir::new().expect("tempdir");

    // `Client::open` declares `default` alone
    let err = open(&root).open_bucket("second".to_owned()).await.expect_err("undeclared");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::NotFound(_))), "{err}");

    for name in ["", "..", "a/b", "a.b", "a:b"] {
        let err = connect(&root, &[name]).await.expect_err("must refuse");
        assert!(err.to_string().contains("bucket name"), "`{name}`: {err}");
    }
}
