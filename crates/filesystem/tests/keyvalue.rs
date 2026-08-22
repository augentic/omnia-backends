//! Filesystem keyvalue contract: bucket round-trip, durability across
//! reopen, shared-root isolation from the blobstore, name sanitization, and
//! the CAS exit criterion — absent-expected, stale-expected, and contention
//! under the per-key lock.

use std::sync::Arc;

use omnia_filesystem::Client;
use omnia_wasi_blobstore::{Bytes, WasiBlobstoreCtx};
use omnia_wasi_keyvalue::{Bucket, Cas, WasiKeyValueCtx};
use tempfile::TempDir;

fn client(root: &TempDir) -> Client {
    Client::open(root.path()).expect("open")
}

async fn state(client: &Client) -> Arc<dyn Bucket> {
    client.open_bucket("state".to_string()).await.expect("open bucket")
}

fn cas(bucket: &Arc<dyn Bucket>, key: &str, current: Option<&[u8]>) -> Cas {
    Cas {
        bucket: Arc::clone(bucket),
        key: key.to_string(),
        current: current.map(<[u8]>::to_vec),
    }
}

#[tokio::test]
async fn round_trip() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;

    assert_eq!(bucket.get("missing".to_string()).await.expect("get"), None);
    bucket.set("a".to_string(), b"one".to_vec()).await.expect("set");
    bucket.set("b/c".to_string(), b"two".to_vec()).await.expect("set");
    assert_eq!(bucket.get("a".to_string()).await.expect("get").as_deref(), Some(b"one".as_slice()));
    assert!(bucket.exists("b/c".to_string()).await.expect("exists"));

    let mut keys = bucket.keys().await.expect("keys");
    keys.sort();
    assert_eq!(keys, ["a", "b/c"]);

    // Delete is idempotent.
    bucket.delete("a".to_string()).await.expect("delete");
    bucket.delete("a".to_string()).await.expect("delete again");
    assert!(!bucket.exists("a".to_string()).await.expect("exists"));

    // Durability: a fresh client over the same root still sees the write.
    drop(bucket);
    drop(store);
    let bucket = state(&client(&root)).await;
    assert_eq!(
        bucket.get("b/c".to_string()).await.expect("get").as_deref(),
        Some(b"two".as_slice())
    );
}

#[tokio::test]
async fn same_named_bucket_and_container_disjoint() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);
    let bucket = state(&store).await;
    let container = store.create_container("state".to_string()).await.expect("create container");

    bucket.set("entry".to_string(), b"kv".to_vec()).await.expect("set");
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
        let err = bucket.set(key.to_string(), b"x".to_vec()).await.expect_err("must refuse");
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

    // `current: None` on an absent key creates it.
    let handle = cas(&bucket, "pointer", None);
    bucket.swap(handle, b"first".to_vec()).await.expect("swap").expect("must commit");
    assert_eq!(
        bucket.get("pointer".to_string()).await.expect("get").as_deref(),
        Some(b"first".as_slice())
    );

    // A second absent-expected swap mismatches, reports the observed value,
    // and leaves the key untouched.
    let stale = cas(&bucket, "pointer", None);
    let fresh =
        bucket.swap(stale, b"second".to_vec()).await.expect("swap").expect_err("must mismatch");
    assert_eq!(fresh.current.as_deref(), Some(b"first".as_slice()));
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

    bucket.set("pointer".to_string(), b"v1".to_vec()).await.expect("seed");
    let handle = cas(&bucket, "pointer", Some(b"v1"));

    // An interfering write invalidates the snapshot.
    bucket.set("pointer".to_string(), b"v2".to_vec()).await.expect("interfere");

    let fresh =
        bucket.swap(handle, b"lost".to_vec()).await.expect("swap").expect_err("must mismatch");
    assert_eq!(fresh.current.as_deref(), Some(b"v2".as_slice()), "refreshed at observed value");
    assert_eq!(
        bucket.get("pointer".to_string()).await.expect("get").as_deref(),
        Some(b"v2".as_slice()),
        "stale swap must not overwrite"
    );

    // The refreshed handle retries cleanly.
    bucket.swap(fresh, b"v3".to_vec()).await.expect("swap").expect("retry commits");
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
    bucket.set("pointer".to_string(), b"seed".to_vec()).await.expect("seed");

    // Every contender swaps from the same snapshot; the per-key lock must
    // admit exactly one.
    let mut attempts = tokio::task::JoinSet::new();
    for i in 0..16_u8 {
        let bucket = Arc::clone(&bucket);
        attempts.spawn(async move {
            let handle = Cas {
                bucket: Arc::clone(&bucket),
                key: "pointer".to_string(),
                current: Some(b"seed".to_vec()),
            };
            let won = bucket.swap(handle, vec![i]).await.expect("swap").is_ok();
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

    // Absent starts from zero; deltas may be negative.
    assert_eq!(bucket.increment("counter".to_string(), 5).await.expect("increment"), 5);
    assert_eq!(bucket.increment("counter".to_string(), -2).await.expect("increment"), 3);

    // A non-integer value refuses rather than corrupting.
    bucket.set("text".to_string(), b"not a number".to_vec()).await.expect("set");
    let err = bucket.increment("text".to_string(), 1).await.expect_err("must refuse");
    assert!(err.to_string().contains("big-endian"), "unexpected error: {err}");
}
