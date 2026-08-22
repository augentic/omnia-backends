//! Live key-value round-trip for the Redis backend, driven through the
//! `omnia:keyvalue` host boundary (`WasiKeyValueCtx`).
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a reachable
//! Redis (`REDIS_URL`, default `redis://localhost:6379`):
//! `cargo nextest run -p omnia-redis --run-ignored all`.

use std::sync::Arc;

use anyhow::Result;
use omnia::Backend;
use omnia_redis::Client;
use omnia_wasi_keyvalue::{Bucket, Cas, WasiKeyValueCtx};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Redis; run with --run-ignored"]
async fn set_get_delete() -> Result<()> {
    let client = <Client as Backend>::connect().await?;
    let bucket: Arc<dyn Bucket> = client.open_bucket("omnia-live".to_owned()).await?;

    let key = unique("k");
    bucket.set(key.clone(), b"payload".to_vec()).await?;
    assert_eq!(bucket.get(key.clone()).await?.as_deref(), Some(b"payload".as_slice()));
    assert!(bucket.exists(key.clone()).await?, "key exists after set");

    bucket.delete(key.clone()).await?;
    assert!(!bucket.exists(key).await?, "key gone after delete");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Redis; run with --run-ignored"]
async fn atomics_swap_and_increment() -> Result<()> {
    let client = <Client as Backend>::connect().await?;
    let bucket: Arc<dyn Bucket> = client.open_bucket("omnia-live".to_owned()).await?;

    // Increment is native `INCRBY` plus the same one-day expiry as `set`.
    let counter = unique("counter");
    assert_eq!(bucket.increment(counter.clone(), 5).await?, 5);
    assert_eq!(bucket.increment(counter, -2).await?, 3);

    // Absent-expected swap creates the key.
    let pointer = unique("pointer");
    let handle = Cas {
        bucket: Arc::clone(&bucket),
        key: pointer.clone(),
        current: None,
    };
    bucket.swap(handle, b"first".to_vec()).await?.expect("absent-expected commits");

    // A stale snapshot refuses, leaves the value, and refreshes the handle.
    let stale = Cas {
        bucket: Arc::clone(&bucket),
        key: pointer.clone(),
        current: None,
    };
    let fresh = bucket.swap(stale, b"lost".to_vec()).await?.expect_err("stale must mismatch");
    assert_eq!(fresh.current.as_deref(), Some(b"first".as_slice()));
    assert_eq!(bucket.get(pointer.clone()).await?.as_deref(), Some(b"first".as_slice()));

    // The refreshed handle retries cleanly.
    bucket.swap(fresh, b"second".to_vec()).await?.expect("retry commits");
    assert_eq!(bucket.get(pointer).await?.as_deref(), Some(b"second".as_slice()));
    Ok(())
}

/// A collision-resistant suffix so parallel runs never share a live key.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}
