//! Live key-value for the Redis backend at the `omnia:keyvalue` host
//! boundary: the `omnia_test::contract::state` rows from a real guest, and
//! the round trip and atomics no guest row reaches.
//!
//! `#[ignore]`d so it never touches the network in CI. The rows empty the
//! `default` and `second` buckets first, so run against a local Redis
//! (`REDIS_URL`, default `redis://localhost:6379`):
//! `cargo nextest run -p omnia-redis --all-features --run-ignored all`.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use anyhow::Result;
use omnia::{Connect as _, ExitStatus, FromEnv as _};
use omnia_redis::{Client, ConnectOptions};
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_keyvalue::{
    Bucket, Entry, Error, KeyValueClient, Lifetime, Precondition, WasiKeyValue,
};

// Every keyvalue program in `crates/test-programs` must have a matching
// test here; a new program without one fails to compile.
test_programs::foreach_keyvalue!();

// the buckets the rows open, and the one the other tests share
async fn connect() -> Result<Client> {
    let mut options = ConnectOptions::load_env()?;
    options.buckets = ["default", "second", "omnia-live"].map(str::to_owned).to_vec();
    Client::connect_with(options).await
}

async fn empty(bucket: &Arc<dyn Bucket>) -> Result<()> {
    for key in bucket.keys().await? {
        assert_eq!(bucket.delete(key, None).await?, Ok(()), "an unconditioned delete");
    }
    Ok(())
}

async fn value(bucket: &Arc<dyn Bucket>, key: &str) -> Result<Option<Vec<u8>>> {
    Ok(bucket.get(key.to_owned()).await?.map(|entry| entry.value))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: empties the `default` and `second` buckets of a local Redis (REDIS_URL); run with --run-ignored"]
async fn keyvalue_state_store() -> Result<()> {
    let client = connect().await?;
    for name in ["default", "second"] {
        empty(&client.open_bucket(name.to_owned()).await?).await?;
    }
    let backends = Backends::defaults().await.keyvalue(client);

    let status = Deployment::new()
        .guest("guest", test_programs::KEYVALUE_STATE_STORE)
        .run_host::<WasiKeyValue, _>(backends.clone())
        .await?;
    assert_eq!(status, ExitStatus::SUCCESS, "guest failed");

    // the guest's bytes are stored as they are, and Redis expires them
    assert_eq!(backends.state("host.raw").await, Some(b"raw".to_vec()));
    assert_eq!(backends.state("host.persist").await, Some(b"kept".to_vec()));
    assert_eq!(backends.state("host.expiring").await, None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Redis (REDIS_URL); run with --run-ignored"]
async fn set_get_delete() -> Result<()> {
    let bucket = connect().await?.open_bucket("omnia-live".to_owned()).await?;

    let key = unique("k");
    let written =
        bucket.set(key.clone(), b"payload".to_vec(), Lifetime::Replace(None), None).await?;
    let entry = bucket.get(key.clone()).await?.expect("the entry set wrote");
    assert_eq!(entry.value, b"payload");
    assert_eq!(written, Ok(entry.revision), "get reads the revision set wrote");
    assert!(bucket.exists(key.clone()).await?, "key exists after set");
    assert!(bucket.keys().await?.contains(&key), "keys() returns unprefixed key");

    assert_eq!(bucket.delete(key.clone(), None).await?, Ok(()));
    assert!(!bucket.exists(key).await?, "key gone after delete");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Redis (REDIS_URL); run with --run-ignored"]
async fn atomics() -> Result<()> {
    let bucket = connect().await?.open_bucket("omnia-live".to_owned()).await?;

    // counters are native `INCRBY` decimal text
    let counter = unique("counter");
    assert_eq!(bucket.increment(counter.clone(), 5, None).await?, 5);
    assert_eq!(bucket.increment(counter.clone(), -2, None).await?, 3);
    assert_eq!(value(&bucket, &counter).await?, Some(b"3".to_vec()));

    let pointer = unique("pointer");
    let forever = Lifetime::Replace(None);
    let absent = Some(Precondition::Absent);
    let first = bucket.set(pointer.clone(), b"first".to_vec(), forever, absent.clone()).await?;
    let first = first.expect("an absent key takes the write");
    let stale = bucket.set(pointer.clone(), b"lost".to_vec(), forever, absent).await?;
    let found = Entry {
        value: b"first".to_vec(),
        revision: first.clone(),
    };
    assert_eq!(stale, Err(Some(found)));

    let matching = Some(Precondition::Matches(first.clone()));
    let second =
        bucket.set(pointer.clone(), b"second".to_vec(), Lifetime::Keep(None), matching).await?;
    let second = second.expect("the matching revision takes the write");
    let moved = bucket.delete(pointer.clone(), Some(first)).await?;
    assert!(matches!(&moved, Err(Some(entry)) if entry.value == b"second"), "{moved:?}");
    assert_eq!(bucket.delete(pointer.clone(), Some(second)).await?, Ok(()));
    assert_eq!(value(&bucket, &pointer).await?, None);

    let text = unique("text");
    bucket.set(text.clone(), b"not a number".to_vec(), forever, None).await?.expect("set");
    let err = bucket.increment(text, 1, None).await.expect_err("a non-counter increments");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "{err}");
    Ok(())
}

// A collision-resistant suffix so parallel runs never share a live key.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}
