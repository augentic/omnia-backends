//! Live publish, key-value, and blob containers for the NATS backend, driven
//! through the `omnia:messaging`, `omnia:keyvalue`, and `wasi:blobstore`
//! host boundaries.
//!
//! Beside the `omnia_test::contract::state` and `omnia_test::contract::blob`
//! rows from a real guest, the key-value tests cover what no guest row
//! reaches:
//!
//! - the atomics, and their revision retry loop under contention;
//! - a bucket made without per-message TTL taking one when opened.
//!
//! `#[ignore]`d so it never touches the network in CI. The state rows empty
//! the `default` and `second` buckets first, so run against a local server
//! (`nats-server -js`, then `NATS_ADDR=localhost:4222`):
//! `cargo nextest run -p omnia-nats --all-features --run-ignored all`.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, ensure};
use async_nats::jetstream::{self, kv};
use omnia::{Connect, ExitStatus, FromEnv as _};
use omnia_nats::{Client, ConnectOptions};
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_blobstore::{BlobStoreClient, WasiBlobStore};
use omnia_wasi_keyvalue::{Bucket, Error, KeyValueClient, WasiKeyValue};
use omnia_wasi_messaging::{Endpoint, Message, MessagingClient};
use tokio::task::JoinSet;

// Every keyvalue and blobstore program in `crates/test-programs` must have a
// matching test here; a new program without one fails to compile.
test_programs::foreach_keyvalue!();
test_programs::foreach_blobstore!();

const LEGACY: &str = "omnia-legacy";

// The state rows empty two buckets, the blob rows create a dozen object
// stores, and the upgrade replaces a bucket, so none runs against the public
// server `NATS_ADDR` defaults to.
fn local() -> Result<ConnectOptions> {
    let options = ConnectOptions::load_env()?;
    ensure!(options.address != "demo.nats.io", "set NATS_ADDR to a local server");
    Ok(options)
}

async fn connect(mut options: ConnectOptions, buckets: &[&str]) -> Result<Client> {
    options.buckets = buckets.iter().map(|&bucket| bucket.to_owned()).collect();
    Client::connect_with(options).await
}

async fn empty(bucket: &Arc<dyn Bucket>) -> Result<()> {
    for key in bucket.keys().await? {
        bucket.delete(key).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable NATS server (NATS_ADDR); run with --run-ignored"]
async fn publish() -> Result<()> {
    let backend = <Client as Connect>::connect().await?;
    let producer: Arc<dyn Endpoint> = MessagingClient::endpoint(&backend).await?;

    producer.send("omnia.live".to_owned(), Message::new(b"omnia-live".to_vec())).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: empties the `default` and `second` buckets of a local NATS server (NATS_ADDR); run with --run-ignored"]
async fn keyvalue_state_store() -> Result<()> {
    let client = connect(local()?, &["default", "second"]).await?;
    for name in ["default", "second"] {
        empty(&client.open_bucket(name.to_owned()).await?).await?;
    }
    let backends = Backends::defaults().await.keyvalue(client);

    let status = Deployment::new()
        .guest("guest", test_programs::KEYVALUE_STATE_STORE)
        .run_host::<WasiKeyValue, _>(backends.clone())
        .await?;
    assert_eq!(status, ExitStatus::SUCCESS, "guest failed");

    // the guest's bytes are stored as they are, and the deadline expires them
    assert_eq!(backends.state("host.raw").await, Some(b"raw".to_vec()));
    assert_eq!(backends.state("host.persist").await, Some(b"kept".to_vec()));
    assert_eq!(backends.state("host.expiring").await, None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable NATS server (NATS_ADDR); run with --run-ignored"]
async fn keyvalue_atomics() -> Result<()> {
    let client = connect(ConnectOptions::load_env()?, &["omnia-live"]).await?;
    let bucket = client.open_bucket("omnia-live".to_owned()).await?;

    let counter = unique("counter");
    assert_eq!(bucket.increment(counter.clone(), 5, None).await?, 5);
    assert_eq!(bucket.increment(counter, -2, None).await?, 3);

    let pointer = unique("pointer");
    let created = bucket.compare_and_swap(pointer.clone(), None, b"first".to_vec(), None).await?;
    assert_eq!(created, Ok(()));
    let stale = bucket.compare_and_swap(pointer.clone(), None, b"lost".to_vec(), None).await?;
    assert_eq!(stale, Err(Some(b"first".to_vec())));
    let first = Some(b"first".to_vec());
    let retried = bucket.compare_and_swap(pointer.clone(), first, b"second".to_vec(), None).await?;
    assert_eq!(retried, Ok(()));
    assert_eq!(bucket.get(pointer).await?, Some(b"second".to_vec()));

    let text = unique("text");
    bucket.set(text.clone(), b"not a number".to_vec(), None).await?;
    let err = bucket.increment(text, 1, None).await.expect_err("a non-counter increments");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "{err}");

    let err =
        bucket.set("not portable".to_owned(), b"v".to_vec(), None).await.expect_err("a space");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "{err}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: needs a reachable NATS server (NATS_ADDR); run with --run-ignored"]
async fn keyvalue_contention() -> Result<()> {
    let client = connect(ConnectOptions::load_env()?, &["omnia-live"]).await?;
    let bucket = client.open_bucket("omnia-live".to_owned()).await?;

    let counter = unique("counter");
    let mut increments = JoinSet::new();
    for _ in 0..16 {
        let (bucket, counter) = (Arc::clone(&bucket), counter.clone());
        increments.spawn(async move { bucket.increment(counter, 1, None).await });
    }
    let mut observed = increments.join_all().await.into_iter().collect::<Result<Vec<_>>>()?;
    observed.sort_unstable();
    assert_eq!(observed, (1..=16).collect::<Vec<i64>>(), "every increment lands exactly once");

    let pointer = unique("pointer");
    bucket.set(pointer.clone(), b"seed".to_vec(), None).await?;
    let mut swaps = JoinSet::new();
    for i in 0..8_u8 {
        let (bucket, pointer) = (Arc::clone(&bucket), pointer.clone());
        let seed = Some(b"seed".to_vec());
        swaps.spawn(async move { bucket.compare_and_swap(pointer, seed, vec![i], None).await });
    }
    let swapped = swaps.join_all().await.into_iter().collect::<Result<Vec<_>>>()?;
    assert_eq!(swapped.iter().filter(|swap| swap.is_ok()).count(), 1, "{swapped:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: replaces the `omnia-legacy` bucket of a local NATS server (NATS_ADDR); run with --run-ignored"]
async fn keyvalue_upgrade() -> Result<()> {
    let options = local()?;
    let jetstream = jetstream::new(async_nats::connect(&options.address).await?);
    if jetstream.get_key_value(LEGACY).await.is_ok() {
        jetstream.delete_key_value(LEGACY).await?;
    }
    let config = kv::Config {
        bucket: LEGACY.to_owned(),
        history: 1,
        ..kv::Config::default()
    };
    jetstream.create_key_value(config).await?;

    let bucket = connect(options, &[LEGACY]).await?.open_bucket(LEGACY.to_owned()).await?;
    bucket.set("expiring".to_owned(), b"v".to_vec(), Some(Duration::from_secs(1))).await?;

    let stream = jetstream.get_stream(format!("KV_{LEGACY}")).await?;
    assert!(stream.cached_info().config.allow_message_ttl, "the bucket kept no per-message TTL");
    Ok(())
}

// A failed run leaves its object stores behind, so each run scopes the rows'
// containers by its process id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: creates object stores on a local NATS server (NATS_ADDR); run with --run-ignored"]
async fn blobstore_blob_store() -> Result<()> {
    let client = Client::connect_with(local()?).await?;
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

// A collision-resistant suffix so parallel runs never share a live key.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}
