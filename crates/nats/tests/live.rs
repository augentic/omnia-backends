//! Live publish and key-value for the NATS backend, driven through the
//! `omnia:messaging` and `wasi:keyvalue` contracts. NATS also serves
//! `wasi:blobstore` from the same client; its ignored test can be added here
//! as that live env is set up.
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a reachable
//! server (`NATS_ADDR`, default `demo.nats.io`):
//! `cargo nextest run -p omnia-nats --run-ignored all`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use omnia_backend::Connect;
use omnia_backend::keyvalue::{Bucket, KeyValueClient};
use omnia_backend::messaging::{Endpoint, Message, MessagingClient};
use omnia_nats::Client;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable NATS server (NATS_ADDR); run with --run-ignored"]
async fn publishes_message() -> Result<()> {
    let backend = <Client as Connect>::connect().await?;
    let producer: Arc<dyn Endpoint> = MessagingClient::endpoint(&backend).await?;

    producer.send("omnia.live".to_owned(), Message::new(b"omnia-live".to_vec())).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable NATS server (NATS_ADDR); run with --run-ignored"]
async fn keyvalue_atomics() -> Result<()> {
    let backend = <Client as Connect>::connect().await?;
    let bucket: Arc<dyn Bucket> = backend.open_bucket("omnia-live".to_owned()).await?;

    // Increment is a revision-conditioned read-modify-write loop.
    let counter = unique("counter");
    assert_eq!(bucket.increment(counter.clone(), 5).await?, 5);
    assert_eq!(bucket.increment(counter, -2).await?, 3);

    // Absent-expected CAS creates the key via JetStream `create`.
    let pointer = unique("pointer");
    bucket.cas(pointer.clone(), None, b"first".to_vec()).await?.expect("absent-expected commits");

    // A stale snapshot refuses, leaves the value, and returns the observed value.
    let observed = bucket
        .cas(pointer.clone(), None, b"lost".to_vec())
        .await?
        .expect_err("stale must mismatch");
    assert_eq!(observed.as_deref(), Some(b"first".as_slice()));
    assert_eq!(bucket.get(pointer.clone()).await?.as_deref(), Some(b"first".as_slice()));

    // Retrying with the observed value commits cleanly via revision-conditioned `update`.
    bucket.cas(pointer.clone(), observed, b"second".to_vec()).await?.expect("retry commits");
    assert_eq!(bucket.get(pointer).await?.as_deref(), Some(b"second".as_slice()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable NATS server 2.11+ (NATS_ADDR); run with --run-ignored"]
async fn keyvalue_ttl() -> Result<()> {
    let backend = <Client as Connect>::connect().await?;
    // A bucket of its own: one created before per-message TTL refuses the writes.
    let bucket: Arc<dyn Bucket> = backend.open_bucket("omnia-live-ttl".to_owned()).await?;

    // The server expires a value set with a TTL; one set without stays.
    let fleeting = unique("fleeting");
    let lasting = unique("lasting");
    bucket.set(fleeting.clone(), b"soon".to_vec(), Some(Duration::from_secs(1))).await?;
    bucket.set(lasting.clone(), b"still".to_vec(), None).await?;
    assert!(bucket.exists(fleeting.clone()).await?);

    // A swap on a key with a TTL keeps the TTL running; the swapped value expires with it.
    bucket.cas(fleeting.clone(), Some(b"soon".to_vec()), b"swapped".to_vec()).await?.expect("swap");
    assert_eq!(bucket.get(fleeting.clone()).await?.as_deref(), Some(b"swapped".as_slice()));

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(bucket.get(fleeting.clone()).await?, None);
    assert!(!bucket.exists(fleeting).await?);
    assert_eq!(bucket.get(lasting.clone()).await?.as_deref(), Some(b"still".as_slice()));

    bucket.delete(lasting.clone()).await?;
    assert_eq!(bucket.get(lasting).await?, None);
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
