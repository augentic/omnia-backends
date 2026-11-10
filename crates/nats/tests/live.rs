//! Live publish and key-value for the NATS backend, driven through the
//! `omnia:messaging` and `wasi:keyvalue` contracts. NATS also serves
//! `wasi:blobstore` from the same client; its ignored test can be added here
//! as that live env is set up.
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a reachable
//! server (`NATS_ADDR`, default `demo.nats.io`):
//! `cargo nextest run -p omnia-nats --run-ignored all`.

use std::sync::Arc;

use anyhow::Result;
use omnia::Connect;
use omnia_nats::Client;
use omnia_wasi_keyvalue::{Bucket, Cas, KeyValueClient};
use omnia_wasi_messaging::{Endpoint, Message, MessagingClient};

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

    // Absent-expected swap creates the key via JetStream `create`.
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

    // The refreshed handle retries cleanly via revision-conditioned `update`.
    bucket.swap(fresh, b"second".to_vec()).await?.expect("retry commits");
    assert_eq!(bucket.get(pointer).await?.as_deref(), Some(b"second".as_slice()));
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
