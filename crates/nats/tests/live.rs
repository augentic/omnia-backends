//! Live publish for the NATS backend, driven through the `omnia:messaging`
//! contract (`MessagingClient` + `Endpoint`). NATS also serves
//! `wasi:keyvalue` and `wasi:blobstore` from the same client; a dedicated
//! ignored test per surface can be added here as those live envs are set up.
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a reachable
//! server (`NATS_ADDR`, default `demo.nats.io`):
//! `cargo nextest run -p omnia-nats --run-ignored all`.

use std::sync::Arc;

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

// A collision-resistant suffix so parallel runs never share a live key.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}
