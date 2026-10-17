//! Live blob round-trip for the MongoDB backend, driven through the
//! `omnia:blobstore` host boundary (`WasiBlobstoreCtx`; containers map to
//! MongoDB collections).
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a reachable
//! MongoDB (`MONGODB_URL`, including a default database):
//! `cargo nextest run -p omnia-mongodb --run-ignored all`.

use anyhow::Result;
use omnia::Backend;
use omnia_mongodb::Client;
use omnia_wasi_blobstore::{Container, WasiBlobstoreCtx};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable MongoDB (MONGODB_URL); run with --run-ignored"]
async fn write_read_delete() -> Result<()> {
    let client = <Client as Backend>::connect().await?;

    let container = format!("omnia-live-{}", std::process::id());
    let store: std::sync::Arc<dyn Container> = client.create_container(container.clone()).await?;
    assert!(client.container_exists(container.clone()).await?, "container exists after create");

    let object = "greeting".to_owned();
    store.write_data(object.clone(), b"payload".to_vec().into()).await?;

    assert_eq!(store.get_data(object.clone(), 0, 0).await?.as_deref(), Some(b"payload".as_slice()));
    assert!(store.has_object(object.clone()).await?, "object exists after write");

    store.delete_object(object).await?;
    client.delete_container(container.clone()).await?;
    assert!(!client.container_exists(container).await?, "container gone after delete");
    Ok(())
}
