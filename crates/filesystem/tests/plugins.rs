//! Filesystem plugin-store contract tests.

use omnia_filesystem::Client;
use omnia_plugin::{ContentStore, ReleaseRecord, ReleaseStore, sha256_digest as digest_of};
use omnia_wasi_blobstore::{Bytes, WasiBlobstoreCtx};
use omnia_wasi_keyvalue::WasiKeyValueCtx;
use tempfile::TempDir;

fn client(root: &TempDir) -> Client {
    Client::open(root.path()).expect("open")
}

fn record(bytes: &[u8]) -> ReleaseRecord {
    ReleaseRecord {
        version: "1.2.3".to_string(),
        content_digest: digest_of(bytes),
    }
}

#[tokio::test]
async fn content_round_trip() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);

    let bytes = b"component bytes";
    let digest = digest_of(bytes);
    assert_eq!(ContentStore::get(&store, &digest).await.expect("get"), None);

    ContentStore::put(&store, &digest, bytes).await.expect("put");
    assert_eq!(
        ContentStore::get(&store, &digest).await.expect("get").as_deref(),
        Some(bytes.as_slice())
    );

    // Entries survive a reopen (the store is durable, not process state).
    drop(store);
    let store = client(&root);
    assert_eq!(
        ContentStore::get(&store, &digest).await.expect("get").as_deref(),
        Some(bytes.as_slice())
    );
}

#[tokio::test]
async fn mismatched_content_refused() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);

    let digest = digest_of(b"the real bytes");
    let err = ContentStore::put(&store, &digest, b"other bytes").await.expect_err("must refuse");
    assert!(err.to_string().contains("refusing to persist"), "unexpected error: {err}");
    assert_eq!(ContentStore::get(&store, &digest).await.expect("get"), None, "no entry lands");
}

#[tokio::test]
async fn release_round_trip() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);

    assert_eq!(
        ReleaseStore::get(&store, "omnia.host", "emery:intent", "1.2.3").await.expect("get"),
        None
    );

    let record = record(b"component bytes");
    ReleaseStore::put(&store, "omnia.host", "emery:intent", &record).await.expect("put");
    assert_eq!(
        ReleaseStore::get(&store, "omnia.host", "emery:intent", "1.2.3").await.expect("get"),
        Some(record.clone())
    );

    // A re-put overwrites the record in place.
    let repinned = ReleaseRecord {
        content_digest: digest_of(b"rebuilt bytes"),
        ..record
    };
    ReleaseStore::put(&store, "omnia.host", "emery:intent", &repinned).await.expect("re-put");
    assert_eq!(
        ReleaseStore::get(&store, "omnia.host", "emery:intent", "1.2.3").await.expect("get"),
        Some(repinned)
    );
}

#[tokio::test]
async fn releases_scoped_per_registry() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);

    let record = record(b"component bytes");
    ReleaseStore::put(&store, "omnia.host", "emery:intent", &record).await.expect("put");

    // An endpoint override is never answered from another registry's record.
    assert_eq!(
        ReleaseStore::get(&store, "registry.example", "emery:intent", "1.2.3").await.expect("get"),
        None
    );

    // Content stays shared: the digest is the identity, whichever registry's
    // release points at it.
    let bytes = b"component bytes";
    ContentStore::put(&store, &digest_of(bytes), bytes).await.expect("put content");
    let other = ReleaseRecord {
        version: "1.2.3".to_string(),
        content_digest: digest_of(bytes),
    };
    ReleaseStore::put(&store, "registry.example", "emery:intent", &other).await.expect("put");
    assert_eq!(
        ContentStore::get(&store, &digest_of(bytes)).await.expect("get").as_deref(),
        Some(bytes.as_slice())
    );
}

#[tokio::test]
async fn plugins_tree_disjoint_from_guest_storage() {
    let root = TempDir::new().expect("tempdir");
    let store = client(&root);

    let bytes = b"component bytes";
    let digest = digest_of(bytes);
    ContentStore::put(&store, &digest, bytes).await.expect("put content");
    ReleaseStore::put(&store, "omnia.host", "emery:intent", &record(bytes))
        .await
        .expect("put release");

    // A guest container or bucket named `plugins` lands under `blobstore/`
    // or `keyvalue/`, never the plugins tree — same-name writes coexist.
    let container = store.create_container("plugins".to_string()).await.expect("create container");
    container.write_data("content".to_string(), Bytes::from_static(b"blob")).await.expect("write");
    let bucket = store.open_bucket("plugins".to_string()).await.expect("open bucket");
    bucket.set("content".to_string(), b"kv".to_vec()).await.expect("set");

    assert_eq!(
        ContentStore::get(&store, &digest).await.expect("get").as_deref(),
        Some(bytes.as_slice())
    );
    assert_eq!(
        container.list_objects().await.expect("list"),
        ["content"],
        "the container sees only its own object, not the plugin store"
    );
    assert_eq!(
        bucket.keys().await.expect("keys"),
        ["content"],
        "the bucket sees only its own key, not the plugin store"
    );
    assert_eq!(
        container.get_data("content".to_string(), 0, 0).await.expect("get"),
        Some(Bytes::from_static(b"blob"))
    );
}
