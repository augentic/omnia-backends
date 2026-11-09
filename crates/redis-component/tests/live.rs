//! Live acceptance against a real Redis: the shared `wasi:keyvalue` guests
//! run over the Redis package, whose links the host dials to the server
//! `REDIS_ENDPOINT` names (`host:port`, default `localhost:6379`), with
//! `REDIS_PASSWORD` when the server requires one. `#[ignore]`d so it never
//! touches the network in CI; run with `cargo nextest run -p
//! omnia-redis-component --all-features --run-ignored all --test live`.

mod support;

use omnia::backends::{BackendOptions, ComponentKeyValue};
use omnia_wasi_keyvalue::KeyValueClient as _;
use support::harness::{package_bytes, run_guest};

const DEFAULT_ENDPOINT: &str = "localhost:6379";

// The guests share the bucket named `bucket` and assert its whole key set,
// so they run in turn over an emptied bucket rather than as two rows that
// would race at one server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Redis (REDIS_ENDPOINT, REDIS_PASSWORD); run with --run-ignored"]
async fn bucket_then_ttl_expires() {
    let endpoint =
        std::env::var("REDIS_ENDPOINT").unwrap_or_else(|_unset| DEFAULT_ENDPOINT.to_owned());
    let mut config = Vec::new();
    if let Ok(password) = std::env::var("REDIS_PASSWORD") {
        config.push(("password".to_owned(), password));
    }
    let options = BackendOptions {
        egress: vec![endpoint],
        config,
    };
    let client = ComponentKeyValue::load(package_bytes().await, options)
        .await
        .expect("the package connects to Redis");

    clear(&client).await;
    let bucket = client.open_bucket("bucket".to_owned()).await.expect("bucket");
    bucket.set("counter".to_owned(), 37_i64.to_be_bytes().to_vec(), None).await.expect("seed");
    run_guest(test_programs::KEYVALUE_BUCKET, &client).await;
    assert_eq!(
        bucket.get("counter".to_owned()).await.expect("get"),
        Some(42_i64.to_be_bytes().to_vec())
    );

    clear(&client).await;
    run_guest(test_programs::KEYVALUE_TTL_EXPIRES, &client).await;
    assert_eq!(bucket.keys().await.expect("keys"), ["live"]);
    clear(&client).await;
}

async fn clear(client: &ComponentKeyValue) {
    let bucket = client.open_bucket("bucket".to_owned()).await.expect("bucket");
    for key in bucket.keys().await.expect("keys") {
        bucket.delete(key).await.expect("delete");
    }
}
