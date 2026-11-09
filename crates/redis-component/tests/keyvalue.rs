//! End-to-end tests at the `wasi:keyvalue` boundary over the Redis package:
//! every scenario runs a guest component from `crates/test-programs` through
//! the omnia runtime over a `ComponentKeyValue`, whose package talks RESP2
//! over host-dialled links to a fake Redis served in-process. The guest
//! asserts what it observes and traps on failure; the test seeds and reads
//! the store through the same client and asserts what reached the fake.

mod support;

use omnia::backends::{BackendError, ComponentKeyValue};
use omnia_wasi_keyvalue::KeyValueClient as _;
use support::fake_redis::{FakeRedis, Recorded};
use support::harness::{client, client_with, options, package_bytes, run_guest};

// Every guest program in `crates/test-programs` must have a matching test
// here; a new program without one fails to compile.
test_programs::foreach_keyvalue!();

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyvalue_bucket() {
    let fake = FakeRedis::serve().await;
    let client = client(&fake).await;
    let bucket = client.open_bucket("bucket".to_owned()).await.expect("bucket");
    bucket.set("counter".to_owned(), 37_i64.to_be_bytes().to_vec(), None).await.expect("seed");

    run_guest(test_programs::KEYVALUE_BUCKET, &client).await;

    // Persisted state: the increments landed, the deletes took, and the CAS
    // retry's value is what survived.
    let bucket = client.open_bucket("bucket".to_owned()).await.expect("bucket");
    let mut keys = bucket.keys().await.expect("keys");
    keys.sort();
    assert_eq!(keys, ["b", "cas", "counter", "fresh"]);
    assert_eq!(
        bucket.get("counter".to_owned()).await.expect("get"),
        Some(42_i64.to_be_bytes().to_vec())
    );
    assert_eq!(
        bucket.get("fresh".to_owned()).await.expect("get"),
        Some(3_i64.to_be_bytes().to_vec())
    );
    assert_eq!(bucket.get("b".to_owned()).await.expect("get"), Some(b"2".to_vec()));
    assert_eq!(bucket.get("cas".to_owned()).await.expect("get"), Some(b"retried".to_vec()));

    // Every key reached the fake under the bucket's prefix, and the swaps
    // went through the CAS script rather than a plain SET.
    let commands = fake.commands();
    assert!(
        commands.iter().filter_map(Recorded::key).all(|key| key.starts_with("bucket:")),
        "an unprefixed key reached Redis"
    );
    let cas_swaps = commands
        .iter()
        .filter(|command| {
            command.name() == "EVAL" && command.key().as_deref() == Some("bucket:cas")
        })
        .count();
    assert_eq!(cas_swaps, 3, "a fresh swap, a stale swap, and the retry");
    assert_eq!(fake.connections(), 2, "the package holds the two links it was granted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyvalue_ttl_expires() {
    let fake = FakeRedis::serve().await;
    let client = client(&fake).await;

    run_guest(test_programs::KEYVALUE_TTL_EXPIRES, &client).await;

    // Raw bytes landed without an envelope; the expired key is gone.
    let bucket = client.open_bucket("bucket".to_owned()).await.expect("bucket");
    assert_eq!(bucket.get("ephemeral".to_owned()).await.expect("get"), None);
    assert_eq!(bucket.get("live".to_owned()).await.expect("get"), Some(b"raw-bytes".to_vec()));
    assert_eq!(bucket.keys().await.expect("keys"), ["live"]);

    // The TTL reached Redis in milliseconds on the SET itself.
    let ephemeral_set = fake
        .commands()
        .into_iter()
        .find(|command| {
            command.name() == "SET" && command.key().as_deref() == Some("bucket:ephemeral")
        })
        .expect("the ephemeral SET");
    assert_eq!(&ephemeral_set.args[3..], [b"PX".to_vec(), b"1000".to_vec()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn password() {
    let fake = FakeRedis::serve_secured("hunter2").await;
    let client = client_with(&fake, 2, Some("hunter2")).await;
    let bucket = client.open_bucket("bucket".to_owned()).await.expect("bucket");
    bucket.set("k".to_owned(), b"v".to_vec(), None).await.expect("set");
    assert_eq!(bucket.get("k".to_owned()).await.expect("get"), Some(b"v".to_vec()));

    // Each link authenticated as it opened, before anything else it carried.
    let commands = fake.commands();
    for link in 0..fake.connections() {
        let first = commands.iter().find(|command| command.connection == link).expect("a command");
        assert_eq!(first.name(), "AUTH", "link {link} spoke before authenticating");
        assert_eq!(first.args[1], b"hunter2");
    }
    assert_eq!(commands.iter().filter(|command| command.name() == "AUTH").count(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_password() {
    let fake = FakeRedis::serve_secured("hunter2").await;
    let refused =
        ComponentKeyValue::load(package_bytes().await, options(&fake, 1, Some("letmein")))
            .await
            .expect_err("a wrong password refuses the connect");
    assert!(
        matches!(&refused, BackendError::Connect(reason) if reason.contains("WRONGPASS")),
        "{refused:?}"
    );
}
