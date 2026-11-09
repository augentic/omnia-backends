//! The cost of the component boundary: the native `omnia_redis::Client` and
//! the Redis package behind `ComponentKeyValue`, both over the same
//! in-process fake, timed per operation in sequence and under a 64-way
//! fan-out. The package runs twice: over the slice's two links, for the
//! comparison with its figures, and over the default four. `#[ignore]`d as a
//! measurement rather than a test; run with `cargo nextest run -p
//! omnia-redis-component --all-features --run-ignored all --test bench
//! --no-capture` and read the table.

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use omnia_backend::Connect as _;
use omnia_backend::keyvalue::{Bucket, KeyValueClient as _};
use omnia_redis::{Client, ConnectOptions};
use support::fake_redis::FakeRedis;
use support::harness::{LINKS, client, client_with};

const SEQUENTIAL_OPS: usize = 10_000;
const FANOUT: usize = 64;
const PER_TASK_OPS: usize = 256;
const DEFAULT_LINKS: usize = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: prints µs/op for the native client and the package; run with --run-ignored"]
#[expect(clippy::print_stdout, reason = "the table is the result")]
async fn native_vs_component() {
    let fake = FakeRedis::serve().await;

    let native = Client::connect_with(ConnectOptions {
        url: format!("redis://{}", fake.endpoint()),
        max_retries: 3,
        max_delay: 1_000,
    })
    .await
    .expect("the native client connects to the fake");
    let native = native.open_bucket("bench".to_owned()).await.expect("bucket");

    let component = client(&fake).await;
    let component = component.open_bucket("bench".to_owned()).await.expect("bucket");
    let wider = client_with(&fake, DEFAULT_LINKS, None).await;
    let wider = wider.open_bucket("bench".to_owned()).await.expect("bucket");

    let native_sequential = sequential(&native).await;
    let component_sequential = sequential(&component).await;
    let wider_sequential = sequential(&wider).await;
    let native_fanout = fanout(&native).await;
    let component_fanout = fanout(&component).await;
    let wider_fanout = fanout(&wider).await;

    println!();
    println!("{:<28} {:>12} {:>12} {:>8}", "scenario", "native", "component", "ratio");
    row(&format!("sequential, {LINKS} links"), native_sequential, component_sequential);
    row(&format!("sequential, {DEFAULT_LINKS} links"), native_sequential, wider_sequential);
    row(&format!("{FANOUT}-way, {LINKS} links"), native_fanout, component_fanout);
    row(&format!("{FANOUT}-way, {DEFAULT_LINKS} links"), native_fanout, wider_fanout);
    println!();
}

#[expect(clippy::print_stdout, reason = "the table is the result")]
fn row(scenario: &str, native: Duration, component: Duration) {
    let native_us = native.as_secs_f64() * 1e6;
    let component_us = component.as_secs_f64() * 1e6;
    println!(
        "{scenario:<28} {native_us:>9.2} µs {component_us:>9.2} µs {:>7.2}x",
        component_us / native_us
    );
}

// one SET then one GET per key, back to back; the mean cost of one call
async fn sequential(bucket: &Arc<dyn Bucket>) -> Duration {
    let started = Instant::now();
    for index in 0..SEQUENTIAL_OPS / 2 {
        let key = format!("seq-{index}");
        bucket.set(key.clone(), b"value".to_vec(), None).await.expect("set");
        assert_eq!(bucket.get(key).await.expect("get"), Some(b"value".to_vec()));
    }
    started.elapsed() / u32::try_from(SEQUENTIAL_OPS).expect("fits")
}

// FANOUT tasks each run their own SET/GET loop; the mean cost of one call
// across every task's wall time
async fn fanout(bucket: &Arc<dyn Bucket>) -> Duration {
    let started = Instant::now();
    let tasks: Vec<_> = (0..FANOUT)
        .map(|task| {
            let bucket = Arc::clone(bucket);
            tokio::spawn(async move {
                for index in 0..PER_TASK_OPS / 2 {
                    let key = format!("fan-{task}-{index}");
                    bucket.set(key.clone(), b"value".to_vec(), None).await.expect("set");
                    assert_eq!(bucket.get(key).await.expect("get"), Some(b"value".to_vec()));
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("task");
    }
    started.elapsed() / u32::try_from(FANOUT * PER_TASK_OPS).expect("fits")
}
