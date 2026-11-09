//! The harness the suites share: the package's bytes acquired the way a
//! deployment acquires a release, a `ComponentKeyValue` over it pointed at
//! the fake, and a guest run through `omnia_test::host`.

use omnia::backends::{BackendOptions, ComponentKeyValue};
use omnia::{ExitStatus, FsStore, Reference, RegistryClient, RegistrySource as _};
use omnia_test::host::{Backends, Deployment, scratch};
use omnia_wasi_keyvalue::WasiKeyValue;

use super::BACKEND_OMNIA_REDIS_COMPONENT;
use super::fake_redis::FakeRedis;

/// The release a deployment would name for the package.
pub const PACKAGE: &str = "augentic:redis-backend@0.1.0";

/// The links the suites hold to the fake: the slice's count, so the bench
/// compares like with like.
pub const LINKS: usize = 2;

/// The package's bytes, acquired through a `RegistryClient` from a store
/// the build's artifact is staged in under the release's file name, so no
/// registry is reached.
pub async fn package_bytes() -> Vec<u8> {
    let reference: Reference = PACKAGE.parse().expect("a valid reference");
    let staged = scratch();
    std::fs::copy(BACKEND_OMNIA_REDIS_COMPONENT, staged.path().join(reference.file_name()))
        .expect("stage the package");
    let acquirer =
        RegistryClient::from_toml(None, FsStore::open(staged.path())).expect("an acquirer");
    acquirer.acquire(PACKAGE, None).await.expect("the store serves the release")
}

/// The options granting the package `fake` alone, with `links` links and
/// `password` when the fake requires one.
pub fn options(fake: &FakeRedis, links: usize, password: Option<&str>) -> BackendOptions {
    let mut config = vec![("links".to_owned(), links.to_string())];
    if let Some(password) = password {
        config.push(("password".to_owned(), password.to_owned()));
    }
    BackendOptions {
        egress: vec![fake.endpoint()],
        config,
    }
}

/// A `ComponentKeyValue` over the package, connected to `fake`.
pub async fn client(fake: &FakeRedis) -> ComponentKeyValue {
    client_with(fake, LINKS, None).await
}

/// A `ComponentKeyValue` over the package, connected to `fake` under
/// `options`' parameters.
pub async fn client_with(
    fake: &FakeRedis, links: usize, password: Option<&str>,
) -> ComponentKeyValue {
    ComponentKeyValue::load(package_bytes().await, options(fake, links, password))
        .await
        .expect("the package loads and connects")
}

/// Run one guest program over `client`, requiring a clean exit.
pub async fn run_guest(wasm: &str, client: &ComponentKeyValue) {
    let backends = Backends::defaults().await.keyvalue(client.clone());
    let status = Deployment::new()
        .guest("guest", wasm)
        .run_host::<WasiKeyValue, _>(backends)
        .await
        .expect("guest runs");
    assert_eq!(status, ExitStatus::SUCCESS, "guest `{wasm}` failed");
}
