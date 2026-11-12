//! The Azure Identity backend at the `omnia:identity` host boundary, over a
//! fake App Service managed-identity endpoint. A real guest asks for the
//! unnamed, a declared, and an undeclared identity, and the fake sees which
//! managed identity each declared name resolves to.
//!
//! No service, so it runs in CI.

#![cfg(not(target_arch = "wasm32"))]

mod support;

use omnia::{Connect as _, ExitStatus};
use omnia_azure_id::Client;
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_identity::WasiIdentity;
use support::FakeIdentity;

// Every identity program in `crates/test-programs` must have a matching
// test here; a new program without one fails to compile.
test_programs::foreach_identity!();

const DEFAULT_ID: &str = "0f6b7ad4-3c1e-4a8e-9d4b-6a1f2e3d4c5b";
const REPORTS_ID: &str = "7c2d9e1a-5b4f-4c3d-8e2a-1b9c8d7e6f5a";
const RESOURCE: &str = "https://storage.azure.com";

// `default` is the system-assigned identity. The unnamed handle shares its
// credential, so the second ask is answered from the credential's cache.
#[tokio::test]
async fn identity_named() {
    let fake = FakeIdentity::serve(&[REPORTS_ID]).await;
    fake.plant(&[
        ("IDENTITY_NAMES", "default,reports"),
        ("IDENTITY_CLIENT_ID__REPORTS", REPORTS_ID),
    ]);

    run_guest().await;

    let reports = Some(REPORTS_ID.to_owned());
    assert_eq!(fake.asked(), [(None, RESOURCE.to_owned()), (reports, RESOURCE.to_owned())]);
}

#[tokio::test]
async fn default_client_id() {
    let fake = FakeIdentity::serve(&[DEFAULT_ID, REPORTS_ID]).await;
    fake.plant(&[
        ("IDENTITY_NAMES", "default,reports"),
        ("IDENTITY_CLIENT_ID", DEFAULT_ID),
        ("IDENTITY_CLIENT_ID__REPORTS", REPORTS_ID),
    ]);

    run_guest().await;

    let default = Some(DEFAULT_ID.to_owned());
    let reports = Some(REPORTS_ID.to_owned());
    assert_eq!(fake.asked(), [(default, RESOURCE.to_owned()), (reports, RESOURCE.to_owned())]);
}

#[tokio::test]
async fn missing_client_id() {
    let fake = FakeIdentity::serve(&[REPORTS_ID]).await;
    fake.plant(&[("IDENTITY_NAMES", "default,reports")]);

    let error = Client::connect().await.expect_err("`reports` has no client id");
    assert!(format!("{error:#}").contains("IDENTITY_CLIENT_ID__REPORTS"), "{error:#}");
}

async fn run_guest() {
    let client = Client::connect().await.expect("connect");
    let backends = Backends::defaults().await.identity(client);

    let status = Deployment::new()
        .guest("guest", test_programs::IDENTITY_NAMED)
        .run_host::<WasiIdentity, _>(backends)
        .await
        .expect("guest runs");
    assert_eq!(status, ExitStatus::SUCCESS, "guest failed");
}
