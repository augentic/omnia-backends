//! Named identities through the `omnia_sdk` `Identity`, one scope per
//! request: the unnamed handle is `default`, two declared names issue
//! distinct tokens, a token's lifetime arrives as a `Duration`, and an
//! undeclared name fails.

#![cfg(target_arch = "wasm32")]

use std::time::Duration;

use omnia_sdk::{Identity, WasiProvider};

omnia_sdk::api::command::export!(scenario);

const SCOPE: &str = "https://storage.azure.com/.default";

async fn scenario() {
    let unnamed = WasiProvider.token(&[SCOPE]).await.expect("unnamed token");
    let default = WasiProvider.named("default").token(&[SCOPE]).await.expect("default token");
    let reports = WasiProvider.named("reports").token(&[SCOPE]).await.expect("reports token");

    assert_eq!(unnamed.token, default.token, "the unnamed handle is `default`");
    assert_ne!(default.token, reports.token, "each identity has its own token");
    let hour = Duration::from_mins(59)..=Duration::from_hours(1);
    assert!(hour.contains(&reports.expires_in), "an hour-long token: {reports:?}");

    let error = WasiProvider.named("undeclared").token(&[SCOPE]).await.expect_err("undeclared");
    assert!(error.to_string().contains("not declared"), "unexpected error: {error}");
}
