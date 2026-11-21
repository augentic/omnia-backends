//! Named identities through the `omnia_sdk` identity helpers, one scope per
//! request: two declared names issue distinct tokens, a token's lifetime
//! reads back as a `Duration`, and an undeclared name fails with the code
//! `not-found`.

#![cfg(target_arch = "wasm32")]

use std::time::Duration;

use omnia_sdk::identity::{self, AccessToken, DEFAULT};
use omnia_sdk::{ErrorKind, Result};

omnia_sdk::command::export!(scenario);

const SCOPE: &str = "https://storage.azure.com/.default";

async fn scenario() {
    let default = token(DEFAULT).await.expect("default token");
    let reports = token("reports").await.expect("reports token");

    assert_ne!(default.token, reports.token, "each identity has its own token");
    let hour = Duration::from_mins(59)..=Duration::from_hours(1);
    assert!(hour.contains(&reports.expires_in()), "an hour-long token: {reports:?}");

    let error = token("undeclared").await.expect_err("undeclared");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::ServerError, "not-found"),
        "unexpected error: {error}"
    );
}

async fn token(name: &str) -> Result<AccessToken> {
    identity::open(name).await?.token(&[SCOPE]).await
}
