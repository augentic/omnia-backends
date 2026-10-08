//! What `Client::connect` holds the operator's `git` to before any
//! operation runs: present, and no older than the first with
//! `config --show-scope`.

mod support;

use omnia_git::Client;
use support::Hermetic;

#[tokio::test]
async fn version_refused() {
    let git = Hermetic::new();
    let error = Client::connect(git.reporting("2.25.0")).await.expect_err("too old");
    assert!(error.to_string().contains("older than 2.26"), "{error:#}");

    Client::connect(git.reporting("2.26.0")).await.expect("the first with config --show-scope");

    let error = Client::connect(git.path("no-such-git")).await.expect_err("missing binary");
    assert!(error.to_string().contains("spawning"), "{error:#}");
}
