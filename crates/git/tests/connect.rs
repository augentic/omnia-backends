//! What `Client::connect` holds the operator's `git` to before any
//! operation runs: present, no older than the first with
//! `config --show-scope`, and speaking the C locale whatever the operator's
//! shell exports.

mod support;

use std::fs;

use omnia_git::Client;
use support::Hermetic;

// The refusal classes read what git says in English; `LC_ALL` outranks the
// `LANG`, `LANGUAGE`, and `LC_MESSAGES` an operator's shell exports.
#[tokio::test]
async fn operator_locale() {
    let git = Hermetic::new();
    let (script, record) = git.recording("LC_ALL");
    Client::connect(script).await.expect("a current git");
    assert_eq!(fs::read_to_string(record).expect("the git recorded its locale"), "C\n");
}

#[tokio::test]
async fn version_refused() {
    let git = Hermetic::new();
    let error = Client::connect(git.reporting("2.25.0")).await.expect_err("too old");
    assert!(error.to_string().contains("older than 2.26"), "{error:#}");

    Client::connect(git.reporting("2.26.0")).await.expect("the first with config --show-scope");

    let error = Client::connect(git.path("no-such-git")).await.expect_err("missing binary");
    assert!(error.to_string().contains("spawning"), "{error:#}");
}
