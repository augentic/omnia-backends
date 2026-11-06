//! What `Client::connect_with` holds the operator's `git` to before any
//! operation runs: present, no older than the first with
//! `config --show-scope`, and speaking the C locale whatever the operator's
//! shell exports.

mod support;

use std::fs;

use support::{Hermetic, connect};

// The refusal classes read what git says in English; `LC_ALL` outranks the
// `LANG`, `LANGUAGE`, and `LC_MESSAGES` an operator's shell exports.
#[tokio::test]
async fn operator_locale() {
    let git = Hermetic::new();
    let (script, record) = git.recording("LC_ALL");
    connect(script).await.expect("a current git");
    assert_eq!(fs::read_to_string(record).expect("the git recorded its locale"), "C\n");
}

#[tokio::test]
async fn git_version() {
    let git = Hermetic::new();
    for old in ["2.25.0", "2.5"] {
        let error = connect(git.reporting(old)).await.expect_err("too old");
        assert!(error.to_string().contains("older than 2.26"), "{error:#}");
    }

    for current in ["2.26.0", "2.50.1 (Apple Git-155)", "2.47.0.windows.1", "2.39.5-rc1"] {
        connect(git.reporting(current)).await.expect("supported git version");
    }

    let error = connect(git.reporting("x.y")).await.expect_err("unrecognised version");
    assert!(error.to_string().contains("unrecognised `git --version`"), "{error:#}");

    let error = connect(git.path("no-such-git")).await.expect_err("missing binary");
    assert!(error.to_string().contains("spawning"), "{error:#}");
}
