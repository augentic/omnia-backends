//! Exercises Git containment against configuration a repository gains while
//! an operation runs.
//!
//! The host's git plants keys a guest could write meanwhile:
//!
//! - an ssh command, right after a fetch's listing;
//! - a clean filter, right after a commit's listing;
//! - a smudge filter, right after the listing of `add`;
//! - a merge driver, right after a merge's listing;
//! - a smudge filter and an ssh command, into a clone's destination as git
//!   lays it;
//! - a URL rewrite, right after a fetch's listing.
//!
//! It also fails one repository's listing. The guest runs each operation,
//! then commits in a repository whose identity comes from a quoted value,
//! an included file, and a worktree's own configuration. The host verifies
//! that no planted program ran, every dial reached the host's ssh, the
//! rewrite carried nothing, the failed listing sealed nothing, and the
//! commit carries the identity git itself reads.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs::{self, Workspace};
use test_programs::{arguments, fails_with};

omnia_sdk::command::export!(scenario);

async fn scenario() {
    let arguments = arguments();
    let origin = arguments.first().expect("the planted origin's URL is the first argument");

    a_planted_ssh_command_never_dials().await;
    a_planted_clean_filter_never_runs().await;
    a_planted_smudge_filter_never_runs().await;
    a_planted_merge_driver_never_runs().await;
    a_clone_destination_plant_never_runs(origin).await;
    a_planted_rewrite_carries_nothing().await;
    a_failed_listing_refuses().await;
    the_identity_is_the_repository_own().await;
}

async fn open(path: &str) -> Workspace {
    vcs::open(path).await.unwrap_or_else(|error| panic!("open {path}: {error}"))
}

// The repository gains an ssh command once the fetch's listing has
// returned; the fetch dials the host's ssh, which answers nothing.
async fn a_planted_ssh_command_never_dials() {
    let fetched = open("./planted-ssh").await.fetch("evil").await;
    let error = fetched.expect_err("the host's ssh answers nothing");
    assert_eq!(error.code(), "unavailable", "{error:?}");
}

// The repository gains a clean filter its attributes select once the
// commit's listing has returned; the change seals unfiltered.
async fn a_planted_clean_filter_never_runs() {
    let sealed = open("./planted-filter").await.commit("seal".to_owned()).await;
    sealed.expect("commit").expect("a commit");
}

// The repository gains a smudge filter once the listing of `add` has
// returned; the working copy is checked out unfiltered.
async fn a_planted_smudge_filter_never_runs() {
    let repo = open("./planted-add").await;
    let head = repo.head().await.expect("head").expect("a commit");
    repo.add("./planted-copy", &head).await.expect("add").expect("a new workspace");
    assert_eq!(fs::read_to_string("planted-copy/a.txt").expect("the copy's file"), "a\n");
}

// The repository gains a merge driver its attributes select once the
// merge's listing has returned; git's own merge seals both sides' lines.
async fn a_planted_merge_driver_never_runs() {
    let repo = open("./planted-merge").await;
    let merged = repo.merge("feature".to_owned(), "merge".to_owned(), vec![]).await;
    merged.expect("merge").expect("a clean merge");
    let file = fs::read_to_string("planted-merge/m.txt").expect("the merged file");
    assert_eq!(file, "ONE\ntwo\nthree\nfour\nFIVE\n");
}

// A clone's destination gains a smudge filter and an ssh command as git
// lays it; the clone is checked out unfiltered, and a clone over ssh dials
// the host's ssh.
async fn a_clone_destination_plant_never_runs(origin: &str) {
    let cloned = vcs::clone(origin, "./planted-clone", None).await;
    cloned.expect("clone").expect("a new workspace");
    assert_eq!(fs::read_to_string("planted-clone/a.txt").expect("the clone's file"), "a\n");
    let dialled = vcs::clone("ssh://localhost/x.git", "./planted-ssh-clone", None).await;
    let error = dialled.expect_err("the host's ssh answers nothing");
    assert_eq!(error.code(), "unavailable", "{error:?}");
}

// The repository gains a rewrite of its unanswered remote onto a local
// origin once the fetch's listing has returned; the fetch dials the remote
// the repository named.
async fn a_planted_rewrite_carries_nothing() {
    let fetched = open("./planted-rewrite").await.fetch("evil").await;
    let error = fetched.expect_err("nothing answers the remote");
    assert_eq!(error.code(), "unavailable", "{error:?}");
}

// The repository's listing fails, so no pin can be built from it; the
// commit is refused rather than run under the forced pins alone.
async fn a_failed_listing_refuses() {
    let refused = open("./unlisted").await.commit("seal".to_owned()).await;
    fails_with(refused.expect_err("the commit is refused"), "other", "configuration");
}

// The repository's identity is a quoted name in its worktree's own
// configuration and an email in a file its configuration includes.
async fn the_identity_is_the_repository_own() {
    let sealed = open("./faithful").await.commit("faithful".to_owned()).await;
    sealed.expect("commit").expect("a commit");
}
