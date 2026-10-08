//! The guest half of the containment the backend owes: every program the
//! repository's own configuration could choose to run on the host is
//! neutralised before it runs. The host lays a repository salted with a
//! hostile `core.hooksPath`, `core.fsmonitor`, signing program, smudge
//! filter, and remote receive-pack, a bare origin with its own hostile
//! hooks, and a repository with no identity to seal a merge; this guest
//! drives the operations that would fire each, and the host asserts no
//! marker was written and the work still landed.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs::{Vcs as _, WasiVcs};

omnia_sdk::command!(scenario);

const REPO: &str = "./repo";
const WORK: &str = "./work";
const NOIDENT: &str = "./noident";

async fn scenario() {
    repo_config_runs_nothing().await;
    a_failed_merge_leaves_no_merge().await;
}

async fn repo_config_runs_nothing() {
    let head = WasiVcs.head(REPO).await.expect("head");

    // a working copy checked out of the hostile repository runs no smudge
    WasiVcs.add(REPO, WORK, &head).await.expect("add");

    // a commit runs no hook, fsmonitor, or signing program, and still seals
    fs::write("repo/b.txt", "b\n").expect("writing b");
    assert_eq!(WasiVcs.pending(REPO).await.expect("pending").len(), 1, "fsmonitor saw the change");
    let sealed = WasiVcs.commit(REPO, "seal").await.expect("commit").expect("a commit");
    assert_ne!(sealed, head, "the commit landed");
    assert_eq!(WasiVcs.pending(REPO).await.expect("pending"), []);

    // a push runs neither the client's receive-pack nor the origin's hooks
    WasiVcs.label(REPO, "emery/hardened", &sealed).await.expect("label");
    WasiVcs.push(REPO, "origin", "emery/hardened").await.expect("push");
}

async fn a_failed_merge_leaves_no_merge() {
    let before = WasiVcs.head(NOIDENT).await.expect("head");
    let merged = WasiVcs.merge(NOIDENT, "feature", "merge", &[]).await;
    assert!(merged.is_err(), "a merge with no identity cannot be sealed: {merged:?}");
    assert_eq!(WasiVcs.head(NOIDENT).await.expect("head"), before, "the head did not move");
    assert_eq!(WasiVcs.pending(NOIDENT).await.expect("pending"), [], "nothing is left staged");
    assert!(fs::metadata("noident/.git/MERGE_HEAD").is_err(), "no merge is left in progress");
}
