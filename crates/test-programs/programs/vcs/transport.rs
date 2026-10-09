//! The transport operations over git, from a guest: a bare origin cloned
//! beneath the `.` mount, a commit labelled and pushed, a second clone that
//! sees the label and takes a later push only by fetch, and a shallow clone
//! that holds every label at the one commit it asked for. The origin's
//! `file://` URL is the one operator argument; the host reads the origin
//! and the clones with git afterwards.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs::{CloneOptions, Vcs as _, WasiVcs};
use test_programs::arguments;

omnia_sdk::command!(scenario);

const WHOLE: CloneOptions = CloneOptions { depth: None };
const FIRST: &str = "./clones/first";
const SECOND: &str = "./clones/second";
const SHALLOW: &str = "./clones/shallow";
const LABEL: &str = "emery/rev-1";
const REMOTE_LABEL: &str = "origin/emery/rev-1";

async fn scenario() {
    let arguments = arguments();
    let url = arguments.first().expect("the origin's URL is the first argument");

    // a clone of the empty origin, with one commit labelled and pushed
    WasiVcs.clone_repo(url, FIRST, WHOLE).await.expect("clone");
    fs::write("clones/first/a.txt", "a\n").expect("writing a");
    let first = WasiVcs.commit(FIRST, "a").await.expect("commit").expect("a");
    WasiVcs.label(FIRST, LABEL, &first).await.expect("label");
    WasiVcs.push(FIRST, "origin", LABEL).await.expect("push");

    // a second clone sees the label; a later push reaches it by fetch
    WasiVcs.clone_repo(url, SECOND, WHOLE).await.expect("clone");
    assert_eq!(WasiVcs.resolve(SECOND, REMOTE_LABEL).await.expect("resolve"), first);
    fs::write("clones/first/b.txt", "b\n").expect("writing b");
    let next = WasiVcs.commit(FIRST, "b").await.expect("commit").expect("b");
    WasiVcs.label(FIRST, LABEL, &next).await.expect("move label");
    WasiVcs.push(FIRST, "origin", LABEL).await.expect("push");
    assert_eq!(
        WasiVcs.resolve(SECOND, REMOTE_LABEL).await.expect("stale"),
        first,
        "a clone sees nothing before it fetches"
    );
    WasiVcs.fetch(SECOND, "origin").await.expect("fetch");
    assert_eq!(WasiVcs.resolve(SECOND, REMOTE_LABEL).await.expect("fresh"), next);

    // a shallow clone holds every label at the one commit it asked for
    WasiVcs.clone_repo(url, SHALLOW, CloneOptions { depth: Some(1) }).await.expect("shallow");
    assert!(fs::metadata("clones/shallow/.git/shallow").is_ok(), "the clone is shallow");
    assert_eq!(WasiVcs.resolve(SHALLOW, REMOTE_LABEL).await.expect("resolve"), next);
}
