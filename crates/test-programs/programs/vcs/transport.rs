//! The transport operations over git, from a guest: a bare origin cloned
//! beneath the `.` mount, a commit bookmarked and pushed, a second clone that
//! sees the bookmark and takes a later push only by fetch, a shallow clone
//! that holds every bookmark at the one commit it asked for and takes a later
//! push by fetch, and a fetch that prunes a remote-tracking ref the origin
//! lacks from a repository the host laid. The origin's `file://` URL is the
//! one operator argument; the host reads the origin and the clones with git
//! afterwards.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs::{self, CommitId, Workspace};
use test_programs::arguments;

omnia_sdk::command::export!(scenario);

const FIRST: &str = "./clones/first";
const SECOND: &str = "./clones/second";
const SHALLOW: &str = "./clones/shallow";
const PRUNED: &str = "./pruned";
const BOOKMARK: &str = "emery/rev-1";
const GONE: &str = "emery/gone";

async fn scenario() {
    let arguments = arguments();
    let url = arguments.first().expect("the origin's URL is the first argument");

    // a clone of the empty origin, with one commit bookmarked and pushed
    let first_clone = clone(url, FIRST, None).await;
    fs::write("clones/first/a.txt", "a\n").expect("writing a");
    let first = commit(&first_clone, "a").await;
    publish(&first_clone, &first).await;

    // a second clone sees the bookmark; a later push reaches it by fetch
    let second_clone = clone(url, SECOND, None).await;
    assert_eq!(seen(&second_clone).await, Some(first.clone()));
    fs::write("clones/first/b.txt", "b\n").expect("writing b");
    let next = commit(&first_clone, "b").await;
    publish(&first_clone, &next).await;
    assert_eq!(seen(&second_clone).await, Some(first), "a clone sees nothing before it fetches");
    second_clone.fetch("origin").await.expect("fetch");
    assert_eq!(seen(&second_clone).await, Some(next.clone()));

    // a shallow clone holds every bookmark at the one commit it asked for, and
    // a later push reaches it by fetch
    let shallow = clone(url, SHALLOW, Some(1)).await;
    assert_eq!(seen(&shallow).await, Some(next));
    fs::write("clones/first/c.txt", "c\n").expect("writing c");
    let last = commit(&first_clone, "c").await;
    publish(&first_clone, &last).await;
    shallow.fetch("origin").await.expect("fetch into the shallow clone");
    assert_eq!(seen(&shallow).await, Some(last));

    // a fetch into a repository that prunes drops the packed remote-tracking
    // ref whose bookmark the origin lacks
    let pruned = vcs::open(PRUNED).await.expect("open the pruning repository");
    let stale = pruned.remote_bookmark("origin", GONE).await.expect("remote bookmark");
    assert!(stale.is_some(), "the stale ref is there before the fetch");
    pruned.fetch("origin").await.expect("fetch with pruning");
    let stale = pruned.remote_bookmark("origin", GONE).await.expect("remote bookmark");
    assert_eq!(stale, None, "the fetch pruned the stale ref");
}

async fn clone(url: &str, path: &str, depth: Option<u32>) -> Workspace {
    vcs::clone(url, path, depth).await.expect("clone").expect("a new workspace")
}

async fn commit(workspace: &Workspace, message: &str) -> CommitId {
    workspace.commit(message.to_owned()).await.expect("commit").expect("a commit")
}

// `commit` bookmarked and pushed to the origin
async fn publish(workspace: &Workspace, commit: &str) {
    let moved = workspace.set_bookmark(BOOKMARK.to_owned(), commit.to_owned(), None).await;
    moved.expect("bookmark").expect("no precondition to fail");
    workspace.push("origin", BOOKMARK).await.expect("push").expect("a forward push");
}

// the origin's bookmark as `workspace` last saw it
async fn seen(workspace: &Workspace) -> Option<CommitId> {
    workspace.remote_bookmark("origin", BOOKMARK).await.expect("remote bookmark")
}
