//! What git refuses, read into the typed error, from a guest. An unknown
//! revision is `not-found`, or nothing found where the call is a lookup. A
//! plain directory is `not-found` as no repository. A place already holding
//! a repository or files is occupied. A remote that does not exist is
//! `not-found` where one nothing answers is `unavailable`, and an address or
//! a name only a legacy remote file defines is no remote. A push the
//! remote's bookmark has moved past is diverged, and what git alone can say
//! stays `other`. The operator arguments are the origin's `file://` URL, a
//! `file://` URL nothing holds, and a URL nothing answers; the host asserts
//! the repository came through untouched and the origin's bookmark unforced.

#![cfg(target_arch = "wasm32")]

use std::{fmt, fs};

use omnia_sdk::vcs::{self, Occupied};
use test_programs::{arguments, fails_with, not_found};

omnia_sdk::command::export!(scenario);

const REPO: &str = "./repo";
const ELSEWHERE: &str = "./elsewhere";
const TAKEN: &str = "./taken";

// A refusal naming the place it stood at: the path is the host's, so only
// its tail is the guest's to check.
fn occupied_at<T: fmt::Debug>(outcome: omnia_sdk::Result<Result<T, Occupied>>, name: &str) {
    match outcome {
        Ok(Err(occupied)) if occupied.detail.ends_with(name) => {}
        outcome => panic!("expected `{name}` occupied, got {outcome:?}"),
    }
}

async fn scenario() {
    let arguments = arguments();
    let [url, missing, unanswered] = arguments.as_slice() else {
        panic!("the origin, missing, and unanswered URLs are the arguments");
    };

    let repo = vcs::init(REPO).await.expect("init").expect("a new workspace");
    fs::write("repo/a.txt", "a\n").expect("writing a");
    let first = repo.commit("a".to_owned()).await.expect("commit").expect("a");
    fs::create_dir_all("elsewhere").expect("creating a plain directory");
    fs::create_dir_all("taken").expect("creating a directory with files");
    fs::write("taken/x", "x\n").expect("filling it");

    // store and workspace
    assert_eq!(repo.resolve("nope".to_owned()).await.expect("resolve"), None);
    let elsewhere = vcs::open(ELSEWHERE).await.expect_err("a plain directory");
    fails_with(elsewhere, "not-found", "not a repository");
    not_found(repo.merge("nope".to_owned(), "x".to_owned(), vec![]).await, "nope");

    // workspaces
    occupied_at(vcs::init(REPO).await, "/repo");
    occupied_at(repo.add(TAKEN, &first).await, "/taken");
    not_found(repo.add("./fresh", "nope").await, "nope");
    let main = repo.remove().await.expect_err("the main working tree stays");
    fails_with(main, "other", "main working tree");

    // remotes
    occupied_at(vcs::clone(url, TAKEN, None).await, "/taken");
    not_found(vcs::clone(missing, "./clone-missing", None).await, missing);
    let dialled = vcs::clone(unanswered, "./clone-unanswered", None).await;
    let dialled = dialled.expect_err("a remote nothing answers");
    assert_eq!(dialled.code(), "unavailable", "{dialled:?}");
    not_found(repo.fetch("nope").await, "nope");
    not_found(repo.set_bookmark("lbl".to_owned(), "nope".to_owned(), None).await, "nope");
    not_found(repo.push("nope", "main").await, "nope");
    not_found(repo.remote_bookmark("nope", "main").await, "nope");

    // a remote is a name the repository's configuration records: an address
    // is none, and nor is a name only a legacy remote file defines, so the
    // URL nothing answers is never dialled
    not_found(repo.fetch(unanswered).await, unanswered);
    not_found(repo.push(unanswered, "main").await, unanswered);
    fs::create_dir_all("repo/.git/remotes").expect("the legacy remotes directory");
    fs::write("repo/.git/remotes/legacy", format!("URL: {unanswered}\n")).expect("a legacy remote");
    fs::create_dir_all("repo/.git/branches").expect("the legacy branches directory");
    fs::write("repo/.git/branches/old", format!("{unanswered}\n")).expect("a legacy branch remote");
    for name in ["legacy", "old"] {
        not_found(repo.fetch(name).await, name);
        not_found(repo.push(name, "main").await, name);
    }
    let clone = vcs::clone(url, "./clone", None).await.expect("clone").expect("a new workspace");
    not_found(clone.push("origin", "nolabel").await, "nolabel");
    let unseen = clone.remote_bookmark("origin", "nolabel").await.expect("remote bookmark");
    assert_eq!(unseen, None);

    // a guest string git would read as an option, or the empty one it reads
    // as a default, is nothing the repository knows — refused before any git
    assert_eq!(repo.resolve(String::new()).await.expect("resolve"), None);
    assert_eq!(repo.resolve("-x".to_owned()).await.expect("resolve"), None);
    not_found(repo.merge(String::new(), "m".to_owned(), vec![]).await, "");
    not_found(repo.add("./dashed", "-x").await, "-x");
    not_found(repo.set_bookmark("-b".to_owned(), first.clone(), None).await, "-b");
    not_found(repo.fetch("--upload-pack=sh").await, "--upload-pack=sh");
    assert_eq!(repo.remote_bookmark("origin", "-x").await.expect("remote bookmark"), None);
    not_found(repo.push("--receive-pack=sh", "main").await, "--receive-pack=sh");
    not_found(vcs::clone("-x", "./dash-clone", None).await, "-x");

    // two clones bookmark one name at commits of their own: the second's push
    // is refused while the remote's tip is unknown to it, and again once
    // fetched, since the bookmark is behind the remote's either way
    let clone_a = vcs::clone(url, "./clone-a", None).await.expect("clone a").expect("a new one");
    let clone_b = vcs::clone(url, "./clone-b", None).await.expect("clone b").expect("a new one");
    fs::write("clone-a/a.txt", "a\n").expect("writing a's file");
    let ours = clone_a.commit("a's".to_owned()).await.expect("commit").expect("a's commit");
    let moved = clone_a.set_bookmark("emery/x".to_owned(), ours.clone(), None).await;
    moved.expect("bookmark a").expect("no precondition to fail");
    clone_a.push("origin", "emery/x").await.expect("push a").expect("a forward push");
    fs::write("clone-b/b.txt", "b\n").expect("writing b's file");
    let theirs = clone_b.commit("b's".to_owned()).await.expect("commit").expect("b's commit");
    let moved = clone_b.set_bookmark("emery/x".to_owned(), theirs, None).await;
    moved.expect("bookmark b").expect("no precondition to fail");
    let diverged =
        clone_b.push("origin", "emery/x").await.expect("push b").expect_err("fetch first");
    assert_eq!(diverged.current, None, "b has not seen the remote's bookmark");
    clone_b.fetch("origin").await.expect("fetch");
    let pushed = clone_b.push("origin", "emery/x").await.expect("push b");
    let diverged = pushed.expect_err("non-fast-forward");
    assert_eq!(diverged.current, Some(ours), "the remote's bookmark as b fetched it");
}
