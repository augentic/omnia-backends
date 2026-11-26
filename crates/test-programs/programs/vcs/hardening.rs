//! Exercises Git containment against hostile repository configuration.
//!
//! The host prepares repositories that attempt to execute or redirect
//! through:
//!
//! - Hooks, monitors, filters, signing, and alternate-reference commands.
//! - Worktree settings and an enclosing repository.
//! - URL rewrites, remote helpers, proxies, disabled TLS verification, and a
//!   remote URL across lines.
//! - SSH, remote pack commands, and a promisor remote.
//! - Credential helpers, for every URL and for one remote's host alone.
//! - Bundle URIs, the repository's own and one an origin advertises.
//! - Configuration keys whose raw bytes cannot be pinned.
//! - A bare repository nested inside a sealed repository.
//! - Local addresses outside the guest's mounts, beneath a read-only one, or
//!   naming no repository exactly.
//! - Places whose `.git` file, `commondir`, alternates, or included
//!   configuration lead outside the mounts, or whose `.git` is swapped for a
//!   link out once the backend has held it.
//! - A git that, inside the boundary, tries to write the mirror it reads,
//!   read a file outside the mounts, and open a socket.
//!
//! The guest runs the operations that would trigger each vector. The host
//! verifies that no marker ran, no data or credentials escaped the place,
//! no bundle of the host's reached a repository, SSH received only Git's
//! expected commands, an HTTP host was offered the host's credentials
//! alone, the enclosing repository was untouched, no repository outside the
//! mounts or in the read-only one took a push, the boundary refused git
//! everything outside it, and valid work still completed. The operator
//! arguments are the bundling origin's URL, the path of an origin outside
//! every mount, and an inside origin's URL naming a host, its path
//! percent-encoded.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs::{self, CommitId, Workspace};
use test_programs::{arguments, fails_with, not_found};

omnia_sdk::command::export!(scenario);

const REPO: &str = "./repo";
const WORK: &str = "./work";
const NOIDENT: &str = "./noident";
const REWRITER: &str = "./rewriter";
const PROXIED: &str = "./proxied";
const HELPER: &str = "./helper";
const SPLIT: &str = "./split";
const PACKER: &str = "./packer";
const ASKING: &str = "./asking";
const LAZY: &str = "./lazy";
const BUNDLED: &str = "./bundled";
const BYTES: &str = "./bytes";
const INNER: &str = "./outer/inner";
const HOLDER: &str = "./holder";
const LEADING_OUT: [&str; 5] = [
    "./beyond-gitfile",
    "./beyond-commondir",
    "./beyond-alternate",
    "./beyond-include",
    "./beyond-swap",
];
const PROBING: &str = "./beyond-probe";
const OUTSIDE: &str = "outside the guest's mounts";

async fn scenario() {
    let arguments = arguments();
    let [bundling, outside, encoded] = arguments.as_slice() else {
        panic!("the bundling, outside, and encoded addresses are the arguments");
    };
    repo_config_runs_nothing().await;
    a_failed_merge_leaves_no_merge().await;
    a_shaping_repository_is_refused().await;
    a_repository_names_no_pack_program().await;
    a_repository_names_no_credential_helper().await;
    a_lazy_fetch_reaches_no_command().await;
    a_clone_takes_no_advertised_bundle(bundling).await;
    an_unnameable_key_is_refused().await;
    an_enclosing_repository_is_not_reached().await;
    a_local_address_is_held(outside, encoded).await;
    a_place_leading_out_is_refused().await;
}

// Each place leads git outside the guest's mounts once the backend has held
// it, through a `.git` file, a `commondir`, alternates, an included file, or
// a `.git` swapped for a link out; and the git at the last place tries to
// leave the boundary itself. The kernel refuses git each time, so the call
// fails, and nothing outside reaches the guest.
async fn a_place_leading_out_is_refused() {
    for path in LEADING_OUT {
        let sealed = match vcs::open(path).await {
            Ok(workspace) => workspace.commit("seal".to_owned()).await.map_err(Into::into),
            Err(error) => Err(error),
        };
        assert!(sealed.is_err(), "{path} sealed: {sealed:?}");
    }
    let fetched = open(PROBING).await.fetch("served").await;
    assert!(fetched.is_err(), "the probing fetch ran: {fetched:?}");
}

// Each local address a transport would dial is placed beneath the guest's
// mounts and held to the repository it names exactly, before anything is
// dialled.
async fn a_local_address_is_held(outside: &str, encoded: &str) {
    // beneath no mount, through a `.git` file or a link that leads out, or
    // where any one push URL lies outside
    let cloned = vcs::clone(outside, "./held-clone", None).await;
    fails_with(cloned.expect_err("the clone is refused"), "denied", OUTSIDE);
    let holder = open(HOLDER).await;
    for remote in ["outside", "linked", "gitfile"] {
        let fetched = holder.fetch(remote).await.expect_err("the fetch is refused");
        let pushed = holder.push(remote, "main").await.expect_err("the push is refused");
        for error in [fetched, pushed] {
            fails_with(error, "denied", OUTSIDE);
        }
    }
    let split = holder.push("split", "main").await.expect_err("the push is refused");
    fails_with(split, "denied", OUTSIDE);

    // a read-only mount's origin is fetched from but never pushed to, and a
    // gitdir holding a `.git` file of its own is fetched from as itself,
    // which a push cannot be held to
    holder.fetch("readonly").await.expect("fetch from the read-only origin");
    let pushed = holder.push("readonly", "main").await.expect_err("the push is refused");
    fails_with(pushed, "denied", "beneath no writable mount");
    holder.fetch("planted").await.expect("fetch from the planted repository");
    let pushed = holder.push("planted", "main").await.expect_err("the push is refused");
    fails_with(pushed, "denied", "a .git of its own");

    // a mount's root beside a repository of its name, a bundle, and an
    // address in place of a remote's name are no repository to dial
    not_found(holder.fetch("rooted").await, "rooted");
    not_found(holder.fetch("bundle").await, "bundle");
    not_found(holder.fetch(encoded).await, encoded);

    // a `file://` URL naming a host, its path percent-encoded, reaches the
    // origin beneath the place
    let cloned = vcs::clone(encoded, "./encoded", None).await;
    cloned.expect("clone").expect("a new workspace");
}

async fn open(path: &str) -> Workspace {
    vcs::open(path).await.unwrap_or_else(|error| panic!("open {path}: {error}"))
}

async fn head(workspace: &Workspace) -> CommitId {
    workspace.head().await.expect("head").expect("a commit")
}

// The place holds no `.git` of its own and sits inside a repository that
// does; git's search for one stops at the place, so a guest lent the
// directory alone opens no workspace there, and neither reads nor seals the
// repository around it.
async fn an_enclosing_repository_is_not_reached() {
    let error = vcs::open(INNER).await.expect_err("no workspace of its own");
    fails_with(error, "not-found", "not a repository");
}

// The repository names a clean filter under a subsection that is not UTF-8,
// which a pin spelled as text cannot name; the operation is refused rather
// than run it over the change.
async fn an_unnameable_key_is_refused() {
    let refused = open(BYTES).await.commit("seal".to_owned()).await;
    fails_with(refused.expect_err("the commit is refused"), "other", "UTF-8");
}

// The repository's `remote.evil.uploadPack` and `receivePack` name what the
// operator's ssh would run on the far host; the program git sends is pinned
// on the command line instead, and the operation fails on the host's ssh,
// which answers nothing.
async fn a_repository_names_no_pack_program() {
    let packer = open(PACKER).await;
    let fetched = packer.fetch("evil").await.expect_err("the fetch fails");
    let pushed = packer.push("evil", "main").await.expect_err("the push fails");
    for error in [fetched, pushed] {
        assert_eq!(error.code(), "unavailable", "the host's ssh answered: {error:?}");
    }
}

// The repository names a credential helper for every URL and one for its
// remote's host alone. The host's own helper for that host answers the
// challenge instead, with credentials the remote refuses, and neither of the
// repository's helpers runs to supply them or to erase them.
async fn a_repository_names_no_credential_helper() {
    let fetched = open(ASKING).await.fetch("guarded").await;
    let refused = fetched.expect_err("the remote refuses the credentials");
    fails_with(refused, "denied", "Authentication failed");
}

// The repository's promisor remote is an ssh host, and the pack command the
// operator's ssh would carry there is the repository's own; a checkout that
// needs a blob it lacks fails rather than fetch it, so the ssh never runs.
async fn a_lazy_fetch_reaches_no_command() {
    let lazy = open(LAZY).await;
    let added = lazy.add("./lazy-copy", &head(&lazy).await).await;
    assert!(added.is_err(), "a blob only the promisor could supply: {added:?}");
}

// The origin advertises a bundle file on the host, and the host's own
// configuration asks a clone to take advertised bundles; the clone lands
// from the origin alone.
async fn a_clone_takes_no_advertised_bundle(url: &str) {
    let cloned = vcs::clone(url, BUNDLED, None).await;
    cloned.expect("clone").expect("a new workspace");
}

// A repository that sets how a transport runs is refused on every transport,
// the key named. Each would otherwise carry its remote somewhere else:
//
// - `url.<local>.insteadOf=https://` turns an `https` remote into a local
//   path the protocol rule then lets through;
// - `http.proxy` with `http.sslVerify=false` carries the host's credentials
//   for the real remote through a proxy of its own;
// - `remote.evil.vcs` hands the remote to a helper that reads the URL its
//   own way;
// - a `remote.evil.url` across lines is dialled whole, though its listing
//   splits it into two URLs.
async fn a_shaping_repository_is_refused() {
    let shaping = [
        (REWRITER, "insteadof"),
        (PROXIED, "http."),
        (HELPER, "remote.evil.vcs"),
        (SPLIT, "remote.evil.url"),
    ];
    for (path, key) in shaping {
        let repo = open(path).await;
        let fetched = repo.fetch("evil").await.expect_err("the fetch is refused");
        let pushed = repo.push("evil", "main").await.expect_err("the push is refused");
        for error in [fetched, pushed] {
            fails_with(error, "other", key);
        }
    }
}

async fn repo_config_runs_nothing() {
    let repo = open(REPO).await;
    let head = head(&repo).await;

    // a revision naming an index path reads the index, and runs no fsmonitor
    let indexed = repo.resolve(":a.txt".to_owned()).await.expect("resolve");
    assert_eq!(indexed, None, "an index path names no commit");

    // a working copy checked out of the hostile repository runs no smudge
    repo.add(WORK, &head).await.expect("add").expect("a new workspace");

    // a commit runs no hook, fsmonitor, or signing program, reads the place
    // and not the `core.worktree` the repository points elsewhere, and seals
    fs::write("repo/b.txt", "b\n").expect("writing b");
    let pending = repo.status_all().await.expect("status");
    assert_eq!(pending.len(), 1, "one change, in the place: {pending:?}");
    assert_eq!(pending[0].path, "b.txt");
    let sealed = repo.commit("seal".to_owned()).await.expect("commit").expect("a commit");
    assert_ne!(sealed, head, "the commit landed");
    assert!(repo.status_all().await.expect("status").is_empty());

    // a push runs neither the client's receive-pack nor the origin's hooks or
    // alternate-refs command, signs no certificate the origin asks for, and a
    // fetch runs no alternate-refs command of the repository's own and takes
    // no bundle it names
    let moved = repo.set_bookmark("emery/hardened".to_owned(), sealed, None).await;
    moved.expect("bookmark").expect("no precondition to fail");
    repo.push("origin", "emery/hardened").await.expect("push").expect("a forward push");
    repo.fetch("origin").await.expect("fetch");

    // a push into a working copy lands a branch it has not checked out, and
    // refuses the one it has rather than write the copy's tree through its
    // own filters and monitor, as its `receive.denyCurrentBranch` asks
    let landed = repo.push("checked", "emery/hardened").await.expect("push to a working copy");
    landed.expect("a forward push");
    let current = repo.push("checked", "main").await;
    assert!(
        matches!(&current, Err(error) if error.code() == "other"),
        "the checked-out branch stays: {current:?}"
    );
}

async fn a_failed_merge_leaves_no_merge() {
    let noident = open(NOIDENT).await;
    let before = head(&noident).await;
    let merged = noident.merge("feature".to_owned(), "merge".to_owned(), vec![]).await;
    assert!(merged.is_err(), "a merge with no identity cannot be sealed: {merged:?}");
    assert_eq!(head(&noident).await, before, "the head did not move");
    let pending = noident.status_all().await.expect("status");
    assert!(pending.is_empty(), "nothing is left staged: {pending:?}");
}
