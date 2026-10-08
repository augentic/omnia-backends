//! The guest half of the containment the backend owes: every program the
//! repository's own configuration could choose to run on the host is
//! neutralised before it runs, and every path it could point git at stays
//! the place. The host lays a repository salted with a hostile
//! `core.hooksPath`, `core.fsmonitor`, `core.alternateRefsCommand`,
//! `core.worktree`, signing program, smudge filter, and remote receive-pack,
//! a working copy of its history that would write its checked-out branch on
//! receive through a filter and monitor of its own, a bare origin with its
//! own hostile hooks and alternate-refs command, a
//! repository with no identity to seal a merge, one whose own `insteadOf`
//! would carry an `https` remote onto a local path, one whose own `http.proxy`
//! and `http.sslVerify` would carry the host's credentials through a proxy of
//! its choosing, one naming the pack program the operator's ssh would run on
//! a far host, and a partial clone whose promisor remote is an `ext::`
//! command; this guest drives the operations that would fire each, and the
//! host asserts no marker was written, nothing was carried or read from
//! outside the place, the ssh was handed git's own programs, and the work
//! still landed.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs::{Error, Vcs as _, WasiVcs};

omnia_sdk::command!(scenario);

const REPO: &str = "./repo";
const WORK: &str = "./work";
const NOIDENT: &str = "./noident";
const REWRITER: &str = "./rewriter";
const PROXIED: &str = "./proxied";
const PACKER: &str = "./packer";
const LAZY: &str = "./lazy";

async fn scenario() {
    repo_config_runs_nothing().await;
    a_failed_merge_leaves_no_merge().await;
    a_shaping_repository_is_refused().await;
    a_repository_names_no_pack_program().await;
    a_lazy_fetch_reaches_no_command().await;
}

// The repository's `remote.evil.uploadPack` and `receivePack` name what the
// operator's ssh would run on the far host; the program git sends is pinned
// on the command line instead, and the operation fails on the host's ssh,
// which answers nothing.
async fn a_repository_names_no_pack_program() {
    for result in [WasiVcs.fetch(PACKER, "evil").await, WasiVcs.push(PACKER, "evil", "main").await]
    {
        assert!(matches!(result, Err(Error::Access(_))), "the host's ssh answered: {result:?}");
    }
}

// The repository's promisor remote is an `ext::` command, allowed by its own
// `protocol.ext.allow`; a checkout that needs a blob it lacks fails rather
// than fetch it through that command.
async fn a_lazy_fetch_reaches_no_command() {
    let head = WasiVcs.head(LAZY).await.expect("head");
    let added = WasiVcs.add(LAZY, "./lazy-copy", &head).await;
    assert!(added.is_err(), "a blob only the ext remote could supply: {added:?}");
}

// One repository's `url.<local>.insteadOf=https://` would turn its `https`
// remote into a local path the protocol rule then lets through; another's
// `http.proxy` with `http.sslVerify=false` would carry the host's credentials
// for the real remote through a proxy of its own. A repository that sets how
// a transport runs is refused on every transport instead, the key named.
async fn a_shaping_repository_is_refused() {
    for (repo, key) in [(REWRITER, "insteadof"), (PROXIED, "http.")] {
        for result in [WasiVcs.fetch(repo, "evil").await, WasiVcs.push(repo, "evil", "main").await]
        {
            match result {
                Err(Error::Other(message)) => {
                    assert!(message.contains(key), "{key} is named: {message}");
                }
                other => panic!("{repo} is refused, not {other:?}"),
            }
        }
    }
}

async fn repo_config_runs_nothing() {
    let head = WasiVcs.head(REPO).await.expect("head");

    // a working copy checked out of the hostile repository runs no smudge
    WasiVcs.add(REPO, WORK, &head).await.expect("add");

    // a commit runs no hook, fsmonitor, or signing program, reads the place
    // and not the `core.worktree` the repository points elsewhere, and seals
    fs::write("repo/b.txt", "b\n").expect("writing b");
    let pending = WasiVcs.pending(REPO).await.expect("pending");
    assert_eq!(pending.len(), 1, "one change, in the place: {pending:?}");
    assert_eq!(pending[0].path, "b.txt");
    let sealed = WasiVcs.commit(REPO, "seal").await.expect("commit").expect("a commit");
    assert_ne!(sealed, head, "the commit landed");
    assert_eq!(WasiVcs.pending(REPO).await.expect("pending"), []);

    // a push runs neither the client's receive-pack nor the origin's hooks or
    // alternate-refs command, and a fetch runs no alternate-refs command of
    // the repository's own
    WasiVcs.label(REPO, "emery/hardened", &sealed).await.expect("label");
    WasiVcs.push(REPO, "origin", "emery/hardened").await.expect("push");
    WasiVcs.fetch(REPO, "origin").await.expect("fetch");

    // a push into a working copy lands a branch it has not checked out, and
    // refuses the one it has rather than write the copy's tree through its
    // own filters and monitor, as its `receive.denyCurrentBranch` asks
    WasiVcs.push(REPO, "checked", "emery/hardened").await.expect("push to a working copy");
    let current = WasiVcs.push(REPO, "checked", "main").await;
    assert!(matches!(current, Err(Error::Other(_))), "the checked-out branch stays: {current:?}");
}

async fn a_failed_merge_leaves_no_merge() {
    let before = WasiVcs.head(NOIDENT).await.expect("head");
    let merged = WasiVcs.merge(NOIDENT, "feature", "merge", &[]).await;
    assert!(merged.is_err(), "a merge with no identity cannot be sealed: {merged:?}");
    assert_eq!(WasiVcs.head(NOIDENT).await.expect("head"), before, "the head did not move");
    assert_eq!(WasiVcs.pending(NOIDENT).await.expect("pending"), [], "nothing is left staged");
    assert!(fs::metadata("noident/.git/MERGE_HEAD").is_err(), "no merge is left in progress");
}
