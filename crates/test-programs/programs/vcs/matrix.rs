//! Exercises Git store and workspace operations from a guest.
//!
//! The matrix covers:
//!
//! - Repository creation, reads, commits, copies, bookmarks, and merges.
//! - Merge conflicts and policy-based resolution.
//! - Logs, ancestry checks, and branch-versus-tag bookmark lookup.
//! - Every kind of pending change.
//! - Empty commits and removal of dirty working copies.
//! - A working copy whose `.git` file was rewritten.
//!
//! Each section uses its own repository beneath the `.` mount for host-side
//! inspection. Operator arguments identify the tagged repository and its
//! branch and tag commits.

#![cfg(target_arch = "wasm32")]

use std::fs;
use std::path::Path;

use omnia_sdk::vcs::{self, ChangeKind, CommitId, Rule, Strategy, Unmerged, Workspace, types};
use test_programs::{arguments, not_found};

omnia_sdk::command::export!(scenario);

async fn scenario() {
    let arguments = arguments();
    let [shadowed, branch, tag] = arguments.as_slice() else {
        panic!(
            "the repository the host laid a tag in, its bookmark's commit, and its tag's are the arguments"
        );
    };

    init_commit_add_merge().await;
    merge_conflict_without_rule().await;
    merge_policy_resolves().await;
    merge_policy_beats_attributes().await;
    merge_policy_modify_delete().await;
    merge_policy_delete_modify().await;
    merge_policy_one_sided().await;
    merge_policy_rename().await;
    merge_policy_binary().await;
    merge_up_to_date().await;
    log_over_base().await;
    ancestry_and_bookmarks().await;
    bookmark_under_a_tag(&format!("./{shadowed}"), branch, tag).await;
    bookmark_the_checked_out_branch().await;
    pending_kinds().await;
    commit_nothing().await;
    remove_dirty_worktree().await;
    worktree_gitdir_pinned().await;
}

fn write(path: &str, content: &str) {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent).expect("creating parent");
    }
    fs::write(path, content).expect("writing");
}

fn read(path: &str) -> String {
    fs::read_to_string(path).expect("reading")
}

fn change(path: &str, kind: ChangeKind) -> (String, ChangeKind) {
    (path.to_owned(), kind)
}

fn rule(pattern: &str, strategy: Strategy) -> Rule {
    Rule {
        pattern: pattern.to_owned(),
        strategy,
    }
}

async fn init(path: &str) -> Workspace {
    vcs::init(path).await.expect("init").expect("a new workspace")
}

async fn add(repo: &Workspace, path: &str, revision: &str) -> Workspace {
    repo.add(path, revision).await.expect("add").expect("a new workspace")
}

async fn head(workspace: &Workspace) -> CommitId {
    workspace.head().await.expect("head").expect("a commit")
}

async fn resolve(workspace: &Workspace, revision: &str) -> CommitId {
    workspace.resolve(revision.to_owned()).await.expect("resolve").expect("a commit")
}

async fn commit(workspace: &Workspace, message: &str) -> CommitId {
    workspace.commit(message.to_owned()).await.expect("commit").expect("a commit")
}

async fn pending(workspace: &Workspace) -> Vec<(String, ChangeKind)> {
    let changes = workspace.status_all().await.expect("status");
    changes.into_iter().map(|change| (change.path, change.kind)).collect()
}

async fn bookmark(workspace: &Workspace, name: &str, revision: &str) {
    let moved = workspace.set_bookmark(name.to_owned(), revision.to_owned(), None).await;
    moved.expect("bookmark").expect("no precondition to fail");
}

async fn merge(
    workspace: &Workspace, revision: &str, message: &str, policy: &[Rule],
) -> Result<CommitId, Unmerged> {
    let merged = workspace.merge(revision.to_owned(), message.to_owned(), policy.to_vec()).await;
    merged.expect("merge")
}

async fn log(workspace: &Workspace, revision: &str, base: &str) -> Vec<(String, String)> {
    let entries = workspace.log_all(revision, Some(base)).await.expect("log");
    entries.into_iter().map(|entry| (entry.id, entry.message)).collect()
}

// a repository with one committed file, and its first commit
async fn seeded(path: &str) -> (Workspace, CommitId) {
    let repo = init(path).await;
    write(&format!("{path}/a.txt"), "a\n");
    let first = commit(&repo, "a").await;
    (repo, first)
}

async fn init_commit_add_merge() {
    let repo = init("./repo").await;
    assert_eq!(repo.head().await.expect("head"), None, "no commit yet");
    assert_eq!(pending(&repo).await, []);

    write("repo/a.txt", "a\n");
    assert_eq!(pending(&repo).await, [change("a.txt", ChangeKind::Added)]);
    let first = commit(&repo, "a").await;
    assert_eq!(head(&repo).await, first);
    assert_eq!(resolve(&repo, &first).await, first, "the whole id");
    assert_eq!(resolve(&repo, &first[..8]).await, first, "a prefix");
    assert_eq!(pending(&repo).await, []);

    // a detached working copy at the first commit, committed on its own
    let work = add(&repo, "./work", &first).await;
    assert_eq!(read("work/a.txt"), "a\n");
    assert_eq!(head(&work).await, first);
    write("work/b.txt", "b\n");
    let second = commit(&work, "b").await;
    assert_ne!(second, first);
    assert_eq!(head(&repo).await, first, "the repository's head stays");

    // bookmarked and merged back by bookmark
    bookmark(&repo, "feature", &second).await;
    assert_eq!(resolve(&repo, "feature").await, second);
    let merge = merge(&repo, "feature", "merge feature", &[]).await.expect("a merge commit");
    assert_eq!(head(&repo).await, merge);
    assert_eq!(read("repo/b.txt"), "b\n");
}

async fn merge_conflict_without_rule() {
    let (repo, first) = seeded("./conflict").await;

    let work = add(&repo, "./conflict-work", &first).await;
    write("conflict-work/a.txt", "work\n");
    let theirs = commit(&work, "work").await;
    write("conflict/a.txt", "repo\n");
    let ours = commit(&repo, "repo").await;

    let unmerged = merge(&repo, &theirs, "merge", &[]).await.expect_err("a conflict is data");
    let Unmerged::Conflicts(conflicts) = unmerged else {
        panic!("expected conflicts, got {unmerged:?}");
    };
    assert_eq!(conflicts, ["a.txt"]);

    // the working copy is back on its head, nothing pending
    assert_eq!(head(&repo).await, ours);
    assert_eq!(read("conflict/a.txt"), "repo\n");
    assert_eq!(pending(&repo).await, []);
}

async fn merge_policy_resolves() {
    let repo = init("./policy").await;
    write("policy/Cargo.lock", "lock 0\n");
    write("policy/a.txt", "a 0\n");
    write("policy/list.txt", "base\n");
    let first = commit(&repo, "base").await;

    let work = add(&repo, "./policy-work", &first).await;
    write("policy-work/Cargo.lock", "lock work\n");
    write("policy-work/a.txt", "a work\n");
    write("policy-work/list.txt", "base\nwork\n");
    let theirs = commit(&work, "work").await;
    write("policy/Cargo.lock", "lock repo\n");
    write("policy/a.txt", "a repo\n");
    write("policy/list.txt", "base\nrepo\n");
    commit(&repo, "repo").await;

    // a message git would read as commentary alone is the message all the same
    let policy = [
        rule("*.lock", Strategy::Ours),
        rule("a.txt", Strategy::Theirs),
        rule("list.txt", Strategy::Union),
    ];
    merge(&repo, &theirs, "# merge under policy", &policy).await.expect("the merge sealed");
    assert_eq!(read("policy/Cargo.lock"), "lock repo\n", "ours keeps the working copy's side");
    assert_eq!(read("policy/a.txt"), "a work\n", "theirs takes the merged-in side");
    assert_eq!(read("policy/list.txt"), "base\nrepo\nwork\n", "union keeps both");
    assert_eq!(pending(&repo).await, []);
}

// A repository's own `merge=union` attribute would auto-resolve the path with
// no conflict to act on; the guest's policy is resolved from the commits and
// wins regardless.
async fn merge_policy_beats_attributes() {
    let repo = init("./attributed").await;
    write("attributed/.gitattributes", "data.txt merge=union\n");
    write("attributed/data.txt", "base\n");
    let base = commit(&repo, "base").await;
    let work = add(&repo, "./attributed-work", &base).await;
    write("attributed-work/data.txt", "theirs\n");
    let theirs = commit(&work, "theirs").await;
    write("attributed/data.txt", "ours\n");
    commit(&repo, "ours").await;

    let policy = [rule("data.txt", Strategy::Theirs)];
    merge(&repo, &theirs, "merge", &policy).await.expect("the merge sealed");
    assert_eq!(read("attributed/data.txt"), "theirs\n", "the policy beat the attribute");
}

// The chain over a base follows first parents alone, newest first, so a
// merge is one entry and the side it brought in none; the base is left out,
// and a message is read back as it was sealed, its own `#` line kept and a
// conflict the policy resolved leaving no hint of itself in it.
async fn log_over_base() {
    let (repo, base) = seeded("./logged").await;
    let work = add(&repo, "./logged-work", &base).await;
    write("logged-work/a.txt", "theirs\n");
    write("logged-work/b.txt", "b\n");
    let side = commit(&work, "side").await;
    bookmark(&repo, "slice", &side).await;
    write("logged/a.txt", "ours\n");
    let ours = commit(&repo, "ours").await;
    let message = "merge slice\n\n# notes\nSlice: SLICE-001\nWave: 1";
    let policy = [rule("a.txt", Strategy::Theirs)];
    let merge = merge(&repo, "slice", message, &policy).await.expect("the policy sealed the merge");
    write("logged/c.txt", "c\n");
    let after = commit(&repo, "after").await;

    // the first-parent chain: the merged-in side is not walked
    assert_eq!(
        log(&repo, &after, &base).await,
        [
            (after.clone(), "after".to_owned()),
            (merge.clone(), message.to_owned()),
            (ours.clone(), "ours".to_owned()),
        ]
    );
    assert_eq!(log(&repo, &after, &ours).await.len(), 2, "a base along the chain cuts it there");
    assert_eq!(
        log(&repo, &merge, &ours).await,
        [(merge, message.to_owned())],
        "a merge counts once, its message whole"
    );
    assert!(log(&repo, &base, &base).await.is_empty(), "nothing over itself");
    not_found(repo.log_all("nope", Some(&base)).await, "nope");
    not_found(repo.log_all(&after, Some("nope")).await, "nope");
}

// Ancestry is a question of the graph, a commit its own ancestor and the
// reversed pair none; a bookmark is read back from the branch `set-bookmark`
// wrote, by its name alone, never by what git's revision syntax would make
// of it.
async fn ancestry_and_bookmarks() {
    let (repo, base) = seeded("./lineage").await;
    let work = add(&repo, "./lineage-work", &base).await;
    write("lineage-work/b.txt", "b\n");
    let side = commit(&work, "side").await;
    bookmark(&repo, "slice", &side).await;

    assert!(is_ancestor(&repo, &base, &side).await.expect("is-ancestor"));
    assert!(is_ancestor(&repo, &base, &base).await.expect("is-ancestor"), "itself included");
    assert!(!is_ancestor(&repo, &side, &base).await.expect("is-ancestor"), "the reversed pair");
    not_found(is_ancestor(&repo, "nope", &base).await, "nope");
    assert_eq!(repo.bookmark("slice".to_owned()).await.expect("bookmark"), Some(side));
    for name in ["nope", "slice~1", "slice^", "slice@{0}"] {
        let found = repo.bookmark(name.to_owned()).await.expect("bookmark");
        assert_eq!(found, None, "`{name}` names no bookmark");
    }
}

async fn is_ancestor(
    repo: &Workspace, ancestor: &str, descendant: &str,
) -> Result<bool, types::Error> {
    repo.is_ancestor(ancestor.to_owned(), descendant.to_owned()).await
}

// The host laid a branch and a tag of one name at two commits; the bookmark
// is read back as the branch whichever of the two the bare spelling resolves
// to.
async fn bookmark_under_a_tag(path: &str, branch: &str, tag: &str) {
    let repo = vcs::open(path).await.expect("open");
    let found = repo.bookmark("slice".to_owned()).await.expect("bookmark");
    assert_eq!(found.as_deref(), Some(branch));
    let bare = resolve(&repo, "slice").await;
    assert!(bare == branch || bare == tag, "the bare spelling is one of the two: {bare}");
}

// One side edited the path and the other deleted it: keeping the deleting side
// removes the path.
async fn merge_policy_modify_delete() {
    let repo = init("./moddel").await;
    write("moddel/keep.txt", "base\n");
    let base = commit(&repo, "base").await;
    let work = add(&repo, "./moddel-work", &base).await;
    fs::remove_file("moddel-work/keep.txt").expect("delete on the merged-in side");
    let theirs = commit(&work, "delete").await;
    write("moddel/keep.txt", "ours\n");
    commit(&repo, "edit").await;

    let policy = [rule("keep.txt", Strategy::Theirs)];
    merge(&repo, &theirs, "merge", &policy).await.expect("the merge sealed");
    assert!(fs::metadata("moddel/keep.txt").is_err(), "the deleting side was kept");
    assert!(pending(&repo).await.is_empty(), "the copy is sealed");
}

// The working copy deleted the path and the merged-in side edited it: `ours`
// keeps the deletion, over the edited version the merge staged.
async fn merge_policy_delete_modify() {
    let repo = init("./delmod").await;
    write("delmod/gone.txt", "base\n");
    let base = commit(&repo, "base").await;
    let work = add(&repo, "./delmod-work", &base).await;
    write("delmod-work/gone.txt", "theirs\n");
    let theirs = commit(&work, "edit").await;
    fs::remove_file("delmod/gone.txt").expect("delete on the working copy's side");
    commit(&repo, "delete").await;

    let policy = [rule("gone.txt", Strategy::Ours)];
    merge(&repo, &theirs, "merge", &policy).await.expect("the merge sealed");
    assert!(fs::metadata("delmod/gone.txt").is_err(), "the deleting side was kept");
    assert!(pending(&repo).await.is_empty(), "the copy is sealed");
}

// A path one side alone added is git's clean merge: a rule naming it keeps
// nothing from the side that lacks it, whichever side that is.
async fn merge_policy_one_sided() {
    let (repo, base) = seeded("./onesided").await;
    let work = add(&repo, "./onesided-work", &base).await;
    write("onesided-work/theirs-only.txt", "theirs\n");
    let theirs = commit(&work, "theirs").await;
    write("onesided/ours-only.txt", "ours\n");
    commit(&repo, "ours").await;

    let policy = [rule("theirs-only.txt", Strategy::Ours), rule("ours-only.txt", Strategy::Theirs)];
    merge(&repo, &theirs, "merge", &policy).await.expect("the merge sealed");
    assert_eq!(read("onesided/theirs-only.txt"), "theirs\n", "the merged-in side's addition stays");
    assert_eq!(read("onesided/ours-only.txt"), "ours\n", "the working copy's addition stays");
    assert!(pending(&repo).await.is_empty(), "the copy is sealed");
}

// a repository whose working copy edited `old.txt` while the merged-in side
// renamed it to `new.txt`, and the merged-in commit
async fn renamed(path: &str, work_path: &str) -> (Workspace, CommitId) {
    let repo = init(path).await;
    write(&format!("{path}/old.txt"), "base\n");
    let base = commit(&repo, "base").await;
    let work = add(&repo, work_path, &base).await;
    fs::rename(format!("{work_path}/old.txt"), format!("{work_path}/new.txt"))
        .expect("rename on their side");
    let theirs = commit(&work, "rename").await;
    write(&format!("{path}/old.txt"), "ours\n");
    commit(&repo, "edit").await;
    (repo, theirs)
}

// The merged-in side renamed a path the working copy edited, so git carries
// the edit to the new name: a rule over both names keeps one side whole,
// `ours` the old name with its bytes and `theirs` the new name with its, not
// the old name restored beside git's blend of the new.
async fn merge_policy_rename() {
    let (repo, theirs) = renamed("./renamed", "./renamed-work").await;
    let policy = [rule("*.txt", Strategy::Ours)];
    merge(&repo, &theirs, "merge", &policy).await.expect("the merge sealed");
    assert_eq!(read("renamed/old.txt"), "ours\n");
    assert!(fs::metadata("renamed/new.txt").is_err(), "the new name was kept beside the old");
    assert!(pending(&repo).await.is_empty(), "the copy is sealed");

    let (repo, theirs) = renamed("./renamed-theirs", "./renamed-theirs-work").await;
    let policy = [rule("*.txt", Strategy::Theirs)];
    merge(&repo, &theirs, "merge", &policy).await.expect("the merge sealed");
    assert_eq!(
        read("renamed-theirs/new.txt"),
        "base\n",
        "git's blend stood where theirs' bytes go"
    );
    assert!(fs::metadata("renamed-theirs/old.txt").is_err(), "the old name stayed");
    assert!(pending(&repo).await.is_empty(), "the copy is sealed");
}

// A blob no text encoding holds: a `union` over it cannot be sealed and leaves
// the copy on its head with nothing of the attempt behind, and `theirs` lays
// it byte for byte.
async fn merge_policy_binary() {
    let ours_bytes: Vec<u8> = vec![0, 1, 2, 0xff, 0xfe, b'\n', 0x80];
    let theirs_bytes: Vec<u8> = vec![0, 1, 3, 0xff, 0xc0, b'\n', 0x81];
    let repo = init("./binary").await;
    fs::write("binary/blob.bin", [0u8, 1, 0xff]).expect("writing the base blob");
    let base = commit(&repo, "base").await;
    let work = add(&repo, "./binary-work", &base).await;
    fs::write("binary-work/blob.bin", &theirs_bytes).expect("writing theirs");
    let theirs = commit(&work, "theirs").await;
    fs::write("binary/blob.bin", &ours_bytes).expect("writing ours");
    let ours = commit(&repo, "ours").await;

    let union = vec![rule("*.bin", Strategy::Union)];
    let refused = repo.merge(theirs.clone(), "merge".to_owned(), union).await;
    assert!(
        matches!(refused, Err(types::Error::Other(_))),
        "a binary has no lines to union: {refused:?}"
    );
    assert_eq!(head(&repo).await, ours, "the head did not move");
    assert!(pending(&repo).await.is_empty(), "nothing of the attempt is left");
    assert_eq!(
        fs::read("binary/blob.bin").expect("the blob"),
        ours_bytes,
        "the tree is ours again"
    );

    let policy = [rule("*.bin", Strategy::Theirs)];
    merge(&repo, &theirs, "merge", &policy).await.expect("the merge sealed");
    assert_eq!(fs::read("binary/blob.bin").expect("the blob"), theirs_bytes, "byte for byte");
}

// Bookmarking the branch the working copy sits on moves the branch and leaves
// the copy on its sealed commit.
async fn bookmark_the_checked_out_branch() {
    let (repo, first) = seeded("./checked-out").await;
    write("checked-out/b.txt", "b\n");
    let second = commit(&repo, "second").await;
    bookmark(&repo, "main", &first).await;
    assert_eq!(resolve(&repo, "main").await, first, "main moved");
    assert_eq!(head(&repo).await, second, "the working copy stayed");
}

async fn merge_up_to_date() {
    let (repo, first) = seeded("./ancestor").await;
    let merged = merge(&repo, &first, "nothing", &[]).await.expect("merging an ancestor");
    assert_eq!(merged, first);
}

async fn pending_kinds() {
    let repo = init("./kinds").await;
    write("kinds/a.txt", "a\n");
    write("kinds/b.txt", "b\n");
    commit(&repo, "ab").await;

    write("kinds/a.txt", "a changed\n");
    fs::remove_file("kinds/b.txt").expect("deleting b");
    write("kinds/c.txt", "c\n");
    write("kinds/nested/deep/d.txt", "d\n");
    assert_eq!(
        pending(&repo).await,
        [
            change("a.txt", ChangeKind::Modified),
            change("b.txt", ChangeKind::Deleted),
            change("c.txt", ChangeKind::Added),
            change("nested/deep/d.txt", ChangeKind::Added),
        ]
    );

    // everything pending seals as one commit
    commit(&repo, "all").await;
    assert_eq!(pending(&repo).await, []);
}

async fn commit_nothing() {
    let (repo, first) = seeded("./nothing").await;
    assert_eq!(repo.commit("nothing".to_owned()).await.expect("commit"), None);
    assert_eq!(head(&repo).await, first);
}

async fn remove_dirty_worktree() {
    let (repo, first) = seeded("./dirty").await;
    let work = add(&repo, "./copies/work", &first).await;
    write("copies/work/dirty.txt", "unsealed\n");
    write("copies/work/a.txt", "changed\n");

    work.remove().await.expect("remove");
    assert!(fs::metadata("copies/work").is_err(), "the files went with the working copy");
    assert_eq!(read("dirty/a.txt"), "a\n", "the repository is untouched");
}

// A guest lent the copy rewrites its `.git` file to point at a repository
// it laid beneath; the copy stays addressed through the gitdir it was cut
// with, so the fake repository is an untracked directory the commit seals
// as a gitlink, never the repository the copy's head or commit reaches.
// The copy stays for the host to read the sealed commit from the gitdir.
async fn worktree_gitdir_pinned() {
    let (repo, base) = seeded("./pinned").await;
    let work = add(&repo, "./pinned-work", &base).await;

    let fake = init("./pinned-work/fake").await;
    write("pinned-work/fake/decoy.txt", "decoy\n");
    let decoy = commit(&fake, "fake").await;
    write("pinned-work/.git", "gitdir: fake/.git\n");

    assert_eq!(head(&work).await, base, "the copy's own head");
    write("pinned-work/b.txt", "b\n");
    let pending = pending(&work).await;
    assert!(pending.contains(&change("b.txt", ChangeKind::Added)), "{pending:?}");
    let sealed = commit(&work, "sealed").await;
    assert_ne!(sealed, decoy);
    assert_eq!(head(&work).await, sealed);
    assert_eq!(head(&fake).await, decoy, "the fake is untouched");
    assert_eq!(head(&repo).await, base, "the repository's head stays");
}
