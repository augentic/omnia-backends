//! The store and workspace operations over git, from a guest: a repository
//! initialised, read, committed, copied, labelled, and merged back; a merge
//! that conflicts and one resolved under a policy; the log over a base;
//! whether one commit descends from another, and a label read back as the
//! branch alone where the host laid a tag of its name; what `pending`
//! reports for each kind of change; a commit with nothing to seal; a dirty
//! working copy removed. Each section runs in a repository of its own
//! beneath the `.` mount, so the host reads each with git afterwards; the
//! arguments name the repository the host laid with the tag, the commit
//! its label sits at, and the commit its tag does.

#![cfg(target_arch = "wasm32")]

use std::fs;
use std::path::Path;

use omnia_sdk::vcs::{Change, ChangeKind, Entry, Error, Rule, Strategy, Vcs as _, WasiVcs};
use test_programs::arguments;

omnia_sdk::command!(scenario);

async fn scenario() {
    let arguments = arguments();
    let [shadowed, branch, tag] = arguments.as_slice() else {
        panic!(
            "the repository the host laid a tag in, its label's commit, and its tag's are the arguments"
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
    descends_and_labelled().await;
    labelled_under_a_tag(&format!("./{shadowed}"), branch, tag).await;
    label_the_checked_out_branch().await;
    pending_kinds().await;
    commit_nothing().await;
    remove_dirty_worktree().await;
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

fn change(path: &str, kind: ChangeKind) -> Change {
    Change {
        path: path.to_owned(),
        kind,
    }
}

fn rule(paths: &str, strategy: Strategy) -> Rule {
    Rule {
        paths: paths.to_owned(),
        strategy,
    }
}

// a repository with one committed file, and its first commit
async fn seeded(repo: &str) -> String {
    WasiVcs.init(repo).await.expect("init");
    write(&format!("{repo}/a.txt"), "a\n");
    WasiVcs.commit(repo, "a").await.expect("commit").expect("a commit")
}

async fn init_commit_add_merge() {
    let repo = "./repo";
    WasiVcs.init(repo).await.expect("init");
    assert!(matches!(WasiVcs.head(repo).await, Err(Error::NotFound(_))), "no commit yet");
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), []);

    write("repo/a.txt", "a\n");
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), [change("a.txt", ChangeKind::Added)]);
    let first = WasiVcs.commit(repo, "a").await.expect("commit").expect("a commit");
    assert_eq!(WasiVcs.head(repo).await.expect("head"), first);
    assert_eq!(WasiVcs.resolve(repo, &first).await.expect("whole id"), first);
    assert_eq!(WasiVcs.resolve(repo, &first[..8]).await.expect("prefix"), first);
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), []);

    // a detached working copy at the first commit, committed on its own
    let work = "./work";
    WasiVcs.add(repo, work, &first).await.expect("add");
    assert_eq!(read("work/a.txt"), "a\n");
    assert_eq!(WasiVcs.head(work).await.expect("head"), first);
    write("work/b.txt", "b\n");
    let second = WasiVcs.commit(work, "b").await.expect("commit").expect("b");
    assert_ne!(second, first);
    assert_eq!(WasiVcs.head(repo).await.expect("head"), first, "the repository's head stays");

    // labelled and merged back by label
    WasiVcs.label(repo, "feature", &second).await.expect("label");
    assert_eq!(WasiVcs.resolve(repo, "feature").await.expect("resolve"), second);
    let merged = WasiVcs.merge(repo, "feature", "merge feature", &[]).await.expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    let merge = merged.commit.expect("a merge commit");
    assert_eq!(WasiVcs.head(repo).await.expect("head"), merge);
    assert_eq!(read("repo/b.txt"), "b\n");
}

async fn merge_conflict_without_rule() {
    let repo = "./conflict";
    let first = seeded(repo).await;

    let work = "./conflict-work";
    WasiVcs.add(repo, work, &first).await.expect("add");
    write("conflict-work/a.txt", "work\n");
    let theirs = WasiVcs.commit(work, "work").await.expect("commit").expect("work");
    write("conflict/a.txt", "repo\n");
    let ours = WasiVcs.commit(repo, "repo").await.expect("commit").expect("repo");

    let merged = WasiVcs.merge(repo, &theirs, "merge", &[]).await.expect("a conflict is data");
    assert_eq!(merged.commit, None);
    assert_eq!(merged.conflicts, ["a.txt"]);

    // the working copy is back on its head, nothing pending
    assert_eq!(WasiVcs.head(repo).await.expect("head"), ours);
    assert_eq!(read("conflict/a.txt"), "repo\n");
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), []);
}

async fn merge_policy_resolves() {
    let repo = "./policy";
    WasiVcs.init(repo).await.expect("init");
    write("policy/Cargo.lock", "lock 0\n");
    write("policy/a.txt", "a 0\n");
    write("policy/list.txt", "base\n");
    let first = WasiVcs.commit(repo, "base").await.expect("commit").expect("base");

    let work = "./policy-work";
    WasiVcs.add(repo, work, &first).await.expect("add");
    write("policy-work/Cargo.lock", "lock work\n");
    write("policy-work/a.txt", "a work\n");
    write("policy-work/list.txt", "base\nwork\n");
    let theirs = WasiVcs.commit(work, "work").await.expect("commit").expect("work");
    write("policy/Cargo.lock", "lock repo\n");
    write("policy/a.txt", "a repo\n");
    write("policy/list.txt", "base\nrepo\n");
    WasiVcs.commit(repo, "repo").await.expect("commit").expect("repo");

    // a message git would read as commentary alone is the message all the same
    let policy = [
        rule("*.lock", Strategy::Ours),
        rule("a.txt", Strategy::Theirs),
        rule("list.txt", Strategy::Union),
    ];
    let merged =
        WasiVcs.merge(repo, &theirs, "# merge under policy", &policy).await.expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert!(merged.commit.is_some());
    assert_eq!(read("policy/Cargo.lock"), "lock repo\n", "ours keeps the working copy's side");
    assert_eq!(read("policy/a.txt"), "a work\n", "theirs takes the merged-in side");
    assert_eq!(read("policy/list.txt"), "base\nrepo\nwork\n", "union keeps both");
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), []);
}

// A repository's own `merge=union` attribute would auto-resolve the path with
// no conflict to act on; the guest's policy is resolved from the commits and
// wins regardless.
async fn merge_policy_beats_attributes() {
    let repo = "./attributed";
    let work = "./attributed-work";
    WasiVcs.init(repo).await.expect("init");
    write("attributed/.gitattributes", "data.txt merge=union\n");
    write("attributed/data.txt", "base\n");
    let base = WasiVcs.commit(repo, "base").await.expect("commit").expect("base");
    WasiVcs.add(repo, work, &base).await.expect("add");
    write("attributed-work/data.txt", "theirs\n");
    let theirs = WasiVcs.commit(work, "theirs").await.expect("commit").expect("theirs");
    write("attributed/data.txt", "ours\n");
    WasiVcs.commit(repo, "ours").await.expect("commit").expect("ours");

    let policy = [rule("data.txt", Strategy::Theirs)];
    let merged = WasiVcs.merge(repo, &theirs, "merge", &policy).await.expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert!(merged.commit.is_some(), "the merge sealed");
    assert_eq!(read("attributed/data.txt"), "theirs\n", "the policy beat the attribute");
}

// The chain over a base follows first parents alone, newest first, so a
// merge is one entry and the side it brought in none; the base is left out,
// and a message is read back as it was sealed, its own `#` line kept and a
// conflict the policy resolved leaving no hint of itself in it.
async fn log_over_base() {
    let repo = "./logged";
    let work = "./logged-work";
    let base = seeded(repo).await;
    WasiVcs.add(repo, work, &base).await.expect("add");
    write("logged-work/a.txt", "theirs\n");
    write("logged-work/b.txt", "b\n");
    let side = WasiVcs.commit(work, "side").await.expect("commit").expect("side");
    WasiVcs.label(repo, "slice", &side).await.expect("label");
    write("logged/a.txt", "ours\n");
    let ours = WasiVcs.commit(repo, "ours").await.expect("commit").expect("ours");
    let message = "merge slice\n\n# notes\nSlice: SLICE-001\nWave: 1";
    let policy = [rule("a.txt", Strategy::Theirs)];
    let merged = WasiVcs.merge(repo, "slice", message, &policy).await.expect("merge");
    let merge = merged.commit.expect("the policy sealed the merge");
    write("logged/c.txt", "c\n");
    let after = WasiVcs.commit(repo, "after").await.expect("commit").expect("after");

    // the first-parent chain: the merged-in side is not walked
    assert_eq!(
        WasiVcs.log(repo, &after, &base).await.expect("log"),
        [
            Entry {
                id: after.clone(),
                message: "after".to_owned(),
            },
            Entry {
                id: merge.clone(),
                message: message.to_owned(),
            },
            Entry {
                id: ours.clone(),
                message: "ours".to_owned(),
            },
        ]
    );
    assert_eq!(
        WasiVcs.log(repo, &after, &ours).await.expect("log").len(),
        2,
        "a base along the chain cuts it there"
    );
    assert_eq!(
        WasiVcs.log(repo, &merge, &ours).await.expect("log"),
        [Entry {
            id: merge,
            message: message.to_owned(),
        }],
        "a merge counts once, its message whole"
    );
    assert_eq!(WasiVcs.log(repo, &base, &base).await.expect("log"), [], "nothing over itself");
    assert_eq!(
        WasiVcs.log(repo, "nope", &base).await,
        Err(Error::NotFound("nope".to_owned())),
        "a revision the repository lacks"
    );
    assert_eq!(
        WasiVcs.log(repo, &after, "nope").await,
        Err(Error::NotFound("nope".to_owned())),
        "a base the repository lacks"
    );
}

// Ancestry is a question of the graph, a commit its own ancestor and the
// reversed pair none; a label is read back from the branch `label` wrote,
// by its name alone, never by what git's revision syntax would make of it.
async fn descends_and_labelled() {
    let repo = "./lineage";
    let work = "./lineage-work";
    let base = seeded(repo).await;
    WasiVcs.add(repo, work, &base).await.expect("add");
    write("lineage-work/b.txt", "b\n");
    let side = WasiVcs.commit(work, "side").await.expect("commit").expect("side");
    WasiVcs.label(repo, "slice", &side).await.expect("label");

    assert!(WasiVcs.descends(repo, &base, &side).await.expect("descends"));
    assert!(WasiVcs.descends(repo, &base, &base).await.expect("descends"), "itself included");
    assert!(!WasiVcs.descends(repo, &side, &base).await.expect("descends"), "the reversed pair");
    assert_eq!(
        WasiVcs.descends(repo, "nope", &base).await,
        Err(Error::NotFound("nope".to_owned())),
        "an endpoint the repository lacks"
    );
    assert_eq!(WasiVcs.labelled(repo, "slice").await.expect("labelled"), side);
    assert_eq!(
        WasiVcs.labelled(repo, "nope").await,
        Err(Error::NotFound("nope".to_owned())),
        "a label the repository lacks, named as given"
    );
    for name in ["slice~1", "slice^", "slice@{0}"] {
        assert_eq!(
            WasiVcs.labelled(repo, name).await,
            Err(Error::NotFound(name.to_owned())),
            "a name that walks from a label is no label"
        );
    }
}

// The host laid a label and a tag of one name at two commits; the label is
// read back as the label whichever of the two the bare spelling resolves to.
async fn labelled_under_a_tag(repo: &str, branch: &str, tag: &str) {
    assert_eq!(WasiVcs.labelled(repo, "slice").await.expect("labelled"), branch);
    let bare = WasiVcs.resolve(repo, "slice").await.expect("the bare spelling");
    assert!(bare == branch || bare == tag, "the bare spelling is one of the two: {bare}");
}

// One side edited the path and the other deleted it: keeping the deleting side
// removes the path.
async fn merge_policy_modify_delete() {
    let repo = "./moddel";
    let work = "./moddel-work";
    WasiVcs.init(repo).await.expect("init");
    write("moddel/keep.txt", "base\n");
    let base = WasiVcs.commit(repo, "base").await.expect("commit").expect("base");
    WasiVcs.add(repo, work, &base).await.expect("add");
    fs::remove_file("moddel-work/keep.txt").expect("delete on the merged-in side");
    let theirs = WasiVcs.commit(work, "delete").await.expect("commit").expect("delete");
    write("moddel/keep.txt", "ours\n");
    WasiVcs.commit(repo, "edit").await.expect("commit").expect("edit");

    let policy = [rule("keep.txt", Strategy::Theirs)];
    let merged = WasiVcs.merge(repo, &theirs, "merge", &policy).await.expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert!(merged.commit.is_some(), "the merge sealed");
    assert!(fs::metadata("moddel/keep.txt").is_err(), "the deleting side was kept");
    assert!(WasiVcs.pending(repo).await.expect("pending").is_empty(), "the copy is sealed");
}

// The working copy deleted the path and the merged-in side edited it: `ours`
// keeps the deletion, over the edited version the merge staged.
async fn merge_policy_delete_modify() {
    let repo = "./delmod";
    let work = "./delmod-work";
    WasiVcs.init(repo).await.expect("init");
    write("delmod/gone.txt", "base\n");
    let base = WasiVcs.commit(repo, "base").await.expect("commit").expect("base");
    WasiVcs.add(repo, work, &base).await.expect("add");
    write("delmod-work/gone.txt", "theirs\n");
    let theirs = WasiVcs.commit(work, "edit").await.expect("commit").expect("edit");
    fs::remove_file("delmod/gone.txt").expect("delete on the working copy's side");
    WasiVcs.commit(repo, "delete").await.expect("commit").expect("delete");

    let policy = [rule("gone.txt", Strategy::Ours)];
    let merged = WasiVcs.merge(repo, &theirs, "merge", &policy).await.expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert!(merged.commit.is_some(), "the merge sealed");
    assert!(fs::metadata("delmod/gone.txt").is_err(), "the deleting side was kept");
    assert!(WasiVcs.pending(repo).await.expect("pending").is_empty(), "the copy is sealed");
}

// A path one side alone added is git's clean merge: a rule naming it keeps
// nothing from the side that lacks it, whichever side that is.
async fn merge_policy_one_sided() {
    let repo = "./onesided";
    let work = "./onesided-work";
    let base = seeded(repo).await;
    WasiVcs.add(repo, work, &base).await.expect("add");
    write("onesided-work/theirs-only.txt", "theirs\n");
    let theirs = WasiVcs.commit(work, "theirs").await.expect("commit").expect("theirs");
    write("onesided/ours-only.txt", "ours\n");
    WasiVcs.commit(repo, "ours").await.expect("commit").expect("ours");

    let policy = [rule("theirs-only.txt", Strategy::Ours), rule("ours-only.txt", Strategy::Theirs)];
    let merged = WasiVcs.merge(repo, &theirs, "merge", &policy).await.expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert!(merged.commit.is_some(), "the merge sealed");
    assert_eq!(read("onesided/theirs-only.txt"), "theirs\n", "the merged-in side's addition stays");
    assert_eq!(read("onesided/ours-only.txt"), "ours\n", "the working copy's addition stays");
    assert!(WasiVcs.pending(repo).await.expect("pending").is_empty(), "the copy is sealed");
}

// a repository whose working copy edited `old.txt` while the merged-in side
// renamed it to `new.txt`, and the merged-in commit
async fn renamed(repo: &str, work: &str) -> String {
    WasiVcs.init(repo).await.expect("init");
    write(&format!("{repo}/old.txt"), "base\n");
    let base = WasiVcs.commit(repo, "base").await.expect("commit").expect("base");
    WasiVcs.add(repo, work, &base).await.expect("add");
    fs::rename(format!("{work}/old.txt"), format!("{work}/new.txt")).expect("rename on their side");
    let theirs = WasiVcs.commit(work, "rename").await.expect("commit").expect("rename");
    write(&format!("{repo}/old.txt"), "ours\n");
    WasiVcs.commit(repo, "edit").await.expect("commit").expect("edit");
    theirs
}

// The merged-in side renamed a path the working copy edited, so git carries
// the edit to the new name: a rule over both names keeps one side whole,
// `ours` the old name with its bytes and `theirs` the new name with its, not
// the old name restored beside git's blend of the new.
async fn merge_policy_rename() {
    let theirs = renamed("./renamed", "./renamed-work").await;
    let policy = [rule("*.txt", Strategy::Ours)];
    let merged = WasiVcs.merge("./renamed", &theirs, "merge", &policy).await.expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert!(merged.commit.is_some(), "the merge sealed");
    assert_eq!(read("renamed/old.txt"), "ours\n");
    assert!(fs::metadata("renamed/new.txt").is_err(), "the new name was kept beside the old");
    assert!(WasiVcs.pending("./renamed").await.expect("pending").is_empty(), "the copy is sealed");

    let theirs = renamed("./renamed-theirs", "./renamed-theirs-work").await;
    let policy = [rule("*.txt", Strategy::Theirs)];
    let merged = WasiVcs.merge("./renamed-theirs", &theirs, "merge", &policy).await.expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert!(merged.commit.is_some(), "the merge sealed");
    assert_eq!(
        read("renamed-theirs/new.txt"),
        "base\n",
        "git's blend stood where theirs' bytes go"
    );
    assert!(fs::metadata("renamed-theirs/old.txt").is_err(), "the old name stayed");
    let pending = WasiVcs.pending("./renamed-theirs").await.expect("pending");
    assert!(pending.is_empty(), "the copy is sealed");
}

// A blob no text encoding holds: a `union` over it cannot be sealed and leaves
// the copy on its head with nothing of the attempt behind, and `theirs` lays
// it byte for byte.
async fn merge_policy_binary() {
    let repo = "./binary";
    let work = "./binary-work";
    let ours_bytes: Vec<u8> = vec![0, 1, 2, 0xff, 0xfe, b'\n', 0x80];
    let theirs_bytes: Vec<u8> = vec![0, 1, 3, 0xff, 0xc0, b'\n', 0x81];
    WasiVcs.init(repo).await.expect("init");
    fs::write("binary/blob.bin", [0u8, 1, 0xff]).expect("writing the base blob");
    let base = WasiVcs.commit(repo, "base").await.expect("commit").expect("base");
    WasiVcs.add(repo, work, &base).await.expect("add");
    fs::write("binary-work/blob.bin", &theirs_bytes).expect("writing theirs");
    let theirs = WasiVcs.commit(work, "theirs").await.expect("commit").expect("theirs");
    fs::write("binary/blob.bin", &ours_bytes).expect("writing ours");
    let ours = WasiVcs.commit(repo, "ours").await.expect("commit").expect("ours");

    let union = [rule("*.bin", Strategy::Union)];
    let refused = WasiVcs.merge(repo, &theirs, "merge", &union).await;
    assert!(matches!(refused, Err(Error::Other(_))), "a binary has no lines to union: {refused:?}");
    assert_eq!(WasiVcs.head(repo).await.expect("head"), ours, "the head did not move");
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), [], "nothing of the attempt is left");
    assert_eq!(
        fs::read("binary/blob.bin").expect("the blob"),
        ours_bytes,
        "the tree is ours again"
    );

    let policy = [rule("*.bin", Strategy::Theirs)];
    let merged = WasiVcs.merge(repo, &theirs, "merge", &policy).await.expect("merge");
    assert!(merged.commit.is_some(), "the merge sealed");
    assert_eq!(fs::read("binary/blob.bin").expect("the blob"), theirs_bytes, "byte for byte");
}

// Labelling the branch the working copy sits on moves the branch and leaves the
// copy on its sealed commit.
async fn label_the_checked_out_branch() {
    let repo = "./checked-out";
    let first = seeded(repo).await;
    write("checked-out/b.txt", "b\n");
    let second = WasiVcs.commit(repo, "second").await.expect("commit").expect("second");
    WasiVcs.label(repo, "main", &first).await.expect("label the branch HEAD is on");
    assert_eq!(WasiVcs.resolve(repo, "main").await.expect("resolve"), first, "main moved");
    assert_eq!(WasiVcs.head(repo).await.expect("head"), second, "the working copy stayed");
}

async fn merge_up_to_date() {
    let repo = "./ancestor";
    let first = seeded(repo).await;
    let merged = WasiVcs.merge(repo, &first, "nothing", &[]).await.expect("merging an ancestor");
    assert_eq!(merged.commit, Some(first));
    assert_eq!(merged.conflicts, Vec::<String>::new());
}

async fn pending_kinds() {
    let repo = "./kinds";
    WasiVcs.init(repo).await.expect("init");
    write("kinds/a.txt", "a\n");
    write("kinds/b.txt", "b\n");
    WasiVcs.commit(repo, "ab").await.expect("commit").expect("ab");

    write("kinds/a.txt", "a changed\n");
    fs::remove_file("kinds/b.txt").expect("deleting b");
    write("kinds/c.txt", "c\n");
    write("kinds/nested/deep/d.txt", "d\n");
    assert_eq!(
        WasiVcs.pending(repo).await.expect("pending"),
        [
            change("a.txt", ChangeKind::Modified),
            change("b.txt", ChangeKind::Deleted),
            change("c.txt", ChangeKind::Added),
            change("nested/deep/d.txt", ChangeKind::Added),
        ]
    );

    // everything pending seals as one commit
    WasiVcs.commit(repo, "all").await.expect("commit").expect("all");
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), []);
}

async fn commit_nothing() {
    let repo = "./nothing";
    let first = seeded(repo).await;
    assert_eq!(WasiVcs.commit(repo, "nothing").await.expect("commit"), None);
    assert_eq!(WasiVcs.head(repo).await.expect("head"), first);
}

async fn remove_dirty_worktree() {
    let repo = "./dirty";
    let first = seeded(repo).await;
    let work = "./copies/work";
    WasiVcs.add(repo, work, &first).await.expect("add");
    write("copies/work/dirty.txt", "unsealed\n");
    write("copies/work/a.txt", "changed\n");

    WasiVcs.remove(work).await.expect("remove");
    assert!(fs::metadata("copies/work").is_err(), "the files went with the working copy");
    assert_eq!(read("dirty/a.txt"), "a\n", "the repository is untouched");
}
