//! The store and workspace operations over git, from a guest: a repository
//! initialised, read, committed, copied, labelled, and merged back; a merge
//! that conflicts and one resolved under a policy; what `pending` reports
//! for each kind of change; a commit with nothing to seal; a dirty working
//! copy removed. Each section runs in a repository of its own beneath the
//! `.` mount, so the host reads each with git afterwards.

#![cfg(target_arch = "wasm32")]

use std::fs;
use std::path::Path;

use omnia_sdk::vcs::{Change, ChangeKind, Error, Rule, Strategy, Vcs as _, WasiVcs};

omnia_sdk::command!(scenario);

async fn scenario() {
    init_commit_add_merge().await;
    merge_conflict_without_rule().await;
    merge_policy_resolves().await;
    merge_up_to_date().await;
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
    assert_eq!(WasiVcs.head(repo).await, Err(Error::NotFound("HEAD".to_owned())));
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), []);

    write("repo/a.txt", "a\n");
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), [change("a.txt", ChangeKind::Added)]);
    let first = WasiVcs.commit(repo, "a").await.expect("commit").expect("a commit");
    assert_eq!(WasiVcs.head(repo).await.expect("head"), first);
    assert_eq!(WasiVcs.resolve(repo, "HEAD").await.expect("resolve"), first);
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
    assert!(fs::metadata("conflict/.git/MERGE_HEAD").is_err(), "no merge left in progress");
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

    let policy = [
        rule("*.lock", Strategy::Ours),
        rule("a.txt", Strategy::Theirs),
        rule("list.txt", Strategy::Union),
    ];
    let merged = WasiVcs.merge(repo, &theirs, "merge under policy", &policy).await.expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert!(merged.commit.is_some());
    assert_eq!(read("policy/Cargo.lock"), "lock repo\n", "ours keeps the working copy's side");
    assert_eq!(read("policy/a.txt"), "a work\n", "theirs takes the merged-in side");
    assert_eq!(read("policy/list.txt"), "base\nrepo\nwork\n", "union keeps both");
    assert_eq!(WasiVcs.pending(repo).await.expect("pending"), []);
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
