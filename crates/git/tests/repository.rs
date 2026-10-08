//! The `WasiVcsCtx` contract over real repositories: every operation and
//! every refusal, driven directly over scratch repositories and a bare
//! origin, with git itself as the oracle.

mod support;

use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};

use omnia_git::Client;
use omnia_wasi_vcs::{Change, ChangeKind, CloneOptions, Error, Rule, Strategy, WasiVcsCtx as _};
use support::Hermetic;

const WHOLE: CloneOptions = CloneOptions { depth: None };

fn rule(paths: &str, strategy: Strategy) -> Rule {
    Rule {
        paths: paths.to_owned(),
        strategy,
    }
}

fn write(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("creating parent");
    }
    fs::write(path, content).expect("writing");
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).expect("reading")
}

// the pending changes as comparable pairs
fn kinds(changes: Vec<Change>) -> Vec<(String, ChangeKind)> {
    changes.into_iter().map(|change| (change.path, change.kind)).collect()
}

fn pair(path: &str, kind: ChangeKind) -> (String, ChangeKind) {
    (path.to_owned(), kind)
}

// The typed variant a failed operation carries, past the anyhow wrapping,
// spelled for comparison: the generated error derives no equality.
fn refused<T: Debug>(result: anyhow::Result<T>) -> String {
    match result {
        Ok(value) => panic!("expected a refusal, got {value:?}"),
        Err(error) => {
            let typed = error
                .downcast::<Error>()
                .unwrap_or_else(|other| panic!("untyped failure: {other:#}"));
            format!("{typed:?}")
        }
    }
}

fn shown(error: &Error) -> String {
    format!("{error:?}")
}

// a repository with one committed file, and its first commit
async fn seeded(git: &Hermetic, client: &Client, name: &str) -> (PathBuf, String) {
    let repo = git.path(name);
    client.init(repo.clone()).await.expect("init");
    write(&repo.join("a.txt"), "a\n");
    let first =
        client.commit(repo.clone(), "a".to_owned()).await.expect("commit").expect("a commit");
    (repo, first)
}

#[tokio::test]
async fn init_commit_add_merge() {
    let git = Hermetic::new();
    let client = git.client().await;
    let repo = git.path("repo");

    client.init(repo.clone()).await.expect("init");
    assert_eq!(refused(client.head(repo.clone()).await), shown(&Error::NotFound("HEAD".into())));
    assert_eq!(kinds(client.pending(repo.clone()).await.expect("pending")), []);

    write(&repo.join("a.txt"), "a\n");
    assert_eq!(
        kinds(client.pending(repo.clone()).await.expect("pending")),
        [pair("a.txt", ChangeKind::Added)]
    );
    let first =
        client.commit(repo.clone(), "a".to_owned()).await.expect("commit").expect("a commit");
    assert_eq!(client.head(repo.clone()).await.expect("head"), first);
    assert_eq!(client.resolve(repo.clone(), "HEAD".to_owned()).await.expect("resolve"), first);
    assert_eq!(client.resolve(repo.clone(), first[..8].to_owned()).await.expect("prefix"), first);
    assert_eq!(kinds(client.pending(repo.clone()).await.expect("pending")), []);

    // a detached working copy at the first commit, committed on its own
    let work = git.path("work");
    client.add(repo.clone(), work.clone(), first.clone()).await.expect("add");
    assert_eq!(read(&work.join("a.txt")), "a\n");
    assert_eq!(client.head(work.clone()).await.expect("head"), first);
    write(&work.join("b.txt"), "b\n");
    let second = client.commit(work.clone(), "b".to_owned()).await.expect("commit").expect("b");
    assert_ne!(second, first);
    assert_eq!(
        client.head(repo.clone()).await.expect("head"),
        first,
        "the repository's head stays"
    );

    // labelled and merged back by label
    client.label(repo.clone(), "feature".to_owned(), second.clone()).await.expect("label");
    assert_eq!(client.resolve(repo.clone(), "feature".to_owned()).await.expect("resolve"), second);
    let merged = client
        .merge(repo.clone(), "feature".to_owned(), "merge feature".to_owned(), vec![])
        .await
        .expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    let merge = merged.commit.expect("a merge commit");
    assert_eq!(client.head(repo.clone()).await.expect("head"), merge);
    assert_eq!(read(&repo.join("b.txt")), "b\n");
    assert_eq!(git.git(&repo, &["log", "--first-parent", "--format=%s"]), "merge feature\na");
    assert_eq!(
        git.git(&repo, &["rev-list", "--parents", "-1", "HEAD"]),
        format!("{merge} {first} {second}")
    );
}

#[tokio::test]
async fn merge_conflict_without_rule() {
    let git = Hermetic::new();
    let client = git.client().await;
    let (repo, first) = seeded(&git, &client, "repo").await;

    let work = git.path("work");
    client.add(repo.clone(), work.clone(), first).await.expect("add");
    write(&work.join("a.txt"), "work\n");
    let theirs =
        client.commit(work.clone(), "work".to_owned()).await.expect("commit").expect("work");
    write(&repo.join("a.txt"), "repo\n");
    let ours = client.commit(repo.clone(), "repo".to_owned()).await.expect("commit").expect("repo");

    let merged = client
        .merge(repo.clone(), theirs, "merge".to_owned(), vec![])
        .await
        .expect("a conflict is data");
    assert_eq!(merged.commit, None);
    assert_eq!(merged.conflicts, ["a.txt"]);

    // the working copy is back on its head, nothing pending
    assert_eq!(client.head(repo.clone()).await.expect("head"), ours);
    assert_eq!(read(&repo.join("a.txt")), "repo\n");
    assert_eq!(kinds(client.pending(repo.clone()).await.expect("pending")), []);
    assert!(!repo.join(".git/MERGE_HEAD").exists(), "no merge left in progress");
}

#[tokio::test]
async fn merge_policy_resolves() {
    let git = Hermetic::new();
    let client = git.client().await;
    let repo = git.path("repo");
    client.init(repo.clone()).await.expect("init");
    write(&repo.join("Cargo.lock"), "lock 0\n");
    write(&repo.join("a.txt"), "a 0\n");
    write(&repo.join("list.txt"), "base\n");
    let first =
        client.commit(repo.clone(), "base".to_owned()).await.expect("commit").expect("base");

    let work = git.path("work");
    client.add(repo.clone(), work.clone(), first).await.expect("add");
    write(&work.join("Cargo.lock"), "lock work\n");
    write(&work.join("a.txt"), "a work\n");
    write(&work.join("list.txt"), "base\nwork\n");
    let theirs =
        client.commit(work.clone(), "work".to_owned()).await.expect("commit").expect("work");
    write(&repo.join("Cargo.lock"), "lock repo\n");
    write(&repo.join("a.txt"), "a repo\n");
    write(&repo.join("list.txt"), "base\nrepo\n");
    client.commit(repo.clone(), "repo".to_owned()).await.expect("commit").expect("repo");

    let policy = vec![
        rule("*.lock", Strategy::Ours),
        rule("a.txt", Strategy::Theirs),
        rule("list.txt", Strategy::Union),
    ];
    let merged = client
        .merge(repo.clone(), theirs, "merge under policy".to_owned(), policy)
        .await
        .expect("merge");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert!(merged.commit.is_some());
    assert_eq!(read(&repo.join("Cargo.lock")), "lock repo\n", "ours keeps the working copy's side");
    assert_eq!(read(&repo.join("a.txt")), "a work\n", "theirs takes the merged-in side");
    assert_eq!(read(&repo.join("list.txt")), "base\nrepo\nwork\n", "union keeps both");
    assert_eq!(kinds(client.pending(repo.clone()).await.expect("pending")), []);
    assert_eq!(git.git(&repo, &["status", "--porcelain"]), "", "no policy file in the tree");
}

#[tokio::test]
async fn merge_up_to_date() {
    let git = Hermetic::new();
    let client = git.client().await;
    let (repo, first) = seeded(&git, &client, "repo").await;
    let merged = client
        .merge(repo.clone(), first.clone(), "nothing".to_owned(), vec![])
        .await
        .expect("merging an ancestor");
    assert_eq!(merged.commit, Some(first));
    assert_eq!(merged.conflicts, Vec::<String>::new());
}

#[tokio::test]
async fn label_push_clone_fetch() {
    let git = Hermetic::new();
    let client = git.client().await;
    let (origin, url) = git.bare("origin.git");

    // a clone of the empty origin, with one commit labelled and pushed
    let clone = git.path("clones/first");
    client.clone_repo(url.clone(), clone.clone(), WHOLE).await.expect("clone");
    let first = {
        write(&clone.join("a.txt"), "a\n");
        client.commit(clone.clone(), "a".to_owned()).await.expect("commit").expect("a")
    };
    client.label(clone.clone(), "emery/rev-1".to_owned(), first.clone()).await.expect("label");
    client.push(clone.clone(), "origin".to_owned(), "emery/rev-1".to_owned()).await.expect("push");
    assert_eq!(git.git(&origin, &["rev-parse", "emery/rev-1"]), first);

    // a second clone sees the label; a later push reaches it by fetch
    let second = git.path("clones/second");
    client.clone_repo(url.clone(), second.clone(), WHOLE).await.expect("clone");
    let remote_label = "origin/emery/rev-1".to_owned();
    assert_eq!(client.resolve(second.clone(), remote_label.clone()).await.expect("resolve"), first);
    write(&clone.join("b.txt"), "b\n");
    let next = client.commit(clone.clone(), "b".to_owned()).await.expect("commit").expect("b");
    client.label(clone.clone(), "emery/rev-1".to_owned(), next.clone()).await.expect("move label");
    client.push(clone.clone(), "origin".to_owned(), "emery/rev-1".to_owned()).await.expect("push");
    assert_eq!(
        client.resolve(second.clone(), remote_label.clone()).await.expect("stale"),
        first,
        "a clone sees nothing before it fetches"
    );
    client.fetch(second.clone(), "origin".to_owned()).await.expect("fetch");
    assert_eq!(client.resolve(second.clone(), remote_label.clone()).await.expect("fresh"), next);

    // a shallow clone holds every label at the one commit it asked for
    let shallow = git.path("clones/shallow");
    client
        .clone_repo(url, shallow.clone(), CloneOptions { depth: Some(1) })
        .await
        .expect("shallow clone");
    assert!(shallow.join(".git/shallow").exists());
    assert_eq!(client.resolve(shallow.clone(), remote_label).await.expect("resolve"), next);
    assert_eq!(git.git(&shallow, &["rev-list", "--count", "origin/emery/rev-1"]), "1");
}

#[tokio::test]
async fn pending_kinds() {
    let git = Hermetic::new();
    let client = git.client().await;
    let repo = git.path("repo");
    client.init(repo.clone()).await.expect("init");
    write(&repo.join("a.txt"), "a\n");
    write(&repo.join("b.txt"), "b\n");
    client.commit(repo.clone(), "ab".to_owned()).await.expect("commit").expect("ab");

    write(&repo.join("a.txt"), "a changed\n");
    fs::remove_file(repo.join("b.txt")).expect("deleting b");
    write(&repo.join("c.txt"), "c\n");
    write(&repo.join("nested/deep/d.txt"), "d\n");
    assert_eq!(
        kinds(client.pending(repo.clone()).await.expect("pending")),
        [
            pair("a.txt", ChangeKind::Modified),
            pair("b.txt", ChangeKind::Deleted),
            pair("c.txt", ChangeKind::Added),
            pair("nested/deep/d.txt", ChangeKind::Added),
        ]
    );

    // everything pending seals as one commit
    let sealed = client.commit(repo.clone(), "all".to_owned()).await.expect("commit").expect("all");
    assert_eq!(kinds(client.pending(repo.clone()).await.expect("pending")), []);
    assert_eq!(
        git.git(&repo, &["show", "--format=", "--name-status", &sealed]),
        "M\ta.txt\nD\tb.txt\nA\tc.txt\nA\tnested/deep/d.txt"
    );
}

#[tokio::test]
async fn commit_nothing() {
    let git = Hermetic::new();
    let client = git.client().await;
    let (repo, first) = seeded(&git, &client, "repo").await;
    assert_eq!(client.commit(repo.clone(), "nothing".to_owned()).await.expect("commit"), None);
    assert_eq!(client.head(repo.clone()).await.expect("head"), first);
    assert_eq!(git.git(&repo, &["rev-list", "--count", "HEAD"]), "1");
}

#[tokio::test]
async fn remove_dirty_worktree() {
    let git = Hermetic::new();
    let client = git.client().await;
    let (repo, first) = seeded(&git, &client, "repo").await;
    let work = git.path("copies/work");
    client.add(repo.clone(), work.clone(), first).await.expect("add");
    write(&work.join("dirty.txt"), "unsealed\n");
    write(&work.join("a.txt"), "changed\n");

    client.remove(work.clone()).await.expect("remove");
    assert!(!work.exists(), "the files went with the working copy");
    let listed = git.git(&repo, &["worktree", "list", "--porcelain"]);
    assert_eq!(listed.matches("worktree ").count(), 1, "{listed}");
    assert_eq!(read(&repo.join("a.txt")), "a\n", "the repository is untouched");
}

#[tokio::test]
async fn refusals() {
    let git = Hermetic::new();
    let client = git.client().await;
    let (repo, first) = seeded(&git, &client, "repo").await;
    let (_, url) = git.bare("origin.git");
    let elsewhere = git.path("elsewhere");
    fs::create_dir_all(&elsewhere).expect("creating a plain directory");
    let taken = git.path("taken");
    write(&taken.join("x"), "x\n");
    let nope = || "nope".to_owned();
    let not_found = shown(&Error::NotFound("nope".into()));
    let not_a_repository = shown(&Error::NotARepository);

    // store
    assert_eq!(refused(client.resolve(repo.clone(), nope()).await), not_found);
    assert_eq!(
        refused(client.resolve(elsewhere.clone(), "HEAD".to_owned()).await),
        not_a_repository
    );
    assert_eq!(refused(client.head(elsewhere.clone()).await), not_a_repository);
    assert_eq!(
        refused(client.fetch(git.path("nowhere"), "origin".to_owned()).await),
        not_a_repository,
        "a location that does not exist holds no repository"
    );
    assert_eq!(refused(client.commit(elsewhere.clone(), "x".to_owned()).await), not_a_repository);
    assert_eq!(
        refused(client.merge(repo.clone(), nope(), "x".to_owned(), vec![]).await),
        not_found
    );

    // workspace
    assert_eq!(
        refused(client.init(repo.clone()).await),
        shown(&Error::Exists(repo.display().to_string()))
    );
    assert_eq!(
        refused(client.add(repo.clone(), taken.clone(), first.clone()).await),
        shown(&Error::Exists(taken.display().to_string()))
    );
    assert_eq!(refused(client.add(repo.clone(), git.path("fresh"), nope()).await), not_found);
    assert_eq!(refused(client.remove(elsewhere.clone()).await), not_a_repository);
    let main = refused(client.remove(repo.clone()).await);
    assert!(main.starts_with("Error::Other(") && main.contains("main working tree"), "{main}");
    assert_eq!(refused(client.pending(elsewhere.clone()).await), not_a_repository);

    // transport
    assert_eq!(
        refused(client.clone_repo(url.clone(), taken.clone(), WHOLE).await),
        shown(&Error::Exists(taken.display().to_string()))
    );
    let missing = format!("file://{}", git.path("missing.git").display());
    assert_eq!(
        refused(client.clone_repo(missing.clone(), git.path("clone"), WHOLE).await),
        shown(&Error::NotFound(missing))
    );
    let unanswered = "https://127.0.0.1:1/x.git".to_owned();
    let access = refused(client.clone_repo(unanswered, git.path("clone"), WHOLE).await);
    assert!(access.starts_with("Error::Access("), "a remote nothing answers is access: {access}");
    assert_eq!(refused(client.fetch(repo.clone(), nope()).await), not_found);
    assert_eq!(refused(client.label(repo.clone(), "lbl".to_owned(), nope()).await), not_found);
    assert_eq!(refused(client.push(repo.clone(), nope(), "main".to_owned()).await), not_found);
    let clone = git.path("clone");
    client.clone_repo(url, clone.clone(), WHOLE).await.expect("clone");
    assert_eq!(
        refused(client.push(clone.clone(), "origin".to_owned(), "nolabel".to_owned()).await),
        shown(&Error::NotFound("nolabel".into()))
    );
}

#[tokio::test]
async fn version_refused() {
    let git = Hermetic::new();
    let error = Client::connect(git.reporting("2.4.0")).await.expect_err("too old");
    assert!(error.to_string().contains("older than 2.5"), "{error:#}");

    Client::connect(git.reporting("2.5.0")).await.expect("the first with worktree");

    let error = Client::connect(git.path("no-such-git")).await.expect_err("missing binary");
    assert!(error.to_string().contains("spawning"), "{error:#}");
}
