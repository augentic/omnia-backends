//! End-to-end tests for the git backend at the `omnia:vcs` boundary: a
//! guest component from `crates/test-programs` runs through the omnia
//! runtime over an `omnia_git::Client`, driving real repositories beneath
//! its `.` mount. The guest asserts what it observes and traps on failure;
//! the test asserts what git left behind, with git as the oracle.

mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use omnia::ExitStatus;
use omnia_test::host::{Backends, Deployment, Scratch, scratch};
use omnia_wasi_vcs::WasiVcs;
use support::Hermetic;

// Every guest program in `crates/test-programs` must have a matching test
// here; a new program without one fails to compile.
test_programs::foreach_vcs!();

// Run `program` over the hermetic git with the scratch root as its `.`
// mount, held to a clean exit.
async fn run(git: &Hermetic, scratch: &Scratch, program: &str, args: Vec<String>) {
    let backends = Backends::defaults().await.vcs(git.client().await);
    let status = Deployment::new()
        .guest("guest", program)
        .args(args)
        .mount(scratch.mount(true))
        .run_host::<WasiVcs, _>(backends)
        .await
        .expect("guest runs");
    assert_eq!(status, ExitStatus::SUCCESS, "the guest failed");
}

#[tokio::test]
async fn vcs_journey() {
    let git = Hermetic::new();
    let (origin, url) = git.bare("origin.git");
    let scratch = scratch();
    run(&git, &scratch, test_programs::VCS_JOURNEY, vec![url]).await;

    // the origin holds the one label pushed, at the slice it names
    assert_eq!(git.git(&origin, &["for-each-ref", "--format=%(refname:short)"]), "emery/rev1");
    assert_eq!(git.git(&origin, &["log", "-1", "--format=%s", "emery/rev1"]), "slice 1");

    // the project kept the resolved merge and dropped the conflicted one
    let project = scratch.path().join("project");
    assert_eq!(
        git.git(&project, &["log", "--first-parent", "--format=%s"]),
        "project again\nmerge rev1\nproject edit\ninitial"
    );
    assert_eq!(git.git(&project, &["status", "--porcelain"]), "");
    assert!(project.join("src/lib.rs").is_file(), "the first slice's file reached the project");

    // the working copy is gone from the tree and from git's list
    assert!(!scratch.path().join("work").exists());
    let listed = git.git(&project, &["worktree", "list", "--porcelain"]);
    assert_eq!(listed.matches("worktree ").count(), 1, "{listed}");
}

#[tokio::test]
async fn vcs_matrix() {
    let git = Hermetic::new();
    let scratch = scratch();
    run(&git, &scratch, test_programs::VCS_MATRIX, vec![]).await;
    let at = |name: &str| scratch.path().join(name);

    // the merge by label is one commit over both parents
    assert_eq!(git.git(&at("repo"), &["log", "--first-parent", "--format=%s"]), "merge feature\na");
    let parents = git.git(&at("repo"), &["rev-list", "--parents", "-1", "HEAD"]);
    assert_eq!(parents.split_whitespace().count(), 3, "{parents}");

    // a policy leaves nothing of itself in the tree
    assert_eq!(git.git(&at("policy"), &["status", "--porcelain"]), "");

    // the policy's side is what the merge commit holds, over the repository's
    // own `merge=union` attribute, and the sealed tree is clean of temporaries
    assert_eq!(git.git(&at("attributed"), &["show", "HEAD:data.txt"]), "theirs");
    assert_eq!(git.git(&at("attributed"), &["status", "--porcelain"]), "");
    assert_eq!(git.git(&at("moddel"), &["diff", "--name-status", "HEAD^1", "HEAD"]), "D\tkeep.txt");

    // labelling the branch the copy sat on left the copy detached on its commit
    assert_eq!(git.git(&at("checked-out"), &["rev-parse", "--abbrev-ref", "HEAD"]), "HEAD");
    assert_eq!(git.git(&at("checked-out"), &["log", "-1", "--format=%s", "main"]), "a");
    assert_eq!(git.git(&at("checked-out"), &["log", "-1", "--format=%s"]), "second");

    // every kind of change sealed as the one commit
    assert_eq!(
        git.git(&at("kinds"), &["show", "--format=", "--name-status", "HEAD"]),
        "M\ta.txt\nD\tb.txt\nA\tc.txt\nA\tnested/deep/d.txt"
    );
    assert_eq!(git.git(&at("nothing"), &["rev-list", "--count", "HEAD"]), "1");

    // the dirty working copy left git's list with its files
    assert!(!at("copies/work").exists());
    let listed = git.git(&at("dirty"), &["worktree", "list", "--porcelain"]);
    assert_eq!(listed.matches("worktree ").count(), 1, "{listed}");
}

#[tokio::test]
async fn vcs_transport() {
    let git = Hermetic::new();
    let (origin, url) = git.bare("origin.git");
    let scratch = scratch();
    run(&git, &scratch, test_programs::VCS_TRANSPORT, vec![url]).await;
    let at = |name: &str| scratch.path().join(name);

    // the origin holds the moved label at the first clone's head
    assert_eq!(
        git.git(&origin, &["rev-parse", "emery/rev-1"]),
        git.git(&at("clones/first"), &["rev-parse", "HEAD"])
    );
    assert_eq!(git.git(&origin, &["log", "-1", "--format=%s", "emery/rev-1"]), "b");

    // the shallow clone holds one commit under the label
    assert!(at("clones/shallow/.git/shallow").is_file());
    assert_eq!(git.git(&at("clones/shallow"), &["rev-list", "--count", "origin/emery/rev-1"]), "1");
}

#[tokio::test]
async fn vcs_hardening() {
    let git = Hermetic::new();
    let scratch = scratch();
    let at = |name: &str| scratch.path().join(name);
    let markers = at("markers");
    fs::create_dir_all(&markers).expect("the markers directory");

    // a repository whose own configuration would run a program on the host at
    // the next checkout, status, commit, or push
    let repo = at("repo");
    seed(&git, &repo, "a.txt");
    fs::write(repo.join(".gitattributes"), "* filter=evil\n").expect("the attributes");
    git.git(&repo, &["add", "-A"]);
    git.git(&repo, &["commit", "-qm", "attributes"]);
    let hooks = at("hostile-hooks");
    fs::create_dir_all(&hooks).expect("the hooks directory");
    executable(&hooks.join("pre-commit"), &touch(&markers, "pre-commit"));
    let fsmonitor = at("fsmonitor");
    executable(&fsmonitor, &touch(&markers, "fsmonitor"));
    let gpg = at("gpg");
    executable(&gpg, &touch(&markers, "gpg"));
    let smudge = at("smudge");
    executable(&smudge, &filter(&markers, "smudge"));
    for (key, value) in [
        ("core.hooksPath", hooks.display().to_string()),
        ("core.fsmonitor", fsmonitor.display().to_string()),
        ("commit.gpgsign", "true".to_owned()),
        ("gpg.program", gpg.display().to_string()),
        ("filter.evil.smudge", smudge.display().to_string()),
        ("filter.evil.clean", smudge.display().to_string()),
    ] {
        git.git(&repo, &["config", "--local", key, value.as_str()]);
    }

    // a bare origin whose receive would run a hook of its own, reached through
    // a client-side receive-pack the repository also salted
    let origin = at("origin.git");
    fs::create_dir_all(&origin).expect("the origin");
    git.git(&origin, &["init", "--quiet", "--bare"]);
    let origin_hooks = at("origin-hooks");
    fs::create_dir_all(&origin_hooks).expect("the origin hooks");
    executable(&origin_hooks.join("pre-receive"), &blocking(&markers, "pre-receive"));
    let origin_hooks_path = origin_hooks.display().to_string();
    git.git(&origin, &["config", "--local", "core.hooksPath", &origin_hooks_path]);
    let receive_pack = at("receive-pack");
    executable(&receive_pack, &blocking(&markers, "receive-pack"));
    let origin_url = format!("file://{}", origin.display());
    git.git(&repo, &["remote", "add", "origin", &origin_url]);
    let receive_pack_path = receive_pack.display().to_string();
    git.git(&repo, &["config", "--local", "remote.origin.receivepack", &receive_pack_path]);

    // a repository with no identity, so a clean merge cannot be sealed
    let noident = at("noident");
    seed(&git, &noident, "a.txt");
    git.git(&noident, &["add", "-A"]);
    git.git(&noident, &["commit", "-qm", "base"]);
    git.git(&noident, &["branch", "feature"]);
    git.git(&noident, &["checkout", "--quiet", "feature"]);
    fs::write(noident.join("b.txt"), "b\n").expect("writing b");
    git.git(&noident, &["add", "-A"]);
    git.git(&noident, &["commit", "-qm", "feature"]);
    git.git(&noident, &["checkout", "--quiet", "main"]);
    git.git(&noident, &["config", "--local", "user.name", ""]);
    git.git(&noident, &["config", "--local", "user.email", ""]);

    run(&git, &scratch, test_programs::VCS_HARDENING, vec![]).await;

    // nothing the repositories salted ran
    let ran: Vec<_> = fs::read_dir(&markers)
        .expect("markers")
        .filter_map(Result::ok)
        .map(|e| e.file_name())
        .collect();
    assert!(ran.is_empty(), "a planted program ran: {ran:?}");

    // the commit and the push still landed, and the merge left nothing behind
    assert_eq!(git.git(&origin, &["log", "-1", "--format=%s", "emery/hardened"]), "seal");
    assert!(!noident.join(".git/MERGE_HEAD").exists(), "the failed merge was unwound");
    assert_eq!(git.git(&noident, &["rev-list", "--count", "HEAD"]), "1", "its head did not move");
}

#[tokio::test]
async fn vcs_refusals() {
    let git = Hermetic::new();
    let (_, url) = git.bare("origin.git");
    let missing = format!("file://{}", git.path("missing.git").display());
    let unanswered = "https://127.0.0.1:1/x.git".to_owned();
    let scratch = scratch();
    run(&git, &scratch, test_programs::VCS_REFUSALS, vec![url, missing, unanswered]).await;
    let at = |name: &str| scratch.path().join(name);

    // nothing refused reached the repository
    assert_eq!(git.git(&at("repo"), &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(git.git(&at("repo"), &["status", "--porcelain"]), "");
    let listed = git.git(&at("repo"), &["worktree", "list", "--porcelain"]);
    assert_eq!(listed.matches("worktree ").count(), 1, "{listed}");

    // a clone that failed left the place the runtime laid for it empty
    let left = fs::read_dir(at("clone-missing")).expect("the place was laid").count();
    assert_eq!(left, 0);
}

// A repository at `repo` with one uncommitted file, for a suite to salt and
// seal as it needs.
fn seed(git: &Hermetic, repo: &Path, file: &str) {
    fs::create_dir_all(repo).expect("the repository directory");
    git.git(repo, &["init", "--quiet"]);
    fs::write(repo.join(file), "a\n").expect("the first file");
}

fn executable(path: &Path, body: &str) {
    fs::write(path, body).expect("writing a script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("marking it executable");
}

// A program that only records that it ran.
fn touch(markers: &Path, name: &str) -> String {
    format!("#!/bin/sh\ntouch '{}/{}'\n", markers.display(), name)
}

// A clean or smudge filter must pass content through, so one left to run both
// records and mangles.
fn filter(markers: &Path, name: &str) -> String {
    format!("#!/bin/sh\ntouch '{}/{}'\ncat\n", markers.display(), name)
}

// A receive-pack or pre-receive that records and refuses, so a push that
// reached it would fail outright.
fn blocking(markers: &Path, name: &str) -> String {
    format!("#!/bin/sh\ntouch '{}/{}'\nexit 1\n", markers.display(), name)
}
