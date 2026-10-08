//! End-to-end tests for the git backend at the `omnia:vcs` boundary: a
//! guest component from `crates/test-programs` runs through the omnia
//! runtime over an `omnia_git::Client`, driving real repositories beneath
//! its `.` mount. The guest asserts what it observes and traps on failure;
//! the test asserts what git left behind, with git as the oracle.

mod support;

use std::fs;

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
