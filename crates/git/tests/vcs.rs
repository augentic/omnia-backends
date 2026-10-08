//! End-to-end tests for the git backend at the `omnia:vcs` boundary: a
//! guest component from `crates/test-programs` runs through the omnia
//! runtime over an `omnia_git::Client`, driving real repositories beneath
//! its `.` mount. The guest asserts what it observes and traps on failure;
//! the test asserts what git left behind, with git as the oracle.

mod support;

use omnia::ExitStatus;
use omnia_test::host::{Backends, Deployment, scratch};
use omnia_wasi_vcs::WasiVcs;
use support::Hermetic;

// Every guest program in `crates/test-programs` must have a matching test
// here; a new program without one fails to compile.
test_programs::foreach_vcs!();

#[tokio::test]
async fn vcs_journey() {
    let git = Hermetic::new();
    let client = git.client().await;
    let (origin, url) = git.bare("origin.git");
    let scratch = scratch();

    let backends = Backends::defaults().await.vcs(client);
    let status = Deployment::new()
        .guest("guest", test_programs::VCS_JOURNEY)
        .args([url])
        .mount(scratch.mount(true))
        .run_host::<WasiVcs, _>(backends)
        .await
        .expect("guest runs");
    assert_eq!(status, ExitStatus::SUCCESS, "the journey guest failed");

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
