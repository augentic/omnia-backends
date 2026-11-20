//! The integration journey one build runs over git, end to end from a
//! guest: clone a bare origin into the `.` mount, seal the project, cut a
//! working copy at its head, commit slices there, bookmark them, merge the
//! bookmark back under a policy and once with a conflict, push the bookmark,
//! and remove the working copy. The origin's `file://` URL is the one
//! operator argument; the host asserts the origin and the layout with git
//! itself.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs::{self, ChangeKind, CommitId, Rule, Strategy, Unmerged, Workspace};
use test_programs::arguments;

omnia_sdk::command::export!(scenario);

const PROJECT: &str = "./project";
const WORK: &str = "./work";

async fn scenario() {
    let arguments = arguments();
    let url = arguments.first().expect("the origin's URL is the first argument");

    // the project is a clone of an empty origin, sealed with its first commit
    let project = vcs::clone(url, PROJECT, None).await.expect("clone").expect("a new workspace");
    assert_eq!(project.head().await.expect("head"), None, "no commit yet");
    fs::write("project/README.md", "# project\n").expect("writing the readme");
    assert_eq!(pending(&project).await, [("README.md".to_owned(), ChangeKind::Added)]);
    let base = commit(&project, "initial").await;
    assert_eq!(project.head().await.expect("head"), Some(base.clone()));

    // a working copy at the base takes the first slice
    let work = project.add(WORK, &base).await.expect("add").expect("a new workspace");
    fs::write("work/README.md", "# project\n\nbuilt\n").expect("writing the readme");
    fs::create_dir_all("work/src").expect("creating src");
    fs::write("work/src/lib.rs", "pub fn slice() {}\n").expect("writing the slice");
    let slice_1 = commit(&work, "slice 1").await;
    bookmark(&project, "emery/rev1", &slice_1).await;

    // merged into the project under a policy, over an edit of its own
    fs::write("project/README.md", "# project\n\nedited\n").expect("editing the readme");
    commit(&project, "project edit").await;
    let policy = vec![Rule {
        pattern: "README.md".to_owned(),
        strategy: Strategy::Theirs,
    }];
    project
        .merge("emery/rev1".to_owned(), "merge rev1".to_owned(), policy)
        .await
        .expect("merge")
        .expect("the policy resolves the one conflict");
    assert_eq!(fs::read_to_string("project/README.md").expect("readme"), "# project\n\nbuilt\n");
    assert!(fs::metadata("project/src/lib.rs").is_ok(), "the slice reached the project");

    // a second slice, merged with no policy, conflicts and leaves the project as it was
    fs::write("work/README.md", "# project\n\nslice 2\n").expect("editing the readme");
    let slice_2 = commit(&work, "slice 2").await;
    bookmark(&project, "emery/rev2", &slice_2).await;
    fs::write("project/README.md", "# project\n\nproject again\n").expect("editing the readme");
    let before = commit(&project, "project again").await;
    let unmerged = project
        .merge("emery/rev2".to_owned(), "merge rev2".to_owned(), vec![])
        .await
        .expect("merge")
        .expect_err("the merge conflicts");
    let Unmerged::Conflicts(conflicts) = unmerged else {
        panic!("expected conflicts, got {unmerged:?}");
    };
    assert_eq!(conflicts, ["README.md"]);
    assert_eq!(project.head().await.expect("head"), Some(before));
    assert_eq!(
        fs::read_to_string("project/README.md").expect("readme"),
        "# project\n\nproject again\n"
    );
    assert_eq!(pending(&project).await, []);

    // the first bookmark reaches the origin and comes back by fetch
    project.push("origin", "emery/rev1").await.expect("push").expect("a forward push");
    project.fetch("origin").await.expect("fetch");
    let fetched = project.remote_bookmark("origin", "emery/rev1").await.expect("remote bookmark");
    assert_eq!(fetched, Some(slice_1));

    // the working copy goes
    work.remove().await.expect("remove");
    assert!(fs::metadata("work").is_err(), "the working copy is gone");
}

async fn pending(workspace: &Workspace) -> Vec<(String, ChangeKind)> {
    let changes = workspace.status_all().await.expect("status");
    changes.into_iter().map(|change| (change.path, change.kind)).collect()
}

async fn commit(workspace: &Workspace, message: &str) -> CommitId {
    workspace.commit(message.to_owned()).await.expect("commit").expect("a commit")
}

async fn bookmark(workspace: &Workspace, name: &str, revision: &str) {
    let moved = workspace.set_bookmark(name.to_owned(), revision.to_owned(), None).await;
    moved.expect("bookmark").expect("no precondition to fail");
}
