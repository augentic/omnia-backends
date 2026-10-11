//! The integration journey one build runs over git, end to end from a
//! guest: clone a bare origin into the `.` mount, seal the project, cut a
//! working copy at its head, commit slices there, label them, merge the
//! label back under a policy and once with a conflict, push the label, and
//! remove the working copy. The origin's `file://` URL is the one operator
//! argument; the host asserts the origin and the layout with git itself.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs::{Change, ChangeKind, Error, Rule, Strategy, Vcs as _, WasiVcs};
use test_programs::arguments;

omnia_sdk::command!(scenario);

const PROJECT: &str = "./project";
const WORK: &str = "./work";

async fn scenario() {
    let arguments = arguments();
    let url = arguments.first().expect("the origin's URL is the first argument");

    // the project is a clone of an empty origin, sealed with its first commit
    WasiVcs
        .clone_repo(url, PROJECT, omnia_sdk::vcs::CloneOptions { depth: None })
        .await
        .expect("clone");
    assert!(matches!(WasiVcs.head(PROJECT).await, Err(Error::NotFound(_))), "no commit yet");
    fs::write("project/README.md", "# project\n").expect("writing the readme");
    assert_eq!(
        WasiVcs.pending(PROJECT).await.expect("pending"),
        [Change {
            path: "README.md".to_owned(),
            kind: ChangeKind::Added,
        }]
    );
    let base = WasiVcs.commit(PROJECT, "initial").await.expect("commit").expect("a commit");
    assert_eq!(WasiVcs.head(PROJECT).await.expect("head"), base);

    // a working copy at the base takes the first slice
    WasiVcs.add(PROJECT, WORK, &base).await.expect("add");
    fs::write("work/README.md", "# project\n\nbuilt\n").expect("writing the readme");
    fs::create_dir_all("work/src").expect("creating src");
    fs::write("work/src/lib.rs", "pub fn slice() {}\n").expect("writing the slice");
    let slice_1 = WasiVcs.commit(WORK, "slice 1").await.expect("commit").expect("a commit");
    WasiVcs.label(PROJECT, "emery/rev1", &slice_1).await.expect("label");

    // merged into the project under a policy, over an edit of its own
    fs::write("project/README.md", "# project\n\nedited\n").expect("editing the readme");
    WasiVcs.commit(PROJECT, "project edit").await.expect("commit").expect("a commit");
    let policy = [Rule {
        paths: "README.md".to_owned(),
        strategy: Strategy::Theirs,
    }];
    let merged = WasiVcs.merge(PROJECT, "emery/rev1", "merge rev1", &policy).await.expect("merge");
    assert!(merged.commit.is_some(), "the policy resolves the one conflict");
    assert_eq!(merged.conflicts, Vec::<String>::new());
    assert_eq!(fs::read_to_string("project/README.md").expect("readme"), "# project\n\nbuilt\n");
    assert!(fs::metadata("project/src/lib.rs").is_ok(), "the slice reached the project");

    // a second slice, merged with no policy, conflicts and leaves the project as it was
    fs::write("work/README.md", "# project\n\nslice 2\n").expect("editing the readme");
    let slice_2 = WasiVcs.commit(WORK, "slice 2").await.expect("commit").expect("a commit");
    WasiVcs.label(PROJECT, "emery/rev2", &slice_2).await.expect("label");
    fs::write("project/README.md", "# project\n\nproject again\n").expect("editing the readme");
    let before = WasiVcs.commit(PROJECT, "project again").await.expect("commit").expect("a commit");
    let conflicted = WasiVcs.merge(PROJECT, "emery/rev2", "merge rev2", &[]).await.expect("merge");
    assert_eq!(conflicted.commit, None);
    assert_eq!(conflicted.conflicts, ["README.md"]);
    assert_eq!(WasiVcs.head(PROJECT).await.expect("head"), before);
    assert_eq!(
        fs::read_to_string("project/README.md").expect("readme"),
        "# project\n\nproject again\n"
    );
    assert_eq!(WasiVcs.pending(PROJECT).await.expect("pending"), []);

    // the first label reaches the origin and comes back by fetch
    WasiVcs.push(PROJECT, "origin", "emery/rev1").await.expect("push");
    WasiVcs.fetch(PROJECT, "origin").await.expect("fetch");
    assert_eq!(WasiVcs.fetched(PROJECT, "origin", "emery/rev1").await.expect("fetched"), slice_1);

    // the working copy goes
    WasiVcs.remove(WORK).await.expect("remove");
    assert!(fs::metadata("work").is_err(), "the working copy is gone");
}
