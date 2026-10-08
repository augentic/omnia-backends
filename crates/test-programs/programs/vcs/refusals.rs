//! What git refuses, read into the typed error, from a guest: an unknown
//! revision is `not-found`, a plain directory is `not-a-repository`, a
//! place already holding a repository or files is `exists`, a remote that
//! does not exist is `not-found` where one nothing answers is `access`, and
//! what git alone can say stays `other`. The operator arguments are the
//! origin's `file://` URL, a `file://` URL nothing holds, and a URL nothing
//! answers; the host asserts the repository came through untouched.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs::{CloneOptions, Error, Vcs as _, WasiVcs};
use test_programs::arguments;

omnia_sdk::command!(scenario);

const WHOLE: CloneOptions = CloneOptions { depth: None };
const REPO: &str = "./repo";
const ELSEWHERE: &str = "./elsewhere";
const TAKEN: &str = "./taken";

fn not_found(name: &str) -> Error {
    Error::NotFound(name.to_owned())
}

// A refusal naming the place it stood at: the path is the host's, so only
// its tail is the guest's to check.
fn exists_at<T>(result: Result<T, Error>, name: &str) {
    let error = result.err().expect("a refusal");
    assert!(
        matches!(error, Error::Exists(ref path) if path.ends_with(name)),
        "expected exists at `{name}`, got {error:?}"
    );
}

async fn scenario() {
    let arguments = arguments();
    let [url, missing, unanswered] = arguments.as_slice() else {
        panic!("the origin, missing, and unanswered URLs are the arguments");
    };

    WasiVcs.init(REPO).await.expect("init");
    fs::write("repo/a.txt", "a\n").expect("writing a");
    let first = WasiVcs.commit(REPO, "a").await.expect("commit").expect("a");
    fs::create_dir_all("elsewhere").expect("creating a plain directory");
    fs::create_dir_all("taken").expect("creating a directory with files");
    fs::write("taken/x", "x\n").expect("filling it");

    // store
    assert_eq!(WasiVcs.resolve(REPO, "nope").await, Err(not_found("nope")));
    assert_eq!(WasiVcs.resolve(ELSEWHERE, "HEAD").await, Err(Error::NotARepository));
    assert_eq!(WasiVcs.head(ELSEWHERE).await, Err(Error::NotARepository));
    assert_eq!(WasiVcs.commit(ELSEWHERE, "x").await, Err(Error::NotARepository));
    assert_eq!(WasiVcs.merge(REPO, "nope", "x", &[]).await, Err(not_found("nope")));

    // workspace
    exists_at(WasiVcs.init(REPO).await, "/repo");
    exists_at(WasiVcs.add(REPO, TAKEN, &first).await, "/taken");
    assert_eq!(WasiVcs.add(REPO, "./fresh", "nope").await, Err(not_found("nope")));
    assert_eq!(WasiVcs.remove(ELSEWHERE).await, Err(Error::NotARepository));
    let main = WasiVcs.remove(REPO).await.expect_err("the main working tree stays");
    assert!(
        matches!(main, Error::Other(ref message) if message.contains("main working tree")),
        "{main:?}"
    );
    assert_eq!(WasiVcs.pending(ELSEWHERE).await, Err(Error::NotARepository));

    // transport
    exists_at(WasiVcs.clone_repo(url, TAKEN, WHOLE).await, "/taken");
    assert_eq!(
        WasiVcs.clone_repo(missing, "./clone-missing", WHOLE).await,
        Err(not_found(missing))
    );
    let access = WasiVcs.clone_repo(unanswered, "./clone-unanswered", WHOLE).await;
    assert!(matches!(access, Err(Error::Access(_))), "a remote nothing answers: {access:?}");
    assert_eq!(WasiVcs.fetch(REPO, "nope").await, Err(not_found("nope")));
    assert_eq!(WasiVcs.label(REPO, "lbl", "nope").await, Err(not_found("nope")));
    assert_eq!(WasiVcs.push(REPO, "nope", "main").await, Err(not_found("nope")));
    WasiVcs.clone_repo(url, "./clone", WHOLE).await.expect("clone");
    assert_eq!(WasiVcs.push("./clone", "origin", "nolabel").await, Err(not_found("nolabel")));
}
