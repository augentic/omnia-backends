//! End-to-end tests for the git backend at the `omnia:vcs` boundary: a
//! guest component from `crates/test-programs` runs through the omnia
//! runtime over an `omnia_git::Client`, driving real repositories beneath
//! its `.` mount. The guest asserts what it observes and traps on failure;
//! the test asserts what git left behind, with git as the oracle.

mod support;

use std::ffi::OsString;
use std::io::{BufRead as _, BufReader, Write as _};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{fs, thread};

use omnia::ExitStatus;
use omnia_git::Client;
use omnia_test::host::{Backends, Deployment, Scratch, scratch};
use omnia_wasi_vcs::WasiVcs;
use support::Hermetic;

// Every guest program in `crates/test-programs` must have a matching test
// here; a new program without one fails to compile.
test_programs::foreach_vcs!();

// Run `program` over the hermetic git with the scratch root as its `.`
// mount, held to a clean exit.
async fn run(git: &Hermetic, scratch: &Scratch, program: &str, args: Vec<String>) {
    run_over(git.client().await, scratch, program, args).await;
}

// `run`, over a client of the scenario's own.
async fn run_over(client: Client, scratch: &Scratch, program: &str, args: Vec<String>) {
    let backends = Backends::defaults().await.vcs(client);
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
    let at = |name: &str| scratch.path().join(name);

    // a branch and a tag of one name at two commits, which the guest cannot
    // lay itself since the boundary makes no tags
    let shadowed = at("shadowed");
    seed(&git, &shadowed, "a.txt");
    git.git(&shadowed, &["add", "-A"]);
    git.git(&shadowed, &["commit", "-qm", "first"]);
    git.git(&shadowed, &["branch", "slice"]);
    fs::write(shadowed.join("b.txt"), "b\n").expect("a second file");
    git.git(&shadowed, &["add", "-A"]);
    git.git(&shadowed, &["commit", "-qm", "second"]);
    git.git(&shadowed, &["tag", "slice"]);
    let branch = git.git(&shadowed, &["rev-parse", "refs/heads/slice"]);
    let tag = git.git(&shadowed, &["rev-parse", "refs/tags/slice"]);

    let arguments = vec!["shadowed".to_owned(), branch.clone(), tag.clone()];
    run(&git, &scratch, test_programs::VCS_MATRIX, arguments).await;

    // the merge by label is one commit over both parents
    assert_eq!(git.git(&at("repo"), &["log", "--first-parent", "--format=%s"]), "merge feature\na");
    let parents = git.git(&at("repo"), &["rev-list", "--parents", "-1", "HEAD"]);
    assert_eq!(parents.split_whitespace().count(), 3, "{parents}");

    // the conflicted merge was unwound, nothing of it left in progress
    assert!(!at("conflict/.git/MERGE_HEAD").exists(), "a merge was left in progress");

    // a policy leaves nothing of itself in the tree, and seals a message that
    // is commentary to git whole
    assert_eq!(git.git(&at("policy"), &["status", "--porcelain"]), "");
    assert_eq!(git.git(&at("policy"), &["log", "-1", "--format=%B"]), "# merge under policy");

    // the policy's side is what the merge commit holds, over the repository's
    // own `merge=union` attribute, and the sealed tree is clean of temporaries
    assert_eq!(git.git(&at("attributed"), &["show", "HEAD:data.txt"]), "theirs");
    assert_eq!(git.git(&at("attributed"), &["status", "--porcelain"]), "");
    assert_eq!(git.git(&at("moddel"), &["diff", "--name-status", "HEAD^1", "HEAD"]), "D\tkeep.txt");
    assert_eq!(git.git(&at("delmod"), &["diff", "--name-status", "HEAD^2", "HEAD"]), "D\tgone.txt");

    // a rule over a path one side alone added resolved nothing: each side's
    // addition is what the merge brought the other
    assert_eq!(
        git.git(&at("onesided"), &["diff", "--name-status", "HEAD^1", "HEAD"]),
        "A\ttheirs-only.txt"
    );
    assert_eq!(
        git.git(&at("onesided"), &["diff", "--name-status", "HEAD^2", "HEAD"]),
        "A\tours-only.txt"
    );
    assert_eq!(git.git(&at("onesided"), &["status", "--porcelain"]), "");

    // a rename followed: the sealed tree is the kept side's, whole
    assert_eq!(git.git(&at("renamed"), &["diff", "--name-status", "HEAD^1", "HEAD"]), "");
    assert_eq!(git.git(&at("renamed-theirs"), &["diff", "--name-status", "HEAD^2", "HEAD"]), "");
    assert_eq!(git.git(&at("renamed"), &["status", "--porcelain"]), "");
    assert_eq!(git.git(&at("renamed-theirs"), &["status", "--porcelain"]), "");
    assert_eq!(
        git.git(&at("binary"), &["rev-parse", "HEAD:blob.bin"]),
        git.git(&at("binary"), &["rev-parse", "HEAD^2:blob.bin"]),
        "the merge holds theirs' blob, not a re-encoding of it"
    );
    assert_eq!(git.git(&at("binary"), &["status", "--porcelain"]), "");

    // the log the guest read is git's own first-parent chain, and the merge
    // the policy sealed carries the message as given, `#` line and all
    assert_eq!(
        git.git(&at("logged"), &["log", "--first-parent", "--format=%s"]),
        "after\nmerge slice\nours\na"
    );
    assert_eq!(
        git.git(&at("logged"), &["log", "-1", "--format=%B", "HEAD~1"]),
        "merge slice\n\n# notes\nSlice: SLICE-001\nWave: 1"
    );

    // the label the guest read back is the branch, under the tag git prefers
    assert_ne!(branch, tag);
    assert_eq!(git.git(&shadowed, &["log", "-1", "--format=%s", &branch]), "first");
    assert_eq!(git.git(&shadowed, &["log", "-1", "--format=%s", &tag]), "second");
    assert_eq!(
        git.git(&shadowed, &["rev-parse", "slice"]),
        tag,
        "git resolves the bare name to the tag"
    );

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
    let config = git.git(&at("kinds"), &["config", "--local", "--list"]);
    assert!(!config.contains("core.worktree"), "init recorded a work tree: {config}");

    // the dirty working copy left git's list with its files
    assert!(!at("copies/work").exists());
    let listed = git.git(&at("dirty"), &["worktree", "list", "--porcelain"]);
    assert_eq!(listed.matches("worktree ").count(), 1, "{listed}");

    // the copy whose `.git` file was rewritten sealed into the repository it
    // was cut from, read through the gitdir `worktree add` laid, its detached
    // HEAD the commit's id: the fake repository beneath it is one gitlink in
    // the commit (`add -A` over an embedded repository records the gitlink
    // and warns) and holds its one decoy commit
    assert_eq!(fs::read_to_string(at("pinned-work/.git")).expect(".git"), "gitdir: fake/.git\n");
    let head = fs::read_to_string(at("pinned/.git/worktrees/pinned-work/HEAD")).expect("HEAD");
    let sealed = head.trim();
    assert_eq!(git.git(&at("pinned"), &["log", "-1", "--format=%s", sealed]), "sealed");
    assert_eq!(
        git.git(&at("pinned"), &["show", "--format=", "--name-status", sealed]),
        "A\tb.txt\nA\tfake"
    );
    assert_eq!(git.git(&at("pinned-work/fake"), &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(git.git(&at("pinned-work/fake"), &["log", "-1", "--format=%s"]), "fake");
}

#[tokio::test]
async fn vcs_transport() {
    let git = Hermetic::new();
    let (origin, url) = git.bare("origin.git");
    let scratch = scratch();
    let at = |name: &str| scratch.path().join(name);

    // a repository that prunes on fetch, holding a packed remote-tracking ref
    // whose bookmark the origin lacks, so the prune rewrites `packed-refs`
    let pruned = at("pruned");
    seed(&git, &pruned, "a.txt");
    git.git(&pruned, &["add", "-A"]);
    git.git(&pruned, &["commit", "-qm", "base"]);
    git.git(&pruned, &["remote", "add", "origin", &url]);
    git.git(&pruned, &["update-ref", "refs/remotes/origin/emery/gone", "HEAD"]);
    git.git(&pruned, &["pack-refs", "--all"]);
    git.git(&pruned, &["config", "--local", "fetch.prune", "true"]);

    run(&git, &scratch, test_programs::VCS_TRANSPORT, vec![url]).await;

    // the origin holds the moved label at the first clone's head
    assert_eq!(
        git.git(&origin, &["rev-parse", "emery/rev-1"]),
        git.git(&at("clones/first"), &["rev-parse", "HEAD"])
    );
    assert_eq!(git.git(&origin, &["log", "-1", "--format=%s", "emery/rev-1"]), "c");

    // the shallow clone holds the commit it asked for and the one it fetched
    assert!(at("clones/shallow/.git/shallow").is_file());
    assert_eq!(git.git(&at("clones/shallow"), &["rev-list", "--count", "origin/emery/rev-1"]), "2");
}

#[tokio::test]
async fn vcs_hardening() {
    let git = Hermetic::new();
    let scratch = scratch();
    let salt = Salt::new(&git, scratch.path());
    let alternates = salt.alternates();
    let (bundle, bundled) = salt.host_bundle();
    let (repo, elsewhere, checked) = salt.hostile_repo(&alternates, &bundle);
    salt.checked_out(&checked);
    let origin = salt.hostile_origin(&repo, &alternates);
    let noident = salt.identityless();
    salt.rewriter(&origin);
    salt.proxied();
    let ssh_argv = salt.packer();
    let asking = salt.asking();
    salt.lazy();
    let bundling = salt.bundling_origin(&bundle);
    salt.bytes();
    let outer = salt.enclosing();

    run(&git, &scratch, test_programs::VCS_HARDENING, vec![bundling]).await;

    // nothing the repositories salted ran
    let ran = salt.ran();
    assert!(ran.is_empty(), "a planted program ran: {ran:?}");

    // the commit and the push still landed, the commit holding the place's
    // change alone and nothing from where core.worktree pointed, and the merge
    // left nothing behind
    assert_eq!(git.git(&origin, &["log", "-1", "--format=%s", "emery/hardened"]), "seal");
    assert_eq!(
        git.git(&origin, &["show", "--format=", "--name-status", "emery/hardened"]),
        "A\tb.txt"
    );
    assert!(
        elsewhere.join("secret.txt").exists(),
        "the directory outside the place was left alone"
    );
    // the local push landed the label in the working copy and left its
    // checked-out branch and its tree as they were
    assert_eq!(git.git(&checked, &["log", "-1", "--format=%s", "emery/hardened"]), "seal");
    assert_eq!(git.git(&checked, &["rev-list", "--count", "main"]), "1");
    assert!(!checked.join("b.txt").exists(), "the copy's tree was not written");
    assert!(!noident.join(".git/MERGE_HEAD").exists(), "the failed merge was unwound");
    assert_eq!(git.git(&noident, &["rev-list", "--count", "HEAD"]), "1", "its head did not move");

    // the rewriting repository carried nothing onto the origin
    let refs = git.git(&origin, &["for-each-ref", "--format=%(refname)"]);
    assert_eq!(refs, "refs/heads/emery/hardened", "{refs}");

    // the operator's ssh was handed git's own pack programs, never the
    // repository's, once for the fetch and once for the push, and nothing at
    // all for the partial clone's promisor
    let argv = fs::read_to_string(&ssh_argv).expect("the host's ssh ran");
    let carried: Vec<_> = argv.lines().filter(|line| line.ends_with(".git'")).collect();
    assert_eq!(carried, ["git-upload-pack '/x.git'", "git-receive-pack '/x.git'"], "{argv}");

    // the asking host was sent its refs request twice: bare, then with the
    // credentials the host's own helper supplied, `omnia:secret`
    let refs = "GET /x.git/info/refs?service=git-upload-pack HTTP/1.1";
    assert_eq!(asking.sent(), [format!("{refs} -"), format!("{refs} Basic b21uaWE6c2VjcmV0")]);

    // the host's bundle reached neither the repository that named it nor the
    // clone its origin advertised it to
    for place in [&repo, &scratch.path().join("bundled")] {
        let objects =
            git.git(place, &["cat-file", "--batch-all-objects", "--batch-check=%(objectname)"]);
        assert!(!objects.contains(&bundled), "the host's bundle reached {}", place.display());
    }

    // the repository with the unnameable key sealed nothing
    assert_eq!(git.git(&scratch.path().join("bytes"), &["rev-list", "--count", "HEAD"]), "1");

    // the repository enclosing a lent directory was neither read nor sealed
    assert_eq!(git.git(&outer, &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(git.git(&outer, &["status", "--porcelain"]), "?? inner/");
}

#[tokio::test]
async fn vcs_planted() {
    let git = Hermetic::new();
    let scratch = scratch();
    let salt = Salt::new(&git, scratch.path());
    let ssh_argv = salt.host_ssh();
    let (origin, url) = salt.planted_origin();
    let template = salt.template();
    salt.planted_ssh();
    let filtered = salt.planted_filter();
    salt.planted_add();
    let merged = salt.planted_merge();
    let rewritten = salt.planted_rewrite(&origin);
    let unlisted = salt.unlisted();
    let faithful = salt.faithful();

    let planting = support::connect(git.planting(&template)).await.expect("the planting git");
    run_over(planting, &scratch, test_programs::VCS_PLANTED, vec![url]).await;

    // nothing the plants named ran, the merge's included once it landed
    let ran = salt.ran();
    assert!(ran.is_empty(), "a planted program ran: {ran:?}");
    assert!(!merged.join(".git/plant-merge").exists(), "the merge's plant landed");

    // the fetch and the clone over ssh dialled the host's ssh
    let argv = fs::read_to_string(&ssh_argv).expect("the host's ssh ran");
    let carried: Vec<_> = argv.lines().filter(|line| line.ends_with(".git'")).collect();
    assert_eq!(carried, ["git-upload-pack '/x.git'", "git-upload-pack '/x.git'"], "{argv}");

    // the filtered change sealed, the rewritten fetch carried nothing, and
    // the repository whose listing failed sealed nothing
    assert_eq!(git.git(&filtered, &["log", "-1", "--format=%s"]), "seal");
    assert_eq!(git.git(&rewritten, &["for-each-ref", "refs/remotes"]), "");
    assert_eq!(git.git(&unlisted, &["rev-list", "--count", "HEAD"]), "1");

    // the commit carries the identity git reads from the repository's scopes
    let ident = git.git(&faithful, &["var", "GIT_AUTHOR_IDENT"]);
    let identity = ident.rsplitn(3, ' ').nth(2).expect("a name and an email");
    assert_eq!(git.git(&faithful, &["log", "-1", "--format=%an <%ae>"]), identity);
}

#[tokio::test]
async fn vcs_refusals() {
    let git = Hermetic::new();
    let (origin, url) = git.bare("origin.git");
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

    // the origin's label is the first clone's, the second's refused twice
    // and never forced over it
    assert_eq!(
        git.git(&origin, &["rev-parse", "emery/x"]),
        git.git(&at("clone-a"), &["rev-parse", "HEAD"])
    );
    assert_eq!(git.git(&origin, &["log", "-1", "--format=%s", "emery/x"]), "a's");
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

// The scratch a hardening scenario is laid in: the oracle git, the root, and
// the directory every planted program records itself in.
struct Salt<'a> {
    git: &'a Hermetic,
    root: &'a Path,
    markers: PathBuf,
}

impl<'a> Salt<'a> {
    fn new(git: &'a Hermetic, root: &'a Path) -> Self {
        let markers = root.join("markers");
        fs::create_dir_all(&markers).expect("the markers directory");
        Self { git, root, markers }
    }

    fn at(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn config(&self, repo: &Path, key: &str, value: &str) {
        self.git.git(repo, &["config", "--local", key, value]);
    }

    // The programs that recorded themselves running.
    fn ran(&self) -> Vec<OsString> {
        fs::read_dir(&self.markers)
            .expect("markers")
            .filter_map(Result::ok)
            .map(|e| e.file_name())
            .collect()
    }

    // The command a repository names to list its alternates' refs.
    fn alternate_refs(&self) -> String {
        format!("touch {}; echo", self.markers.join("alternate-refs").display())
    }

    // A shell command that only records that it ran, and fails, quoted for a
    // configuration file, where an unquoted `;` starts a comment.
    fn marking(&self, name: &str) -> String {
        format!("\"touch {}; false\"", self.markers.join(name).display())
    }

    // The host's ssh, which records what it was handed and answers nothing;
    // the file the record lands in.
    fn host_ssh(&self) -> PathBuf {
        let argv = self.git.path("ssh-argv");
        let ssh = self.git.path("ssh");
        executable(
            &ssh,
            &format!("#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\nexit 255\n", argv.display()),
        );
        self.git.host("core.sshCommand", &ssh.display().to_string());
        argv
    }

    // A sealed repository beside the others, as an alternates line for a
    // fetch and a receive to have alternate refs to list.
    fn alternates(&self) -> String {
        let nested = self.at("nested");
        seed(self.git, &nested, "n.txt");
        self.git.git(&nested, &["add", "-A"]);
        self.git.git(&nested, &["commit", "-qm", "nested"]);
        format!("{}\n", nested.join(".git/objects").display())
    }

    // A bundle on the host, outside the place, of a commit no repository the
    // guest holds has; the bundle and the commit.
    fn host_bundle(&self) -> (PathBuf, String) {
        let source = self.git.path("host-repo");
        seed(self.git, &source, "h.txt");
        self.git.git(&source, &["add", "-A"]);
        self.git.git(&source, &["commit", "-qm", "host"]);
        let bundle = self.git.path("host.bundle");
        let path = bundle.display().to_string();
        self.git.git(&source, &["bundle", "create", "--quiet", &path, "--all"]);
        (bundle, self.git.git(&source, &["rev-parse", "HEAD"]))
    }

    // A repository whose own configuration would run a program on the host at
    // the next checkout, status, commit, fetch, or push, would take `bundle`
    // at the next fetch, and points its work tree at a directory outside the
    // place; with the directory, and with a working copy of its history,
    // cloned before the salting, for a local push to land in.
    fn hostile_repo(&self, alternates: &str, bundle: &Path) -> (PathBuf, PathBuf, PathBuf) {
        let repo = self.at("repo");
        seed(self.git, &repo, "a.txt");
        fs::write(repo.join(".gitattributes"), "* filter=evil\n").expect("the attributes");
        self.git.git(&repo, &["add", "-A"]);
        self.git.git(&repo, &["commit", "-qm", "attributes"]);
        let checked = self.at("checked");
        let (from, to) = (repo.display().to_string(), checked.display().to_string());
        self.git.git(self.root, &["clone", "--quiet", &from, &to]);
        self.git.git(&repo, &["remote", "add", "checked", &to]);
        let hooks = self.at("hostile-hooks");
        fs::create_dir_all(&hooks).expect("the hooks directory");
        executable(&hooks.join("pre-commit"), &touch(&self.markers, "pre-commit"));
        let fsmonitor = self.at("fsmonitor");
        executable(&fsmonitor, &touch(&self.markers, "fsmonitor"));
        let gpg = self.at("gpg");
        executable(&gpg, &touch(&self.markers, "gpg"));
        let smudge = self.at("smudge");
        executable(&smudge, &filter(&self.markers, "smudge"));
        let elsewhere = self.at("elsewhere");
        fs::create_dir_all(&elsewhere).expect("the directory the repository points its tree at");
        fs::write(elsewhere.join("secret.txt"), "secret\n").expect("a file outside the place");
        for (key, value) in [
            ("core.hooksPath", hooks.display().to_string()),
            ("core.fsmonitor", fsmonitor.display().to_string()),
            ("core.alternateRefsCommand", self.alternate_refs()),
            ("core.worktree", elsewhere.display().to_string()),
            ("commit.gpgsign", "true".to_owned()),
            ("push.gpgSign", "true".to_owned()),
            ("gpg.program", gpg.display().to_string()),
            ("filter.evil.smudge", smudge.display().to_string()),
            ("filter.evil.clean", smudge.display().to_string()),
            ("fetch.bundleURI", bundle.display().to_string()),
        ] {
            self.config(&repo, key, &value);
        }
        fs::create_dir_all(repo.join(".git/objects/info")).expect("the objects info directory");
        fs::write(repo.join(".git/objects/info/alternates"), alternates).expect("the alternates");
        (repo, elsewhere, checked)
    }

    // The working copy a local push lands in, whose own configuration would
    // write its checked-out branch on receive, through a smudge filter and a
    // file-system monitor of its own.
    fn checked_out(&self, checked: &Path) {
        let fsmonitor = self.at("checked-fsmonitor");
        executable(&fsmonitor, &touch(&self.markers, "checked-fsmonitor"));
        let smudge = self.at("checked-smudge");
        executable(&smudge, &filter(&self.markers, "checked-smudge"));
        for (key, value) in [
            ("receive.denyCurrentBranch", "updateInstead".to_owned()),
            ("core.fsmonitor", fsmonitor.display().to_string()),
            ("filter.evil.smudge", smudge.display().to_string()),
            ("filter.evil.required", "true".to_owned()),
        ] {
            self.config(checked, key, &value);
        }
    }

    // A bare origin whose receive would run a hook and an alternate-refs
    // command of its own and asks for a signed push certificate, reached
    // from `repo` through a client-side receive-pack the repository also
    // salted.
    fn hostile_origin(&self, repo: &Path, alternates: &str) -> PathBuf {
        let origin = self.at("origin.git");
        fs::create_dir_all(&origin).expect("the origin");
        self.git.git(&origin, &["init", "--quiet", "--bare"]);
        let hooks = self.at("origin-hooks");
        fs::create_dir_all(&hooks).expect("the origin hooks");
        executable(&hooks.join("pre-receive"), &blocking(&self.markers, "pre-receive"));
        self.config(&origin, "core.hooksPath", &hooks.display().to_string());
        self.config(&origin, "core.alternateRefsCommand", &self.alternate_refs());
        self.config(&origin, "receive.certNonceSeed", "seed");
        fs::create_dir_all(origin.join("objects/info")).expect("the origin's objects info");
        fs::write(origin.join("objects/info/alternates"), alternates)
            .expect("the origin's alternates");
        let receive_pack = self.at("receive-pack");
        executable(&receive_pack, &blocking(&self.markers, "receive-pack"));
        let url = format!("file://{}", origin.display());
        self.git.git(repo, &["remote", "add", "origin", &url]);
        self.config(repo, "remote.origin.receivepack", &receive_pack.display().to_string());
        origin
    }

    // A repository with no identity, so a clean merge cannot be sealed.
    fn identityless(&self) -> PathBuf {
        let noident = self.at("noident");
        seed(self.git, &noident, "a.txt");
        self.git.git(&noident, &["add", "-A"]);
        self.git.git(&noident, &["commit", "-qm", "base"]);
        self.git.git(&noident, &["branch", "feature"]);
        self.git.git(&noident, &["checkout", "--quiet", "feature"]);
        fs::write(noident.join("b.txt"), "b\n").expect("writing b");
        self.git.git(&noident, &["add", "-A"]);
        self.git.git(&noident, &["commit", "-qm", "feature"]);
        self.git.git(&noident, &["checkout", "--quiet", "main"]);
        self.config(&noident, "user.name", "");
        self.config(&noident, "user.email", "");
        noident
    }

    // A repository whose own insteadOf carries its https remote onto the local
    // origin, where the protocol rule would then let it through.
    fn rewriter(&self, origin: &Path) {
        let rewriter = self.at("rewriter");
        seed(self.git, &rewriter, "a.txt");
        self.git.git(&rewriter, &["add", "-A"]);
        self.git.git(&rewriter, &["commit", "-qm", "base"]);
        self.git.git(&rewriter, &["remote", "add", "evil", "https://example.invalid/x.git"]);
        let rewrite = format!("url.{}.insteadOf", origin.display());
        self.config(&rewriter, &rewrite, "https://example.invalid/x.git");
        assert_eq!(
            self.git.git(&rewriter, &["remote", "get-url", "evil"]),
            origin.display().to_string(),
            "git itself would follow the rewrite onto the local origin"
        );
    }

    // A repository whose own proxy and TLS trust would carry the host's
    // credentials for its https remote through a connection of its choosing.
    fn proxied(&self) {
        let proxied = self.at("proxied");
        seed(self.git, &proxied, "a.txt");
        self.git.git(&proxied, &["add", "-A"]);
        self.git.git(&proxied, &["commit", "-qm", "base"]);
        self.git.git(&proxied, &["remote", "add", "evil", "https://example.invalid/x.git"]);
        self.config(&proxied, "http.proxy", "http://127.0.0.1:1");
        self.config(&proxied, "http.sslVerify", "false");
    }

    // A repository naming the program the operator's ssh would carry to the
    // far host for a fetch and a push, over a host ssh that records what it
    // was handed and answers nothing; the file the record lands in.
    fn packer(&self) -> PathBuf {
        let packer = self.at("packer");
        seed(self.git, &packer, "a.txt");
        self.git.git(&packer, &["add", "-A"]);
        self.git.git(&packer, &["commit", "-qm", "base"]);
        self.git.git(&packer, &["remote", "add", "evil", "ssh://localhost/x.git"]);
        for (key, marker) in [
            ("remote.evil.uploadpack", "ssh-upload-pack"),
            ("remote.evil.receivepack", "ssh-receive-pack"),
        ] {
            self.config(&packer, key, &format!("touch {}", self.markers.join(marker).display()));
        }
        self.host_ssh()
    }

    // A repository whose remote `guarded` is an HTTP host that refuses every
    // request for want of credentials, with a credential helper of its own for
    // every URL and one for that host alone, under a host whose own helper for
    // it answers with credentials; the HTTP host.
    fn asking(&self) -> Asking {
        let asking = Asking::start();
        let repo = self.at("asking");
        seed(self.git, &repo, "a.txt");
        self.git.git(&repo, &["add", "-A"]);
        self.git.git(&repo, &["commit", "-qm", "base"]);
        self.git.git(&repo, &["remote", "add", "guarded", &format!("{}/x.git", asking.url)]);
        let helper = self.at("credential-helper");
        executable(&helper, &touch(&self.markers, "credential-helper"));
        let scoped = self.at("scoped-credential-helper");
        executable(&scoped, &touch(&self.markers, "scoped-credential-helper"));
        let host = self.git.path("host-credential-helper");
        executable(
            &host,
            "#!/bin/sh\ncat > /dev/null\n\
             [ \"$1\" = get ] && printf 'username=omnia\\npassword=secret\\n'\nexit 0\n",
        );
        let scope = format!("credential.{}.helper", asking.url);
        self.config(&repo, "credential.helper", &helper.display().to_string());
        self.config(&repo, &scope, &scoped.display().to_string());
        self.git.host(&scope, &host.display().to_string());
        asking
    }

    // A partial clone whose promisor remote is an ssh host, under a pack
    // command of the repository's own, missing the one blob a checkout needs.
    fn lazy(&self) {
        let lazy = self.at("lazy");
        seed(self.git, &lazy, "a.txt");
        self.git.git(&lazy, &["add", "-A"]);
        self.git.git(&lazy, &["commit", "-qm", "base"]);
        let pack = format!("touch {}", self.markers.join("lazy-upload-pack").display());
        for (key, value) in [
            ("extensions.partialClone", "evil"),
            ("remote.evil.url", "ssh://localhost/lazy.git"),
            ("remote.evil.promisor", "true"),
            ("remote.evil.uploadpack", pack.as_str()),
        ] {
            self.config(&lazy, key, value);
        }
        let blob = self.git.git(&lazy, &["rev-parse", "HEAD:a.txt"]);
        fs::remove_file(lazy.join(format!(".git/objects/{}/{}", &blob[..2], &blob[2..])))
            .expect("removing the blob");
    }

    // An origin on the host that advertises `bundle` to a clone as a file
    // URI, under a host whose own configuration asks a clone for advertised
    // bundles; the origin's URL.
    fn bundling_origin(&self, bundle: &Path) -> String {
        let origin = self.git.path("bundling");
        seed(self.git, &origin, "a.txt");
        self.git.git(&origin, &["add", "-A"]);
        self.git.git(&origin, &["commit", "-qm", "base"]);
        let uri = format!("file://{}", bundle.display());
        for (key, value) in [
            ("uploadpack.advertiseBundleURIs", "true"),
            ("bundle.version", "1"),
            ("bundle.mode", "all"),
            ("bundle.host.uri", uri.as_str()),
        ] {
            self.config(&origin, key, value);
        }
        self.git.host("transfer.bundleURI", "true");
        format!("file://{}", origin.display())
    }

    // A sealed repository with a bare directory inside it, holding a file,
    // for a guest lent the directory alone to try to read and seal the
    // repository through; the repository.
    fn enclosing(&self) -> PathBuf {
        let outer = self.at("outer");
        seed(self.git, &outer, "a.txt");
        self.git.git(&outer, &["add", "-A"]);
        self.git.git(&outer, &["commit", "-qm", "base"]);
        fs::create_dir(outer.join("inner")).expect("the inner directory");
        fs::write(outer.join("inner/c.txt"), "c\n").expect("a file to seal");
        outer
    }

    // A repository naming a clean filter under a subsection that is not
    // UTF-8, which no pin spelled as text can name, with a change for a
    // commit to run it over.
    fn bytes(&self) {
        let bytes = self.at("bytes");
        seed(self.git, &bytes, "a.txt");
        self.git.git(&bytes, &["add", "-A"]);
        self.git.git(&bytes, &["commit", "-qm", "base"]);
        let clean = self.at("bytes-clean");
        executable(&clean, &filter(&self.markers, "bytes-clean"));
        let mut planted = b"[filter \"\xff\"]\n\tclean = ".to_vec();
        planted.extend_from_slice(clean.display().to_string().as_bytes());
        planted.push(b'\n');
        let config = fs::read(bytes.join(".git/config")).expect("the config");
        fs::write(bytes.join(".git/config"), [config, planted].concat())
            .expect("planting the filter");
        fs::write(bytes.join(".gitattributes"), b"* filter=\xff\n").expect("the attributes");
        fs::write(bytes.join("b.txt"), "b\n").expect("a change to filter");
    }

    // A sealed repository whose attributes select the filter `evil` for its
    // one file.
    fn attributed(&self, name: &str) -> PathBuf {
        let repo = self.at(name);
        seed(self.git, &repo, "a.txt");
        fs::write(repo.join(".gitattributes"), "* filter=evil\n").expect("the attributes");
        self.git.git(&repo, &["add", "-A"]);
        self.git.git(&repo, &["commit", "-qm", "base"]);
        repo
    }

    // `attributed`, with `plant` for the planting git to append to its
    // configuration once its first listing returns.
    fn planted(&self, name: &str, plant: &str) -> PathBuf {
        let repo = self.attributed(name);
        fs::write(repo.join(".git/plant"), plant).expect("the plant");
        repo
    }

    // A bare origin holding `attributed`'s one commit; its path, and the
    // `file://` URL a clone reaches it by.
    fn planted_origin(&self) -> (PathBuf, String) {
        let seeded = self.attributed("planted-seed");
        let origin = self.at("planted-origin.git");
        let (from, to) = (seeded.display().to_string(), origin.display().to_string());
        self.git.git(self.root, &["clone", "--quiet", "--bare", &from, &to]);
        let url = format!("file://{}", origin.display());
        (origin, url)
    }

    // What the planting git lays a clone's destination from: a configuration
    // naming the filter `evil` and an ssh command, each recording that it
    // ran.
    fn template(&self) -> PathBuf {
        let template = self.git.path("template");
        fs::create_dir_all(&template).expect("the template");
        let program = self.at("template-filter");
        executable(&program, &filter(&self.markers, "template-filter"));
        let config = format!(
            "[filter \"evil\"]\n\tsmudge = {program}\n\tclean = {program}\n[core]\n\tsshCommand = {ssh}\n",
            program = program.display(),
            ssh = self.marking("template-ssh"),
        );
        fs::write(template.join("config"), config).expect("the template's configuration");
        template
    }

    // A repository whose remote `evil` is an ssh host, planted with an ssh
    // command of its own.
    fn planted_ssh(&self) {
        let plant = format!("[core]\n\tsshCommand = {}\n", self.marking("planted-ssh"));
        let repo = self.planted("planted-ssh", &plant);
        self.git.git(&repo, &["remote", "add", "evil", "ssh://localhost/x.git"]);
    }

    // A repository with a change to seal, planted with the clean filter its
    // attributes select.
    fn planted_filter(&self) -> PathBuf {
        let clean = self.at("planted-clean");
        executable(&clean, &filter(&self.markers, "planted-clean"));
        let plant = format!("[filter \"evil\"]\n\tclean = {}\n", clean.display());
        let repo = self.planted("planted-filter", &plant);
        fs::write(repo.join("b.txt"), "b\n").expect("a change to filter");
        repo
    }

    // A repository planted with the smudge filter its attributes select.
    fn planted_add(&self) {
        let smudge = self.at("planted-smudge");
        executable(&smudge, &filter(&self.markers, "planted-smudge"));
        let plant = format!("[filter \"evil\"]\n\tsmudge = {}\n", smudge.display());
        self.planted("planted-add", &plant);
    }

    // A repository whose branch `feature` and `main` each change their own
    // line of the file its attributes give a merge driver, planted with a
    // driver that would leave the file in conflict for the planting git to
    // append once the merge's listing has returned, as the merge starts.
    fn planted_merge(&self) -> PathBuf {
        let repo = self.at("planted-merge");
        seed(self.git, &repo, "a.txt");
        fs::write(repo.join(".gitattributes"), "m.txt merge=evil\n").expect("the attributes");
        let lay = |content: &str| fs::write(repo.join("m.txt"), content).expect("the merged file");
        lay("one\ntwo\nthree\nfour\nfive\n");
        self.git.git(&repo, &["add", "-A"]);
        self.git.git(&repo, &["commit", "-qm", "base"]);
        self.git.git(&repo, &["checkout", "--quiet", "-b", "feature"]);
        lay("ONE\ntwo\nthree\nfour\nfive\n");
        self.git.git(&repo, &["commit", "-qam", "feature"]);
        self.git.git(&repo, &["checkout", "--quiet", "main"]);
        lay("one\ntwo\nthree\nfour\nFIVE\n");
        self.git.git(&repo, &["commit", "-qam", "main"]);
        let plant = format!("[merge \"evil\"]\n\tdriver = {}\n", self.marking("planted-driver"));
        fs::write(repo.join(".git/plant-merge"), plant).expect("the plant");
        repo
    }

    // A repository whose remote `evil` nothing answers, planted with a
    // rewrite of it onto `origin`, where the protocol rule would then let it
    // through.
    fn planted_rewrite(&self, origin: &Path) -> PathBuf {
        let remote = "https://127.0.0.1:1/x.git";
        let plant = format!("[url \"{}\"]\n\tinsteadOf = {remote}\n", origin.display());
        let repo = self.planted("planted-rewrite", &plant);
        self.git.git(&repo, &["remote", "add", "evil", remote]);
        repo
    }

    // A sealed repository with a change to seal, whose listing the planting
    // git fails.
    fn unlisted(&self) -> PathBuf {
        let repo = self.at("unlisted");
        seed(self.git, &repo, "a.txt");
        self.git.git(&repo, &["add", "-A"]);
        self.git.git(&repo, &["commit", "-qm", "base"]);
        fs::write(repo.join(".git/unlisted"), "").expect("the marker");
        fs::write(repo.join("b.txt"), "b\n").expect("a change to seal");
        repo
    }

    // A repository with a change to seal whose identity is a quoted name in
    // its worktree's own configuration and an email in a file its
    // configuration includes.
    fn faithful(&self) -> PathBuf {
        let repo = self.at("faithful");
        seed(self.git, &repo, "a.txt");
        fs::write(repo.join(".git/identity"), "[user]\n\temail = included@omnia.invalid\n")
            .expect("the included file");
        self.config(&repo, "include.path", "identity");
        self.config(&repo, "extensions.worktreeConfig", "true");
        self.git.git(&repo, &["config", "--worktree", "user.name", "Quoted \"Name\" \\ ;# end"]);
        repo
    }
}

// An HTTP host on a loopback port that refuses every request for want of
// credentials, and the requests it was sent.
struct Asking {
    url: String,
    sent: Arc<Mutex<Vec<String>>>,
}

impl Asking {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let url = format!("http://{}", listener.local_addr().expect("the port's address"));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&sent);
        thread::spawn(move || {
            for stream in listener.incoming() {
                refuse(&stream.expect("a connection"), &record);
            }
        });
        Self { url, sent }
    }

    // Each request as its request line and the credentials it carried, `-`
    // for none.
    fn sent(&self) -> Vec<String> {
        self.sent.lock().expect("the requests").clone()
    }
}

// Every header is read before the answer, since closing a connection with
// unread input resets it and can lose the answer.
fn refuse(mut stream: &TcpStream, sent: &Mutex<Vec<String>>) {
    let mut lines = BufReader::new(stream).lines().map(|line| line.expect("a request line"));
    let request = lines.next().expect("a request");
    let head: Vec<String> = lines.take_while(|line| !line.is_empty()).collect();
    let credentials =
        head.iter().find_map(|line| line.strip_prefix("Authorization: ")).unwrap_or("-");
    sent.lock().expect("the requests").push(format!("{request} {credentials}"));
    stream
        .write_all(
            b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"omnia\"\r\n\
              Content-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .expect("answering the request");
}
