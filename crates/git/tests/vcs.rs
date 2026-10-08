//! End-to-end tests for the git backend at the `omnia:vcs` boundary: a
//! guest component from `crates/test-programs` runs through the omnia
//! runtime over an `omnia_git::Client`, driving real repositories beneath
//! its `.` mount. The guest asserts what it observes and traps on failure;
//! the test asserts what git left behind, with git as the oracle.

mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

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
    assert_eq!(
        git.git(&at("binary"), &["rev-parse", "HEAD:blob.bin"]),
        git.git(&at("binary"), &["rev-parse", "HEAD^2:blob.bin"]),
        "the merge holds theirs' blob, not a re-encoding of it"
    );
    assert_eq!(git.git(&at("binary"), &["status", "--porcelain"]), "");

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
    let salt = Salt::new(&git, scratch.path());
    let alternates = salt.alternates();
    let (repo, elsewhere, checked) = salt.hostile_repo(&alternates);
    salt.checked_out(&checked);
    let origin = salt.hostile_origin(&repo, &alternates);
    let noident = salt.identityless();
    salt.rewriter(&origin);
    salt.proxied();
    let ssh_argv = salt.packer();
    salt.lazy();
    salt.bytes();

    run(&git, &scratch, test_programs::VCS_HARDENING, vec![]).await;

    // nothing the repositories salted ran
    let ran: Vec<_> = fs::read_dir(&salt.markers)
        .expect("markers")
        .filter_map(Result::ok)
        .map(|e| e.file_name())
        .collect();
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

    // the repository with the unnameable key sealed nothing
    assert_eq!(git.git(&scratch.path().join("bytes"), &["rev-list", "--count", "HEAD"]), "1");
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

    // The command a repository names to list its alternates' refs.
    fn alternate_refs(&self) -> String {
        format!("touch {}; echo", self.markers.join("alternate-refs").display())
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

    // A repository whose own configuration would run a program on the host at
    // the next checkout, status, commit, fetch, or push, and whose work tree
    // points at a directory outside the place; with the directory, and with
    // a working copy of its history, cloned before the salting, for a local
    // push to land in.
    fn hostile_repo(&self, alternates: &str) -> (PathBuf, PathBuf, PathBuf) {
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
        let argv = self.git.path("ssh-argv");
        let ssh = self.git.path("ssh");
        executable(
            &ssh,
            &format!("#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\nexit 255\n", argv.display()),
        );
        self.git.host("core.sshCommand", &ssh.display().to_string());
        argv
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
}
