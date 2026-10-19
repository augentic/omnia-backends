//! What a process inside a session sees of its working directory and its
//! environment, what connecting refuses, and what a grant is held to.

mod support;

use std::collections::BTreeMap;
use std::env;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use omnia_sandbox::{Access, Grant, Options, Sandbox, Session};
use support::{BOUND, Captured, connect, proxy_of, run, session, sh};
use tracing_subscriber::layer::SubscriberExt as _;

const PLANTED: &str = "OMNIA_SANDBOX_PLANTED";

async fn environment(session: &Session) -> BTreeMap<String, String> {
    let output = sh(session, "env").output(BOUND).await.expect("env answers");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

async fn pwd(session: &Session) -> String {
    let output = sh(session, "pwd").output(BOUND).await.expect("pwd answers");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

// What `prepare` runs `workspace` with, which `access` says beforehand.
async fn access(sandbox: &Sandbox, workspace: Option<&Path>) -> Access {
    let queried = sandbox.access(workspace).await.expect("the workspace resolves");
    let prepared = session(sandbox, workspace).await.access();
    assert_eq!(queried, prepared, "`access` and `prepare` agree on {workspace:?}");
    prepared
}

// --- Environment ---

// The variable is planted before any thread of the runtime exists.
#[test]
#[expect(unsafe_code, reason = "the planted variable is set on the one thread of the test")]
fn inherited_env() {
    // SAFETY: nothing else runs yet; the runtime below is built after the write.
    unsafe { env::set_var(PLANTED, "leaked") };
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    runtime.block_on(async {
        let sandbox = connect(Options::default()).await;
        let session = session(&sandbox, None).await;
        let scratch = session.scratch().to_string_lossy().into_owned();
        let seen = environment(&session).await;

        assert!(seen.contains_key("PATH"), "PATH is inherited: {seen:#?}");
        assert_eq!(seen.get("HOME"), Some(&scratch), "HOME is the scratch: {seen:#?}");
        assert_eq!(seen.get("TMPDIR"), Some(&scratch), "TMPDIR is the scratch: {seen:#?}");
        assert!(!seen.contains_key(PLANTED), "the host's variable leaked: {seen:#?}");
        for name in ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"] {
            assert!(!seen.contains_key(name), "{name} reached the process: {seen:#?}");
        }

        let sandbox = connect(Options {
            inherit: &[PLANTED],
            ..Options::default()
        })
        .await;
        let seen = environment(&support::session(&sandbox, None).await).await;
        assert_eq!(seen.get(PLANTED).map(String::as_str), Some("leaked"), "{seen:#?}");
    });
}

// --- Working directory ---

#[tokio::test]
async fn workspace_cwd() {
    let workspace = tempfile::tempdir().expect("a workspace");
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, Some(&workspace.path().join("tree"))).await;

    assert_eq!(session.access(), Access::Read);
    assert!(session.access().is_lent());
    let cwd = session.cwd().to_owned();
    assert_eq!(
        cwd,
        std::fs::canonicalize(workspace.path().join("tree")).expect("the workspace was created"),
        "the workspace is created and canonical"
    );
    assert_eq!(pwd(&session).await, cwd.to_string_lossy());
}

#[tokio::test]
async fn private_cwd() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;

    assert_eq!(session.access(), Access::Private);
    assert!(!session.access().is_lent());
    let cwd = session.cwd().to_owned();
    let scratch = std::fs::canonicalize(session.scratch()).expect("the scratch exists");
    assert!(cwd.starts_with(&scratch), "{} is under the scratch", cwd.display());
    assert_eq!(pwd(&session).await, cwd.to_string_lossy());
}

// --- Shell by location ---

#[tokio::test]
async fn shell_roots() {
    let roots = tempfile::tempdir().expect("roots");
    let a = roots.path().join("a");
    let link = roots.path().join("link");
    let missing = roots.path().join("missing");
    let late_link = roots.path().join("late-link");
    std::fs::create_dir(&a).expect("root a");
    std::os::unix::fs::symlink(&a, &link).expect("a symlink to root a");

    let sandbox = connect(Options {
        shell_roots: vec![
            a.clone(),
            missing.clone(),
            late_link.clone(),
            Path::new("relative/root").to_path_buf(),
        ],
        ..Options::default()
    })
    .await;
    assert!(
        sandbox.options().shell_roots.iter().all(|root| root.is_absolute()),
        "shell roots are made absolute: {:?}",
        sandbox.options().shell_roots
    );

    let target = roots.path().join("target");
    std::fs::create_dir(&target).expect("late target");
    std::os::unix::fs::symlink(&target, &late_link).expect("a symlink created after connect");

    assert_eq!(access(&sandbox, Some(&a.join("x"))).await, Access::Shell, "beneath the root");
    assert_eq!(access(&sandbox, Some(&a)).await, Access::Shell, "at the root");
    assert_eq!(access(&sandbox, Some(&link.join("y"))).await, Access::Shell, "through a symlink");
    assert_eq!(
        access(&sandbox, Some(&missing.join("x"))).await,
        Access::Shell,
        "beneath a root created on prepare"
    );
    assert_eq!(
        access(&sandbox, Some(&late_link.join("x"))).await,
        Access::Shell,
        "beneath a symlink root created after connect"
    );
    assert_eq!(
        access(&sandbox, Some(&target.join("y"))).await,
        Access::Shell,
        "beneath the target of a symlink root created after connect"
    );
    assert_eq!(access(&sandbox, Some(&roots.path().join("b"))).await, Access::Read, "beside it");
    assert_eq!(access(&sandbox, None).await, Access::Private, "private");
}

// --- Programs ---

#[tokio::test]
async fn program_lookup() {
    let sandbox = connect(Options::default()).await;

    let error = session(&sandbox, None)
        .await
        .command("omnia-sandbox-no-such-program")
        .expect_err("not on PATH");
    assert!(error.to_string().contains("is not on PATH"), "{error:#}");

    let mut command = session(&sandbox, None).await.command("sh").expect("sh is on PATH");
    command.args(["-c", "echo found"]);
    let output = command.output(BOUND).await.expect("sh answers");
    assert_eq!(output.stdout, b"found\n");
}

// A grant's `PATH` reaches the process, so a toolchain is found inside. The
// program the backend names is resolved against the host's `PATH`, so the
// `sh` planted first on the grant's is never the one spawned.
#[tokio::test]
async fn grant_path() {
    let workspace = tempfile::tempdir().expect("a workspace");
    let tree = workspace.path().join("tree");
    let bin = tree.join("bin");
    std::fs::create_dir_all(&bin).expect("a bin directory");
    std::fs::write(bin.join("sh"), "#!/bin/sh\necho planted\n").expect("a planted sh");
    std::fs::set_permissions(bin.join("sh"), std::fs::Permissions::from_mode(0o755))
        .expect("the planted sh is executable");
    let path = format!("{}:{}", bin.display(), env::var("PATH").expect("the host has a PATH"));
    let sandbox = connect(Options::default()).await;
    let grant = Grant {
        env: vec![("PATH".to_owned(), path.clone())],
        ..Grant::default()
    };
    let session = support::granted(&sandbox, Some(&tree), &grant).await;

    let mut command = session.command("sh").expect("sh is on the host's PATH");
    command.args(["-c", "echo found && printf '%s' \"$PATH\""]);
    let output = command.output(BOUND).await.expect("sh answers");
    assert_eq!(String::from_utf8_lossy(&output.stdout), format!("found\n{path}"));
}

// A variable set on the command reaches the process beside the grant's.
#[tokio::test]
async fn command_env() {
    let sandbox = connect(Options::default()).await;
    let mut command = sh(&session(&sandbox, None).await, "printf '%s' \"$ANSWER\"");
    command.env("ANSWER", "42");
    let output = command.output(BOUND).await.expect("sh answers");
    assert_eq!(output.stdout, b"42");
}

// --- Connect ---

#[tokio::test]
async fn inherit_refused() {
    const HELD: &[(&str, &str)] = &[
        ("HOME", "is set by the sandbox"),
        ("TMPDIR", "is set by the sandbox"),
        ("HTTPS_PROXY", "is set by the sandbox"),
        ("no_proxy", "is set by the sandbox"),
        ("NODE_USE_SYSTEM_CA", "is refused"),
        ("BUN_OPTIONS", "is refused"),
        ("LD_PRELOAD", "is read by the dynamic loader"),
        ("DYLD_INSERT_LIBRARIES", "is read by the dynamic loader"),
        ("GIT_DIR", "selects a repository"),
        ("GIT_INDEX_FILE", "selects a repository"),
        ("CURSOR_API_KEY", "is refused"),
    ];
    for (name, why) in HELD {
        let error = Sandbox::connect(Options {
            inherit: std::slice::from_ref(name),
            refused: &["CURSOR_API_KEY"],
            ..Options::default()
        })
        .await
        .err()
        .unwrap_or_else(|| panic!("`{name}` is refused"));
        let text = error.to_string();
        assert!(text.contains(name) && text.contains(why), "{name}: {text}");
    }
}

// --- The grant ---

// A root the home lacks grants nothing and ends nothing. A toolchain the
// adapter names and this machine does not have is no reason to refuse the
// turn. The roots that exist are granted beside it, canonical.
#[test]
fn absent_read_root() {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(captured.clone()))
        .expect("this test owns the process's subscriber");
    support::under_home(async |home| {
        std::fs::create_dir(home.join("present")).expect("a read root");
        std::os::unix::fs::symlink(home.join("present"), home.join("link")).expect("a link");
        let sandbox = connect(Options::default()).await;

        let session = support::granted(
            &sandbox,
            None,
            &support::reading(&["~/missing", "~/present", "~/link"]),
        )
        .await;
        assert_eq!(session.read_roots(), [home.join("present")], "present once, canonical");

        let events = captured.events();
        let missing = home.join("missing");
        assert!(
            events.iter().any(|event| {
                event.starts_with("WARN ")
                    && event.contains("read root does not exist")
                    && event.contains(&missing.display().to_string())
            }),
            "the skipped root is logged at WARN: {events:#?}"
        );
    });
}

// A root that steps through `.` or `..` is refused as spelled, before it
// resolves, so no spelling reaches around the credential-store check.
#[test]
fn read_root_bounds() {
    support::under_home(async |home| {
        std::fs::create_dir(home.join("tools")).expect("a directory");
        std::fs::write(home.join("note.txt"), "a file\n").expect("a file");
        let outside = tempfile::tempdir().expect("outside the home");
        std::os::unix::fs::symlink(outside.path(), home.join("away")).expect("a link out");
        let sandbox = connect(Options::default()).await;

        for (root, why) in [
            (home.join("tools").display().to_string(), "beneath the home, `~/<path>`"),
            ("~/".to_owned(), "beneath the home, `~/<path>`"),
            ("~".to_owned(), "beneath the home, `~/<path>`"),
            ("tools".to_owned(), "beneath the home, `~/<path>`"),
            ("~/note.txt".to_owned(), "is not a directory"),
            ("~/tools/..".to_owned(), "must not step through `.` or `..`"),
            ("~/./tools".to_owned(), "must not step through `.` or `..`"),
            ("~/tools/../.ssh".to_owned(), "must not step through `.` or `..`"),
            ("~/away".to_owned(), "outside the home"),
        ] {
            let error = support::denied(&sandbox, &support::reading(&[&root])).await;
            assert!(error.contains(why), "`{root}`: {error}");
        }
    });
}

// Nothing of the host bounds what a grant reads of the home but the
// credential stores. A toolchain anywhere beneath the home is granted with
// nothing set. A store is refused as spelled, through a link, from above,
// and on a machine that lacks it.
#[test]
fn credential_roots() {
    support::under_home(async |home| {
        std::fs::create_dir_all(home.join("tools/node")).expect("a toolchain");
        std::fs::create_dir_all(home.join(".config/nvim")).expect("a tool's settings");
        std::fs::create_dir(home.join(".ssh")).expect("a store");
        std::fs::create_dir(home.join("dotfiles")).expect("a tree a store resolves into");
        std::os::unix::fs::symlink(home.join("tools"), home.join("link")).expect("a link in");
        std::os::unix::fs::symlink(home.join(".ssh"), home.join("keys"))
            .expect("a link to a store");
        std::os::unix::fs::symlink(home.join("dotfiles"), home.join(".gnupg"))
            .expect("a store that is a link");
        let sandbox = connect(Options::default()).await;

        let grant = support::reading(&["~/tools", "~/tools/node", "~/link", "~/.config/nvim"]);
        let session = support::granted(&sandbox, None, &grant).await;
        assert_eq!(
            session.read_roots(),
            [home.join("tools"), home.join("tools/node"), home.join(".config/nvim")]
        );

        for root in [
            "~/.ssh",
            "~/.ssh/keys",
            "~/.aws",
            "~/.config/gh",
            "~/.config",
            "~/Library",
            "~/keys",
            "~/dotfiles",
        ] {
            let error = support::denied(&sandbox, &support::reading(&[root])).await;
            assert!(error.contains("a credential store"), "`{root}`: {error}");
        }
    });
}

// A grant's variables reach the process, a leading `~/` spelling the home.
// A name the sandbox sets, one the backend inherits, or no name at all is
// refused as an `inherit` is.
#[test]
fn grant_env() {
    support::under_home(async |home| {
        let sandbox = connect(Options {
            inherit: &["SSL_CERT_FILE"],
            refused: &["CURSOR_API_KEY"],
            ..Options::default()
        })
        .await;

        let grant = Grant {
            env: vec![
                ("RUSTUP_HOME".to_owned(), "~/.rustup".to_owned()),
                ("CARGO_TERM_COLOR".to_owned(), "never".to_owned()),
                ("TILDE".to_owned(), "a~/b".to_owned()),
            ],
            ..Grant::default()
        };
        let seen = environment(&support::granted(&sandbox, None, &grant).await).await;
        let rustup = home.join(".rustup").display().to_string();
        assert_eq!(seen.get("RUSTUP_HOME"), Some(&rustup), "{seen:#?}");
        assert_eq!(seen.get("CARGO_TERM_COLOR").map(String::as_str), Some("never"));
        assert_eq!(seen.get("TILDE").map(String::as_str), Some("a~/b"), "only a leading `~/`");

        for (name, why) in [
            ("HOME", "is set by the sandbox"),
            ("https_proxy", "is set by the sandbox"),
            ("NODE_USE_SYSTEM_CA", "is refused"),
            ("BUN_OPTIONS", "is refused"),
            ("NODE_OPTIONS", "is refused"),
            ("CURSOR_API_KEY", "is refused"),
            ("LD_PRELOAD", "is read by the dynamic loader"),
            ("DYLD_INSERT_LIBRARIES", "is read by the dynamic loader"),
            ("GIT_DIR", "selects a repository"),
            ("SSL_CERT_FILE", "inherited from the host"),
            ("1ST", "not an environment variable name"),
            ("A=B", "not an environment variable name"),
            ("", "not an environment variable name"),
        ] {
            let grant = Grant {
                env: vec![(name.to_owned(), "x".to_owned())],
                ..Grant::default()
            };
            let error = support::denied(&sandbox, &grant).await;
            assert!(error.contains(name) && error.contains(why), "`{name}`: {error}");
        }
    });
}

// Without a home, nothing `~/` spells can be granted or expanded.
#[test]
#[expect(unsafe_code, reason = "the home is unset on the one thread of the test")]
fn missing_home() {
    // SAFETY: nothing else runs yet; the runtime below is built after the write.
    unsafe { env::remove_var("HOME") };
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    runtime.block_on(async {
        let sandbox = connect(Options::default()).await;
        let error = support::denied(&sandbox, &support::reading(&["~/tools"])).await;
        assert!(error.contains("names the home, which the host has none of"), "{error}");

        let grant = Grant {
            env: vec![("RUSTUP_HOME".to_owned(), "~/.rustup".to_owned())],
            ..Grant::default()
        };
        let error = support::denied(&sandbox, &grant).await;
        assert!(error.contains("names the home, which the host has none of"), "{error}");

        // nothing `~/` asked, nothing missed
        let seen = environment(&session(&sandbox, None).await).await;
        assert!(seen.contains_key("PATH"), "{seen:#?}");
    });
}

// Each session binds a proxy of its own, admitting its grant alone.
#[tokio::test]
async fn proxy_per_session() {
    let sandbox = connect(Options::default()).await;
    let (first, _first_child) = proxy_of(&session(&sandbox, None).await).await;
    let (second, _second_child) = proxy_of(&session(&sandbox, None).await).await;
    assert_ne!(first, second);

    let seen = environment(&session(&sandbox, None).await).await;
    let third = seen.get("HTTPS_PROXY").expect("the proxy is in the environment");
    assert_ne!(third, &format!("http://{first}"));
    assert_ne!(third, &format!("http://{second}"));
}

// One session runs any number of commands in turn, every one in the same
// scratch and behind the same proxy; the scratch goes with the last handle.
#[tokio::test]
async fn session_shared() {
    let sandbox = connect(Options::default()).await;
    let session = session(&sandbox, None).await;
    let scratch = session.scratch().to_owned();

    let first = run(&session, "printf '%s' \"$HTTPS_PROXY\" > \"$HOME/proxy\"").await;
    assert!(first.status.success(), "the first command writes under HOME");
    assert!(scratch.join("proxy").is_file(), "the scratch stands between the commands");

    let second = run(&session, "cat \"$HOME/proxy\"; printf ' %s' \"$HTTPS_PROXY\"").await;
    let text = String::from_utf8_lossy(&second.stdout);
    let (written, seen) =
        text.split_once(' ').expect("the first command's proxy, then the second's");
    assert!(written.starts_with("http://"), "the first command saw the proxy: {written}");
    assert_eq!(written, seen, "both commands run behind the one proxy");

    drop(second);
    drop(session);
    assert!(!scratch.exists(), "the scratch goes with the last handle: {}", scratch.display());
}

// --- Seatbelt (macOS) ---

#[cfg(target_os = "macos")]
mod seatbelt {
    use std::path::{Path, PathBuf};
    use std::process::Output;

    use omnia_sandbox::Options;

    use super::support::{self, passed, run, text};

    // A workspace with a source file, a repository, and the engine's store.
    fn tree() -> (tempfile::TempDir, PathBuf) {
        let workspace = tempfile::tempdir().expect("a workspace");
        let tree = workspace.path().join("tree");
        for (path, text) in [
            ("src/main.rs", "fn main() {}\n"),
            (".git/HEAD", "ref: refs/heads/main\n"),
            (".emery/storage/revision.json", "{}\n"),
        ] {
            let path = tree.join(path);
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("the directory");
            std::fs::write(path, text).expect("the file");
        }
        (workspace, tree)
    }

    #[track_caller]
    fn refused(output: &Output, what: &str) {
        assert!(!output.status.success(), "{what} was not refused: {}", text(&output.stdout));
    }

    #[tokio::test]
    async fn outside_write() {
        let (_workspace, tree) = tree();
        let outside = tempfile::tempdir().expect("outside");
        let escape = outside.path().join("escape");
        let sandbox = support::connect(Options::default()).await;

        let inside = run(
            &support::session(&sandbox, Some(&tree)).await,
            "echo made > made.txt && cat made.txt && mkdir -p deep/er && rm -r deep",
        )
        .await;
        passed(&inside, "writing inside the tree");
        assert_eq!(text(&inside.stdout), "made");

        let script = format!("echo x > '{}'", escape.display());
        let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
        refused(&output, "writing outside the tree");
        assert!(!escape.exists(), "the write landed");

        let script = format!("echo x > '{}'", outside.path().join("sibling").display());
        let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
        refused(&output, "creating beside the tree's parent");
    }

    #[tokio::test]
    async fn host_home() {
        let (_workspace, tree) = tree();
        let home = std::env::var("HOME").expect("the host has a home");
        let sandbox = support::connect(Options::default()).await;
        let session = support::session(&sandbox, Some(&tree)).await;
        let scratch = std::fs::canonicalize(session.scratch()).expect("the scratch");

        let script = format!("cd \"$HOME\" && pwd && ls '{home}'");
        let output = run(&session, &script).await;
        refused(&output, "listing the host's home");
        assert_eq!(
            text(&output.stdout),
            scratch.to_string_lossy(),
            "the process's own home is its scratch"
        );
    }

    #[tokio::test]
    async fn engine_dirs() {
        let (_workspace, tree) = tree();
        let sandbox = support::connect(Options::default()).await;

        let output = run(&support::session(&sandbox, Some(&tree)).await, "cat src/main.rs").await;
        passed(&output, "reading a source file");
        assert_eq!(text(&output.stdout), "fn main() {}");

        for attempt in [
            "cat .git/HEAD",
            "ls .git",
            "ls .emery",
            "cat .emery/storage/revision.json",
            "echo x > .emery/storage/revision.json",
            "touch .git/index",
            "mv .git elsewhere",
            "rm -r .emery",
        ] {
            let output = run(&support::session(&sandbox, Some(&tree)).await, attempt).await;
            refused(&output, attempt);
        }
        // `rm -f` swallows the refusal, so what it removed is the check
        let output =
            run(&support::session(&sandbox, Some(&tree)).await, "rm -rf .git .emery").await;
        passed(&output, "rm -rf reports nothing");
        assert_eq!(
            std::fs::read_to_string(tree.join(".git/HEAD")).expect("HEAD stays"),
            "ref: refs/heads/main\n"
        );
        assert_eq!(
            std::fs::read_to_string(tree.join(".emery/storage/revision.json"))
                .expect("the store stays"),
            "{}\n"
        );
    }

    #[tokio::test]
    async fn linked_worktree() {
        let workspace = tempfile::tempdir().expect("a workspace");
        let repository = workspace.path().join("repository");
        let copy = workspace.path().join("copy");
        let git = |args: &[&str], cwd: &Path| {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(cwd)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@t")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@t")
                .output()
                .expect("git runs on the host");
            assert!(output.status.success(), "git {args:?}: {}", text(&output.stderr));
        };
        std::fs::create_dir(&repository).expect("the repository");
        git(&["init", "-q", "-b", "main"], &repository);
        git(&["commit", "-q", "--allow-empty", "-m", "init"], &repository);
        git(&["worktree", "add", "-q", copy.to_str().expect("utf-8")], &repository);
        std::fs::write(copy.join("note.txt"), "a linked copy\n").expect("a file in the copy");

        let sandbox = support::connect(Options::default()).await;
        let output = run(&support::session(&sandbox, Some(&copy)).await, "git --version").await;
        passed(&output, "git itself runs inside");

        let output = run(&support::session(&sandbox, Some(&copy)).await, "git status").await;
        refused(&output, "git status in the linked copy");
        assert!(text(&output.stderr).contains("not a git repository"), "{}", text(&output.stderr));

        let output = run(&support::session(&sandbox, Some(&copy)).await, "cat note.txt").await;
        passed(&output, "the copy's own files");
    }

    // The host process's arguments carry the callback token. Neither `ps` nor
    // the sysctl it reads them through answers for a process outside. `ps` is
    // setuid, so it never runs inside at all.
    #[tokio::test]
    async fn host_process() {
        let (_workspace, tree) = tree();
        support::probe(&tree);
        let sandbox = support::connect(Options::default()).await;
        let host = std::process::id();

        let probe = support::probe_outside(&tree, &["procargs", &host.to_string()]);
        passed(&probe, "the sysctl answers outside the session");

        let script = format!("ps -o command= -p {host}");
        let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
        assert!(
            !output.status.success() || text(&output.stdout).is_empty(),
            "the host process is visible to ps: {}",
            text(&output.stdout)
        );

        let script = format!("./probe procargs {host}");
        let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
        refused(&output, "the host process's arguments");
    }

    #[tokio::test]
    async fn loopback_bind() {
        let (_workspace, tree) = tree();
        support::probe(&tree);
        let sandbox = support::connect(Options::default()).await;
        let output = run(&support::session(&sandbox, Some(&tree)).await, "./probe loopback").await;
        passed(&output, "a server on port 0 over loopback");
        assert!(text(&output.stdout).parse::<u16>().is_ok(), "{}", text(&output.stdout));
    }

    // A root is one session's: the one granted it reads it, one granted
    // nothing does not.
    #[test]
    fn read_root() {
        support::under_home(async |home| {
            let (_workspace, tree) = tree();
            let shared = home.join("shared");
            let private = home.join("private");
            std::fs::create_dir_all(&shared).expect("shared");
            std::fs::create_dir_all(&private).expect("private");
            std::fs::write(shared.join("note.txt"), "shared\n").expect("a shared file");
            std::fs::write(private.join("secret.txt"), "secret\n").expect("a private file");
            let sandbox = support::connect(Options::default()).await;
            let grant = support::reading(&["~/shared"]);

            let script = format!("cat '{}'", shared.join("note.txt").display());
            let output = run(&support::granted(&sandbox, Some(&tree), &grant).await, &script).await;
            passed(&output, "reading a read root");
            assert_eq!(text(&output.stdout), "shared");

            let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
            refused(&output, "reading a root another session was granted");

            let script = format!("echo x > '{}'", shared.join("written").display());
            let output = run(&support::granted(&sandbox, Some(&tree), &grant).await, &script).await;
            refused(&output, "writing a read root");

            let script = format!("cat '{}'", private.join("secret.txt").display());
            let output = run(&support::granted(&sandbox, Some(&tree), &grant).await, &script).await;
            refused(&output, "reading beside a read root");
        });
    }
}

// --- Landlock and seccomp (Linux) ---

#[cfg(target_os = "linux")]
mod landlock {
    use std::path::PathBuf;
    use std::process::Output;

    use omnia_sandbox::Options;

    use super::support::{self, passed, run, text};

    // A workspace holding the probe.
    fn tree() -> (tempfile::TempDir, PathBuf) {
        let workspace = tempfile::tempdir().expect("a workspace");
        let tree = workspace.path().join("tree");
        std::fs::create_dir_all(tree.join("src")).expect("the tree");
        std::fs::write(tree.join("src/main.rs"), "fn main() {}\n").expect("a source file");
        support::probe(&tree);
        (workspace, tree)
    }

    #[track_caller]
    fn refused(output: &Output, what: &str, errno: &str) {
        assert!(!output.status.success(), "{what} was not refused: {}", text(&output.stdout));
        assert!(text(&output.stderr).contains(errno), "{what}: {}", text(&output.stderr));
    }

    // ai-jail's escape matrix, one syscall a row, each answered `EPERM` by
    // the filter where the kernel alone would answer otherwise.
    #[tokio::test]
    async fn syscalls_refused() {
        let (_workspace, tree) = tree();
        let sandbox = support::connect(Options::default()).await;

        let control = support::probe_outside(&tree, &["syscall", "socket", "inet", "dgram"]);
        passed(&control, "a datagram socket opens on the host");
        let control = support::probe_outside(&tree, &["syscall", "ioctl", "tiocsti"]);
        refused(&control, "TIOCSTI on /dev/null outside", "ENOTTY");

        for row in [
            "socket inet dgram",
            "socket inet dgram+nonblock+cloexec",
            "socket inet6 raw",
            "socket packet raw",
            "socket netlink-audit raw",
            "ptrace",
            "io-uring",
            "unshare",
            "mount",
            "bpf",
            "ioctl tiocsti",
            "ioctl tioclinux",
        ] {
            let script = format!("./probe syscall {row}");
            let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
            refused(&output, row, "EPERM");
        }
    }

    #[tokio::test]
    async fn sockets_admitted() {
        let (_workspace, tree) = tree();
        let sandbox = support::connect(Options::default()).await;
        for row in [
            "socket inet stream",
            "socket inet6 stream+nonblock+cloexec",
            "socket netlink-route raw",
        ] {
            let script = format!("./probe syscall {row}");
            let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
            passed(&output, row);
        }
    }

    // glibc issues `clone3` for every thread and falls back to `clone` on
    // `ENOSYS` alone, so the filter's answer is what keeps threads running.
    #[tokio::test]
    async fn clone3_and_threads() {
        let (_workspace, tree) = tree();
        let sandbox = support::connect(Options::default()).await;

        let control = support::probe_outside(&tree, &["syscall", "clone3"]);
        refused(&control, "clone3 with too small a block outside", "EINVAL");

        let output =
            run(&support::session(&sandbox, Some(&tree)).await, "./probe syscall clone3").await;
        refused(&output, "clone3", "ENOSYS");

        let output =
            run(&support::session(&sandbox, Some(&tree)).await, "./probe syscall thread").await;
        passed(&output, "a thread inside");
        assert_eq!(text(&output.stdout), "thread joined with 2");
    }

    #[tokio::test]
    async fn outside_write() {
        let (_workspace, tree) = tree();
        let outside = tempfile::tempdir().expect("outside");
        let escape = outside.path().join("escape");
        let sandbox = support::connect(Options::default()).await;

        let inside = run(
            &support::session(&sandbox, Some(&tree)).await,
            "echo made > made.txt && cat made.txt && mkdir -p deep/er && rm -r deep",
        )
        .await;
        passed(&inside, "writing inside the tree");
        assert_eq!(text(&inside.stdout), "made");

        let script = format!("echo x > '{}'", escape.display());
        let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
        refused(&output, "writing outside the tree", "Permission denied");
        assert!(!escape.exists(), "the write landed");
    }

    #[tokio::test]
    async fn host_home() {
        let (_workspace, tree) = tree();
        let home = std::env::var("HOME").expect("the host has a home");
        let sandbox = support::connect(Options::default()).await;
        let session = support::session(&sandbox, Some(&tree)).await;
        let scratch = session.scratch().to_path_buf();

        let script = format!("cd \"$HOME\" && pwd && ls '{home}'");
        let output = run(&session, &script).await;
        refused(&output, "listing the host's home", "Permission denied");
        assert_eq!(text(&output.stdout), scratch.to_string_lossy(), "the process's own home");
    }

    // A root is one session's: the one granted it reads it, one granted
    // nothing does not.
    #[test]
    fn read_root() {
        support::under_home(async |home| {
            let (_workspace, tree) = tree();
            let shared = home.join("shared");
            std::fs::create_dir_all(&shared).expect("shared");
            std::fs::write(shared.join("note.txt"), "shared\n").expect("a shared file");
            std::fs::write(home.join("secret.txt"), "secret\n").expect("a private file");
            let sandbox = support::connect(Options::default()).await;
            let grant = support::reading(&["~/shared"]);

            let script = format!("cat '{}'", shared.join("note.txt").display());
            let output = run(&support::granted(&sandbox, Some(&tree), &grant).await, &script).await;
            passed(&output, "reading a read root");
            assert_eq!(text(&output.stdout), "shared");

            let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
            refused(&output, "reading a root another session was granted", "Permission denied");

            let script = format!("echo x > '{}'", shared.join("written").display());
            let output = run(&support::granted(&sandbox, Some(&tree), &grant).await, &script).await;
            refused(&output, "writing a read root", "Permission denied");

            let script = format!("cat '{}'", home.join("secret.txt").display());
            let output = run(&support::granted(&sandbox, Some(&tree), &grant).await, &script).await;
            refused(&output, "reading beside a read root", "Permission denied");

            // a rename out of the root is a removal there, which read does not grant
            let script = format!("mv '{}' ./taken.txt", shared.join("note.txt").display());
            let output = run(&support::granted(&sandbox, Some(&tree), &grant).await, &script).await;
            assert!(!output.status.success(), "the file was moved out: {}", text(&output.stdout));
            assert!(shared.join("note.txt").exists(), "the read root kept its file");
        });
    }

    // The host sets itself non-dumpable at connect, so its `/proc/<pid>`
    // is root's from then on, inside the session and out.
    #[tokio::test]
    async fn host_process() {
        let (_workspace, tree) = tree();
        let host = std::process::id().to_string();
        let before = support::probe_outside(&tree, &["environ", &host]);
        passed(&before, "the host's environment reads before connecting");

        let sandbox = support::connect(Options::default()).await;
        let after = support::probe_outside(&tree, &["environ", &host]);
        refused(&after, "the host's environment after connecting", "Permission denied");

        let script = format!("./probe environ {host}");
        let output = run(&support::session(&sandbox, Some(&tree)).await, &script).await;
        refused(&output, "the host's environment inside", "Permission denied");
    }

    #[tokio::test]
    async fn loopback_bind() {
        let (_workspace, tree) = tree();
        let sandbox = support::connect(Options::default()).await;
        let output = run(&support::session(&sandbox, Some(&tree)).await, "./probe loopback").await;
        passed(&output, "a server on port 0 over loopback");
        assert!(text(&output.stdout).parse::<u16>().is_ok(), "{}", text(&output.stdout));
    }
}
