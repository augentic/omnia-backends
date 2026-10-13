//! What a process inside the boundary sees — its working directory and its
//! environment — what connecting refuses, and what an ask is held to.

mod support;

use std::collections::BTreeMap;
use std::env;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use omnia_confine::{Ask, Boundary, Confine, Options};
use support::{boundary, connect, sh};
use tracing_subscriber::layer::SubscriberExt as _;

const BOUND: Duration = Duration::from_secs(5);
const PLANTED: &str = "OMNIA_CONFINE_PLANTED";

async fn environment(boundary: Boundary) -> BTreeMap<String, String> {
    let command = sh(&boundary, "env");
    let output = boundary.output(command, BOUND).await.expect("env answers");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

async fn pwd(boundary: Boundary) -> String {
    let command = sh(&boundary, "pwd");
    let output = boundary.output(command, BOUND).await.expect("pwd answers");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

// --- Environment ---

// The variable is planted before any thread of the runtime exists.
#[test]
#[expect(unsafe_code, reason = "the planted variable is set on the one thread of the test")]
fn environment_rebuilt() {
    // SAFETY: nothing else runs yet; the runtime below is built after the write.
    unsafe { env::set_var(PLANTED, "leaked") };
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    runtime.block_on(async {
        let confine = connect(Options::default()).await;
        let boundary = boundary(&confine, None).await;
        let scratch = boundary.scratch().to_string_lossy().into_owned();
        let seen = environment(boundary).await;

        assert!(seen.contains_key("PATH"), "PATH is inherited: {seen:#?}");
        assert_eq!(seen.get("HOME"), Some(&scratch), "HOME is the scratch: {seen:#?}");
        assert_eq!(seen.get("TMPDIR"), Some(&scratch), "TMPDIR is the scratch: {seen:#?}");
        assert!(!seen.contains_key(PLANTED), "the host's variable leaked: {seen:#?}");
        for name in ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"] {
            assert!(!seen.contains_key(name), "{name} reached the process: {seen:#?}");
        }

        let confine = connect(Options {
            passthrough: vec![PLANTED.to_owned()],
            ..Options::default()
        })
        .await;
        let passed = support::boundary(&confine, None).await;
        let seen = environment(passed).await;
        assert_eq!(seen.get(PLANTED).map(String::as_str), Some("leaked"), "{seen:#?}");
    });
}

// --- Working directory ---

#[tokio::test]
async fn cwd_is_the_lend() {
    let lend = tempfile::tempdir().expect("a lend");
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, Some(&lend.path().join("tree"))).await;

    assert!(boundary.lent());
    assert!(!boundary.shell());
    let tree = boundary.tree().to_owned();
    assert_eq!(
        tree,
        std::fs::canonicalize(lend.path().join("tree")).expect("the lend was created"),
        "the lend is created and canonical"
    );
    assert_eq!(boundary.cwd().expect("utf-8"), tree.to_str().expect("utf-8"));
    assert_eq!(pwd(boundary).await, tree.to_string_lossy());
}

#[tokio::test]
async fn cwd_is_private_without_a_lend() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;

    assert!(!boundary.lent());
    assert!(!boundary.shell());
    let tree = boundary.tree().to_owned();
    let scratch = std::fs::canonicalize(boundary.scratch()).expect("the scratch exists");
    assert!(tree.starts_with(&scratch), "{} is under the scratch", tree.display());
    assert_eq!(pwd(boundary).await, tree.to_string_lossy());
}

// --- Shell by location ---

#[tokio::test]
async fn shell_under_a_root() {
    let roots = tempfile::tempdir().expect("roots");
    let a = roots.path().join("a");
    let link = roots.path().join("link");
    std::fs::create_dir(&a).expect("root a");
    std::os::unix::fs::symlink(&a, &link).expect("a symlink to root a");

    let confine = connect(Options {
        shell_roots: vec![a.clone(), Path::new("relative/root").to_path_buf()],
        ..Options::default()
    })
    .await;
    assert!(
        confine.options().shell_roots.iter().all(|root| root.is_absolute()),
        "shell roots are made absolute: {:?}",
        confine.options().shell_roots
    );

    assert!(boundary(&confine, Some(&a.join("x"))).await.shell(), "beneath the root");
    assert!(boundary(&confine, Some(&a)).await.shell(), "at the root");
    assert!(boundary(&confine, Some(&link.join("y"))).await.shell(), "through a symlink");
    assert!(!boundary(&confine, Some(&roots.path().join("b"))).await.shell(), "beside it");
    assert!(!boundary(&confine, None).await.shell(), "private");
}

// --- Programs ---

#[tokio::test]
async fn program_resolved_on_path() {
    let confine = connect(Options::default()).await;
    let boundary = boundary(&confine, None).await;

    let error = boundary.command("omnia-confine-no-such-program").expect_err("not on PATH");
    assert!(error.to_string().contains("is not on PATH"), "{error:#}");

    let mut command = boundary.command("sh").expect("sh is on PATH");
    command.args(["-c", "echo found"]);
    let output = boundary.output(command, BOUND).await.expect("sh answers");
    assert_eq!(output.stdout, b"found\n");
}

// An ask's `PATH` reaches the process, so a toolchain is found inside; the
// program the backend names is resolved against the host's `PATH`, so the
// `sh` planted first on the ask's is never the one spawned.
#[tokio::test]
async fn path_in_the_ask() {
    let lend = tempfile::tempdir().expect("a lend");
    let tree = lend.path().join("tree");
    let bin = tree.join("bin");
    std::fs::create_dir_all(&bin).expect("a bin directory");
    std::fs::write(bin.join("sh"), "#!/bin/sh\necho planted\n").expect("a planted sh");
    std::fs::set_permissions(bin.join("sh"), std::fs::Permissions::from_mode(0o755))
        .expect("the planted sh is executable");
    let path = format!("{}:{}", bin.display(), env::var("PATH").expect("the host has a PATH"));
    let confine = connect(Options::default()).await;
    let ask = Ask {
        env: vec![("PATH".to_owned(), path.clone())],
        ..Ask::default()
    };
    let boundary = support::asking(&confine, Some(&tree), &ask).await;

    let mut command = boundary.command("sh").expect("sh is on the host's PATH");
    command.args(["-c", "echo found && printf '%s' \"$PATH\""]);
    let output = boundary.output(command, BOUND).await.expect("sh answers");
    assert_eq!(String::from_utf8_lossy(&output.stdout), format!("found\n{path}"));
}

// --- Connect ---

#[tokio::test]
async fn passthrough_refused() {
    for (name, why) in [
        ("HOME", "the boundary's to set"),
        ("TMPDIR", "the boundary's to set"),
        ("HTTPS_PROXY", "the boundary's to set"),
        ("no_proxy", "the boundary's to set"),
        ("NODE_USE_SYSTEM_CA", "refused"),
        ("BUN_OPTIONS", "refused"),
        ("LD_PRELOAD", "the dynamic loader's"),
        ("DYLD_INSERT_LIBRARIES", "the dynamic loader's"),
        ("GIT_DIR", "selects a repository"),
        ("GIT_INDEX_FILE", "selects a repository"),
        ("CURSOR_API_KEY", "refused"),
    ] {
        let error = Confine::connect(Options {
            passthrough: vec![name.to_owned()],
            refused: vec!["CURSOR_API_KEY".to_owned()],
            ..Options::default()
        })
        .await
        .err()
        .unwrap_or_else(|| panic!("`{name}` is refused"));
        let text = error.to_string();
        assert!(text.contains(name) && text.contains(why), "{name}: {text}");
    }
}

// --- The ask ---

// Every event's message and fields, flattened to text.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Captured {
    fn on_event(
        &self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut text = format!("{} ", event.metadata().level());
        event.record(&mut Flatten(&mut text));
        self.0.lock().expect("captured lock").push(text);
    }
}

struct Flatten<'a>(&'a mut String);

impl tracing::field::Visit for Flatten<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        let _ = write!(self.0, "{}={value:?} ", field.name());
    }
}

// A root the home lacks grants nothing and ends nothing: a toolchain the
// adapter names and this machine does not have is no reason to refuse the
// turn. The roots that exist are granted beside it, canonical.
#[test]
fn read_root_absent_skipped() {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(captured.clone()))
        .expect("this test owns the process's subscriber");
    support::under_home(async |home| {
        std::fs::create_dir(home.join("present")).expect("a read root");
        std::os::unix::fs::symlink(home.join("present"), home.join("link")).expect("a link");
        let confine = connect(Options::default()).await;

        let boundary = support::asking(
            &confine,
            None,
            &support::reading(&["~/missing", "~/present", "~/link"]),
        )
        .await;
        assert_eq!(boundary.read(), [home.join("present")], "present once, canonical");

        let events = captured.0.lock().expect("captured lock").clone();
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

#[test]
fn read_root_held_to_the_home() {
    support::under_home(async |home| {
        std::fs::create_dir(home.join("tools")).expect("a directory");
        std::fs::write(home.join("note.txt"), "a file\n").expect("a file");
        let outside = tempfile::tempdir().expect("outside the home");
        std::os::unix::fs::symlink(outside.path(), home.join("away")).expect("a link out");
        let confine = connect(Options::default()).await;

        for (root, why) in [
            (home.join("tools").display().to_string(), "spelled `~/<path>`"),
            ("~/".to_owned(), "spelled `~/<path>`"),
            ("~".to_owned(), "spelled `~/<path>`"),
            ("tools".to_owned(), "spelled `~/<path>`"),
            ("~/note.txt".to_owned(), "is not a directory"),
            ("~/tools/..".to_owned(), "outside the home"),
            ("~/away".to_owned(), "outside the home"),
        ] {
            let error = support::refused(&confine, &support::reading(&[&root])).await;
            assert!(error.contains(why), "`{root}`: {error}");
        }
    });
}

// An ask's variables reach the process, a leading `~/` spelling the home;
// a name the boundary holds, one the host passes through, or no name at
// all, is refused as a passthrough is.
#[test]
fn env_set_and_home_expanded() {
    support::under_home(async |home| {
        let confine = connect(Options {
            passthrough: vec!["SSL_CERT_FILE".to_owned()],
            refused: vec!["CURSOR_API_KEY".to_owned()],
            ..Options::default()
        })
        .await;

        let ask = Ask {
            env: vec![
                ("RUSTUP_HOME".to_owned(), "~/.rustup".to_owned()),
                ("CARGO_TERM_COLOR".to_owned(), "never".to_owned()),
                ("TILDE".to_owned(), "a~/b".to_owned()),
            ],
            ..Ask::default()
        };
        let seen = environment(support::asking(&confine, None, &ask).await).await;
        let rustup = home.join(".rustup").display().to_string();
        assert_eq!(seen.get("RUSTUP_HOME"), Some(&rustup), "{seen:#?}");
        assert_eq!(seen.get("CARGO_TERM_COLOR").map(String::as_str), Some("never"));
        assert_eq!(seen.get("TILDE").map(String::as_str), Some("a~/b"), "only a leading `~/`");

        for (name, why) in [
            ("HOME", "the boundary's to set"),
            ("https_proxy", "the boundary's to set"),
            ("NODE_USE_SYSTEM_CA", "refused"),
            ("BUN_OPTIONS", "refused"),
            ("NODE_OPTIONS", "refused"),
            ("CURSOR_API_KEY", "refused"),
            ("LD_PRELOAD", "the dynamic loader's"),
            ("DYLD_INSERT_LIBRARIES", "the dynamic loader's"),
            ("GIT_DIR", "selects a repository"),
            ("SSL_CERT_FILE", "the host's to pass through"),
            ("1ST", "not an environment variable name"),
            ("A=B", "not an environment variable name"),
            ("", "not an environment variable name"),
        ] {
            let ask = Ask {
                env: vec![(name.to_owned(), "x".to_owned())],
                ..Ask::default()
            };
            let error = support::refused(&confine, &ask).await;
            assert!(error.contains(name) && error.contains(why), "`{name}`: {error}");
        }
    });
}

// Without a home, nothing `~/` spells can be granted or expanded.
#[test]
#[expect(unsafe_code, reason = "the home is unset on the one thread of the test")]
fn home_required_for_tilde() {
    // SAFETY: nothing else runs yet; the runtime below is built after the write.
    unsafe { env::remove_var("HOME") };
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    runtime.block_on(async {
        let confine = connect(Options::default()).await;
        let error = support::refused(&confine, &support::reading(&["~/tools"])).await;
        assert!(error.contains("names the home, which the host has none of"), "{error}");

        let ask = Ask {
            env: vec![("RUSTUP_HOME".to_owned(), "~/.rustup".to_owned())],
            ..Ask::default()
        };
        let error = support::refused(&confine, &ask).await;
        assert!(error.contains("names the home, which the host has none of"), "{error}");

        // nothing `~/` asked, nothing missed
        let seen = environment(boundary(&confine, None).await).await;
        assert!(seen.contains_key("PATH"), "{seen:#?}");
    });
}

// Each boundary binds a proxy of its own, admitting its ask alone.
#[tokio::test]
async fn proxy_per_boundary() {
    let confine = connect(Options::default()).await;
    let first = boundary(&confine, None).await;
    let second = boundary(&confine, None).await;
    assert_ne!(first.egress().address(), second.egress().address());

    let proxy = format!("http://{}", first.egress().address());
    let seen = environment(first).await;
    assert_eq!(seen.get("HTTPS_PROXY"), Some(&proxy), "{seen:#?}");
    assert_ne!(seen.get("HTTPS_PROXY"), Some(&format!("http://{}", second.egress().address())));
}

// --- Seatbelt (macOS) ---

#[cfg(target_os = "macos")]
mod seatbelt {
    use std::path::{Path, PathBuf};
    use std::process::Output;

    use omnia_confine::{Boundary, Options};

    use super::{BOUND, support};

    // A lent tree with a source file, a repository, and the engine's store.
    fn tree() -> (tempfile::TempDir, PathBuf) {
        let lend = tempfile::tempdir().expect("a lend");
        let tree = lend.path().join("tree");
        for (path, text) in [
            ("src/main.rs", "fn main() {}\n"),
            (".git/HEAD", "ref: refs/heads/main\n"),
            (".emery/storage/revision.json", "{}\n"),
        ] {
            let path = tree.join(path);
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("the directory");
            std::fs::write(path, text).expect("the file");
        }
        (lend, tree)
    }

    async fn run(boundary: Boundary, script: &str) -> Output {
        let command = support::sh(&boundary, script);
        boundary.output(command, BOUND).await.expect("the shell answers")
    }

    fn text(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).trim().to_owned()
    }

    #[track_caller]
    fn passed(output: &Output, what: &str) {
        assert!(output.status.success(), "{what}: {}", text(&output.stderr));
    }

    #[track_caller]
    fn refused(output: &Output, what: &str) {
        assert!(!output.status.success(), "{what} was not refused: {}", text(&output.stdout));
    }

    #[tokio::test]
    async fn write_outside_the_tree_refused() {
        let (_lend, tree) = tree();
        let outside = tempfile::tempdir().expect("outside");
        let escape = outside.path().join("escape");
        let confine = support::connect(Options::default()).await;

        let inside = run(
            support::boundary(&confine, Some(&tree)).await,
            "echo made > made.txt && cat made.txt && mkdir -p deep/er && rm -r deep",
        )
        .await;
        passed(&inside, "writing inside the tree");
        assert_eq!(text(&inside.stdout), "made");

        let script = format!("echo x > '{}'", escape.display());
        let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
        refused(&output, "writing outside the tree");
        assert!(!escape.exists(), "the write landed");

        let script = format!("echo x > '{}'", outside.path().join("sibling").display());
        let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
        refused(&output, "creating beside the tree's parent");
    }

    #[tokio::test]
    async fn host_home_unreadable() {
        let (_lend, tree) = tree();
        let home = std::env::var("HOME").expect("the host has a home");
        let confine = support::connect(Options::default()).await;
        let boundary = support::boundary(&confine, Some(&tree)).await;
        let scratch = std::fs::canonicalize(boundary.scratch()).expect("the scratch");

        let script = format!("cd \"$HOME\" && pwd && ls '{home}'");
        let output = run(boundary, &script).await;
        refused(&output, "listing the host's home");
        assert_eq!(
            text(&output.stdout),
            scratch.to_string_lossy(),
            "the process's own home is its scratch"
        );
    }

    #[tokio::test]
    async fn engine_directories_closed() {
        let (_lend, tree) = tree();
        let confine = support::connect(Options::default()).await;

        let output = run(support::boundary(&confine, Some(&tree)).await, "cat src/main.rs").await;
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
            let output = run(support::boundary(&confine, Some(&tree)).await, attempt).await;
            refused(&output, attempt);
        }
        // `rm -f` swallows the refusal; what it removed is the check
        let output =
            run(support::boundary(&confine, Some(&tree)).await, "rm -rf .git .emery").await;
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
    async fn linked_copy_finds_no_repository() {
        let lend = tempfile::tempdir().expect("a lend");
        let repository = lend.path().join("repository");
        let copy = lend.path().join("copy");
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

        let confine = support::connect(Options::default()).await;
        let output = run(support::boundary(&confine, Some(&copy)).await, "git --version").await;
        passed(&output, "git itself runs inside");

        let output = run(support::boundary(&confine, Some(&copy)).await, "git status").await;
        refused(&output, "git status in the linked copy");
        assert!(text(&output.stderr).contains("not a git repository"), "{}", text(&output.stderr));

        let output = run(support::boundary(&confine, Some(&copy)).await, "cat note.txt").await;
        passed(&output, "the copy's own files");
    }

    // The host process's arguments carry the callback token: neither `ps`,
    // which is setuid and so never runs inside, nor the sysctl it reads them
    // through answers for a process outside.
    #[tokio::test]
    async fn outside_process_invisible() {
        let (_lend, tree) = tree();
        support::probe(&tree);
        let confine = support::connect(Options::default()).await;
        let host = std::process::id();

        let probe = support::probe_outside(&tree, &["procargs", &host.to_string()]);
        passed(&probe, "the sysctl answers outside the boundary");

        let script = format!("ps -o command= -p {host}");
        let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
        assert!(
            !output.status.success() || text(&output.stdout).is_empty(),
            "the host process is visible to ps: {}",
            text(&output.stdout)
        );

        let script = format!("./probe procargs {host}");
        let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
        refused(&output, "the host process's arguments");
    }

    #[tokio::test]
    async fn loopback_server_answers() {
        let (_lend, tree) = tree();
        support::probe(&tree);
        let confine = support::connect(Options::default()).await;
        let output = run(support::boundary(&confine, Some(&tree)).await, "./probe loopback").await;
        passed(&output, "a server on port 0 over loopback");
        assert!(text(&output.stdout).parse::<u16>().is_ok(), "{}", text(&output.stdout));
    }

    // A root is one boundary's: the one asking reads it, one asking
    // nothing does not.
    #[test]
    fn read_root_readable() {
        support::under_home(async |home| {
            let (_lend, tree) = tree();
            let shared = home.join("shared");
            let private = home.join("private");
            std::fs::create_dir_all(&shared).expect("shared");
            std::fs::create_dir_all(&private).expect("private");
            std::fs::write(shared.join("note.txt"), "shared\n").expect("a shared file");
            std::fs::write(private.join("secret.txt"), "secret\n").expect("a private file");
            let confine = support::connect(Options::default()).await;
            let ask = support::reading(&["~/shared"]);

            let script = format!("cat '{}'", shared.join("note.txt").display());
            let output = run(support::asking(&confine, Some(&tree), &ask).await, &script).await;
            passed(&output, "reading a read root");
            assert_eq!(text(&output.stdout), "shared");

            let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
            refused(&output, "reading a root another boundary asked");

            let script = format!("echo x > '{}'", shared.join("written").display());
            let output = run(support::asking(&confine, Some(&tree), &ask).await, &script).await;
            refused(&output, "writing a read root");

            let script = format!("cat '{}'", private.join("secret.txt").display());
            let output = run(support::asking(&confine, Some(&tree), &ask).await, &script).await;
            refused(&output, "reading beside a read root");
        });
    }
}

// --- Landlock and seccomp (Linux) ---

#[cfg(target_os = "linux")]
mod landlock {
    use std::path::PathBuf;
    use std::process::Output;

    use omnia_confine::{Boundary, Options};

    use super::{BOUND, support};

    // A lent tree holding the probe.
    fn tree() -> (tempfile::TempDir, PathBuf) {
        let lend = tempfile::tempdir().expect("a lend");
        let tree = lend.path().join("tree");
        std::fs::create_dir_all(tree.join("src")).expect("the tree");
        std::fs::write(tree.join("src/main.rs"), "fn main() {}\n").expect("a source file");
        support::probe(&tree);
        (lend, tree)
    }

    async fn run(boundary: Boundary, script: &str) -> Output {
        let command = support::sh(&boundary, script);
        boundary.output(command, BOUND).await.expect("the shell answers")
    }

    fn text(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).trim().to_owned()
    }

    #[track_caller]
    fn passed(output: &Output, what: &str) {
        assert!(output.status.success(), "{what}: {}", text(&output.stderr));
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
        let (_lend, tree) = tree();
        let confine = support::connect(Options::default()).await;

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
            let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
            refused(&output, row, "EPERM");
        }
    }

    #[tokio::test]
    async fn sockets_admitted() {
        let (_lend, tree) = tree();
        let confine = support::connect(Options::default()).await;
        for row in [
            "socket inet stream",
            "socket inet6 stream+nonblock+cloexec",
            "socket netlink-route raw",
        ] {
            let script = format!("./probe syscall {row}");
            let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
            passed(&output, row);
        }
    }

    // glibc issues `clone3` for every thread and falls back to `clone` on
    // `ENOSYS` alone, so the filter's answer is what keeps threads running.
    #[tokio::test]
    async fn clone3_unsupported_and_threads_run() {
        let (_lend, tree) = tree();
        let confine = support::connect(Options::default()).await;

        let control = support::probe_outside(&tree, &["syscall", "clone3"]);
        refused(&control, "clone3 with too small a block outside", "EINVAL");

        let output =
            run(support::boundary(&confine, Some(&tree)).await, "./probe syscall clone3").await;
        refused(&output, "clone3", "ENOSYS");

        let output =
            run(support::boundary(&confine, Some(&tree)).await, "./probe syscall thread").await;
        passed(&output, "a thread inside");
        assert_eq!(text(&output.stdout), "thread joined with 2");
    }

    #[tokio::test]
    async fn write_outside_the_tree_refused() {
        let (_lend, tree) = tree();
        let outside = tempfile::tempdir().expect("outside");
        let escape = outside.path().join("escape");
        let confine = support::connect(Options::default()).await;

        let inside = run(
            support::boundary(&confine, Some(&tree)).await,
            "echo made > made.txt && cat made.txt && mkdir -p deep/er && rm -r deep",
        )
        .await;
        passed(&inside, "writing inside the tree");
        assert_eq!(text(&inside.stdout), "made");

        let script = format!("echo x > '{}'", escape.display());
        let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
        refused(&output, "writing outside the tree", "Permission denied");
        assert!(!escape.exists(), "the write landed");
    }

    #[tokio::test]
    async fn host_home_unreadable() {
        let (_lend, tree) = tree();
        let home = std::env::var("HOME").expect("the host has a home");
        let confine = support::connect(Options::default()).await;
        let boundary = support::boundary(&confine, Some(&tree)).await;
        let scratch = boundary.scratch().to_path_buf();

        let script = format!("cd \"$HOME\" && pwd && ls '{home}'");
        let output = run(boundary, &script).await;
        refused(&output, "listing the host's home", "Permission denied");
        assert_eq!(text(&output.stdout), scratch.to_string_lossy(), "the process's own home");
    }

    // A root is one boundary's: the one asking reads it, one asking
    // nothing does not.
    #[test]
    fn read_root_readable_and_kept() {
        support::under_home(async |home| {
            let (_lend, tree) = tree();
            let shared = home.join("shared");
            std::fs::create_dir_all(&shared).expect("shared");
            std::fs::write(shared.join("note.txt"), "shared\n").expect("a shared file");
            std::fs::write(home.join("secret.txt"), "secret\n").expect("a private file");
            let confine = support::connect(Options::default()).await;
            let ask = support::reading(&["~/shared"]);

            let script = format!("cat '{}'", shared.join("note.txt").display());
            let output = run(support::asking(&confine, Some(&tree), &ask).await, &script).await;
            passed(&output, "reading a read root");
            assert_eq!(text(&output.stdout), "shared");

            let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
            refused(&output, "reading a root another boundary asked", "Permission denied");

            let script = format!("echo x > '{}'", shared.join("written").display());
            let output = run(support::asking(&confine, Some(&tree), &ask).await, &script).await;
            refused(&output, "writing a read root", "Permission denied");

            let script = format!("cat '{}'", home.join("secret.txt").display());
            let output = run(support::asking(&confine, Some(&tree), &ask).await, &script).await;
            refused(&output, "reading beside a read root", "Permission denied");

            // a rename out of the root is a removal there, which read does not grant
            let script = format!("mv '{}' ./taken.txt", shared.join("note.txt").display());
            let output = run(support::asking(&confine, Some(&tree), &ask).await, &script).await;
            assert!(!output.status.success(), "the file was moved out: {}", text(&output.stdout));
            assert!(shared.join("note.txt").exists(), "the read root kept its file");
        });
    }

    // The host sets itself non-dumpable at connect, so its `/proc/<pid>`
    // is root's from then on, inside the boundary and out.
    #[tokio::test]
    async fn host_process_closed() {
        let (_lend, tree) = tree();
        let host = std::process::id().to_string();
        let before = support::probe_outside(&tree, &["environ", &host]);
        passed(&before, "the host's environment reads before connecting");

        let confine = support::connect(Options::default()).await;
        let after = support::probe_outside(&tree, &["environ", &host]);
        refused(&after, "the host's environment after connecting", "Permission denied");

        let script = format!("./probe environ {host}");
        let output = run(support::boundary(&confine, Some(&tree)).await, &script).await;
        refused(&output, "the host's environment inside", "Permission denied");
    }

    #[tokio::test]
    async fn loopback_server_answers() {
        let (_lend, tree) = tree();
        let confine = support::connect(Options::default()).await;
        let output = run(support::boundary(&confine, Some(&tree)).await, "./probe loopback").await;
        passed(&output, "a server on port 0 over loopback");
        assert!(text(&output.stdout).parse::<u16>().is_ok(), "{}", text(&output.stdout));
    }
}
