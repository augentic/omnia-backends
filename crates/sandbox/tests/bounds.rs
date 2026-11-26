//! What a process `Sandbox::command` holds to its bounds reaches: its roots
//! as their descriptors opened them, nothing beside them, and no network.

mod support;

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::os::fd::AsFd as _;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::Output;

use omnia_sandbox::{Bounds, Options, Sandbox};
use support::{BOUND, passed, text};

// A read root holding a note, a write root holding a repository's `.git` and
// the engine's store, and a directory beside both holding a secret.
struct Tree {
    _dir: tempfile::TempDir,
    read: PathBuf,
    write: PathBuf,
    outside: PathBuf,
}

impl Tree {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("a tree");
        let root = fs::canonicalize(dir.path()).expect("the tree resolves");
        let (read, write, outside) = (root.join("read"), root.join("write"), root.join("outside"));
        for (path, text) in [
            (read.join("note.txt"), "note\n"),
            (write.join(".git/HEAD"), "ref: refs/heads/main\n"),
            (write.join(".emery/revision.json"), "{}\n"),
            (outside.join("secret.txt"), "secret\n"),
        ] {
            fs::create_dir_all(path.parent().expect("a parent")).expect("the directory");
            fs::write(path, text).expect("the file");
        }
        Self {
            _dir: dir,
            read,
            write,
            outside,
        }
    }

    // `/bin/sh -c <script>` working in the write root and reading the read
    // root, with what it wrote.
    async fn run(&self, sandbox: &Sandbox, script: &str) -> Output {
        let (read, write) = (open(&self.read), open(&self.write));
        run(sandbox, &write, &[&read], &[&write], script).await
    }
}

fn open(path: &Path) -> File {
    File::open(path).expect("the root opens")
}

// `/bin/sh -c <script>` in `cwd`, held to `read` and `write`, with what it
// wrote. Its environment is `PATH` alone, since the bounds set none.
async fn run(
    sandbox: &Sandbox, cwd: &File, read: &[&File], write: &[&File], script: &str,
) -> Output {
    let read: Vec<_> = read.iter().map(|file| file.as_fd()).collect();
    let write: Vec<_> = write.iter().map(|file| file.as_fd()).collect();
    let bounds = Bounds {
        cwd: cwd.as_fd(),
        read: &read,
        write: &write,
    };
    let mut command = sandbox.command("/bin/sh", &bounds).expect("the bounds hold");
    command.args(["-c", script]).env("PATH", "/usr/bin:/bin");
    tokio::time::timeout(BOUND, command.output())
        .await
        .expect("the shell answers in time")
        .expect("the shell runs")
}

#[track_caller]
fn refused(output: &Output, what: &str) {
    assert!(!output.status.success(), "{what} was not refused: {}", text(&output.stdout));
}

#[tokio::test]
async fn read_root() {
    let tree = Tree::new();
    let sandbox = support::connect(Options::default()).await;

    let script = format!("cat '{}'", tree.read.join("note.txt").display());
    let output = tree.run(&sandbox, &script).await;
    passed(&output, "reading a read root");
    assert_eq!(text(&output.stdout), "note");

    let written = tree.read.join("written");
    let output = tree.run(&sandbox, &format!("echo x > '{}'", written.display())).await;
    refused(&output, "writing a read root");
    assert!(!written.exists(), "the write landed");
}

// Nothing beneath a write root is closed, a repository's own directories
// included.
#[tokio::test]
async fn write_root() {
    let tree = Tree::new();
    let sandbox = support::connect(Options::default()).await;

    let script = "echo made > made.txt && cat .git/HEAD && echo x > .emery/revision.json \
                  && mkdir -p .git/refs/heads && rm -r .git/refs";
    let output = tree.run(&sandbox, script).await;
    passed(&output, "writing a write root");
    assert_eq!(text(&output.stdout), "ref: refs/heads/main");
    let made = fs::read_to_string(tree.write.join("made.txt")).expect("the write landed");
    assert_eq!(made, "made\n");
    let store = fs::read_to_string(tree.write.join(".emery/revision.json")).expect("the store");
    assert_eq!(store, "x\n");
}

#[tokio::test]
async fn outside() {
    let tree = Tree::new();
    let sandbox = support::connect(Options::default()).await;

    let escape = tree.outside.join("escape");
    for script in [
        format!("cat '{}'", tree.outside.join("secret.txt").display()),
        format!("ls '{}'", tree.outside.display()),
        format!("echo x > '{}'", escape.display()),
    ] {
        let output = tree.run(&sandbox, &script).await;
        refused(&output, &script);
        assert!(text(&output.stdout).is_empty(), "{script} read {}", text(&output.stdout));
    }
    assert!(!escape.exists(), "the write landed");
}

// A link a process finds inside a root, or lays there, leads nowhere outside
// it.
#[tokio::test]
async fn link_out() {
    let tree = Tree::new();
    symlink(&tree.outside, tree.write.join("out")).expect("the link");
    let sandbox = support::connect(Options::default()).await;

    for script in
        ["cat out/secret.txt", "echo x > out/escape", "ln -s .. up && cat up/outside/secret.txt"]
    {
        let output = tree.run(&sandbox, script).await;
        refused(&output, script);
        assert!(text(&output.stdout).is_empty(), "{script} read {}", text(&output.stdout));
    }
    assert!(!tree.outside.join("escape").exists(), "the write landed");
}

// No server on loopback, and no connect to a unix socket the host listens on
// beneath the process's own write root.
#[tokio::test]
async fn no_socket() {
    let tree = Tree::new();
    support::probe(&tree.write);
    let socket = tree.write.join("s.sock");
    let _listener = UnixListener::bind(&socket).expect("a unix listener");
    let shown = socket.display().to_string();
    let control = support::probe_outside(&tree.write, &["unix", &shown]);
    passed(&control, "a unix connect on the host");
    let sandbox = support::connect(Options::default()).await;

    // each refusal is the probe's own, so the attempt was made
    let rows = [
        (String::from("./probe loopback"), "loopback: bind"),
        (format!("./probe unix '{shown}'"), "unix: connect"),
    ];
    #[cfg(target_os = "linux")]
    let rows = rows.into_iter().chain(
        ["inet stream", "inet6 stream", "netlink-route raw"]
            .map(|socket| (format!("./probe syscall socket {socket}"), "EPERM")),
    );
    for (script, said) in rows {
        let output = tree.run(&sandbox, &script).await;
        refused(&output, &script);
        assert!(text(&output.stderr).contains(said), "{script}: {}", text(&output.stderr));
    }
}

// What the process forks is held as the process is.
#[tokio::test]
async fn child_held() {
    let tree = Tree::new();
    let sandbox = support::connect(Options::default()).await;

    let secret = tree.outside.join("secret.txt");
    let escape = tree.outside.join("escape");
    for script in [
        format!("/bin/sh -c \"cat '{}'\"", secret.display()),
        format!("(echo x > '{}') & wait $!", escape.display()),
    ] {
        let output = tree.run(&sandbox, &script).await;
        refused(&output, &script);
        assert!(text(&output.stdout).is_empty(), "{script} read {}", text(&output.stdout));
    }
    assert!(!escape.exists(), "the write landed");
}

// A root is what its descriptor opened: renamed after it was opened, it is
// still the process's, and a directory laid at its old path is not.
#[tokio::test]
async fn renamed_root() {
    let tree = Tree::new();
    let write = open(&tree.write);
    let moved = tree.write.with_file_name("moved");
    fs::rename(&tree.write, &moved).expect("the root moves");
    fs::create_dir(&tree.write).expect("a directory at the old path");
    let sandbox = support::connect(Options::default()).await;

    let output = run(&sandbox, &write, &[], &[&write], "echo x > bound").await;
    passed(&output, "writing the root where it moved");
    assert!(moved.join("bound").exists(), "the process works in the root it was given");

    let planted = tree.write.join("planted");
    let script = format!("echo x > '{}'", planted.display());
    let output = run(&sandbox, &write, &[], &[&write], &script).await;
    refused(&output, "writing the directory at the root's old path");
    assert!(!planted.exists(), "the write landed");
}

// The process inherits nothing of the host's environment: what it holds is
// what its caller set and what the shell sets itself.
#[tokio::test]
async fn empty_environment() {
    let tree = Tree::new();
    let sandbox = support::connect(Options::default()).await;

    let output = tree.run(&sandbox, "env").await;
    passed(&output, "listing the environment");
    let names: BTreeSet<_> = text(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.to_owned()))
        .collect();
    let own = ["PATH", "PWD", "OLDPWD", "SHLVL", "_"];
    let inherited: Vec<_> = names.iter().filter(|name| !own.contains(&name.as_str())).collect();
    assert!(inherited.is_empty(), "inherited from the host: {inherited:?}");
}
