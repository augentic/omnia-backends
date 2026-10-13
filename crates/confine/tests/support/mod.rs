//! What the `child`, `boundary`, `egress`, and `toolchain` suites share: a
//! connected [`Confine`] over `/bin/sh`, a boundary around a lend, and the
//! liveness probes a process-death row ends on.

#![allow(dead_code, reason = "each suite uses its own subset")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use omnia_confine::{Ask, Boundary, Child, Confine, Confinement, Options, Refused};
use tokio::process::Command;

/// How long a process may take to be gone once its death was set in motion.
pub const GONE: Duration = Duration::from_secs(8);

/// A connected `Confine` over `options`.
pub async fn connect(options: Options) -> Confine {
    Confine::connect(options).await.expect("the options hold")
}

/// Runs `test` with the process's `HOME` pointed at a fresh directory, so a
/// row grants `~/` roots without touching the operator's home. The runtime
/// is built after the write, on the test's one thread.
#[expect(unsafe_code, reason = "the home is set on the one thread of the test")]
pub fn under_home(test: impl AsyncFnOnce(&Path)) {
    let home = tempfile::tempdir().expect("a home");
    let path = std::fs::canonicalize(home.path()).expect("the home resolves");
    // SAFETY: nothing else runs yet; the runtime below is built after the write.
    unsafe { std::env::set_var("HOME", &path) };
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    runtime.block_on(test(&path));
}

/// Options bounding an ask's `read` roots to `root` and what is beneath it.
pub fn reading_beneath(root: &Path) -> Options {
    Options {
        read_roots: vec![root.to_path_buf()],
        ..Options::default()
    }
}

/// An `Ask` reading `roots`, each spelled `~/`-relative.
pub fn reading(roots: &[&str]) -> Ask {
    Ask {
        read: roots.iter().map(ToString::to_string).collect(),
        ..Ask::default()
    }
}

/// The probe binary copied into `tree` as `probe`, where the boundary may
/// run it; the path it has inside.
pub fn probe(tree: &Path) -> PathBuf {
    let probe = tree.join("probe");
    std::fs::copy(env!("CARGO_BIN_EXE_confine-probe"), &probe).expect("the probe copies");
    probe
}

/// The probe run on the host, outside any boundary, as a row's control.
pub fn probe_outside(tree: &Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_confine-probe"))
        .args(args)
        .current_dir(tree)
        .output()
        .expect("the probe runs on the host")
}

/// A boundary around `lend` under `confine`, asking nothing.
pub async fn boundary(confine: &Confine, lend: Option<&Path>) -> Boundary {
    asking(confine, lend, &Ask::default()).await
}

/// A boundary around `lend` under `confine`, granted `ask`.
pub async fn asking(confine: &Confine, lend: Option<&Path>, ask: &Ask) -> Boundary {
    let confinement = Confinement::of(lend, confine.options()).await.expect("the lend holds");
    confine.lay(confinement, ask).await.expect("the ask holds")
}

/// What `confine` refuses `ask` for, around a private directory: the
/// `Refused` the lay's error carries, since the ask's fault is typed.
pub async fn refused(confine: &Confine, ask: &Ask) -> String {
    let confinement = Confinement::of(None, confine.options()).await.expect("the lend holds");
    let error = confine.lay(confinement, ask).await.expect_err("the ask is refused");
    error
        .downcast_ref::<Refused>()
        .unwrap_or_else(|| panic!("an ask's fault is typed `Refused`: {error:#}"))
        .to_string()
}

/// `/bin/sh -c <script>` inside `boundary`.
pub fn sh(boundary: &Boundary, script: &str) -> Command {
    let mut command = boundary.command("/bin/sh").expect("`/bin/sh` is a path");
    command.args(["-c", script]);
    command
}

/// `/bin/sh -c <script>` spawned inside `boundary` under supervision.
pub fn spawn_sh(boundary: Boundary, script: &str, ready: Option<&'static str>) -> Child {
    let command = sh(&boundary, script);
    boundary.spawn(command, ready).expect("sh spawns")
}

/// Poll `ready` every 10ms until it holds, or fail after `within`.
pub async fn poll(mut ready: impl FnMut() -> bool, within: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + within;
    while !ready() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait for `pid` to be gone.
pub async fn await_gone(pid: u32) {
    poll(|| !alive(pid), GONE, &format!("pid {pid} gone")).await;
}

/// Whether `pid` is still running: it answers a probe and is not a zombie (a
/// forked child, reparented to a pid 1 that never reaps it).
#[expect(unsafe_code, reason = "a signal-0 probe is the one portable liveness check for a pid")]
pub fn alive(pid: u32) -> bool {
    let Ok(signed) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 probes for existence and delivers nothing.
    if unsafe { libc::kill(signed, 0) } != 0 {
        return false;
    }
    !zombie(pid)
}

#[cfg(target_os = "linux")]
fn zombie(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).ok().and_then(|stat| proc_state(&stat))
        == Some('Z')
}

// Off Linux, what is reparented to pid 1 is reaped as it exits, so there
// is no zombie to tell apart.
#[cfg(not(target_os = "linux"))]
const fn zombie(_pid: u32) -> bool {
    false
}

// The state field of `/proc/<pid>/stat` follows the parenthesised command
// name, which may itself hold spaces and parentheses: split from the right.
#[cfg(target_os = "linux")]
fn proc_state(stat: &str) -> Option<char> {
    let (_, after_name) = stat.rsplit_once(") ")?;
    after_name.chars().next()
}

/// The pid a script wrote as text.
pub fn pid_of(text: &str) -> u32 {
    text.trim().parse().unwrap_or_else(|_| panic!("a pid, not {text:?}"))
}
