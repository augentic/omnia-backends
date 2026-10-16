//! What the `child`, `boundary`, `valve`, and `toolchain` suites share: a
//! connected [`Airlock`] over `/bin/sh`, a boundary around a lend, and the
//! liveness probes a process-death row ends on.

#![allow(dead_code, reason = "each suite uses its own subset")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use omnia_airlock::{Airlock, Child, Clearance, Compartment, Denied, Options};
use tokio::process::Command;

/// How long a process may take to be gone once its death was set in motion.
pub const GONE: Duration = Duration::from_secs(8);

/// A connected `Airlock` over `options`.
pub async fn connect(options: Options) -> Airlock {
    Airlock::connect(options).await.expect("the options hold")
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

/// A [`Clearance`] reading `roots`, each spelled `~/`-relative.
pub fn reading(roots: &[&str]) -> Clearance {
    Clearance {
        read: roots.iter().map(ToString::to_string).collect(),
        ..Clearance::default()
    }
}

/// The probe binary copied into `tree` as `probe`, where the boundary may
/// run it; the path it has inside.
pub fn probe(tree: &Path) -> PathBuf {
    let probe = tree.join("probe");
    std::fs::copy(env!("CARGO_BIN_EXE_airlock-probe"), &probe).expect("the probe copies");
    probe
}

/// The probe run on the host, outside any boundary, as a row's control.
pub fn probe_outside(tree: &Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_airlock-probe"))
        .args(args)
        .current_dir(tree)
        .output()
        .expect("the probe runs on the host")
}

/// A compartment around `lend` under `airlock`, with an empty clearance.
pub async fn compartment(airlock: &Airlock, lend: Option<&Path>) -> Compartment {
    sealing(airlock, lend, &Clearance::default()).await
}

/// A compartment around `lend` under `airlock`, granted `clearance`.
pub async fn sealing(airlock: &Airlock, lend: Option<&Path>, clearance: &Clearance) -> Compartment {
    airlock.seal(lend, clearance).await.expect("the clearance holds")
}

/// What `airlock` denies `clearance` for, around a private directory: the
/// `Denied` the seal's error carries, since the clearance's fault is typed.
pub async fn denied(airlock: &Airlock, clearance: &Clearance) -> String {
    let error = airlock.seal(None, clearance).await.expect_err("the clearance is denied");
    error
        .downcast_ref::<Denied>()
        .unwrap_or_else(|| panic!("a clearance's fault is typed `Denied`: {error:#}"))
        .to_string()
}

/// `/bin/sh -c <script>` inside `compartment`.
pub fn sh(compartment: &Compartment, script: &str) -> Command {
    let mut command = compartment.command("/bin/sh").expect("`/bin/sh` is a path");
    command.args(["-c", script]);
    command
}

/// `/bin/sh -c <script>` spawned inside `compartment` under supervision.
pub fn spawn_sh(compartment: Compartment, script: &str, ready: Option<&'static str>) -> Child {
    let command = sh(&compartment, script);
    compartment.spawn(command, ready).expect("sh spawns")
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

cfg_select! {
    target_os = "linux" => {
        fn zombie(pid: u32) -> bool {
            std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|stat| proc_state(&stat))
                == Some('Z')
        }

        // The state field of `/proc/<pid>/stat` follows the parenthesised command
        // name, which may itself hold spaces and parentheses: split from the right.
        fn proc_state(stat: &str) -> Option<char> {
            let (_, after_name) = stat.rsplit_once(") ")?;
            after_name.chars().next()
        }
    }
    // Off Linux, what is reparented to pid 1 is reaped as it exits, so there
    // is no zombie to tell apart.
    _ => {
        const fn zombie(_pid: u32) -> bool {
            false
        }
    }
}

/// The pid a script wrote as text.
pub fn pid_of(text: &str) -> u32 {
    text.trim().parse().unwrap_or_else(|_| panic!("a pid, not {text:?}"))
}
