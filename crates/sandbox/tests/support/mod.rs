//! What the `child`, `session`, `proxy`, and `toolchain` suites share:
//!
//! - a connected [`Sandbox`] over `/bin/sh`
//! - a session around a workspace, and a script run inside it
//! - the egress proxy a session's process is pointed at
//! - the liveness probes a process-death row ends on
//! - the log events a row reads back

#![allow(dead_code, reason = "each suite uses its own subset")]

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use omnia_sandbox::{Child, Command, Denied, Grant, Options, Sandbox, Session};

/// How long a process may take to be gone once its death was set in motion.
pub const GONE: Duration = Duration::from_secs(8);

/// How long a shell script inside a session may take to answer.
pub const BOUND: Duration = Duration::from_secs(5);

/// A connected `Sandbox` over `options`.
pub async fn connect(options: Options) -> Sandbox {
    Sandbox::connect(options).await.expect("the options hold")
}

/// Runs `test` with the process's `HOME` pointed at a fresh directory.
///
/// A row grants `~/` roots without touching the operator's home. The runtime
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

/// A [`Grant`] reading `roots`, each spelled `~/`-relative.
pub fn reading(roots: &[&str]) -> Grant {
    Grant {
        read: roots.iter().map(ToString::to_string).collect(),
        ..Grant::default()
    }
}

/// The probe binary's path inside `workspace`, copied there as `probe` for the session to run.
pub fn probe(workspace: &Path) -> PathBuf {
    let probe = workspace.join("probe");
    std::fs::copy(env!("CARGO_BIN_EXE_sandbox-probe"), &probe).expect("the probe copies");
    probe
}

/// The probe run on the host, outside any session, as a row's control.
pub fn probe_outside(workspace: &Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_sandbox-probe"))
        .args(args)
        .current_dir(workspace)
        .output()
        .expect("the probe runs on the host")
}

/// A session around `workspace` under `sandbox`, with an empty grant.
pub async fn session(sandbox: &Sandbox, workspace: Option<&Path>) -> Session {
    granted(sandbox, workspace, &Grant::default()).await
}

/// A session around `workspace` under `sandbox`, with `grant`.
pub async fn granted(sandbox: &Sandbox, workspace: Option<&Path>, grant: &Grant) -> Session {
    sandbox.prepare(workspace, grant).await.expect("the grant holds")
}

/// The `Denied` reason `sandbox` refuses `grant` with, around a private directory.
pub async fn denied(sandbox: &Sandbox, grant: &Grant) -> String {
    let error = sandbox.prepare(None, grant).await.expect_err("the grant is denied");
    error
        .downcast_ref::<Denied>()
        .unwrap_or_else(|| panic!("a grant's fault is typed `Denied`: {error:#}"))
        .to_string()
}

/// `/bin/sh -c <script>` inside `session`.
pub fn sh(session: Session, script: &str) -> Command {
    let mut command = session.command("/bin/sh").expect("`/bin/sh` is a path");
    command.args(["-c", script]);
    command
}

/// `/bin/sh -c <script>` spawned inside `session` under supervision.
pub fn spawn_sh(session: Session, script: &str, ready: Option<&'static str>) -> Child {
    let mut command = sh(session, script);
    if let Some(prefix) = ready {
        command.ready_prefix(prefix);
    }
    command.spawn().expect("sh spawns")
}

/// `/bin/sh -c <script>` run once inside `session`, with what it wrote.
pub async fn run(session: Session, script: &str) -> Output {
    sh(session, script).output(BOUND).await.expect("the shell answers")
}

/// The egress proxy `session`'s process is pointed at, read from inside,
/// with the process that keeps it serving.
pub async fn proxy_of(session: Session) -> (SocketAddr, Child) {
    let script = "echo \"ready $HTTPS_PROXY\" >&2; exec sleep 600";
    let mut child = spawn_sh(session, script, Some("ready "));
    let url = child.ready().await.expect("the proxy is in the environment");
    let address = url
        .strip_prefix("http://")
        .and_then(|address| address.parse().ok())
        .unwrap_or_else(|| panic!("a proxy URL, not {url:?}"));
    (address, child)
}

/// What a process wrote, trimmed.
pub fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}

/// Asserts `output` succeeded, naming `what` and its stderr otherwise.
#[track_caller]
pub fn passed(output: &Output, what: &str) {
    assert!(output.status.success(), "{what}: {}", text(&output.stderr));
}

/// Every log event as text: its level, then its message and fields.
#[derive(Clone, Default)]
pub struct Captured(Arc<Mutex<Vec<String>>>);

impl Captured {
    /// The events recorded so far.
    pub fn events(&self) -> Vec<String> {
        self.0.lock().expect("captured lock").clone()
    }
}

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
        let _ = write!(self.0, "{}={value:?} ", field.name());
    }
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

/// Whether `pid` is still running: it answers a probe and is not a zombie.
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
        // A forked child reparented to a pid 1 that never reaps it stays a zombie.
        fn zombie(pid: u32) -> bool {
            std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|stat| proc_state(&stat))
                == Some('Z')
        }

        // The command name before the state field may hold spaces and
        // parentheses, so the split is from the right.
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
