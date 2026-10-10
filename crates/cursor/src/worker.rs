//! Spawn and supervise one `cursor-sdk-bridge` process — a worker — with
//! `sdk.v1` bound over it.
//!
//! The worker leads a process group of its own: a kill reaches the agent
//! processes it forks, and whatever a worker left in its group when it
//! exited is swept as the exit is seen, so nothing of a slot's process
//! outlives it.

mod discovery;

use std::collections::VecDeque;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use discovery::Discovery;
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop, ProcessGroup};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{ChildStderr, ChildStdout, Command};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tracing::{Instrument as _, Span};

use crate::endpoint::Registration;
use crate::git::Pins;
use crate::protocol::Rpc;
use crate::{Failure, elapsed_ms, lock};

const READY_WAIT: Duration = Duration::from_secs(30);
const CONNECT_WAIT: Duration = Duration::from_secs(10);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(1);
const SHUTDOWN_WAIT: Duration = Duration::from_secs(5);
const EXIT_GRACE: Duration = Duration::from_millis(250);
const EXIT_WAIT: Duration = EXIT_GRACE.saturating_mul(2);
const BIN: &str = "cursor-sdk-bridge";

// A spawned `cursor-sdk-bridge` process this client watches, with `sdk.v1`
// bound on it. Dropping it asks the worker to go.
#[derive(Debug)]
pub struct Worker {
    watched: Watched,
    rpc: Rpc,
}

impl Worker {
    // The whole of a worker's environment, taken from the host's: the host
    // process holds every co-located backend's credentials, and a shell
    // command the agent runs prints what the worker has into a tool result
    // the guest reads. The key rides on every RPC instead; git identity is
    // withheld so the agent never points at the host repository.
    const ENV: &[&str] = &[
        // a process, and the programs a shell command finds
        "PATH",
        "HOME",
        "TMPDIR",
        "SHELL",
        "USER",
        "LOGNAME",
        "TERM",
        "LANG",
        "TZ",
        // the bridge's own knobs
        "CURSOR_BACKEND_URL",
        "CURSOR_WEBSITE_URL",
        "CURSOR_DATA_DIR",
        "CURSOR_RIPGREP_PATH",
        "CURSOR_TREE_SITTER_VENDOR_DIR",
        "CURSOR_SDK_BRIDGE_LOG",
        "CURSOR_SDK_BRIDGE_DEBUG_STARTUP",
        // the network it reaches Cursor over
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "no_proxy",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "NODE_EXTRA_CA_CERTS",
        "NODE_USE_SYSTEM_CA",
    ];
    // The entries of `ENV` whose value is a URL, which may carry credentials
    // the bridge needs and a shell command would print.
    const PROXIES: &[&str] = &["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"];

    // What a worker, and any git asked how a worker's git would answer, is
    // spawned with: the allowlisted host variables, then the git pins. The
    // bridge runs git in a lent tree outside the shell's sandbox, and hands
    // its whole environment to each shell command, so both read the pins.
    pub fn environment(pins: &Pins) -> impl Iterator<Item = (OsString, OsString)> {
        std::env::vars_os().filter(|(name, _)| Self::passes(name)).chain(pins.variables())
    }

    // Refuse an environment whose proxy URLs carry credentials: the bridge
    // hands its whole environment to each shell command, so where lends get
    // the shell, the proxy is unauthenticated or there is none.
    pub fn check_proxies(vars: impl IntoIterator<Item = (String, String)>) -> Result<()> {
        for (name, value) in vars {
            if Self::PROXIES.contains(&name.as_str()) && has_userinfo(&value) {
                bail!(
                    "{name} carries credentials, which a shell command would print into a tool \
                     result; with shell_roots set, use a proxy without them"
                );
            }
        }
        Ok(())
    }

    // The handshake is left to `Spawned::handshake`, so a pool lease can
    // occupy the slot first.
    pub fn spawn(callback: &Registration, pins: &Pins) -> Result<Spawned> {
        let state_root = tempfile::Builder::new()
            .prefix("omnia-cursor-")
            .tempdir()
            .context("creating state root")?;

        let mut command = Command::new(Self::locate()?);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear()
            .envs(Self::environment(pins))
            .env("CURSOR_SDK_CLIENT_LANGUAGE", "rust")
            .arg("--state-root")
            .arg(state_root.path())
            .args(["--tool-callback-url", callback.url()])
            .args(["--tool-callback-auth-token", callback.token()]);

        // lead a process group, so a kill reaches the agents the worker forks
        let mut command = CommandWrap::from(command);
        command.wrap(KillOnDrop);
        command.wrap(ProcessGroup::leader());

        let child = command.spawn().context("issue spawning `cursor-sdk-bridge`")?;
        Supervisor::spawn(child, state_root)
    }

    // HACK: resolved here rather than left to `exec`, since std spawns a bare
    // program name through `fork` once the child's environment carries its
    // own PATH, and a path keeps the far cheaper `posix_spawn`.
    fn locate() -> Result<PathBuf> {
        let path = std::env::var_os("PATH").unwrap_or_default();
        std::env::split_paths(&path)
            .map(|dir| dir.join(BIN))
            .find(|bin| bin.is_file())
            .ok_or_else(|| anyhow!("`{BIN}` is not on PATH"))
    }

    // Named in `ENV`, or a locale setting.
    fn passes(name: &OsStr) -> bool {
        name.to_str().is_some_and(|name| Self::ENV.contains(&name) || name.starts_with("LC_"))
    }

    pub const fn rpc(&self) -> &Rpc {
        &self.rpc
    }

    pub fn live_rpc(&self) -> Option<&Rpc> {
        self.watched.is_running().then_some(&self.rpc)
    }

    pub async fn fail_on_exit<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        self.watched.fail_on_exit(future).await
    }

    // Hand the client to the supervisor to ask over. A `Watched` that drops
    // without one is killed outright.
    fn ask(&self) {
        let _ = self.watched.shutdown.send(Some(self.rpc.clone()));
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.ask();
    }
}

// waits on the process, tears it down on request, and publishes its exit
struct Supervisor {
    child: Box<dyn ChildWrapper>,
    state_root: TempDir,
    stderr: JoinHandle<()>,
    state: Arc<State>,
    stop: watch::Receiver<Option<Rpc>>,
    exit: watch::Sender<Option<Exit>>,
}

impl Supervisor {
    // The tasks run under the caller's span, so what the process says and
    // how it exits are reported under the completion that spawned it.
    fn spawn(mut child: Box<dyn ChildWrapper>, state_root: TempDir) -> Result<Spawned> {
        let started_at = Instant::now();

        let pid = child.id().context("no pid for spawned worker")?;
        let (Some(stdout), Some(stderr)) = (child.stdout().take(), child.stderr().take()) else {
            bail!("no piped stdout and stderr for spawned worker");
        };

        let state = Arc::new(State {
            pid,
            started_at,
            tail: Tail::default(),
        });

        drain_stdout(stdout);
        let (discovery, stderr) = read_stderr(stderr, Arc::clone(&state));

        let (shutdown, stop) = watch::channel(None);
        let (exit_tx, exit) = watch::channel(None);
        tokio::spawn(
            Self {
                child,
                state_root,
                stderr,
                state: Arc::clone(&state),
                stop,
                exit: exit_tx,
            }
            .run()
            .instrument(Span::current()),
        );

        Ok(Spawned {
            watched: Watched {
                state,
                exit,
                shutdown,
            },
            discovery,
        })
    }

    async fn run(mut self) {
        let self_exited = tokio::select! {
            // an exit in the same tick as a close is still an exit
            biased;
            _ = self.child.wait() => true,
            _ = self.stop.changed() => {
                // a `Worker` hands over a client to ask; a `Spawned` has none and is killed
                let rpc = self.stop.borrow().clone();
                if let Some(rpc) = rpc {
                    self.ask(&rpc).await;
                }
                false
            }
        };

        // the one kill of the group: nothing of the slot outlives it
        let _ = self.child.start_kill();
        let status = self.child.wait().await.ok();
        let uptime_ms = elapsed_ms(self.state.started_at);

        // a forked child may still hold the stderr pipe; do not wait on it
        if timeout(EXIT_GRACE, &mut self.stderr).await.is_err() {
            self.stderr.abort();
        }

        let exit = Exit {
            status,
            pid: self.state.pid,
        };

        // an exit nobody asked for is a crash
        if self_exited {
            tracing::warn!(pid = exit.pid, uptime_ms, status = %exit, "cursor-sdk-bridge exited");
            self.state.trace_err();
        }

        let _ = self.exit.send(Some(exit));
        drop(self.state_root);
    }

    // Ask over `rpc` and wait for the exit it brings, both under one bound.
    async fn ask(&mut self, rpc: &Rpc) {
        let asked = async {
            let _ = rpc.shutdown(SHUTDOWN_GRACE).await;
            let _ = self.child.wait().await;
        };
        let _ = timeout(SHUTDOWN_WAIT, asked).await;
    }
}

// A process `Worker::spawn` spawned, watched and killable, with its
// ready-line handshake still to run. Dropped, it is killed: nothing is
// bound to ask over.
pub struct Spawned {
    watched: Watched,
    discovery: oneshot::Receiver<Result<Discovery>>,
}

impl Spawned {
    // Resolves once the process exits, however the handshake goes.
    pub fn exited(&self) -> impl Future<Output = Exit> + Send + 'static {
        self.watched.exited()
    }

    // Wait for the ready line and bind `sdk.v1` over it; a handshake that
    // fails kills the process with it.
    pub async fn handshake(self) -> Result<Worker> {
        let Self { watched, discovery } = self;
        let scanned = async {
            discovery.await.unwrap_or_else(|_gone| {
                Err(anyhow!("the stderr reader ended without a ready line"))
            })
        };
        let discovery = watched.step("no ready line", READY_WAIT, scanned).await?;

        let bound = async {
            let base_url = discovery.base_url()?;
            let token = discovery.token().await?;
            Rpc::connect(&base_url, &token).await
        };
        let rpc = watched.step("no answer to the sdk.v1 handshake", CONNECT_WAIT, bound).await?;

        tracing::debug!(
            pid = watched.state.pid,
            spawn_ms = elapsed_ms(watched.state.started_at),
            "cursor-sdk-bridge spawned"
        );

        Ok(Worker { watched, rpc })
    }
}

// The client's end of one watched process: the facts shared with its
// supervisor, the exit the supervisor publishes, and the stop that asks it
// to end the process — over the client the stop carries, or by a kill when
// the sender drops without one.
#[derive(Debug)]
struct Watched {
    state: Arc<State>,
    exit: watch::Receiver<Option<Exit>>,
    shutdown: watch::Sender<Option<Rpc>>,
}

impl Watched {
    fn is_running(&self) -> bool {
        self.exit.borrow().is_none()
    }

    fn exited(&self) -> impl Future<Output = Exit> + Send + 'static {
        let mut exit = self.exit.clone();
        let pid = self.state.pid;
        async move { wait_exit(&mut exit, pid).await }
    }

    async fn fail_on_exit<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        let exited = self.exited();
        tokio::pin!(exited);

        let error = tokio::select! {
            outcome = future => match outcome {
                Ok(value) => return Ok(value),
                Err(error) => error,
            },
            exit = &mut exited => return Err(Failure::WorkerExited(exit).into()),
        };

        // a failure under a dying worker is the worker's exit, if it lands in time
        match timeout(EXIT_WAIT, exited).await {
            Ok(exit) => Err(Failure::WorkerExited(exit).into()),
            Err(_elapsed) => Err(error),
        }
    }

    // Run one handshake step within `bound` and under the process's exit;
    // past the bound it fails as `missing`. The stderr tail behind a failure
    // is logged at DEBUG; it never reaches the error.
    async fn step<T>(
        &self, missing: &str, bound: Duration, step: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let bounded = async {
            timeout(bound, step)
                .await
                .unwrap_or_else(|_elapsed| Err(anyhow!("{missing} within {}s", bound.as_secs())))
        };
        self.fail_on_exit(bounded)
            .await
            .inspect_err(|_error| self.state.trace_err())
            .context("cursor sdk handshake failed")
    }
}

// what the worker, its handshake and its supervisor all see of one process
#[derive(Debug)]
struct State {
    pid: u32,
    started_at: Instant,
    tail: Tail,
}

impl State {
    // The tail is untrusted subprocess output (paths, provider context,
    // session fragments), so it is logged at DEBUG only.
    fn trace_err(&self) {
        let tail = self.tail.to_string();
        if !tail.is_empty() {
            tracing::debug!(%tail, "cursor-sdk-bridge tail");
        }
    }
}

const TAIL_LINES: usize = 20;

#[derive(Debug, Default)]
struct Tail(Mutex<VecDeque<String>>);

impl Tail {
    fn push(&self, line: String) {
        let mut lines = lock(&self.0);
        if lines.len() == TAIL_LINES {
            lines.pop_front();
        }
        lines.push_back(line);
    }
}

impl fmt::Display for Tail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, line) in lock(&self.0).iter().enumerate() {
            if index > 0 {
                f.write_str("\n")?;
            }
            write!(f, "  {line}")?;
        }
        Ok(())
    }
}

/// How a spawned worker ended.
#[derive(Clone, Copy, Debug)]
pub struct Exit {
    /// The exit status, when the wait reported one.
    pub status: Option<ExitStatus>,
    /// The process id.
    pub pid: u32,
}

impl fmt::Display for Exit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.status {
            Some(status) => status.fmt(f),
            None => f.write_str("status unknown"),
        }
    }
}

// A closed channel means the supervisor is gone, and the process with it
// (`kill_on_drop`). The exit is real even if its status never arrives.
async fn wait_exit(exit: &mut watch::Receiver<Option<Exit>>, pid: u32) -> Exit {
    exit.wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|published| *published)
        .unwrap_or(Exit { status: None, pid })
}

const READY_PREFIX: &str = "cursor-sdk-bridge ready ";

// The pipe is held to EOF however the handshake goes, so an abandoned
// handshake never closes it under a live writer; an EOF before the ready
// line reaches the handshake as the dropped sender.
fn read_stderr(
    stderr: ChildStderr, state: Arc<State>,
) -> (oneshot::Receiver<Result<Discovery>>, JoinHandle<()>) {
    let (discovery_tx, discovery_rx) = oneshot::channel();

    let reader = tokio::spawn(
        async move {
            let mut lines = BufReader::new(stderr).lines();
            let mut ready = Some(discovery_tx);
            while let Ok(Some(line)) = lines.next_line().await {
                // the ready line may carry the bearer token: never logged
                if let Some(payload) = line.strip_prefix(READY_PREFIX) {
                    if let Some(tx) = ready.take() {
                        let _ = tx.send(payload.parse());
                    }
                } else {
                    tracing::debug!(%line, stream = "stderr", "cursor-sdk-bridge output");
                    state.tail.push(line);
                }
            }
        }
        .instrument(Span::current()),
    );

    (discovery_rx, reader)
}

// Drain stdout so a full pipe never blocks the process.
fn drain_stdout(stdout: ChildStdout) {
    tokio::spawn(
        async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::debug!(%line, stream = "stdout", "cursor-sdk-bridge output");
            }
        }
        .instrument(Span::current()),
    );
}

// Whether a proxy URL names a user, with or without a scheme: the
// authority runs from after any `://` to the first `/`, `?` or `#`.
fn has_userinfo(url: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.split(['/', '?', '#']).next().is_some_and(|authority| authority.contains('@'))
}

#[cfg(test)]
mod tests {
    use super::{TAIL_LINES, Tail, Worker};

    fn vars(vars: &[(&str, &str)]) -> Vec<(String, String)> {
        vars.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())).collect()
    }

    #[test]
    fn proxy_credentials() {
        Worker::check_proxies(vars(&[
            ("HTTPS_PROXY", "http://proxy.example:3128"),
            ("http_proxy", "proxy.example:3128"),
            ("NO_PROXY", "localhost,127.0.0.1"),
            ("HTTP_PROXY", "http://proxy.example/path?token=a@b#c@d"),
            ("UNRELATED", "http://user:secret@elsewhere"),
        ]))
        .expect("proxies without credentials pass");

        for (name, value) in [
            ("HTTPS_PROXY", "http://user:secret@proxy.example:3128"),
            ("http_proxy", "user:secret@proxy.example:3128"),
            ("HTTP_PROXY", "http://user@proxy.example"),
        ] {
            let error = Worker::check_proxies(vars(&[(name, value)])).expect_err(value);
            assert!(
                error.to_string().starts_with(&format!("{name} carries credentials")),
                "{error}"
            );
            assert!(!error.to_string().contains("secret"), "never repeated: {error}");
        }
    }

    #[test]
    fn tail_bounded() {
        let tail = Tail::default();
        assert_eq!(tail.to_string(), "");
        for index in 0..TAIL_LINES + 5 {
            tail.push(format!("line {index}"));
        }
        let text = tail.to_string();
        assert_eq!(text.lines().count(), TAIL_LINES);
        assert!(text.starts_with("  line 5\n"), "the oldest lines are dropped: {text}");
        assert!(text.ends_with(&format!("  line {}", TAIL_LINES + 4)), "{text}");
    }
}
