//! One process in a compartment: launched, watched to its exit, stopped on
//! request, and killed with everything it forked.
//!
//! The process leads a session of its own, so one kill reaches everything
//! it forked. Whatever it left in its session is swept as its exit is seen.
//! Nothing of a child outlives it. A one-shot runs the same way, held to a
//! bound and killed past it.

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::path::Path;
use std::pin::Pin;
use std::process::{ExitStatus, Output, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use std::{fmt, io};

use anyhow::{Context as _, Result, anyhow, bail};
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop, ProcessSession};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, BufReader};
use tokio::process::{ChildStderr, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tracing::{Instrument as _, Span};

#[cfg(target_os = "macos")]
use crate::seatbelt;
use crate::valve::Valve;

const EXIT_GRACE: Duration = Duration::from_millis(250);
const EXIT_WAIT: Duration = EXIT_GRACE.saturating_mul(2);
const TAIL_LINES: usize = 20;
// A line is cut here, so an endless one is read in pieces and never held whole.
const LINE_BYTES: u64 = 64 * 1024;

/// A spawned process under supervision. Dropping it kills the process.
///
/// The supervisor waits on the process, publishes its [`Exit`], and sweeps
/// the session it led:
///
/// - A drop, or a [`Child::stop`] that goes unanswered, kills the whole
///   session before the process is reaped.
/// - What the process writes to stderr is logged at DEBUG and kept as a
///   tail of its last lines.
/// - An exit nobody asked for is logged at WARN with the tail behind it.
pub struct Child {
    state: Arc<State>,
    exit: watch::Receiver<Option<Exit>>,
    stop: mpsc::Sender<Stop>,
    ready: Option<oneshot::Receiver<String>>,
}

impl Child {
    // The tasks run under the caller's span, so what the process says and
    // how it exits are reported under the work that spawned it. `held` is
    // what the process needs for its life: its scratch and its proxy.
    pub(crate) fn spawn(
        command: Command, held: (TempDir, Valve), ready: Option<&'static str>,
    ) -> Result<Self> {
        let program = program_of(&command);
        let mut child = launch(command)?;
        let started_at = Instant::now();

        let pid = child.id().with_context(|| format!("no pid for spawned `{program}`"))?;
        let (Some(stdout), Some(stderr)) = (child.stdout().take(), child.stderr().take()) else {
            bail!("no piped stdout and stderr for spawned `{program}`");
        };

        let state = Arc::new(State {
            pid,
            program,
            started_at,
            tail: Tail::default(),
        });

        drain_stdout(stdout, Arc::clone(&state));
        let (ready, stderr) = read_stderr(stderr, Arc::clone(&state), ready);

        let (stop_tx, stop) = mpsc::channel(1);
        let (exit_tx, exit) = watch::channel(None);
        tokio::spawn(
            Supervisor {
                child,
                held,
                stderr,
                state: Arc::clone(&state),
                stop,
                exit: exit_tx,
            }
            .run()
            .instrument(Span::current()),
        );

        Ok(Self {
            state,
            exit,
            stop: stop_tx,
            ready: Some(ready),
        })
    }

    /// The process id.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.state.pid
    }

    /// Whether the process has not exited yet.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.exit.borrow().is_none()
    }

    /// Resolves once the process has exited, however it went.
    pub fn exited(&self) -> impl Future<Output = Exit> + Send + 'static {
        let mut exit = self.exit.clone();
        let pid = self.state.pid;
        async move { wait_exit(&mut exit, pid).await }
    }

    /// The payload of the ready line.
    ///
    /// What followed the prefix [`Compartment::spawn`] was given, the first
    /// time the process wrote it to stderr. Resolves to `None` when:
    ///
    /// - the process ends, or stderr closes, before a ready line
    /// - the line was already taken
    ///
    /// [`Compartment::spawn`]: crate::Compartment::spawn
    pub fn ready(&mut self) -> impl Future<Output = Option<String>> + Send + 'static {
        let ready = self.ready.take();
        async move {
            match ready {
                Some(ready) => ready.await.ok(),
                None => None,
            }
        }
    }

    /// Runs `future` under the process's life.
    ///
    /// Resolves with the future's outcome, unless the process exits first.
    /// A failure the future answers while the process is dying is reported
    /// as the exit, so a connection reset by a dying process is the death,
    /// never the reset.
    ///
    /// # Errors
    ///
    /// Returns the [`Exit`] when the process exited before the future
    /// resolved, or within a short grace of its failure.
    pub async fn fail_on_exit<T, E>(
        &self, future: impl Future<Output = Result<T, E>>,
    ) -> Result<Result<T, E>, Exit> {
        let exited = self.exited();
        tokio::pin!(exited);

        let error = tokio::select! {
            outcome = future => match outcome {
                Ok(value) => return Ok(Ok(value)),
                Err(error) => error,
            },
            exit = &mut exited => return Err(exit),
        };

        // a failure under a dying process is its exit, if it lands in time
        match timeout(EXIT_WAIT, exited).await {
            Ok(exit) => Err(exit),
            Err(_elapsed) => Ok(Err(error)),
        }
    }

    /// Asks the process to go, and kills it when the ask goes unanswered.
    ///
    /// `ask` is run, then the exit it brings is waited for, both within
    /// `wait`. Past `wait`, or once `ask` is done without an exit, the
    /// process is killed with its session. Only the first `stop` counts. A
    /// drop after it kills nothing before the ask has had its say.
    pub fn stop(&self, ask: impl Future<Output = ()> + Send + 'static, wait: Duration) {
        let _ = self.stop.try_send(Stop {
            ask: Box::pin(ask),
            wait,
        });
    }

    /// Logs the last lines the process wrote to stderr at DEBUG.
    pub fn log_tail(&self) {
        self.state.log_tail();
    }
}

impl fmt::Debug for Child {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Child")
            .field("pid", &self.state.pid)
            .field("program", &self.state.program)
            .field("running", &self.is_running())
            .finish_non_exhaustive()
    }
}

/// How a supervised process ended.
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

// Past `bound` the process is killed with everything it forked.
pub async fn output(command: Command, bound: Duration) -> Result<Output> {
    let program = program_of(&command);
    let mut child = launch(command)?;
    let mut stdout = child.stdout().take();
    let mut stderr = child.stderr().take();

    let collected = async {
        let (status, stdout, stderr) =
            tokio::try_join!(child.wait(), read(&mut stdout), read(&mut stderr))?;
        io::Result::Ok(Output {
            status,
            stdout,
            stderr,
        })
    };

    match timeout(bound, collected).await {
        Ok(output) => output.with_context(|| format!("running `{program}`")),
        Err(_elapsed) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(anyhow!("`{program}` did not exit within {bound:?}"))
        }
    }
}

// Waits on the process, tears it down on request, and publishes its exit.
struct Supervisor {
    child: Box<dyn ChildWrapper>,
    held: (TempDir, Valve),
    stderr: JoinHandle<()>,
    state: Arc<State>,
    stop: mpsc::Receiver<Stop>,
    exit: watch::Sender<Option<Exit>>,
}

impl Supervisor {
    async fn run(mut self) {
        let self_exited = tokio::select! {
            // an exit in the same tick as a close is still an exit
            biased;
            _ = self.child.wait() => true,
            stop = self.stop.recv() => {
                // a `Child` dropped without a `stop` is killed outright
                if let Some(stop) = stop {
                    self.ask(stop).await;
                }
                false
            }
        };

        // the one kill of the session: nothing of the process outlives it
        let _ = self.child.start_kill();
        let status = self.child.wait().await.ok();
        let uptime_ms = elapsed_ms(self.state.started_at);

        // a forked process may still hold the stderr pipe, so it is not waited on
        if timeout(EXIT_GRACE, &mut self.stderr).await.is_err() {
            self.stderr.abort();
        }

        let exit = Exit {
            status,
            pid: self.state.pid,
        };

        // an exit nobody asked for is a crash
        if self_exited {
            tracing::warn!(
                pid = exit.pid,
                program = %self.state.program,
                uptime_ms,
                status = %exit,
                "child exited"
            );
            self.state.log_tail();
        }

        let _ = self.exit.send(Some(exit));
        drop(self.held);
    }

    async fn ask(&mut self, stop: Stop) {
        let asked = async {
            stop.ask.await;
            let _ = self.child.wait().await;
        };
        let _ = timeout(stop.wait, asked).await;
    }
}

struct Stop {
    ask: Pin<Box<dyn Future<Output = ()> + Send>>,
    wait: Duration,
}

// What the child, its caller, and its supervisor all see of one process.
struct State {
    pid: u32,
    program: String,
    started_at: Instant,
    tail: Tail,
}

impl State {
    // The tail may carry paths, provider context, or session fragments, so it
    // is logged at DEBUG only.
    fn log_tail(&self) {
        let tail = self.tail.to_string();
        if !tail.is_empty() {
            tracing::debug!(program = %self.program, %tail, "child tail");
        }
    }
}

#[derive(Default)]
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

// A session of its own: a kill reaches everything the process forks, and
// `/dev/tty` opens nothing inside.
fn launch(mut command: Command) -> Result<Box<dyn ChildWrapper>> {
    let program = program_of(&command);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut command = CommandWrap::from(command);
    command.wrap(KillOnDrop);
    command.wrap(ProcessSession);
    command.spawn().with_context(|| format!("spawning `{program}`"))
}

fn program_of(command: &Command) -> String {
    let command = command.as_std();
    let program = launched(command).unwrap_or_else(|| command.get_program());
    Path::new(program).file_name().unwrap_or(program).to_string_lossy().into_owned()
}

// The program a substrate's launcher runs in its place, after its `--`.
cfg_select! {
    target_os = "macos" => {
        fn launched(command: &std::process::Command) -> Option<&OsStr> {
            if command.get_program() == seatbelt::SANDBOX_EXEC {
                let mut args = command.get_args();
                args.find(|arg| *arg == "--")?;
                return args.next();
            }
            None
        }
    }
    _ => {
        const fn launched(_command: &std::process::Command) -> Option<&OsStr> {
            None
        }
    }
}

// A closed channel means the supervisor is gone, and the process with it by
// `KillOnDrop`. The exit is real even if its status never arrives.
async fn wait_exit(exit: &mut watch::Receiver<Option<Exit>>, pid: u32) -> Exit {
    exit.wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|published| *published)
        .unwrap_or(Exit { status: None, pid })
}

// The pipe is held to EOF however the handshake goes, so an abandoned
// handshake never closes it under a live writer. An EOF before the ready
// line reaches the caller as the dropped sender.
fn read_stderr(
    stderr: ChildStderr, state: Arc<State>, ready: Option<&'static str>,
) -> (oneshot::Receiver<String>, JoinHandle<()>) {
    let (ready_tx, ready_rx) = oneshot::channel();

    let reader = tokio::spawn(
        async move {
            let mut pending = Some(ready_tx);
            each_line(stderr, |line| {
                // the ready line may carry a credential: never logged
                if let Some(payload) = ready.and_then(|prefix| line.strip_prefix(prefix)) {
                    if let Some(tx) = pending.take() {
                        let _ = tx.send(payload.to_owned());
                    }
                } else {
                    tracing::debug!(program = %state.program, %line, stream = "stderr", "child output");
                    state.tail.push(line);
                }
            })
            .await;
        }
        .instrument(Span::current()),
    );

    (ready_rx, reader)
}

// Drain stdout so a full pipe never blocks the process.
fn drain_stdout(stdout: ChildStdout, state: Arc<State>) {
    tokio::spawn(
        async move {
            each_line(stdout, |line| {
                tracing::debug!(program = %state.program, %line, stream = "stdout", "child output");
            })
            .await;
        }
        .instrument(Span::current()),
    );
}

// Every line to EOF, decoded lossily and cut at `LINE_BYTES`. What the child
// writes is untrusted, and nothing in it may end the read and close the pipe
// under a live writer.
async fn each_line(reader: impl AsyncRead + Unpin, mut each: impl FnMut(String)) {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    loop {
        line.clear();
        match (&mut reader).take(LINE_BYTES).read_until(b'\n', &mut line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        each(String::from_utf8_lossy(&line).into_owned());
    }
}

async fn read<R: AsyncRead + Unpin>(pipe: &mut Option<R>) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    if let Some(pipe) = pipe {
        pipe.read_to_end(&mut bytes).await?;
    }
    Ok(bytes)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{TAIL_LINES, Tail};

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
