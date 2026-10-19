//! The session a completion's processes run in: their working directory,
//! their scratch, their environment, and the commands that spawn them there.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::fd::{AsRawFd as _, RawFd};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fmt, io};

use anyhow::{Context as _, Result};
use tempfile::TempDir;
use tokio::fs;

use crate::child::{self, Child};
use crate::proxy::Proxy;
use crate::{Grant, Kernel, Sandbox};

/// The session a completion's processes run in: where they work, what they
/// inherit, and what their grant opened.
///
/// [`Session::command`] builds a [`Command`] that runs a program inside the
/// session, as many times as the caller asks. A `Session` is a handle: a
/// clone names the same session, and the scratch directory and the egress
/// proxy live as long as any handle or any process a command ran.
#[derive(Clone)]
pub struct Session(Arc<Inner>);

struct Inner {
    cwd: PathBuf,
    access: Access,
    scratch: PathBuf,
    // Removes the scratch as the last handle is dropped.
    _scratch_dir: TempDir,
    dir: File,
    env: BTreeMap<OsString, OsString>,
    path: OsString,
    kernel: Arc<Kernel>,
    read: Vec<PathBuf>,
    proxy: Proxy,
}

impl Session {
    pub(crate) async fn prepare(
        sandbox: &Sandbox, workspace: Option<&Path>, grant: &Grant,
    ) -> Result<Self> {
        // `tempfile` has no async API, and one `mkdir` is not worth a blocking thread
        let scratch_dir = tempfile::Builder::new()
            .prefix("omnia-sandbox-")
            .tempdir()
            .context("creating a scratch directory")?;
        let scratch = fs::canonicalize(scratch_dir.path())
            .await
            .with_context(|| format!("canonicalizing {}", scratch_dir.path().display()))?;

        // the working directory, lent or private
        let (cwd, access) = if let Some(path) = workspace {
            sandbox.workspace(path).await?
        } else {
            let cwd = scratch.join("cwd");
            fs::create_dir(&cwd).await.with_context(|| format!("creating {}", cwd.display()))?;
            (cwd, Access::Private)
        };

        let dir = fs::File::open(&cwd)
            .await
            .with_context(|| format!("opening {}", cwd.display()))?
            .into_std()
            .await;

        // what the grant opens
        let read = sandbox.read_roots(&grant.read).await?;
        let proxy = Proxy::bind(&grant.egress).await?;
        let env = sandbox.environment(&scratch, proxy.address(), &grant.env)?;

        Ok(Self(Arc::new(Inner {
            cwd,
            access,
            scratch,
            _scratch_dir: scratch_dir,
            dir,
            env,
            path: sandbox.host_path(),
            kernel: Arc::clone(sandbox.kernel()),
            read,
            proxy,
        })))
    }

    /// The directory the processes work in, canonical.
    #[must_use]
    pub fn cwd(&self) -> &Path {
        &self.0.cwd
    }

    /// What the processes may do in their working directory.
    #[must_use]
    pub fn access(&self) -> Access {
        self.0.access
    }

    /// The session's own scratch directory: the processes' `HOME` and
    /// `TMPDIR`.
    #[must_use]
    pub fn scratch(&self) -> &Path {
        &self.0.scratch
    }

    /// The directories the processes read beside the system's, their working
    /// directory, and their scratch.
    ///
    /// These are the grant's `read` roots that exist, canonical.
    #[must_use]
    pub fn read_roots(&self) -> &[PathBuf] {
        &self.0.read
    }

    /// A command running `program` inside the session.
    ///
    /// - The command's environment is the session's and nothing of the
    ///   host's. What the caller adds to it rides.
    /// - The command's working directory is the session's, entered through
    ///   its open descriptor before `program` runs.
    /// - A bare `program` is resolved against the host's `PATH` as
    ///   connecting read it, never one the grant set. An absent one is
    ///   refused before any spawn, and no grant chooses the program.
    /// - The command holds a handle on the session, so the session outlives
    ///   the process however the caller's handle goes.
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - a bare `program` is not on the host's `PATH`
    /// - the kernel boundary cannot be laid over the working directory
    pub fn command(&self, program: &str) -> Result<Command> {
        let program = if program.contains('/') {
            PathBuf::from(program)
        } else {
            env::split_paths(&self.0.path)
                .map(|dir| dir.join(program))
                .find(|path| executable(path))
                .with_context(|| format!("`{program}` is not on PATH"))?
        };

        let mut inner = self.0.kernel.command(&Place {
            cwd: &self.0.cwd,
            scratch: &self.0.scratch,
            program: &program,
            read: &self.0.read,
            dir: self.0.dir.as_raw_fd(),
        })?;

        inner.env_clear().envs(&self.0.env);

        Ok(Command {
            session: self.clone(),
            inner,
            ready: None,
        })
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("cwd", &self.0.cwd)
            .field("access", &self.0.access)
            .field("scratch", &self.0.scratch)
            .field("read", &self.0.read)
            .field("proxy", &self.0.proxy)
            .finish_non_exhaustive()
    }
}

/// What a session's processes may do in their working directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// No workspace was lent: the processes work in a private directory
    /// under the scratch.
    Private,
    /// A workspace lent outside every shell root: for reading, with no
    /// shell.
    Read,
    /// A workspace lent at or beneath a shell root: a shell may run in it.
    Shell,
}

impl Access {
    /// Whether a workspace was lent, rather than a private directory.
    #[must_use]
    pub const fn is_lent(self) -> bool {
        !matches!(self, Self::Private)
    }
}

/// A command running inside a [`Session`], built by [`Session::command`].
///
/// Its environment and working directory are the session's;
/// [`Command::env`], [`Command::arg`] and [`Command::args`] add to them.
/// [`Command::spawn`] starts a supervised [`Child`], and
/// [`Command::output`] runs the program once and captures what it wrote.
/// Either takes the command's handle on the session with it, so the
/// session lives at least as long as the process.
pub struct Command {
    session: Session,
    inner: tokio::process::Command,
    ready: Option<&'static str>,
}

impl Command {
    /// Adds an argument for the program.
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.inner.arg(arg);
        self
    }

    /// Adds arguments for the program.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.inner.args(args);
        self
    }

    /// Sets an environment variable for the program, over the session's.
    pub fn env(&mut self, name: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.inner.env(name, value);
        self
    }

    /// Names the stderr line the program writes once it is ready.
    ///
    /// The first stderr line starting with `prefix` is the handshake
    /// [`Child::ready`] resolves with, carrying what followed the prefix.
    /// It is never logged.
    pub const fn ready_prefix(&mut self, prefix: &'static str) -> &mut Self {
        self.ready = Some(prefix);
        self
    }

    /// Spawns the program as a supervised [`Child`].
    ///
    /// - The child reads nothing on stdin.
    /// - Its stdout is drained to DEBUG.
    /// - Its stderr is logged at DEBUG and kept as the child's tail.
    /// - The session lives as long as the process.
    ///
    /// # Errors
    ///
    /// Returns an error when the process cannot be spawned.
    pub fn spawn(self) -> Result<Child> {
        Child::spawn(self.inner, self.session, self.ready)
    }

    /// Runs the program once and captures what it wrote.
    ///
    /// Each stream is kept to sixteen mebibytes; what the process writes
    /// past that is read and dropped. Once the process has exited,
    /// everything it forked is killed. Dropping the run before then kills
    /// the process with everything it forked.
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the process cannot be spawned
    /// - it has not exited within `bound`, in which case it is killed with
    ///   everything it forked and the error downcasts to [`TimedOut`](crate::TimedOut)
    pub async fn output(self, bound: Duration) -> Result<Output> {
        // the session outlives the run
        let Self {
            session: _session,
            inner,
            ..
        } = self;
        child::output(inner, bound).await
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Command")
            .field("session", &self.session)
            .field("ready", &self.ready)
            .finish_non_exhaustive()
    }
}

// Where the kernel puts one process: its working directory, open and as a
// path, its scratch, the program, and the directories its grant opened.
pub struct Place<'a> {
    pub cwd: &'a Path,
    pub scratch: &'a Path,
    pub program: &'a Path,
    pub read: &'a [PathBuf],
    pub dir: RawFd,
}

impl Place<'_> {
    // The child enters the working directory by its open descriptor. Hooks
    // run in the order registered, so the kernel calls this before its rules.
    #[expect(unsafe_code, reason = "`pre_exec` runs in the forked child")]
    pub fn enter(&self, command: &mut tokio::process::Command) {
        let dir = self.dir;
        // SAFETY: the descriptor is inherited across the fork and held open
        // past `spawn`, and `fchdir` is async-signal-safe.
        unsafe {
            command.pre_exec(move || {
                if libc::fchdir(dir) == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
            });
        }
    }
}

fn executable(path: &Path) -> bool {
    path.metadata().is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}
