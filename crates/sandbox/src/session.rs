//! The session a completion's processes run in: their working directory,
//! their scratch, their environment, and the commands that spawn them there.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::fd::{AsFd as _, AsRawFd as _, BorrowedFd};
use std::os::unix::ffi::OsStrExt as _;
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
use crate::{Grant, Kernel, Lend, Sandbox};

/// The session a completion's processes run in: where they work, what they
/// inherit, and what their grant opened.
///
/// [`Session::command`] builds a [`Command`] that runs a program inside the
/// session, as many times as the caller asks. A `Session` is a handle: a
/// clone names the same session, and the scratch directory and the egress
/// proxy live as long as any handle or any process a command ran.
#[derive(Clone)]
pub struct Session(Arc<Inner>);

impl Session {
    pub(crate) async fn prepare(
        sandbox: &Sandbox, lend: Option<Lend<'_>>, grant: &Grant,
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
        let (dir, access, writable) = if let Some(lend) = lend {
            let dir = lend.dir.try_clone_to_owned().context("holding the lent directory")?;
            (File::from(dir), sandbox.access(Some(lend)).await?, lend.writable)
        } else {
            let cwd = scratch.join("cwd");
            fs::create_dir(&cwd).await.with_context(|| format!("creating {}", cwd.display()))?;
            let dir = fs::File::open(&cwd)
                .await
                .with_context(|| format!("opening {}", cwd.display()))?
                .into_std()
                .await;
            (dir, Access::Private, true)
        };

        // what the grant opens
        let read = sandbox.read_roots(&grant.read).await?;
        let proxy = Proxy::bind(&grant.egress).await?;
        let env = sandbox.environment(&scratch, proxy.address(), &grant.env)?;

        Ok(Self(Arc::new(Inner {
            access,
            writable,
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

    /// Where the processes' working directory stands now, canonical.
    ///
    /// Read from the directory's descriptor at each call, so it follows the
    /// directory wherever it has moved. It is for messages, and for a program
    /// told where it works. The processes are bound to the directory itself,
    /// never to this path.
    ///
    /// # Errors
    ///
    /// Returns an error when the kernel cannot report the directory's path.
    pub fn cwd(&self) -> Result<PathBuf> {
        Kernel::held_path(self.0.dir.as_fd())
    }

    /// What the processes may do in their working directory.
    #[must_use]
    pub fn access(&self) -> Access {
        self.0.access
    }

    /// Whether the processes may write their working directory.
    ///
    /// A lend is writable where the backend lent it so. A private directory
    /// always is.
    #[must_use]
    pub fn writable(&self) -> bool {
        self.0.writable
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
    ///   its open descriptor before `program` runs. The kernel boundary is
    ///   laid over that descriptor too, read-only where the session is not
    ///   [`writable`](Self::writable).
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
        let program = resolve(Path::new(program), &self.0.path)?;

        // the working directory by its descriptor, so no path is walked again
        let cwd = Root::Open(self.0.dir.as_fd());
        let mut read: Vec<Root<'_>> = self.0.read.iter().map(|root| Root::Path(root)).collect();
        let mut write = vec![Root::Path(&self.0.scratch)];
        if self.0.writable {
            write.push(cwd);
        } else {
            read.push(cwd);
        }

        let mut inner = self.0.kernel.command(&Place {
            dir: self.0.dir.as_fd(),
            program: &program,
            read,
            write,
            session: true,
            network: true,
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
            .field("cwd", &self.cwd().ok())
            .field("access", &self.0.access)
            .field("writable", &self.0.writable)
            .field("scratch", &self.0.scratch)
            .field("read", &self.0.read)
            .field("proxy", &self.0.proxy)
            .finish_non_exhaustive()
    }
}

struct Inner {
    access: Access,
    writable: bool,
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

// Where the kernel puts one process: the working directory it enters, the
// program, what it reads and what it writes, and whether it reaches the
// network at all.
pub struct Place<'a> {
    pub dir: BorrowedFd<'a>,
    pub program: &'a Path,
    pub read: Vec<Root<'a>>,
    pub write: Vec<Root<'a>>,
    // Whether `dir` is a session's working directory, which its processes
    // never remove, whose ancestors they list, and whose `.git` and `.emery`
    // are closed to them.
    #[cfg_attr(
        target_os = "linux",
        expect(
            dead_code,
            reason = "Landlock cannot deny beneath a grant, so Linux reads none of it"
        )
    )]
    pub session: bool,
    pub network: bool,
}

impl Place<'_> {
    // The child enters the working directory by its open descriptor. Hooks
    // run in the order registered, so the kernel calls this before its rules.
    #[expect(unsafe_code, reason = "`pre_exec` runs in the forked child")]
    pub fn enter(&self, command: &mut tokio::process::Command) -> Result<()> {
        let dir = self.dir.try_clone_to_owned().context("holding the working directory open")?;
        // SAFETY: the duplicate moves into the hook, so it is open in the
        // child whenever the command spawns, and `fchdir` is
        // async-signal-safe.
        unsafe {
            command.pre_exec(move || {
                if libc::fchdir(dir.as_raw_fd()) == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
        }
        Ok(())
    }
}

// A file or directory a process is opened to: by the path it resolved to, or
// by a descriptor its caller holds, which names what was opened whatever its
// path reads as now.
#[derive(Clone, Copy)]
pub enum Root<'a> {
    Path(&'a Path),
    Open(BorrowedFd<'a>),
}

// `program` as given where it holds a separator, else the first executable of
// its name along `path`.
pub fn resolve(program: &Path, path: &OsStr) -> Result<PathBuf> {
    if program.as_os_str().as_bytes().contains(&b'/') {
        return Ok(program.to_owned());
    }
    env::split_paths(path)
        .map(|dir| dir.join(program))
        .find(|path| executable(path))
        .with_context(|| format!("`{}` is not on PATH", program.display()))
}

fn executable(path: &Path) -> bool {
    path.metadata().is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}
