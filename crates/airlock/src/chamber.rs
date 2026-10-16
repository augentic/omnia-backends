//! Where one process is put: its tree, its scratch, and the compartment that
//! spawns there.

use std::collections::BTreeMap;
use std::ffi::OsString;
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
use tokio::process::Command;

use crate::child::{self, Child};
use crate::valve::Valve;
use crate::{Bulkhead, Options};

/// The sealed compartment around one process: where it runs, what it
/// inherits, and what its clearance granted.
///
/// - [`Compartment::command`] builds a command inside it.
/// - [`Compartment::spawn`] starts a supervised [`Child`] there.
/// - [`Compartment::output`] runs a one-shot command there and captures what
///   it wrote.
///
/// The valve the process's proxy variables name is the compartment's own.
/// It serves until the process it was sealed for has exited.
#[derive(Debug)]
pub struct Compartment {
    pub(crate) chamber: Chamber,
    pub(crate) env: BTreeMap<OsString, OsString>,
    pub(crate) path: OsString,
    pub(crate) bulkhead: Arc<Bulkhead>,
    pub(crate) read: Vec<PathBuf>,
    pub(crate) valve: Valve,
}

impl Compartment {
    /// The directory the process works in.
    #[must_use]
    pub fn tree(&self) -> &Path {
        self.chamber.tree()
    }

    /// The trees the process reads beside the system's, its tree, and its scratch.
    ///
    /// These are the clearance's `read` roots that exist, canonical.
    #[must_use]
    pub fn read(&self) -> &[PathBuf] {
        &self.read
    }

    /// The valve the process's proxy variables name.
    #[must_use]
    pub const fn valve(&self) -> &Valve {
        &self.valve
    }

    /// The working directory as text, for a wire that carries it as a string.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is not UTF-8.
    pub fn cwd(&self) -> Result<&str> {
        let path = self.tree();
        path.to_str().with_context(|| format!("invalid working directory {}", path.display()))
    }

    /// Whether the tree was lent, rather than a private directory.
    #[must_use]
    pub const fn lent(&self) -> bool {
        self.chamber.lent()
    }

    /// Whether the process runs with a shell: a lent tree under a shell root.
    #[must_use]
    pub const fn shell(&self) -> bool {
        self.chamber.shell()
    }

    /// The process's own scratch directory: its `HOME` and `TMPDIR`.
    #[must_use]
    pub fn scratch(&self) -> &Path {
        self.chamber.scratch()
    }

    /// A command running `program` inside the boundary.
    ///
    /// - The command's environment is the boundary's and nothing of the
    ///   host's. What the caller adds to it rides.
    /// - A bare `program` is resolved here against the host's `PATH` as
    ///   connecting read it, never one the ask set. An absent one is refused
    ///   before any spawn, and no ask chooses the program.
    /// - Under Seatbelt the command is the substrate's launcher, which
    ///   applies the boundary and runs `program` in its place. The arguments
    ///   the caller adds are still the program's.
    /// - Under Landlock the rules are applied in the child before `program`
    ///   runs.
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - a bare `program` is not on the host's `PATH`
    /// - the substrate cannot lay its rules over the tree
    pub fn command(&self, program: &str) -> Result<Command> {
        let program = self.locate(program)?;
        let mut command = self.bulkhead.command(&Place {
            tree: self.tree(),
            scratch: self.scratch(),
            program: &program,
            read: &self.read,
            dir: self.chamber.dir.as_raw_fd(),
        })?;
        command.env_clear().envs(&self.env);
        Ok(command)
    }

    fn locate(&self, program: &str) -> Result<PathBuf> {
        if program.contains('/') {
            return Ok(PathBuf::from(program));
        }
        env::split_paths(&self.path)
            .map(|dir| dir.join(program))
            .find(|path| executable(path))
            .with_context(|| format!("`{program}` is not on PATH"))
    }

    /// Spawns `command` inside the boundary as a supervised [`Child`].
    ///
    /// - The child reads nothing on stdin.
    /// - Its stdout is drained to DEBUG.
    /// - Its stderr is logged at DEBUG and kept as the child's tail.
    /// - A stderr line starting with `ready` is the handshake the child
    ///   answers [`Child::ready`] with. It is never logged.
    /// - The scratch and the egress proxy live as long as the process.
    ///
    /// # Errors
    ///
    /// Returns an error when the process cannot be spawned.
    pub fn spawn(self, command: Command, ready: Option<&'static str>) -> Result<Child> {
        Child::spawn(command, (self.chamber.scratch, self.valve), ready)
    }

    /// Runs `command` inside the boundary once and captures what it wrote.
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the process cannot be spawned
    /// - its output cannot be read
    /// - it has not exited within `bound`, in which case it is killed with
    ///   everything it forked
    pub async fn output(self, command: Command, bound: Duration) -> Result<Output> {
        child::output(command, bound).await
    }
}

// The place one process is put: the tree it works in and its scratch.
pub struct Chamber {
    tree: PathBuf,
    lent: bool,
    scratch: TempDir,
    scratch_path: PathBuf,
    shell: bool,
    dir: File,
}

impl Chamber {
    pub(crate) async fn prepare(lend: Option<&Path>, options: &Options) -> Result<Self> {
        // `tempfile` has no async API, and one `mkdir` is not worth a blocking thread
        let scratch = tempfile::Builder::new()
            .prefix("omnia-airlock-")
            .tempdir()
            .context("creating a scratch directory")?;
        let scratch_path = fs::canonicalize(scratch.path())
            .await
            .with_context(|| format!("canonicalizing {}", scratch.path().display()))?;

        let (tree, shell) = if let Some(path) = lend {
            // create or update the process's working directory
            fs::create_dir_all(path)
                .await
                .with_context(|| format!("creating {}", path.display()))?;
            let path = fs::canonicalize(path)
                .await
                .with_context(|| format!("canonicalizing {}", path.display()))?;

            // is path beneath any of the shell roots?
            let mut shell = false;
            for root in &options.shell_roots {
                let root = fs::canonicalize(root).await.unwrap_or_else(|_| root.clone());
                if path.starts_with(root) {
                    shell = true;
                    break;
                }
            }

            (path, shell)
        } else {
            // create process's working directory
            let path = scratch_path.join("cwd");
            fs::create_dir(&path).await.with_context(|| format!("creating {}", path.display()))?;
            (path, false)
        };

        let dir = File::open(&tree).with_context(|| format!("opening {}", tree.display()))?;

        Ok(Self {
            tree,
            lent: lend.is_some(),
            scratch,
            scratch_path,
            shell,
            dir,
        })
    }

    pub(crate) fn tree(&self) -> &Path {
        &self.tree
    }

    pub(crate) fn scratch(&self) -> &Path {
        &self.scratch_path
    }

    pub(crate) const fn lent(&self) -> bool {
        self.lent
    }

    pub(crate) const fn shell(&self) -> bool {
        self.shell
    }
}

impl fmt::Debug for Chamber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Chamber")
            .field("tree", &self.tree)
            .field("lent", &self.lent)
            .field("shell", &self.shell)
            .field("scratch", &self.scratch_path)
            .finish_non_exhaustive()
    }
}

// Where the substrate puts one process: its tree, open and as a path, its
// scratch, the program, and the directories its ask was granted to read.
pub struct Place<'a> {
    pub tree: &'a Path,
    pub scratch: &'a Path,
    pub program: &'a Path,
    pub read: &'a [PathBuf],
    pub dir: RawFd,
}

impl Place<'_> {
    // The child enters the tree by its open descriptor. Hooks run in the order
    // registered, so a substrate calls this before its rules.
    #[expect(
        unsafe_code,
        reason = "`pre_exec` runs in the forked child; `fchdir` is async-signal-safe"
    )]
    pub fn enter(&self, command: &mut Command) {
        let dir = self.dir;
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
