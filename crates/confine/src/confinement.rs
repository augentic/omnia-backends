//! Where one process is put: its tree, its scratch, and the boundary that
//! spawns it there.

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::fd::{AsRawFd as _, RawFd};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::Duration;
use std::{env, fmt, io};

use anyhow::{Context as _, Result, anyhow};
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop, ProcessSession};
use tempfile::TempDir;
use tokio::io::AsyncReadExt as _;
use tokio::process::Command;
use tokio::time::timeout;

use crate::child::Child;
use crate::egress::Egress;
#[cfg(target_os = "linux")]
use crate::linux;
#[cfg(target_os = "macos")]
use crate::seatbelt;
use crate::{Options, Wall};

/// The place one process is put: the tree it works in and its scratch.
///
/// Built once per process by [`Confinement::of`] and laid into a
/// [`Boundary`] by [`Confine::lay`](crate::Confine::lay).
pub struct Confinement {
    tree: Tree,
    scratch: TempDir,
    scratch_path: PathBuf,
    shell: bool,
    dir: File,
}

enum Tree {
    Lent(PathBuf),
    Private(PathBuf),
}

impl Tree {
    fn path(&self) -> &Path {
        match self {
            Self::Lent(path) | Self::Private(path) => path,
        }
    }
}

impl Confinement {
    /// Puts a process at `lend`, or in a private directory when nothing is lent.
    ///
    /// A lent tree is created and canonicalised; the process runs with a
    /// shell when the tree is at or beneath one of the `shell_roots` of
    /// `options`. The scratch directory, which serves the process as its
    /// `HOME` and `TMPDIR`, is removed once the process has exited.
    ///
    /// # Errors
    ///
    /// Returns an error when the lent tree or the scratch cannot be created.
    pub async fn of(lend: Option<&Path>, options: &Options) -> Result<Self> {
        // `tempfile` has no async API; one `mkdir` under the temp root is
        // not worth a blocking thread
        let scratch = tempfile::Builder::new()
            .prefix("omnia-confine-")
            .tempdir()
            .context("creating a scratch directory")?;
        let scratch_path = tokio::fs::canonicalize(scratch.path())
            .await
            .with_context(|| format!("canonicalizing {}", scratch.path().display()))?;

        let (tree, shell) = if let Some(path) = lend {
            tokio::fs::create_dir_all(path)
                .await
                .with_context(|| format!("creating {}", path.display()))?;
            let path = tokio::fs::canonicalize(path)
                .await
                .with_context(|| format!("canonicalizing {}", path.display()))?;
            let shell = beneath_any(&path, &options.shell_roots).await;
            (Tree::Lent(path), shell)
        } else {
            let path = scratch_path.join("cwd");
            tokio::fs::create_dir(&path)
                .await
                .with_context(|| format!("creating {}", path.display()))?;
            (Tree::Private(path), false)
        };

        let dir = File::open(tree.path())
            .with_context(|| format!("opening {}", tree.path().display()))?;
        Ok(Self {
            tree,
            scratch,
            scratch_path,
            shell,
            dir,
        })
    }

    /// The directory the process works in, canonical.
    #[must_use]
    pub fn tree(&self) -> &Path {
        self.tree.path()
    }

    /// The process's own scratch directory, canonical: its `HOME` and `TMPDIR`.
    #[must_use]
    pub fn scratch(&self) -> &Path {
        &self.scratch_path
    }

    /// Whether the tree was lent, rather than a private directory.
    #[must_use]
    pub const fn lent(&self) -> bool {
        matches!(self.tree, Tree::Lent(_))
    }

    /// Whether the process runs with a shell: a lent tree under a shell root.
    #[must_use]
    pub const fn shell(&self) -> bool {
        self.shell
    }
}

impl fmt::Debug for Confinement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Confinement")
            .field("tree", &self.tree.path())
            .field("lent", &self.lent())
            .field("shell", &self.shell)
            .field("scratch", &self.scratch_path)
            .finish_non_exhaustive()
    }
}

// A root is compared as spelled and as it resolves, since the lend is
// canonical and a root may be a symlink away from it.
async fn beneath_any(path: &Path, roots: &[PathBuf]) -> bool {
    for root in roots {
        if path.starts_with(root) {
            return true;
        }
        if let Ok(root) = tokio::fs::canonicalize(root).await
            && path.starts_with(&root)
        {
            return true;
        }
    }
    false
}

/// The boundary around one process: where it runs, what it inherits, and
/// what its ask was granted.
///
/// [`Boundary::command`] builds a command inside it, [`Boundary::spawn`]
/// starts a supervised [`Child`] there, and [`Boundary::output`] runs a
/// one-shot command inside it and captures what it wrote. The egress proxy
/// the process's `HTTPS_PROXY` names is the boundary's own, serving until
/// the process it was laid for has exited.
#[derive(Debug)]
pub struct Boundary {
    confinement: Confinement,
    env: Vec<(OsString, OsString)>,
    wall: Wall,
    read: Vec<PathBuf>,
    egress: Egress,
}

impl Boundary {
    pub(crate) const fn new(
        confinement: Confinement, env: Vec<(OsString, OsString)>, wall: Wall, read: Vec<PathBuf>,
        egress: Egress,
    ) -> Self {
        Self {
            confinement,
            env,
            wall,
            read,
            egress,
        }
    }

    /// The directory the process works in.
    #[must_use]
    pub fn tree(&self) -> &Path {
        self.confinement.tree()
    }

    /// The trees the process reads beside the system, its tree, and its
    /// scratch: the ask's `read` roots that exist, canonical.
    #[must_use]
    pub fn read(&self) -> &[PathBuf] {
        &self.read
    }

    /// The egress proxy the process's proxy variables name.
    #[must_use]
    pub const fn egress(&self) -> &Egress {
        &self.egress
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
        self.confinement.lent()
    }

    /// Whether the process runs with a shell: a lent tree under a shell root.
    #[must_use]
    pub const fn shell(&self) -> bool {
        self.confinement.shell()
    }

    /// The process's own scratch directory: its `HOME` and `TMPDIR`.
    #[must_use]
    pub fn scratch(&self) -> &Path {
        self.confinement.scratch()
    }

    /// A command running `program` inside the boundary.
    ///
    /// The command's environment is the boundary's and nothing of the
    /// host's; what the caller adds to it rides. A bare `program` is
    /// resolved against the boundary's `PATH` here, so an absent one is
    /// refused before any spawn. Under Seatbelt the command is the
    /// substrate's launcher, which applies the boundary and runs `program`
    /// in its place, so the arguments the caller adds are the program's;
    /// under Landlock the rules are applied in the child before `program`
    /// runs.
    ///
    /// # Errors
    ///
    /// Returns an error when a bare `program` is not on the boundary's
    /// `PATH`, or when the substrate cannot lay its rules over the tree.
    pub fn command(&self, program: &str) -> Result<Command> {
        let program = self.locate(program)?;
        let mut command = match &self.wall {
            #[cfg(target_os = "linux")]
            Wall::Landlock(_) => Command::new(&program),
            #[cfg(target_os = "macos")]
            Wall::Seatbelt(seatbelt) => seatbelt.command(&seatbelt::Place {
                tree: self.tree(),
                scratch: self.scratch(),
                program: &program,
                read: &self.read,
            })?,
        };
        enter(&mut command, self.confinement.dir.as_raw_fd());
        #[cfg(target_os = "linux")]
        if let Wall::Landlock(linux) = &self.wall {
            linux.restrain(
                &mut command,
                &linux::Place {
                    tree: self.tree(),
                    scratch: self.scratch(),
                    program: &program,
                    read: &self.read,
                },
            )?;
        }
        command.env_clear().envs(self.env.iter().map(|(name, value)| (name, value)));
        Ok(command)
    }

    fn locate(&self, program: &str) -> Result<PathBuf> {
        if program.contains('/') {
            return Ok(PathBuf::from(program));
        }
        let path = self
            .env
            .iter()
            .find(|(name, _)| name == "PATH")
            .map(|(_, value)| value.as_os_str())
            .unwrap_or_default();
        env::split_paths(path)
            .map(|dir| dir.join(program))
            .find(|candidate| executable(candidate))
            .with_context(|| format!("`{program}` is not on PATH"))
    }

    /// Spawns `command` inside the boundary as a supervised [`Child`].
    ///
    /// The child reads nothing on stdin, and what it writes is read by the
    /// supervisor: stdout drained to DEBUG, stderr kept as the child's tail.
    /// A stderr line starting with `ready` is the handshake the child
    /// answers [`Child::ready`] with; it is never logged. The scratch and
    /// the egress proxy live as long as the process.
    ///
    /// # Errors
    ///
    /// Returns an error when the process cannot be spawned.
    pub fn spawn(self, command: Command, ready: Option<&'static str>) -> Result<Child> {
        let program = program_of(&command);
        let child = launch(command)?;
        Child::supervise(child, program, (self.confinement.scratch, self.egress), ready)
    }

    /// Runs `command` inside the boundary once and captures what it wrote.
    ///
    /// # Errors
    ///
    /// Returns an error when the process cannot be spawned, when its output
    /// cannot be read, or when it has not exited within `bound`, in which
    /// case it is killed with everything it forked.
    pub async fn output(self, command: Command, bound: Duration) -> Result<Output> {
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
#[cfg(target_os = "macos")]
fn launched(command: &std::process::Command) -> Option<&OsStr> {
    if command.get_program() == seatbelt::SANDBOX_EXEC {
        let mut args = command.get_args();
        args.find(|arg| *arg == "--")?;
        return args.next();
    }
    None
}

#[cfg(not(target_os = "macos"))]
const fn launched(_command: &std::process::Command) -> Option<&OsStr> {
    None
}

fn executable(path: &Path) -> bool {
    path.metadata().is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}

#[expect(
    unsafe_code,
    reason = "`pre_exec` runs in the forked child; `fchdir` is async-signal-safe"
)]
fn enter(command: &mut Command, dir: RawFd) {
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(dir) == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
        });
    }
}

async fn read<R: tokio::io::AsyncRead + Unpin>(pipe: &mut Option<R>) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    if let Some(pipe) = pipe {
        pipe.read_to_end(&mut bytes).await?;
    }
    Ok(bytes)
}
