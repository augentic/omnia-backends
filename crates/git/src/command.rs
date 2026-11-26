//! One `git` process: spawned in the place the runtime opened, with the
//! operator's repository selection scrubbed from its environment, its
//! output captured whole.
//!
//! The child enters the place's open directory handle before it executes,
//! so git works where the runtime resolved the guest's location and never
//! walks a path a guest could redirect meanwhile. A working copy this
//! client laid is addressed through the gitdir `add` recorded, never
//! through the `.git` file at its root, which a guest lent the copy can
//! rewrite. The process leads a session of its own, with no controlling
//! terminal, and is killed when its future is dropped, so a helper git
//! forks — a credential helper, `ssh`, a merge driver — opens no terminal
//! and never outlives the operation that started it.

mod address;
mod frozen;
mod host;
mod pins;

use std::borrow::Cow;
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::fd::{AsRawFd as _, OwnedFd, RawFd};
use std::path::{self, Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use anyhow::{Context as _, Result};
use omnia_wasi_vcs::Place;
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop, ProcessSession};
use tokio::process::Command;

pub use self::address::Address;
pub use self::frozen::Frozen;
pub use self::host::Host;
pub use self::pins::{Listing, Pins};
use crate::Client;

// The repository selection a shell exports; left in place, it would point
// every operation at the operator's own repository rather than the location
// the guest named.
const SCRUBBED: [&str; 4] = ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

// What an interactive git reads from a terminal, an editor, or a prompt —
// none answerable from a guest, so each is closed before git can block on
// it or run a program to fill it; no protocol and no lazy fetch, so an
// object the repository lacks is missing rather than fetched from a promisor
// remote it names, over the URL, proxy, and pack command it chose, whatever
// `protocol.<name>.allow` it set — a transport operation names the list for
// its own kind in `env`, which applies after, and a git before 2.45 reads
// no `GIT_NO_LAZY_FETCH`, which the empty list covers; and the C locale,
// over whatever the operator's shell exports, so what git says is the
// English the refusal classes read.
const FIXED: [(&str, &str); 6] = [
    ("GIT_TERMINAL_PROMPT", "0"),
    ("GIT_EDITOR", ":"),
    ("GIT_MERGE_AUTOEDIT", "no"),
    ("GIT_ALLOW_PROTOCOL", ""),
    ("GIT_NO_LAZY_FETCH", "1"),
    ("LC_ALL", "C"),
];

// What one git process said: stdout as the bytes git wrote, since a blob
// read for a merge must round-trip whole, and stderr read lossily, since it
// is only ever quoted back to the guest.
#[derive(Debug)]
pub struct Output {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: String,
}

impl Output {
    // stdout as text, for a hash, a path, or a listing on its way to the guest.
    pub fn text(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.stdout)
    }
}

// Where one git process runs, and what holds it there.
#[derive(Clone, Copy)]
pub enum Run<'a> {
    // For no repository, in the backend's own directory, with the
    // operator's environment.
    Backend,
    // In a repository the backend owns, a mirror or a staging, with the
    // operator's environment: the one process that dials a network remote.
    Owned(&'a Path),
    // In a place, beside an operation's scratch, reading the one mirror it
    // dials where it dials one, held as `Host::command` holds it.
    Place { at: &'a Place, scratch: &'a Path, mirror: Option<&'a Path> },
}

impl<'a> Run<'a> {
    pub const fn place(self) -> Option<&'a Place> {
        match self {
            Self::Backend | Self::Owned(_) => None,
            Self::Place { at, .. } => Some(at),
        }
    }
}

// A running git and what it is called, for the errors waiting on it.
pub struct Spawned {
    child: Box<dyn ChildWrapper>,
    binary: String,
}

impl Client {
    // Run git in `at`, beside a scratch of its own, or for no place in the
    // backend's own directory.
    pub(crate) async fn git<I, S>(
        &self, at: Option<&Place>, args: I, env: &[(&str, &OsStr)],
    ) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        match at {
            Some(at) => {
                let scratch = self.host.scratch()?;
                let run = Run::Place {
                    at,
                    scratch: scratch.path(),
                    mirror: None,
                };
                self.run(run, args, env).await
            }
            None => self.run(Run::Backend, args, env).await,
        }
    }

    // Run git as `run` holds it, with `env` set on top of the fixed
    // interactive lockout, and wait for it; a failure here is the spawn's,
    // never git's own exit.
    pub(crate) async fn run<I, S>(
        &self, run: Run<'_>, args: I, env: &[(&str, &OsStr)],
    ) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let command = self.command(run, args, env)?;
        self.spawn(command)?.wait().await
    }

    // The command `run` holds git to, its stdin closed and its output
    // captured, for a caller that rewires a stream before it spawns.
    pub(crate) fn command<I, S>(
        &self, run: Run<'_>, args: I, env: &[(&str, &OsStr)],
    ) -> Result<Command>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = match run {
            Run::Backend => {
                let mut command = Command::new(&self.binary);
                let root = self.host.root();
                command.current_dir(root);
                if let Some(ceiling) = root.parent() {
                    command.env("GIT_CEILING_DIRECTORIES", ceiling);
                }
                command
            }
            Run::Owned(dir) => {
                let mut command = Command::new(&self.binary);
                let mut flag = OsString::from("--git-dir=");
                flag.push(dir);
                command.current_dir(dir).arg(flag);
                command
            }
            Run::Place { at, scratch, mirror } => {
                self.host.check(at)?;
                let mut command = self.host.command(&self.binary, at, scratch, mirror)?;
                match self.gitdir(at) {
                    Some(gitdir) => {
                        let mut flag = OsString::from("--git-dir=");
                        flag.push(gitdir);
                        command.arg(flag);
                    }
                    None => {
                        if let Some(ceiling) = ceiling(at) {
                            command.env("GIT_CEILING_DIRECTORIES", ceiling);
                        }
                    }
                }
                command
            }
        };
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        command.args(args);
        for var in SCRUBBED {
            command.env_remove(var);
        }
        for (key, value) in FIXED.map(|(key, value)| (key, OsStr::new(value))).iter().chain(env) {
            command.env(key, value);
        }
        Ok(command)
    }

    // Spawn `command` leading a session of its own, killed when it is
    // dropped. The command goes with the spawn, and with it every
    // descriptor it held for the child.
    pub(crate) fn spawn(&self, command: Command) -> Result<Spawned> {
        let mut command = CommandWrap::from(command);
        command.wrap(KillOnDrop);
        command.wrap(ProcessSession);
        let child = command.spawn().with_context(|| format!("issue spawning `{}`", self.binary))?;
        Ok(Spawned {
            child,
            binary: self.binary.clone(),
        })
    }
}

impl Spawned {
    // Wait for the process and read what it said.
    pub async fn wait(self) -> Result<Output> {
        let output = Box::into_pin(self.child.wait_with_output())
            .await
            .with_context(|| format!("issue waiting for `{}`", self.binary))?;
        Ok(Output {
            status: output.status,
            stdout: output.stdout,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

// Where git's search for the repository stops: the place's parent, so a
// place holding no `.git` of its own is no repository rather than a
// subdirectory of whichever ancestor holds one, while a repository rooted in
// the place, or a working copy whose `.git` file is, resolves as before. Git
// resolves the entry and the directory the child entered alike before it
// compares them, so the place's recorded path serves, made absolute as git
// requires of an entry.
fn ceiling(place: &Place) -> Option<PathBuf> {
    path::absolute(place.path()).ok()?.parent().map(Path::to_path_buf)
}

// The child's working directory is the open handle itself, entered between
// `fork` and `exec`.
#[expect(unsafe_code, reason = "`pre_exec` runs in the forked child")]
fn enter(command: &mut Command, dir: RawFd) {
    // SAFETY: the descriptor is inherited across the fork, the place that owns
    // it is held until `spawn` returns, and `fchdir` is async-signal-safe.
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(dir) == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
        });
    }
}

// Hand `fds` to the child as its descriptors 3 and 4, the two a `fd::3,4`
// transport reads and writes. The command holds them until it is spawned,
// and closes them as it goes.
#[expect(unsafe_code, reason = "`pre_exec` runs in the forked child")]
pub fn hand(command: &mut Command, fds: [OwnedFd; 2]) {
    // SAFETY: `fcntl` and `dup2` are async-signal-safe. Each descriptor is
    // copied above both targets first, so neither lands on the other.
    unsafe {
        command.pre_exec(move || {
            let mut high = [0; 2];
            for (slot, fd) in high.iter_mut().zip(&fds) {
                *slot = libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 10);
                if *slot < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            for (target, fd) in (3..).zip(high) {
                if libc::dup2(fd, target) < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
}
