//! What every git the client runs for a place is held to on the host: the
//! kernel boundary and what the host's git reads through it, the directory
//! the client owns, and the part of the operator's environment it inherits.
//!
//! A git run for a place reads data a guest can write, so it runs held to
//! the guest's mounts: no network, nothing of the host beside the files the
//! host's git itself needs, and an environment built from a short list.
//! What the client keeps for itself lies in a directory no mount reaches.

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::fd::{AsFd as _, AsRawFd as _, BorrowedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use omnia_sandbox::{Bounds, Sandbox};
use omnia_wasi_vcs::{Error, Place};
use tempfile::TempDir;
use tokio::process::Command;

use crate::Client;

// The operator's variables a git run for a place inherits: where its
// programs and the operator's configuration are, the locale and timezone,
// and who commits. A credential, an agent's socket, or a proxy never
// reaches one.
const ALLOWED: [&str; 6] = ["PATH", "HOME", "XDG_CONFIG_HOME", "TZ", "LANG", "EMAIL"];
const ALLOWED_PREFIXES: [&str; 4] = ["LC_", "GIT_CONFIG", "GIT_AUTHOR_", "GIT_COMMITTER_"];

pub struct Host {
    // None where the operator runs git without the boundary.
    sandbox: Option<Sandbox>,
    reach: Reach,
    // Removes the client's directory as the last client goes.
    _root: TempDir,
    root: PathBuf,
    mirrors: PathBuf,
    env: Vec<(OsString, OsString)>,
}

impl Host {
    // The client's directory, created canonical, with its mirrors beneath it
    // unless the operator names a directory for them.
    pub fn new(mirrors: Option<&str>) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("omnia-git-")
            .tempdir()
            .context("creating the backend's directory")?;
        let root = canonical(dir.path())?;
        let mirrors = mirrors.map_or_else(|| root.join("mirrors"), PathBuf::from);
        std::fs::create_dir_all(&mirrors)
            .with_context(|| format!("creating the mirrors at {}", mirrors.display()))?;
        let env = env::vars_os().filter(|(name, _)| allowed(name)).collect();
        Ok(Self {
            sandbox: None,
            reach: Reach::default(),
            _root: dir,
            mirrors: canonical(&mirrors)?,
            root,
            env,
        })
    }

    // Hold every git run for a place to `sandbox`, reading `reach`.
    pub fn bound(&mut self, sandbox: Sandbox, reach: Reach) {
        self.sandbox = Some(sandbox);
        self.reach = reach;
    }

    // The git a bounded process runs for the client's `binary`.
    pub fn program<'a>(&'a self, binary: &'a str) -> &'a Path {
        self.reach.binary.as_deref().unwrap_or_else(|| Path::new(binary))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn mirrors(&self) -> &Path {
        &self.mirrors
    }

    // A directory of one operation's own, removed when it is dropped: its
    // `TMPDIR`, its frozen configuration, and what it stages.
    pub fn scratch(&self) -> Result<TempDir> {
        tempfile::Builder::new()
            .prefix("op-")
            .tempdir_in(&self.root)
            .context("creating an operation's scratch")
    }

    // The client's own directories refused where they and a mount the guest
    // holds overlap, since the guest could rewrite what git reads there.
    pub fn check(&self, at: &Place) -> Result<()> {
        for dir in [&self.root, &self.mirrors] {
            let beneath = at.mounts().beneath(dir, |_| true)?.is_some();
            let above = at.mounts().entries().iter().any(|mount| {
                std::fs::canonicalize(&mount.host_path).is_ok_and(|path| path.starts_with(dir))
            });
            if beneath || above {
                let shown = dir.display();
                let message =
                    format!("the backend's directory {shown} and the guest's mounts overlap");
                return Err(Error::Other(message).into());
            }
        }
        Ok(())
    }

    // `binary` at `at`, working in the place's open directory, beside the
    // operation's `scratch`: its environment the operator's variables
    // `ALLOWED` names, with `TMPDIR` the scratch, and where the host
    // enforces the boundary, held to the guest's mounts, git's reach, the
    // scratch, and the one `mirror` it reads, if any.
    pub fn command(
        &self, binary: &str, at: &Place, scratch: &Path, mirror: Option<&Path>,
    ) -> Result<Command> {
        let mut command = if let Some(sandbox) = &self.sandbox {
            let open = |path: &Path| {
                File::open(path).with_context(|| format!("opening {}", path.display()))
            };
            let mut reach: Vec<File> =
                self.reach.paths.iter().filter_map(|path| File::open(path).ok()).collect();
            if let Some(mirror) = mirror {
                reach.push(open(mirror)?);
            }
            let scratch_dir = open(scratch)?;
            let mut read: Vec<BorrowedFd<'_>> = reach.iter().map(File::as_fd).collect();
            let mut write = vec![scratch_dir.as_fd()];
            for mount in at.mounts().entries() {
                let roots = if mount.writable { &mut write } else { &mut read };
                roots.push(mount.dir.as_fd());
            }
            let bounds = Bounds {
                cwd: at.dir().as_fd(),
                read: &read,
                write: &write,
            };
            sandbox.command(self.program(binary), &bounds)?
        } else {
            let mut command = Command::new(binary);
            super::enter(&mut command, at.dir().as_raw_fd());
            command.env_clear();
            command
        };
        command.envs(self.env.iter().map(|(name, value)| (name, value)));
        command.env("TMPDIR", scratch);
        Ok(command)
    }
}

fn allowed(name: &OsStr) -> bool {
    name.to_str().is_some_and(|name| {
        ALLOWED.contains(&name) || ALLOWED_PREFIXES.iter().any(|prefix| name.starts_with(prefix))
    })
}

fn canonical(path: &Path) -> Result<PathBuf> {
    std::fs::canonicalize(path).with_context(|| format!("resolving {}", path.display()))
}

// What the host's git reads beside the guest's mounts, as it read it at
// connect: its programs, its templates, each configuration file and each
// file one includes, and its excludes and attributes files. A file the
// host's configuration starts including later is not among them.
#[derive(Default)]
pub struct Reach {
    // Opened afresh for each process, since an edit to a configuration file
    // renames a new one over it.
    paths: Vec<PathBuf>,
    // The git a bounded process runs in place of the client's binary.
    binary: Option<PathBuf>,
}

impl Client {
    pub(crate) async fn reach(&self) -> Result<Reach> {
        let mut reach = Reach::default();
        let paths = &mut reach.paths;

        // the host's git's own programs and templates
        let output = self.git(None, ["--exec-path"], &[]).await?;
        if output.status.success() {
            let programs = PathBuf::from(output.text().trim());
            if let Some(prefix) = programs.parent().and_then(Path::parent) {
                paths.push(prefix.join("share/git-core"));
            }
            #[cfg(target_os = "macos")]
            if let Some(git) = unshimmed(&self.binary, &programs) {
                paths.extend(programs.ancestors().nth(3).map(Path::to_path_buf));
                reach.binary = Some(git);
            }
            paths.push(programs);
        }

        // every configuration file, each one an entry names, and the
        // excludes and attributes files the configuration or git's defaults
        // name
        let output = self.git(None, ["config", "--list", "--show-origin", "-z"], &[]).await?;
        let home = env::var_os("HOME").map(PathBuf::from);
        let mut tokens = output.stdout.split(|&byte| byte == 0);
        while let (Some(origin), Some(entry)) = (tokens.next(), tokens.next()) {
            let Some(file) = origin.strip_prefix(b"file:").map(|file| PathBuf::from(os(file)))
            else {
                continue;
            };
            let (key, value) =
                entry.split_at(entry.iter().position(|&b| b == b'\n').unwrap_or(entry.len()));
            if let Some(value) = value.strip_prefix(b"\n").map(os) {
                let named = expand(&value, home.as_deref());
                if is_include(key) {
                    paths.extend(file.parent().map(|dir| dir.join(&named)));
                } else if names_file(key) && named.is_absolute() {
                    paths.push(named);
                }
            }
            paths.push(file);
        }
        let config = env::var_os("XDG_CONFIG_HOME")
            .filter(|dir| !dir.is_empty())
            .map(PathBuf::from)
            .or_else(|| home.as_deref().map(|home| home.join(".config")));
        if let Some(config) = config {
            paths.extend(["git/ignore", "git/attributes"].map(|file| config.join(file)));
        }

        paths.sort();
        paths.dedup();
        Ok(reach)
    }
}

// On macOS, `/usr/bin/git` is an `xcrun` shim, which picks a git by the
// environment it inherits. A bounded process runs the git it picked at
// connect, and reads the developer directory that git lies in.
#[cfg(target_os = "macos")]
fn unshimmed(binary: &str, programs: &Path) -> Option<PathBuf> {
    let found = if binary.contains('/') {
        PathBuf::from(binary)
    } else {
        env::split_paths(&env::var_os("PATH")?)
            .map(|dir| dir.join(binary))
            .find(|path| path.is_file())?
    };
    let git = programs.join("git");
    (std::fs::canonicalize(found).ok()? == Path::new("/usr/bin/git") && git.is_file())
        .then_some(git)
}

// An entry naming a file git reads as configuration, relative to the file
// that names it.
fn is_include(key: &[u8]) -> bool {
    key == b"include.path" || (key.starts_with(b"includeif.") && key.ends_with(b".path"))
}

// An entry naming a file git reads beside its configuration, relative to
// the directory git works in.
fn names_file(key: &[u8]) -> bool {
    key == b"core.excludesfile" || key == b"core.attributesfile"
}

// A path as git expands one it reads from configuration: a leading `~/` the
// home.
fn expand(value: &OsStr, home: Option<&Path>) -> PathBuf {
    match (value.as_bytes().strip_prefix(b"~/"), home) {
        (Some(rest), Some(home)) => home.join(os(rest)),
        _ => PathBuf::from(value),
    }
}

fn os(bytes: &[u8]) -> OsString {
    OsStr::from_bytes(bytes).to_owned()
}
