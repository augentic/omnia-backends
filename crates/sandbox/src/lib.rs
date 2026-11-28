#![doc = include_str!("../README.md")]

mod child;
mod proxy;
mod session;

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::net::SocketAddr;
use std::os::fd::BorrowedFd;
use std::path::{self, Path, PathBuf};
use std::sync::Arc;
use std::{env, fmt, io};

use anyhow::{Context as _, Result, bail};

pub use crate::child::{Child, Exit, TimedOut};
pub use crate::session::{Access, Command, Session};

// The kernel boundary around a process: Landlock and seccomp on Linux,
// Seatbelt on macOS.
cfg_select! {
    target_os = "linux" => {
        mod linux;
        use linux::Linux as Kernel;
    }
    target_os = "macos" => {
        mod seatbelt;
        use seatbelt::Seatbelt as Kernel;
    }
    _ => {
        compile_error!("omnia-sandbox only runs on Linux or macOS");
    }
}

macro_rules! deny {
    ($($arg:tt)*) => {
        return Err(anyhow::Error::new(Denied(format!($($arg)*))))
    };
}

// Host variables every process inherits, beside `LC_*` and the backend's `inherit`.
const INHERITED: &[&str] = &["PATH", "LANG", "TZ", "TERM"];
// Set to the process's own scratch directory, removed once it has exited.
const SCRATCH: &[&str] = &["HOME", "TMPDIR"];
// Set to the egress proxy bound for the process alone.
const PROXY: &[&str] =
    &["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"];
const NO_PROXY: &[&str] = &["NO_PROXY", "no_proxy"];
// Bun matches an entry as the URL spells the host, so the IPv6 loopback is listed bracketed too.
const LOOPBACK: &str = "127.0.0.1,localhost,::1,[::1]";
// Each lets a grant run code inside the program, or switch its trust store.
const REFUSED: &[&str] = &["NODE_USE_SYSTEM_CA", "BUN_OPTIONS", "NODE_OPTIONS"];
const LOADER: &[&str] = &["LD_", "DYLD_"];
// Exported by a shell, each would point every git the model runs at the operator's repository.
const GIT: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];
// Where the operator's credentials live, relative to the home, denied whatever the grant.
const STORES: &[&str] = &[
    ".ssh",
    ".gnupg",
    ".aws",
    ".azure",
    ".kube",
    ".docker",
    ".config/gh",
    ".config/gcloud",
    ".password-store",
    "Library/Keychains",
];

/// The host's sandbox policy, connected once and applied to every process a
/// backend spawns.
///
/// [`Sandbox::connect`] validates the [`Options`] against the host and reads
/// what a process inherits. [`Sandbox::prepare`] then validates one
/// session's [`Grant`] and prepares the [`Session`] its processes run in.
/// [`Sandbox::command`] instead holds one process to [`Bounds`], with no
/// session and no network.
pub struct Sandbox {
    options: Options,
    inherited: BTreeMap<OsString, OsString>,
    kernel: Arc<Kernel>,
    home: Option<PathBuf>,
}

impl Sandbox {
    /// Connects to the host: validates `options` and reads what a process
    /// inherits.
    ///
    /// - Each of `shell_roots` is resolved: canonical where the directory
    ///   exists, absolute otherwise.
    /// - The kernel boundary is probed.
    /// - The operator's `HOME` is read, canonical, as what a [`Grant`]'s
    ///   `~/` spells.
    /// - The host's environment is read, keeping the variables a process
    ///   inherits.
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - `inherit` names a variable the sandbox sets itself, one in
    ///   `refused`, one the dynamic loader reads, or a `GIT_DIR` sibling
    /// - the host cannot enforce a kernel boundary
    pub async fn connect(mut options: Options) -> Result<Self> {
        // check options
        for name in options.inherit {
            if let Some(why) = reserved(name, options.refused) {
                bail!("inherit `{name}` {why}");
            }
        }
        for root in &mut options.shell_roots {
            let absolute = path::absolute(&*root)
                .with_context(|| format!("resolving shell root {}", root.display()))?;
            *root = tokio::fs::canonicalize(&absolute).await.unwrap_or(absolute);
        }

        let inherited =
            env::vars_os().filter(|(name, _)| inherits(name, options.inherit)).collect();
        let home = async { tokio::fs::canonicalize(env::var_os("HOME")?).await.ok() }.await;

        Ok(Self {
            options,
            inherited,
            kernel: Arc::new(Kernel::probe().await?),
            home,
        })
    }

    /// The options as connected: resolved `shell_roots`.
    #[must_use]
    pub const fn options(&self) -> &Options {
        &self.options
    }

    /// Prepares the [`Session`] a completion's processes run in: their
    /// working directory, their environment, and what `grant` opens to them.
    ///
    /// The processes work in the directory `lend` holds open, or in a private
    /// directory when none is given:
    ///
    /// - A lend is placed where its directory stands as the session is
    ///   prepared, read from the descriptor and never from a path.
    /// - A lend at or beneath one of the connected `shell_roots` runs with
    ///   [`Access::Shell`], any other lend with [`Access::Read`], and a
    ///   private directory with [`Access::Private`].
    /// - The processes write a lend only where it is
    ///   [`writable`](Lend::writable), and a private directory always.
    ///
    /// Their environment is built from nothing:
    ///
    /// - the host's variables, as read at connect
    /// - `HOME` and `TMPDIR`, a scratch directory of its own, removed once
    ///   it has exited
    /// - the proxy variables, naming an egress proxy bound for it alone that
    ///   admits the grant's `egress`
    /// - `NO_PROXY`, excepting the host's own loopback
    /// - the grant's `env`, over all of the above
    ///
    /// Each `read` root is resolved against the operator's home. One that
    /// does not exist is logged at WARN and granted nothing. One that
    /// overlaps a credential store is denied whatever the grant: at it,
    /// beneath it, or above it, as spelled or as either resolves. The stores
    /// are `~/.ssh`, `~/.gnupg`, `~/.aws`, `~/.azure`, `~/.kube`,
    /// `~/.docker`, `~/.config/gh`, `~/.config/gcloud`, `~/.password-store`,
    /// and `~/Library/Keychains`.
    ///
    /// # Errors
    ///
    /// Returns a [`Denied`] where the grant is at fault:
    ///
    /// - a `read` root not spelled `~/<path>`, stepping through `.` or
    ///   `..`, overlapping a credential store, or resolving outside the
    ///   home or to a file
    /// - an `env` name that is not a variable name, or one the sandbox
    ///   reserves: its own, a refused one, the dynamic loader's, a
    ///   `GIT_DIR` sibling, or one in `inherit`
    /// - a `~/` where the host has no home
    ///
    /// Returns a plain error where the host failed:
    ///
    /// - the lend cannot be held or placed
    /// - the scratch cannot be created
    /// - a `read` root cannot be read
    /// - the proxy cannot be bound
    pub async fn prepare(&self, lend: Option<Lend<'_>>, grant: &Grant) -> Result<Session> {
        Session::prepare(self, lend, grant).await
    }

    /// A command running `program` held to `bounds`, with an empty
    /// environment.
    ///
    /// The process works in `bounds.cwd`, entered by its descriptor before
    /// `program` runs. The kernel opens to it the system, the program, and
    /// the roots of `bounds`, each held to what its descriptor opened
    /// whatever its path reads as later. Beside a [`Session`]'s process:
    ///
    /// - it reaches no network, loopback and unix sockets included
    /// - nothing beneath a root is closed to it, `.git` and `.emery`
    ///   included
    /// - it inherits nothing of the host, so its caller sets its whole
    ///   environment
    ///
    /// A bare `program` is resolved against the host's `PATH` as connecting
    /// read it.
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - a bare `program` is not on the host's `PATH`
    /// - the kernel boundary cannot be laid over `bounds`
    pub fn command(
        &self, program: impl AsRef<Path>, bounds: &Bounds<'_>,
    ) -> Result<tokio::process::Command> {
        let program = session::resolve(program.as_ref(), &self.host_path())?;
        let mut command = self.kernel.command(&session::Place {
            dir: bounds.cwd,
            program: &program,
            read: bounds.read.iter().copied().map(session::Root::Open).collect(),
            write: bounds.write.iter().copied().map(session::Root::Open).collect(),
            session: false,
            network: false,
        })?;
        command.env_clear();
        Ok(command)
    }

    /// The [`Access`] a session over `lend` would run with, before any is
    /// prepared.
    ///
    /// The lend is placed as [`Sandbox::prepare`] places it: at or beneath
    /// one of the connected `shell_roots` it is [`Access::Shell`], elsewhere
    /// [`Access::Read`], and none is [`Access::Private`].
    ///
    /// # Errors
    ///
    /// Returns an error when the lend cannot be placed.
    pub async fn access(&self, lend: Option<Lend<'_>>) -> Result<Access> {
        let Some(lend) = lend else {
            return Ok(Access::Private);
        };
        let cwd = Kernel::held_path(lend.dir).context("placing the lent directory")?;
        Ok(if self.under_shell_root(&cwd).await { Access::Shell } else { Access::Read })
    }

    // A shell root is resolved again here: one that did not exist at connect
    // may now.
    async fn under_shell_root(&self, path: &Path) -> bool {
        for root in &self.options.shell_roots {
            let root = tokio::fs::canonicalize(root).await.unwrap_or_else(|_| root.clone());
            if path.starts_with(root) {
                return true;
            }
        }
        false
    }

    pub(crate) const fn kernel(&self) -> &Arc<Kernel> {
        &self.kernel
    }

    // A bare program resolves against the host's `PATH`, never one the grant set.
    pub(crate) fn host_path(&self) -> OsString {
        self.inherited.get(OsStr::new("PATH")).cloned().unwrap_or_default()
    }

    pub(crate) async fn read_roots(&self, read: &[String]) -> Result<Vec<PathBuf>> {
        let mut roots = Vec::new();
        for entry in read {
            if let Some(root) = self.read_root(entry).await?
                && !roots.contains(&root)
            {
                roots.push(root);
            }
        }
        Ok(roots)
    }

    // `None` for a root the home lacks, which is granted nothing rather than denied.
    async fn read_root(&self, entry: &str) -> Result<Option<PathBuf>> {
        // as spelled
        let Some(rest) = entry.strip_prefix("~/").filter(|rest| !rest.is_empty()) else {
            deny!("read root `{entry}` must be a directory beneath the home, `~/<path>`");
        };
        if rest.split('/').any(|segment| matches!(segment, "." | "..")) {
            deny!("read root `{entry}` must not step through `.` or `..`");
        }
        if STORES.iter().any(|store| overlaps(Path::new(rest), Path::new(store))) {
            deny!("read root `{entry}` is a credential store");
        }

        // as it resolves
        let home = self.home(entry)?;
        let asked = home.join(rest);
        let root = match tokio::fs::canonicalize(&asked).await {
            Ok(root) => root,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                tracing::warn!(root = %asked.display(), "read root does not exist; not granted");
                return Ok(None);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("resolving read root {}", asked.display()));
            }
        };

        // outside the home, or a store
        if root == home || !root.starts_with(home) {
            deny!("read root `{entry}` resolves to {}, outside the home", root.display());
        }
        if overlaps_store(home, &root).await {
            deny!("read root `{entry}` resolves to {}, a credential store", root.display());
        }

        // a directory
        if !tokio::fs::metadata(&root)
            .await
            .with_context(|| format!("reading read root {}", root.display()))?
            .is_dir()
        {
            deny!("read root `{entry}` is not a directory");
        }
        Ok(Some(root))
    }

    pub(crate) fn environment(
        &self, scratch: &Path, proxy: SocketAddr, granted: &[(String, String)],
    ) -> Result<BTreeMap<OsString, OsString>> {
        let scratch = scratch.as_os_str();
        let proxy = OsString::from(format!("http://{proxy}"));

        let mut env = self.inherited.clone();
        env.extend(SCRATCH.iter().map(|&name| (name.into(), scratch.to_owned())));
        env.extend(PROXY.iter().map(|&name| (name.into(), proxy.clone())));
        env.extend(NO_PROXY.iter().map(|&name| (name.into(), LOOPBACK.into())));

        // the grant's variables over them, never over the backend's `inherit`:
        // those are the program's own settings, read of the host
        for (name, value) in granted {
            if !is_name(name) {
                deny!("`{name}` is not an environment variable name");
            }
            if let Some(why) = reserved(name, self.options.refused) {
                deny!("env `{name}` {why}");
            }
            if self.options.inherit.contains(&name.as_str()) {
                deny!("env `{name}` is inherited from the host");
            }
            let value = match value.strip_prefix("~/") {
                Some(rest) => self.home(value)?.join(rest).into_os_string(),
                None => OsString::from(value),
            };
            env.insert(name.into(), value);
        }

        Ok(env)
    }

    fn home(&self, spelled: &str) -> Result<&Path> {
        match self.home.as_deref() {
            Some(home) => Ok(home),
            None => deny!("`{spelled}` names the home, which the host has none of"),
        }
    }
}

impl fmt::Debug for Sandbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sandbox")
            .field("options", &self.options)
            .field("inherited", &self.inherited.keys())
            .field("kernel", &self.kernel)
            .field("home", &self.home)
            .finish()
    }
}

/// What every sandboxed process may reach, as plain data.
///
/// A backend fills it from its own configuration. What one process needs
/// beyond it is its [`Grant`].
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Directories a [`Lend`] at or beneath which runs with
    /// [`Access::Shell`].
    ///
    /// Resolved at connect: canonical where the directory exists, absolute
    /// otherwise.
    pub shell_roots: Vec<PathBuf>,
    /// Host environment variables a sandboxed process inherits, by name.
    pub inherit: &'static [&'static str],
    /// Environment variable names that must never reach a sandboxed process.
    pub refused: &'static [&'static str],
}

/// What one process may reach beyond the sandbox's defaults.
///
/// Validated against the host by [`Sandbox::prepare`] before the process
/// runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Grant {
    /// Hosts the process may reach through its egress proxy.
    ///
    /// - A name matches whole.
    /// - `*.suffix` matches the suffix and every name beneath it.
    /// - An IP literal matches itself.
    ///
    /// Every address is vetted before the tunnel opens, a listed name once
    /// resolved and a listed literal as written: the unspecified, loopback,
    /// link-local, multicast and broadcast ranges, the cloud metadata
    /// addresses, and the host's own addresses are refused. A loopback
    /// literal alone is connected as written.
    pub egress: Vec<String>,
    /// Directories the process may read beside the system's own.
    ///
    /// Each is spelled `~/<path>`, a directory beneath the operator's home.
    /// The credential stores [`Sandbox::prepare`] lists are denied whatever
    /// the grant names.
    pub read: Vec<String>,
    /// Environment variables set for the process.
    ///
    /// A value's leading `~/` spells the operator's home.
    pub env: Vec<(String, String)>,
}

/// The workspace a backend lends a [`Session`]: a directory its caller holds
/// open, and whether the session's processes may write it.
///
/// The descriptor is the authority. The processes work in the directory it
/// opened, whatever its path reads as later, so a link laid where the
/// directory stood leads no process anywhere.
#[derive(Debug, Clone, Copy)]
pub struct Lend<'a> {
    /// The directory the processes work in.
    pub dir: BorrowedFd<'a>,
    /// Whether the processes may write the directory, or only read it.
    pub writable: bool,
}

/// What one process [`Sandbox::command`] runs may reach: where it works, and
/// what it reads and writes.
///
/// Each is a descriptor its caller holds open, so the boundary names the
/// file or directory that was opened, never a path a process could
/// redirect meanwhile. A root may be a directory, granting all beneath it,
/// or a file alone.
#[derive(Debug, Clone, Copy)]
pub struct Bounds<'a> {
    /// The directory the process works in, which grants it nothing.
    pub cwd: BorrowedFd<'a>,
    /// Files and directories the process reads and executes, beside the
    /// system's own.
    pub read: &'a [BorrowedFd<'a>],
    /// Files and directories the process reads, writes, and executes.
    pub write: &'a [BorrowedFd<'a>],
}

/// A [`Grant`] the sandbox refuses: an entry that breaks a rule of
/// [`Sandbox::prepare`].
///
/// [`Sandbox::prepare`] fails with one of these, downcastable from its
/// error, where the grant itself is at fault. Where the host failed, it
/// fails with a plain error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denied(String);

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Denied {}

// Why a name is neither the backend's to inherit nor a grant's to set.
fn reserved(name: &str, refused: &[&str]) -> Option<&'static str> {
    if [SCRATCH, PROXY, NO_PROXY].iter().any(|own| own.contains(&name)) {
        Some("is set by the sandbox")
    } else if REFUSED.contains(&name) || refused.contains(&name) {
        Some("is refused")
    } else if LOADER.iter().any(|prefix| name.starts_with(prefix)) {
        Some("is read by the dynamic loader")
    } else if GIT.contains(&name) {
        Some("selects a repository")
    } else {
        None
    }
}

fn inherits(name: &OsStr, inherit: &[&str]) -> bool {
    name.to_str().is_some_and(|name| {
        INHERITED.contains(&name) || name.starts_with("LC_") || inherit.contains(&name)
    })
}

// The spelling may hide a store behind a link, so the canonical root is
// held to each store as spelled and as it resolves.
async fn overlaps_store(home: &Path, root: &Path) -> bool {
    for store in STORES {
        let spelled = home.join(store);
        if overlaps(root, &spelled) {
            return true;
        }
        if let Ok(resolved) = tokio::fs::canonicalize(&spelled).await
            && overlaps(root, &resolved)
        {
            return true;
        }
    }

    false
}

// A grant of a directory grants every store within it, so a root above a
// store is as refused as one beneath it.
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

// `[A-Za-z_][A-Za-z0-9_]*`: what a shell exports and `Command::env` accepts.
fn is_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}
