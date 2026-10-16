#![doc = include_str!("../README.md")]

mod chamber;
mod child;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod seatbelt;
pub mod valve;

use std::ffi::{OsStr, OsString};
use std::path::{self, Path, PathBuf};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::sync::Arc;
use std::{fmt, io};

use anyhow::{Context as _, Result, bail};

pub use crate::chamber::{Chamber, Compartment};
pub use crate::child::{Child, Exit};
use crate::valve::Valve;

/// What every confined process may reach, as plain data.
///
/// A backend fills it from its own configuration; nothing here names an
/// environment variable of the crate's own. What one process asks beyond
/// it is its [`Clearance`].
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Trees a lend at or beneath which runs with a shell.
    pub shell_roots: Vec<PathBuf>,
    /// Host environment variables copied into a confined process, by name.
    pub passthrough: Vec<String>,
    /// Environment variable names that must never reach a confined process.
    pub refused: Vec<String>,
}

/// What one process may reach beyond the boundary's defaults.
///
/// Sealed with the process by [`Airlock::seal`], which holds each entry to the
/// host before the process runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Clearance {
    /// Hosts the process may reach through its egress proxy.
    ///
    /// A name matches whole, `*.suffix` matches the suffix and every name
    /// beneath it, and an IP literal matches itself. Every address, resolved
    /// or literal, is held to [`valve::Refusal`] before the tunnel opens,
    /// a loopback literal alone excepted.
    pub egress: Vec<String>,
    /// Trees the process may read beside the system's own, each spelled
    /// `~/<path>`: a directory beneath the operator's home, apart from the
    /// credential stores [`Airlock::seal`] denies whatever the clearance names.
    pub read: Vec<String>,
    /// Environment variables set for the process, a value's leading `~/`
    /// spelling the operator's home.
    pub env: Vec<(String, String)>,
}

/// A [`Clearance`] the host denies: an entry that breaks a rule of the seal.
///
/// [`Airlock::seal`] fails with one of these, downcastable from its error,
/// where the clearance itself is at fault, and with a plain error where the
/// host failed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denied(String);

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Denied {}

macro_rules! deny {
    ($($arg:tt)*) => {
        return Err(anyhow::Error::new(Denied(format!($($arg)*))))
    };
}

// Kernel wall every laid boundary shares: Landlock and seccomp on Linux,
// Seatbelt on macOS.
#[derive(Debug, Clone)]
pub(crate) enum Wall {
    #[cfg(target_os = "linux")]
    Landlock(Arc<linux::Linux>),
    #[cfg(target_os = "macos")]
    Seatbelt(Arc<seatbelt::Seatbelt>),
}

impl Wall {
    async fn platform() -> Result<Self> {
        cfg_select! {
            target_os = "linux" => Ok(Self::Landlock(Arc::new(linux::Linux::probe().await?))),
            target_os = "macos" => Ok(Self::Seatbelt(Arc::new(seatbelt::Seatbelt::probe().await?))),
            _ => bail!("no chamber substrate runs on this platform"),
        }
    }
}

/// What a connected backend connects once and lays around every process it spawns.
///
/// Connecting holds the [`Options`] to the host once: the roots are
/// resolved, the `passthrough` is checked, the kernel substrate is probed,
/// the operator's home is read, and the environment a process inherits is
/// read. [`Airlock::seal`] then holds each process's [`Clearance`] to the host.
pub struct Airlock {
    options: Options,
    inherited: Vec<(OsString, OsString)>,
    wall: Wall,
    home: Option<PathBuf>,
}

impl Airlock {
    // The repository selection a shell exports, which would point every git
    // the model runs at the operator's own repository.
    const GIT_SELECTION: [&str; 4] =
        ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];
    // What every confined process inherits from the host, beside `LC_*`.
    const INHERITED: [&str; 4] = ["PATH", "LANG", "TZ", "TERM"];
    // The dynamic loader's variables, by prefix: each maps code of an
    // ask's into the program before it runs.
    const LOADER: [&str; 2] = ["LD_", "DYLD_"];
    // What a confined process reaches without the proxy: the host's own
    // loopback, where the callback endpoint and the proxy itself listen.
    // Bun matches an entry against the host as the URL spells it, so the
    // IPv6 loopback is listed bracketed as well as bare.
    const NO_PROXY: &str = "127.0.0.1,localhost,::1,[::1]";
    // What the boundary sets itself, so no `passthrough` or ask puts
    // another value over it.
    const OWN: [&str; 10] = [
        "HOME",
        "TMPDIR",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    ];
    // What never reaches a confined process by the crate's own rule: the
    // trust-store switch, and the option lists Bun and Node read at
    // startup, which would run code of an ask's inside the program.
    const REFUSED: [&str; 3] = ["NODE_USE_SYSTEM_CA", "BUN_OPTIONS", "NODE_OPTIONS"];
    // What no ask reads of the home by the crate's own rule: the stores the
    // operator's credentials live in, beneath which no toolchain is
    // installed, each spelled `~/`-relative.
    const REFUSED_ROOTS: [&str; 10] = [
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

    /// Connects: holds `options` to the host and reads what a process inherits.
    ///
    /// `shell_roots` are made absolute against the current directory, so a
    /// lend is compared with them however they were spelled. The operator's
    /// `HOME` is read here, canonical, as what an [`Clearance`]'s `~/` spells.
    ///
    /// # Errors
    ///
    /// Returns an error when a `passthrough` names a variable the boundary
    /// sets itself, one in `refused`, one of the dynamic loader's, or a
    /// `GIT_DIR` sibling, or when no kernel substrate runs on this host.
    pub async fn connect(mut options: Options) -> Result<Self> {
        for name in &options.passthrough {
            if let Err(why) = Self::settable(name, &options.refused) {
                bail!("passthrough {why}");
            }
        }

        // the roots resolved once
        for root in &mut options.shell_roots {
            *root = path::absolute(&*root)
                .with_context(|| format!("resolving shell root {}", root.display()))?;
        }

        let wall = Wall::platform().await?;
        let home = match std::env::var_os("HOME") {
            Some(home) => tokio::fs::canonicalize(home).await.ok(),
            None => None,
        };
        let inherited = std::env::vars_os()
            .filter(|(name, _)| Self::passes(name, &options.passthrough))
            .collect();
        Ok(Self {
            options,
            inherited,
            wall,
            home,
        })
    }

    fn passes(name: &OsStr, passthrough: &[String]) -> bool {
        name.to_str().is_some_and(|name| {
            Self::INHERITED.contains(&name)
                || name.starts_with("LC_")
                || passthrough.iter().any(|passed| passed == name)
        })
    }

    // Whether a value under `name` may reach a process, whoever puts it
    // there; the `Err` is why not.
    fn settable(name: &str, refused: &[String]) -> Result<(), String> {
        if Self::OWN.contains(&name) {
            return Err(format!("`{name}` is the boundary's to set"));
        }
        if Self::REFUSED.contains(&name) || refused.iter().any(|refused| refused == name) {
            return Err(format!("`{name}` is refused"));
        }
        if Self::LOADER.iter().any(|prefix| name.starts_with(prefix)) {
            return Err(format!("`{name}` is the dynamic loader's"));
        }
        if Self::GIT_SELECTION.contains(&name) {
            return Err(format!("`{name}` selects a repository"));
        }
        Ok(())
    }

    /// The options as connected: absolute `shell_roots`.
    #[must_use]
    pub const fn options(&self) -> &Options {
        &self.options
    }

    /// Puts a process at `lend`, or in a private directory when nothing is lent.
    ///
    /// A lent tree is created and canonicalised; the process runs with a
    /// shell when the tree is at or beneath one of the connected
    /// `shell_roots`. The scratch directory, which serves the process as its
    /// `HOME` and `TMPDIR`, is removed once the process has exited. The
    /// [`Chamber`] is sealed into a [`Compartment`] by [`Airlock::seal`].
    ///
    /// # Errors
    ///
    /// Returns an error when the lent tree or the scratch cannot be created.
    pub async fn prepare(&self, lend: Option<&Path>) -> Result<Chamber> {
        Chamber::prepare(lend, &self.options).await
    }

    /// Seals a compartment around one process, where `chamber` put it,
    /// granting what `clearance` names.
    ///
    /// The process's environment is what connecting inherited, its `HOME`
    /// and `TMPDIR` the chamber's scratch, its proxy variables an egress
    /// proxy bound for it alone and admitting the clearance's `egress`, with
    /// the host's own loopback excepted, and the clearance's `env` over them.
    /// Each of the clearance's `read` roots is resolved against the operator's
    /// home; one that does not exist is logged at WARN and granted nothing. A
    /// root that overlaps a credential store of the home, at it, beneath it, or
    /// above it, as spelled or as either resolves, is denied whatever the
    /// clearance: `~/.ssh`, `~/.gnupg`, `~/.aws`, `~/.azure`, `~/.kube`,
    /// `~/.docker`, `~/.config/gh`, `~/.config/gcloud`, `~/.password-store`,
    /// and `~/Library/Keychains`.
    ///
    /// # Errors
    ///
    /// Returns a [`Denied`] when a `read` root is not spelled `~/<path>`,
    /// overlaps a credential store, resolves outside the home or to a
    /// file, or names a home the host has none of, or when an `env` name is
    /// not a variable name or is one the
    /// compartment holds (its own, a refused one, the dynamic loader's, a
    /// `GIT_DIR` sibling, or a `passthrough` of the host's); and a plain
    /// error when a root cannot be read or the proxy cannot be bound.
    pub async fn seal(&self, chamber: Chamber, clearance: &Clearance) -> Result<Compartment> {
        let read = self.roots(&clearance.read).await?;
        let valve = Valve::bind(&clearance.egress).await?;
        let env = self.environment(&chamber, &valve, &clearance.env)?;
        Ok(Compartment::new(chamber, env, self.path(), self.wall.clone(), read, valve))
    }

    // The host's `PATH` as connecting read it: what a bare program the
    // backend names resolves against, whatever an ask sets.
    fn path(&self) -> OsString {
        self.inherited
            .iter()
            .find(|(name, _)| name == "PATH")
            .map(|(_, value)| value.clone())
            .unwrap_or_default()
    }

    async fn roots(&self, read: &[String]) -> Result<Vec<PathBuf>> {
        let mut roots = Vec::new();
        for entry in read {
            let Some(rest) = entry.strip_prefix("~/").filter(|rest| !rest.is_empty()) else {
                deny!(
                    "read root `{entry}` must be a directory beneath the home, spelled `~/<path>`"
                );
            };
            if Self::REFUSED_ROOTS.iter().any(|store| overlaps(Path::new(rest), Path::new(store))) {
                deny!("read root `{entry}` is a credential store");
            }
            let home = self.home(entry)?;
            let asked = home.join(rest);
            let root = match tokio::fs::canonicalize(&asked).await {
                Ok(root) => root,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    tracing::warn!(root = %asked.display(), "read root does not exist; not granted");
                    continue;
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("resolving read root {}", asked.display()));
                }
            };
            if root == home || !root.starts_with(home) {
                deny!("read root `{entry}` resolves to {}, outside the home", root.display());
            }
            if Self::overlaps_store(home, &root).await {
                deny!("read root `{entry}` resolves to {}, a credential store", root.display());
            }
            let metadata = tokio::fs::metadata(&root)
                .await
                .with_context(|| format!("reading read root {}", root.display()))?;
            if !metadata.is_dir() {
                deny!("read root `{entry}` is not a directory");
            }
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
        Ok(roots)
    }

    // Whether `root`, canonical, overlaps a credential store of `home` as
    // the store is spelled or as it resolves: a store a symlink stands for
    // is reached through its target as through its name.
    async fn overlaps_store(home: &Path, root: &Path) -> bool {
        for store in Self::REFUSED_ROOTS {
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

    fn environment(
        &self, chamber: &Chamber, bound: &Valve, asked: &[(String, String)],
    ) -> Result<Vec<(OsString, OsString)>> {
        let scratch = chamber.scratch().as_os_str().to_owned();
        let proxy = OsString::from(format!("http://{}", bound.address()));
        let mut env = self.inherited.clone();
        env.extend(Self::OWN.iter().map(|&name| {
            let value = match name {
                "HOME" | "TMPDIR" => scratch.clone(),
                "NO_PROXY" | "no_proxy" => OsString::from(Self::NO_PROXY),
                _ => proxy.clone(),
            };
            (OsString::from(name), value)
        }));

        // the ask's variables over them, each held as a passthrough is, and
        // never over a passthrough: those are the program's own settings,
        // read of the host
        for (name, value) in asked {
            if !is_name(name) {
                deny!("`{name}` is not an environment variable name");
            }
            if let Err(why) = Self::settable(name, &self.options.refused) {
                deny!("env {why}");
            }
            if self.options.passthrough.iter().any(|passed| passed == name) {
                deny!("env `{name}` is the host's to pass through");
            }
            let value = match value.strip_prefix("~/") {
                Some(rest) => self.home(value)?.join(rest).into_os_string(),
                None => OsString::from(value),
            };
            env.retain(|(held, _)| held != name.as_str());
            env.push((OsString::from(name), value));
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

impl fmt::Debug for Airlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inherited: Vec<&OsStr> =
            self.inherited.iter().map(|(name, _)| name.as_os_str()).collect();
        f.debug_struct("Airlock")
            .field("options", &self.options)
            .field("inherited", &inherited)
            .field("wall", &self.wall)
            .field("home", &self.home)
            .finish()
    }
}

// Whether one path is at or beneath the other, by component: a grant of
// a tree is a grant of every store within it, so a root above a store is
// as refused as one beneath it.
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

// `[A-Za-z_][A-Za-z0-9_]*`: what a shell exports and `Command::env` accepts.
fn is_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}
