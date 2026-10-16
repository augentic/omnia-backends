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
use std::sync::Arc;
use std::{env, fmt, io};

use anyhow::{Context as _, Result, bail};

use crate::chamber::Chamber;
pub use crate::chamber::Compartment;
pub use crate::child::{Child, Exit};
use crate::valve::Valve;

macro_rules! deny {
    ($($arg:tt)*) => {
        return Err(anyhow::Error::new(Denied(format!($($arg)*))))
    };
}

// Inherited by every confined process, beside `LC_*` and the backend's `inherit`.
const INHERITED: &[&str] = &["PATH", "LANG", "TZ", "TERM"];
// Set to a scratch directory of the process's own, removed once it has exited.
const SCRATCH: &[&str] = &["HOME", "TMPDIR"];
// Set to the egress proxy bound for the process alone, its one way out.
const PROXY: &[&str] =
    &["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"];
const NO_PROXY: &[&str] = &["NO_PROXY", "no_proxy"];
// Bun matches an entry as the URL spells the host, so the IPv6 loopback is listed bracketed too.
const LOOPBACK: &str = "127.0.0.1,localhost,::1,[::1]";
// Each lets an ask run code inside the program, or switch its trust store.
const REFUSED: &[&str] = &["NODE_USE_SYSTEM_CA", "BUN_OPTIONS", "NODE_OPTIONS"];
const LOADER: &[&str] = &["LD_", "DYLD_"];
// Exported by a shell, each would point every git the model runs at the operator's repository.
const GIT: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];
// The stores the operator's credentials live in, `~/`-relative, denied whatever the clearance.
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

/// What a backend connects once and lays around every process it spawns.
///
/// [`Airlock::connect`] holds the [`Options`] to the host and reads what a
/// process inherits. [`Airlock::seal`] then holds each process's
/// [`Clearance`] to the host and seals a [`Compartment`] around it.
pub struct Airlock {
    options: Options,
    inherited: Vec<(OsString, OsString)>,
    wall: Wall,
    home: Option<PathBuf>,
}

impl Airlock {
    /// Connects: holds `options` to the host and reads what a process inherits.
    ///
    /// - `shell_roots` are made absolute against the current directory, so a
    ///   lend is compared with them however they were spelled.
    /// - The kernel substrate is probed.
    /// - The operator's `HOME` is read, canonical, as what a [`Clearance`]'s
    ///   `~/` spells.
    /// - The host's environment is read, keeping what a process inherits.
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - `inherit` names a variable the compartment sets itself, one in
    ///   `refused`, one of the dynamic loader's, or a `GIT_DIR` sibling
    /// - no kernel substrate runs on this host
    pub async fn connect(mut options: Options) -> Result<Self> {
        // check options
        for name in options.inherit {
            if let Some(why) = reserved(name, options.refused) {
                bail!("inherit `{name}` {why}");
            }
        }
        for root in &mut options.shell_roots {
            *root = path::absolute(&*root)
                .with_context(|| format!("resolving shell root {}", root.display()))?;
        }

        let inherited =
            env::vars_os().filter(|(name, _)| inherits(name, options.inherit)).collect();
        let home = match env::var_os("HOME") {
            Some(home) => tokio::fs::canonicalize(home).await.ok(),
            None => None,
        };

        Ok(Self {
            options,
            inherited,
            wall: Wall::platform().await?,
            home,
        })
    }

    /// The options as connected: absolute `shell_roots`.
    #[must_use]
    pub const fn options(&self) -> &Options {
        &self.options
    }

    /// Seals a compartment around one process at `lend`, granting what
    /// `clearance` names.
    ///
    /// The process works in `lend`, created and canonicalised, or in a
    /// private directory when nothing is lent. A lend at or beneath one of
    /// the connected `shell_roots` runs with a shell.
    ///
    /// Its environment is built from nothing:
    ///
    /// - the host's variables, as read at connect
    /// - `HOME` and `TMPDIR`, a scratch directory of its own, removed once
    ///   it has exited
    /// - the proxy variables, naming an egress proxy bound for it alone that
    ///   admits the clearance's `egress`
    /// - `NO_PROXY`, excepting the host's own loopback
    /// - the clearance's `env`, over all of the above
    ///
    /// Each `read` root is resolved against the operator's home. One that
    /// does not exist is logged at WARN and granted nothing. One that
    /// overlaps a credential store is denied whatever the clearance: at it,
    /// beneath it, or above it, as spelled or as either resolves. The stores
    /// are `~/.ssh`, `~/.gnupg`, `~/.aws`, `~/.azure`, `~/.kube`,
    /// `~/.docker`, `~/.config/gh`, `~/.config/gcloud`, `~/.password-store`,
    /// and `~/Library/Keychains`.
    ///
    /// # Errors
    ///
    /// Returns a [`Denied`] where the clearance is at fault:
    ///
    /// - a `read` root not spelled `~/<path>`, overlapping a credential
    ///   store, or resolving outside the home or to a file
    /// - an `env` name that is not a variable name, or one the compartment
    ///   holds: its own, a refused one, the dynamic loader's, a `GIT_DIR`
    ///   sibling, or one in `inherit`
    /// - a `~/` where the host has no home
    ///
    /// Returns a plain error where the host failed:
    ///
    /// - the lent tree or the scratch cannot be created
    /// - a `read` root cannot be read
    /// - the proxy cannot be bound
    pub async fn seal(&self, lend: Option<&Path>, clearance: &Clearance) -> Result<Compartment> {
        let chamber = Chamber::prepare(lend, &self.options).await?;
        let read = self.roots(&clearance.read).await?;
        let valve = Valve::bind(&clearance.egress).await?;
        let env = self.environment(&chamber, &valve, &clearance.env)?;
        Ok(Compartment::new(chamber, env, self.path(), self.wall.clone(), read, valve))
    }

    // A bare program resolves against the host's `PATH`, never one the ask set.
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
            if let Some(root) = self.root(entry).await?
                && !roots.contains(&root)
            {
                roots.push(root);
            }
        }
        Ok(roots)
    }

    // `None` for a root the home lacks, which is granted nothing rather than denied.
    async fn root(&self, entry: &str) -> Result<Option<PathBuf>> {
        // as spelled
        let Some(rest) = entry.strip_prefix("~/").filter(|rest| !rest.is_empty()) else {
            deny!("read root `{entry}` must be a directory beneath the home, `~/<path>`");
        };
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

        // check if the root is outside the home or a store beneath it
        if root == home || !root.starts_with(home) {
            deny!("read root `{entry}` resolves to {}, outside the home", root.display());
        }
        if overlaps_store(home, &root).await {
            deny!("read root `{entry}` resolves to {}, a credential store", root.display());
        }

        // a directory
        let metadata = tokio::fs::metadata(&root)
            .await
            .with_context(|| format!("reading read root {}", root.display()))?;
        if !metadata.is_dir() {
            deny!("read root `{entry}` is not a directory");
        }
        Ok(Some(root))
    }

    fn environment(
        &self, chamber: &Chamber, bound: &Valve, asked: &[(String, String)],
    ) -> Result<Vec<(OsString, OsString)>> {
        let scratch = chamber.scratch().as_os_str();
        let proxy = OsString::from(format!("http://{}", bound.address()));

        let mut env = self.inherited.clone();
        env.extend(SCRATCH.iter().map(|&name| (name.into(), scratch.to_owned())));
        env.extend(PROXY.iter().map(|&name| (name.into(), proxy.clone())));
        env.extend(NO_PROXY.iter().map(|&name| (name.into(), LOOPBACK.into())));

        // the ask's variables over them, never over the backend's `inherit`:
        // those are the program's own settings, read of the host
        for (name, value) in asked {
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

/// What every confined process may reach, as plain data.
///
/// A backend fills it from its own configuration. Nothing here names an
/// environment variable of the crate's own. What one process asks beyond
/// it is its [`Clearance`].
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Trees a lend at or beneath which runs with a shell.
    pub shell_roots: Vec<PathBuf>,
    /// Host environment variables a confined process inherits, by name.
    pub inherit: &'static [&'static str],
    /// Environment variable names that must never reach a confined process.
    pub refused: &'static [&'static str],
}

/// What one process may reach beyond the compartment's defaults.
///
/// Sealed with the process by [`Airlock::seal`], which holds each entry to the
/// host before the process runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Clearance {
    /// Hosts the process may reach through its egress proxy.
    ///
    /// - A name matches whole.
    /// - `*.suffix` matches the suffix and every name beneath it.
    /// - An IP literal matches itself.
    ///
    /// Every address, resolved or literal, is held to [`valve::Refusal`]
    /// before the tunnel opens, a loopback literal alone excepted.
    pub egress: Vec<String>,
    /// Trees the process may read beside the system's own.
    ///
    /// Each is spelled `~/<path>`, a directory beneath the operator's home.
    /// The credential stores [`Airlock::seal`] lists are denied whatever the
    /// clearance names.
    pub read: Vec<String>,
    /// Environment variables set for the process.
    ///
    /// A value's leading `~/` spells the operator's home.
    pub env: Vec<(String, String)>,
}

/// A [`Clearance`] the host denies: an entry that breaks a rule of the seal.
///
/// [`Airlock::seal`] fails with one of these, downcastable from its error,
/// where the clearance itself is at fault. Where the host failed, it fails
/// with a plain error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denied(String);

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Denied {}

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

// Why a name is neither the backend's to inherit nor an ask's to set.
fn reserved(name: &str, refused: &[&str]) -> Option<&'static str> {
    if [SCRATCH, PROXY, NO_PROXY].iter().any(|own| own.contains(&name)) {
        Some("is the compartment's to set")
    } else if REFUSED.contains(&name) || refused.contains(&name) {
        Some("is refused")
    } else if LOADER.iter().any(|prefix| name.starts_with(prefix)) {
        Some("is the dynamic loader's")
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

// The spelling may hide a store behind a `..` or a link, so the canonical
// root is held to each store as spelled and as it resolves.
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

// A grant of a tree grants every store within it, so a root above a store
// is as refused as one beneath it.
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

// `[A-Za-z_][A-Za-z0-9_]*`: what a shell exports and `Command::env` accepts.
fn is_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}
