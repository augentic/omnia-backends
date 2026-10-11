#![doc = include_str!("../README.md")]

mod child;
mod confinement;
pub mod egress;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod seatbelt;

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::{self, PathBuf};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};

pub use crate::child::{Child, Exit};
pub use crate::confinement::{Boundary, Confinement};
use crate::egress::Egress;

/// What a confined process may reach, as plain data.
///
/// A backend fills it from its own configuration; nothing here names an
/// environment variable of the crate's own.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Trees a lend at or beneath which runs with a shell.
    pub shell_roots: Vec<PathBuf>,
    /// Trees a confined process may read beside the system's own.
    pub read_roots: Vec<PathBuf>,
    /// Hosts a confined process may reach through the egress proxy.
    ///
    /// A name matches whole, `*.suffix` matches the suffix and every name
    /// beneath it, and an IP literal is connected as written.
    pub egress: Vec<String>,
    /// Host environment variables copied into a confined process, by name.
    pub passthrough: Vec<String>,
    /// Environment variable names that must never reach a confined process.
    pub refused: Vec<String>,
    /// The mechanism that holds the boundary.
    pub substrate: Substrate,
}

/// The mechanism that holds the boundary around a process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Substrate {
    /// The platform's kernel boundary: Seatbelt on macOS, Landlock and
    /// seccomp on Linux.
    ///
    /// [`Confine::connect`] refuses it where the platform runs none.
    #[default]
    Auto,
    /// No kernel boundary: the working directory, the environment, the
    /// session, and the egress proxy alone.
    Unconfined,
}

// The substrate as connected, which every boundary laid shares.
#[derive(Debug, Clone)]
pub(crate) enum Wall {
    None,
    #[cfg(target_os = "linux")]
    Landlock(Arc<linux::Linux>),
    #[cfg(target_os = "macos")]
    Seatbelt(Arc<seatbelt::Seatbelt>),
}

impl Wall {
    #[cfg(target_os = "linux")]
    async fn platform(read_roots: &[PathBuf]) -> Result<Self> {
        Ok(Self::Landlock(Arc::new(linux::Linux::probe(read_roots).await?)))
    }

    #[cfg(target_os = "macos")]
    async fn platform(read_roots: &[PathBuf]) -> Result<Self> {
        Ok(Self::Seatbelt(Arc::new(seatbelt::Seatbelt::probe(read_roots).await?)))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    async fn platform(_read_roots: &[PathBuf]) -> Result<Self> {
        bail!("no confinement substrate runs on this platform")
    }
}

/// The confinement a connected backend lays around every process it spawns.
///
/// Connecting holds the [`Options`] to the host once: the roots are
/// resolved, the `passthrough` is checked, the substrate is probed, the
/// egress proxy is bound, and the environment a process inherits is read.
/// What [`Confine::lay`] builds from them cannot fail on the operator's
/// configuration.
pub struct Confine {
    options: Options,
    inherited: Vec<(OsString, OsString)>,
    wall: Wall,
    egress: Egress,
}

impl Confine {
    // The repository selection a shell exports, which would point every git
    // the model runs at the operator's own repository.
    const GIT_SELECTION: [&str; 4] =
        ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];
    // What every confined process inherits from the host, beside `LC_*`.
    const INHERITED: [&str; 4] = ["PATH", "LANG", "TZ", "TERM"];
    // What a confined process reaches without the proxy: the host's own
    // loopback, where the callback endpoint and the proxy itself listen.
    const NO_PROXY: &str = "127.0.0.1,localhost,::1";
    // What the boundary sets itself, so no `passthrough` puts the host's
    // value over it.
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
    // What never reaches a confined process by the crate's own rule.
    const REFUSED: [&str; 1] = ["NODE_USE_SYSTEM_CA"];

    /// Connects: holds `options` to the host and reads what a process inherits.
    ///
    /// `shell_roots` are made absolute against the current directory and
    /// `read_roots` are canonicalised, so a lend is compared with them
    /// however either was spelled.
    ///
    /// # Errors
    ///
    /// Returns an error when a `passthrough` names a variable the boundary
    /// sets itself, one in `refused`, or a `GIT_DIR` sibling; when a
    /// `read_roots` entry does not exist; or when [`Substrate::Auto`] finds
    /// no substrate that runs on this host.
    pub async fn connect(mut options: Options) -> Result<Self> {
        // the passthrough held to what may reach the process
        for name in &options.passthrough {
            let name = name.as_str();
            if Self::OWN.contains(&name) {
                bail!("`{name}` is the boundary's to set and cannot be passed through");
            }
            if Self::REFUSED.contains(&name)
                || options.refused.iter().any(|refused| refused == name)
            {
                bail!("`{name}` is refused and cannot be passed through");
            }
            if Self::GIT_SELECTION.contains(&name) {
                bail!("`{name}` selects a repository and cannot be passed through");
            }
        }

        // the roots resolved once
        for root in &mut options.shell_roots {
            *root = path::absolute(&*root)
                .with_context(|| format!("resolving shell root {}", root.display()))?;
        }
        for root in &mut options.read_roots {
            *root = tokio::fs::canonicalize(&*root)
                .await
                .with_context(|| format!("read root {} does not exist", root.display()))?;
        }

        // the substrate probed and the way out bound
        let wall = match options.substrate {
            Substrate::Unconfined => Wall::None,
            Substrate::Auto => Wall::platform(&options.read_roots).await?,
        };
        let egress = Egress::bind(&options.egress).await?;

        let inherited = std::env::vars_os()
            .filter(|(name, _)| Self::passes(name, &options.passthrough))
            .collect();
        Ok(Self {
            options,
            inherited,
            wall,
            egress,
        })
    }

    fn passes(name: &OsStr, passthrough: &[String]) -> bool {
        name.to_str().is_some_and(|name| {
            Self::INHERITED.contains(&name)
                || name.starts_with("LC_")
                || passthrough.iter().any(|passed| passed == name)
        })
    }

    /// The options as connected: absolute `shell_roots`, canonical `read_roots`.
    #[must_use]
    pub const fn options(&self) -> &Options {
        &self.options
    }

    /// The egress proxy every boundary's `HTTPS_PROXY` names.
    #[must_use]
    pub const fn egress(&self) -> &Egress {
        &self.egress
    }

    /// Lays the boundary around one process, where `confinement` put it.
    ///
    /// The process's environment is what connecting inherited, its `HOME`
    /// and `TMPDIR` the confinement's scratch, and its proxy variables the
    /// egress proxy, with the host's own loopback excepted.
    #[must_use]
    pub fn lay(&self, confinement: Confinement) -> Boundary {
        let scratch = confinement.scratch().as_os_str().to_owned();
        let proxy = OsString::from(format!("http://{}", self.egress.address()));
        let mut env = self.inherited.clone();
        env.extend(Self::OWN.iter().map(|&name| {
            let value = match name {
                "HOME" | "TMPDIR" => scratch.clone(),
                "NO_PROXY" | "no_proxy" => OsString::from(Self::NO_PROXY),
                _ => proxy.clone(),
            };
            (OsString::from(name), value)
        }));
        Boundary::new(confinement, env, self.wall.clone())
    }
}

impl fmt::Debug for Confine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inherited: Vec<&OsStr> =
            self.inherited.iter().map(|(name, _)| name.as_os_str()).collect();
        f.debug_struct("Confine")
            .field("options", &self.options)
            .field("inherited", &inherited)
            .field("wall", &self.wall)
            .field("egress", &self.egress.address())
            .finish()
    }
}
