#![doc = include_str!("../README.md")]

mod blobstore;

use std::path::PathBuf;

use anyhow::{Context, Result};
use omnia::Backend;
use tracing::instrument;

/// Filesystem blobstore backend anchored at one root directory.
#[derive(Debug, Clone)]
pub struct Client {
    root: PathBuf,
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "Filesystem::connect")]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        Self::open(options.root)
    }
}

impl Client {
    /// Open a blobstore rooted at `root`, creating the directory when
    /// absent — the programmatic constructor for deployments that
    /// anchor the root themselves rather than through the environment.
    ///
    /// # Errors
    ///
    /// Returns an error when the root cannot be created or resolved.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("creating blobstore root `{}`", root.display()))?;
        // Canonical so container paths stay stable regardless of later
        // working-directory changes.
        let root = root
            .canonicalize()
            .with_context(|| format!("resolving blobstore root `{}`", root.display()))?;
        Ok(Self { root })
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the filesystem backend.
    #[derive(Clone, Debug, FromEnv)]
    pub struct ConnectOptions {
        /// Root directory of the object tree; created when absent.
        #[env(from = "BLOBSTORE_ROOT")]
        pub root: String,
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
