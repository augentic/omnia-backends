#![doc = include_str!("../README.md")]

mod blobstore;
mod keyvalue;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::FutureExt as _;
use omnia::Backend;
use tracing::instrument;

/// Filesystem backend for `wasi:blobstore` and `wasi:keyvalue`.
#[derive(Debug, Clone)]
pub struct Client {
    root: PathBuf,
    locks: keyvalue::LockRegistry,
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "Filesystem::connect")]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        Self::open(options.root)
    }
}

impl Client {
    /// Opens a store, creating its root directory when absent.
    ///
    /// # Errors
    ///
    /// Returns an error when the root cannot be created or resolved.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("creating filesystem root `{}`", root.display()))?;
        let root = root
            .canonicalize()
            .with_context(|| format!("resolving filesystem root `{}`", root.display()))?;
        Ok(Self {
            root,
            locks: Arc::default(),
        })
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the filesystem backend.
    #[derive(Clone, Debug, FromEnv)]
    pub struct ConnectOptions {
        /// Root directory of the store; created when absent.
        #[env(from = "FILESYSTEM_ROOT", default = ".omnia/storage")]
        pub root: String,
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}

fn blocking<T: Send + 'static>(
    task: impl FnOnce() -> Result<T> + Send + 'static,
) -> futures::future::BoxFuture<'static, Result<T>> {
    async move { tokio::task::spawn_blocking(task).await? }.boxed()
}

fn segment_ok(segment: &str) -> bool {
    !segment.is_empty() && segment != "." && segment != ".." && !segment.contains(['/', '\\', ':'])
}

fn collect(dir: &Path, prefix: &str, names: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("listing `{}`", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        // This API cannot create non-UTF-8 names.
        let Some(name) = name.to_str() else {
            continue;
        };
        // Ignore atomic-write temp files.
        if prefix.is_empty() && name.starts_with(".tmp") {
            continue;
        }
        let rel = if prefix.is_empty() { name.to_string() } else { format!("{prefix}/{name}") };
        let kind = entry.file_type()?;
        if kind.is_dir() {
            collect(&entry.path(), &rel, names)?;
        } else if kind.is_file() {
            names.push(rel);
        }
    }
    Ok(())
}
