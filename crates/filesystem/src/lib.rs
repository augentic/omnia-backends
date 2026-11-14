#![doc = include_str!("../README.md")]

mod blobstore;
mod keyvalue;

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::FutureExt as _;
use futures::future::BoxFuture;
use omnia::Connect;
use tracing::instrument;

/// Filesystem backend for `wasi:blobstore` and `wasi:keyvalue`.
#[derive(Debug, Clone)]
pub struct Client {
    root: PathBuf,
    locks: keyvalue::LockRegistry,
    buckets: Arc<HashSet<String>>,
}

impl Client {
    /// Opens a store whose one key-value bucket is `default`, creating its root
    /// directory when absent.
    ///
    /// A store with more buckets connects with [`ConnectOptions::buckets`].
    ///
    /// # Errors
    ///
    /// Returns an error when the root cannot be created or resolved.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        Self::with_buckets(&root.into(), HashSet::from(["default".to_owned()]))
    }

    fn with_buckets(root: &Path, buckets: HashSet<String>) -> Result<Self> {
        fs::create_dir_all(root).context("creating root directory")?;
        let root = root.canonicalize().context("canonicalizing root directory")?;

        Ok(Self {
            root,
            locks: Arc::default(),
            buckets: Arc::new(buckets),
        })
    }
}

impl Connect for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "Filesystem::connect")]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let buckets = keyvalue::declare(options.buckets)?;
        Self::with_buckets(Path::new(&options.root), buckets)
    }
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use fromenv::{FromEnv, ParseResult};

    /// Connection options for the filesystem backend.
    #[derive(Clone, Debug, FromEnv)]
    pub struct ConnectOptions {
        /// Root directory of the store; created when absent.
        #[env(from = "FILESYSTEM_ROOT", default = ".omnia/storage")]
        pub root: String,
        /// Key-value buckets a guest may open, comma-separated.
        #[env(from = "KEYVALUE_BUCKETS", default = "default", with = comma_separated)]
        pub buckets: Vec<String>,
    }

    #[expect(clippy::unnecessary_wraps, reason = "a `FromEnv` `with =` hook returns `ParseResult`")]
    fn comma_separated(value: &str) -> ParseResult<Vec<String>> {
        Ok(value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned)
            .collect())
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
) -> BoxFuture<'static, Result<T>> {
    async move { tokio::task::spawn_blocking(task).await? }.boxed()
}

fn segment_ok(segment: &str) -> bool {
    !segment.is_empty() && segment != "." && segment != ".." && !segment.contains(['/', '\\', ':'])
}

fn collect(dir: &Path, prefix: &str) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).context("issue reading directory")? {
        let entry = entry?;
        let name = entry.file_name();

        let Some(name) = name.to_str() else {
            continue;
        };
        if prefix.is_empty() && name.starts_with(".tmp") {
            continue;
        }

        let rel = if prefix.is_empty() { name.to_string() } else { format!("{prefix}/{name}") };
        let kind = entry.file_type()?;
        if kind.is_dir() {
            names.extend(collect(&entry.path(), &rel)?);
        } else if kind.is_file() {
            names.push(rel);
        }
    }
    Ok(names)
}
