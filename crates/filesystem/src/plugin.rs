//! [`ContentStore`] and [`ReleaseStore`] backed by a `plugins/` subtree of
//! the store root.
//!
//! Content entries are keyed by their `sha256:<hex>` digest and shared across
//! registries; release records are scoped per registry. Writes are
//! verify-before-persist — bytes that do not hash to their digest key are
//! refused — and land by temp file plus atomic rename, so an entry is either
//! complete or absent, never torn. The subtree is a sibling of `blobstore/`
//! and `keyvalue/`, so no guest container or bucket name can reach it.

use std::fmt::Write as _;
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use futures::future::BoxFuture;
use omnia_plugin::{ContentStore, ReleaseRecord, ReleaseStore};
use sha2::{Digest as _, Sha256};

use crate::{Client, blocking};

impl ContentStore for Client {
    fn get<'a>(&'a self, digest: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        tracing::trace!("getting plugin content: {digest}");
        let path = content_path(&self.root, digest);

        blocking(move || maybe_read(&path))
    }

    fn put<'a>(&'a self, digest: &'a str, bytes: &'a [u8]) -> BoxFuture<'a, Result<()>> {
        tracing::trace!("putting plugin content: {digest}");
        let path = content_path(&self.root, digest);
        let digest = digest.to_owned();
        let bytes = bytes.to_vec();

        blocking(move || {
            // Verify before persist: a mismatched write must never become a
            // digest-keyed entry, even torn.
            let resolved = sha256_digest(&bytes);
            if resolved != digest {
                bail!("refusing to persist content keyed {digest}: the bytes hash to {resolved}");
            }
            write_atomic(&path, &bytes)
        })
    }
}

impl ReleaseStore for Client {
    fn get<'a>(
        &'a self, registry: &'a str, package: &'a str, version: &'a str,
    ) -> BoxFuture<'a, Result<Option<ReleaseRecord>>> {
        tracing::trace!("getting plugin release: {package}@{version} from {registry}");
        let path = release_path(&self.root, registry, package, version);

        blocking(move || {
            let Some(bytes) = maybe_read(&path)? else {
                return Ok(None);
            };
            let record = serde_json::from_slice(&bytes).context("decoding release record")?;
            Ok(Some(record))
        })
    }

    fn put<'a>(
        &'a self, registry: &'a str, package: &'a str, record: &'a ReleaseRecord,
    ) -> BoxFuture<'a, Result<()>> {
        tracing::trace!("putting plugin release: {package}@{} in {registry}", record.version);
        let path = release_path(&self.root, registry, package, &record.version);
        let record = record.clone();

        blocking(move || {
            let bytes = serde_json::to_vec(&record).context("encoding release record")?;
            write_atomic(&path, &bytes)
        })
    }
}

fn content_path(root: &Path, digest: &str) -> PathBuf {
    root.join("plugins").join("content").join(digest)
}

fn release_path(root: &Path, registry: &str, package: &str, version: &str) -> PathBuf {
    root.join("plugins").join("releases").join(registry).join(format!("{package}-{version}.json"))
}

/// Hash `bytes` into their canonical `sha256:<hex>` digest string.
fn sha256_digest(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    let mut digest = String::with_capacity("sha256:".len() + 2 * hash.len());
    digest.push_str("sha256:");
    for byte in hash {
        let _ = write!(digest, "{byte:02x}");
    }
    digest
}

fn maybe_read(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).context("reading store entry"),
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().expect("store paths always have a parent");
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating store directory `{}`", dir.display()))?;
    let mut temp = tempfile::NamedTempFile::new_in(dir).context("creating store temp file")?;
    temp.write_all(bytes).context("writing store temp file")?;
    temp.as_file().sync_all().context("syncing store temp file")?;
    temp.persist(path).with_context(|| format!("persisting store entry `{}`", path.display()))?;
    Ok(())
}
