//! The digest-keyed content-addressed store behind [`RegistryAcquire`].
//!
//! [`RegistryAcquire`]: crate::RegistryAcquire

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::PathBuf;

use anyhow::{Context as _, anyhow};
use futures::{StreamExt as _, TryStreamExt as _};
use sha2::{Digest as _, Sha256};
use wasm_pkg_client::caching::Cache;
use wasm_pkg_client::{
    ContentDigest, ContentStream, Error, PackageRef, Registry, Release, Version,
};

/// Digest-keyed content-addressed store for raw component bytes plus the
/// release records that map an exact package version to its digest.
///
/// Writes are verify-before-persist — bytes that do not hash to their digest
/// key are refused — and accepted entries land by temp-file plus atomic
/// rename, so an entry is either complete or absent, never torn. Content
/// entries are registry-agnostic (the digest is the identity); release
/// records are scoped per registry, so an endpoint override can never be
/// answered from another registry's record.
#[derive(Clone, Debug)]
pub struct ContentStore {
    root: PathBuf,
    releases: PathBuf,
}

impl ContentStore {
    /// Store rooted at `root`; directories are created lazily on first write.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let releases = root.join("releases");
        Self { root, releases }
    }

    /// A view of the same store whose release records are scoped to
    /// `registry`; content entries stay shared (digests are the identity).
    pub(crate) fn scoped_to(mut self, registry: &Registry) -> Self {
        self.releases = self.root.join("releases").join(registry.to_string());
        self
    }

    fn content_path(&self, digest: &ContentDigest) -> PathBuf {
        self.root.join("content").join(digest.to_string())
    }

    fn release_path(&self, package: &PackageRef, version: &Version) -> PathBuf {
        self.releases.join(format!("{package}-{version}.json"))
    }
}

impl Cache for ContentStore {
    async fn put_data(&self, digest: ContentDigest, data: ContentStream) -> Result<(), Error> {
        let bytes = collect(data).await?;
        // Verify before persist: a mismatched write must never become a
        // digest-keyed entry, even torn.
        let resolved = sha256_digest(&bytes);
        if resolved != digest {
            return Err(Error::CacheError(anyhow!(
                "refusing to persist content keyed {digest}: the bytes hash to {resolved}"
            )));
        }
        write_atomic(self.content_path(&digest), bytes).await.map_err(Error::CacheError)
    }

    async fn get_data(&self, digest: &ContentDigest) -> Result<Option<ContentStream>, Error> {
        Ok(read_optional(self.content_path(digest)).await?.map(into_stream))
    }

    async fn put_release(&self, package: &PackageRef, release: &Release) -> Result<(), Error> {
        let record = ReleaseRecord {
            version: release.version.clone(),
            content_digest: release.content_digest.clone(),
        };
        let bytes = serde_json::to_vec(&record)
            .map_err(|error| Error::CacheError(anyhow!("encoding release record: {error}")))?;
        write_atomic(self.release_path(package, &release.version), bytes)
            .await
            .map_err(Error::CacheError)
    }

    async fn get_release(
        &self, package: &PackageRef, version: &Version,
    ) -> Result<Option<Release>, Error> {
        let Some(bytes) = read_optional(self.release_path(package, version)).await? else {
            return Ok(None);
        };
        let record: ReleaseRecord = serde_json::from_slice(&bytes)
            .map_err(|error| Error::CacheError(anyhow!("decoding release record: {error}")))?;
        Ok(Some(Release {
            version: record.version,
            content_digest: record.content_digest,
        }))
    }
}

/// The persisted form of a [`Release`]: exact version plus content digest.
#[derive(serde::Deserialize, serde::Serialize)]
struct ReleaseRecord {
    version: Version,
    content_digest: ContentDigest,
}

/// Hash `bytes` into their canonical sha256 content digest.
#[must_use]
pub fn sha256_digest(bytes: &[u8]) -> ContentDigest {
    let hash = Sha256::digest(bytes);
    let mut hex = String::with_capacity(2 * hash.len());
    for byte in hash {
        let _ = write!(hex, "{byte:02x}");
    }
    ContentDigest::Sha256 { hex }
}

/// Drain `stream` into memory; callers hash the whole buffer anyway.
///
/// # Errors
///
/// Propagates the stream's own error, typically an interrupted fetch.
pub async fn collect(mut stream: ContentStream) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn into_stream(bytes: Vec<u8>) -> ContentStream {
    futures::stream::once(async move { Ok(bytes::Bytes::from(bytes)) }).boxed()
}

async fn write_atomic(path: PathBuf, bytes: Vec<u8>) -> anyhow::Result<()> {
    // File writes are blocking I/O; keep them off the async executor.
    tokio::task::spawn_blocking(move || {
        let dir = path.parent().expect("store paths always have a parent");
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating store directory `{}`", dir.display()))?;
        let mut tmp = tempfile::NamedTempFile::new_in(dir).context("creating store temp file")?;
        tmp.write_all(&bytes).context("writing store temp file")?;
        tmp.persist(&path)
            .with_context(|| format!("persisting store entry `{}`", path.display()))?;
        Ok(())
    })
    .await
    .context("store write task panicked")?
}

async fn read_optional(path: PathBuf) -> Result<Option<Vec<u8>>, Error> {
    match tokio::fs::read(&path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Error::CacheError(
            anyhow::Error::new(error).context(format!("reading store entry `{}`", path.display())),
        )),
    }
}
