//! `PluginStore` backed by a dedicated Azure Blob container.
//!
//! The store owns the `omnia-plugins` container — a name the impl chooses,
//! never an operator input — so guest `wasi:blobstore` containers map
//! elsewhere by construction. Content blobs are keyed by their
//! `sha256:<hex>` digest and shared across registries; release records are
//! scoped per registry. Writes are verify-before-persist; a blob PUT is
//! atomic on the service side.

use std::fmt::Write as _;

use anyhow::{Context as _, Result, bail};
use azure_core::http::{RequestContent, StatusCode};
use futures::FutureExt as _;
use futures::future::BoxFuture;
use omnia::{PluginStore, ReleaseRecord};
use sha2::{Digest as _, Sha256};

use crate::Client;

/// The container the plugin store writes; disjoint from guest containers
/// because the store names it itself.
const STORE_CONTAINER: &str = "omnia-plugins";

impl PluginStore for Client {
    fn get_content<'a>(&'a self, digest: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        tracing::trace!("getting plugin content: {digest}");
        let blob = self.service.blob_client(STORE_CONTAINER, &content_name(digest));

        async move { read_optional(&blob).await }.boxed()
    }

    fn put_content<'a>(&'a self, digest: &'a str, bytes: &'a [u8]) -> BoxFuture<'a, Result<()>> {
        tracing::trace!("putting plugin content: {digest}");
        let blob = self.service.blob_client(STORE_CONTAINER, &content_name(digest));

        async move {
            // Verify before persist: a mismatched write must never become a
            // digest-keyed entry.
            let resolved = sha256_digest(bytes);
            if resolved != digest {
                bail!("refusing to persist content keyed {digest}: the bytes hash to {resolved}");
            }
            self.ensure_store_container().await?;
            let content = RequestContent::from(bytes.to_vec());
            blob.upload(content, None).await.context("uploading plugin content")?;
            Ok(())
        }
        .boxed()
    }

    fn get_release<'a>(
        &'a self, registry: &'a str, package: &'a str, version: &'a str,
    ) -> BoxFuture<'a, Result<Option<ReleaseRecord>>> {
        tracing::trace!("getting plugin release: {package}@{version} from {registry}");
        let blob =
            self.service.blob_client(STORE_CONTAINER, &release_name(registry, package, version));

        async move {
            let Some(bytes) = read_optional(&blob).await? else {
                return Ok(None);
            };
            let record = serde_json::from_slice(&bytes).context("decoding release record")?;
            Ok(Some(record))
        }
        .boxed()
    }

    fn put_release<'a>(
        &'a self, registry: &'a str, package: &'a str, record: &'a ReleaseRecord,
    ) -> BoxFuture<'a, Result<()>> {
        tracing::trace!("putting plugin release: {package}@{} in {registry}", record.version);
        let blob = self
            .service
            .blob_client(STORE_CONTAINER, &release_name(registry, package, &record.version));

        async move {
            let bytes = serde_json::to_vec(record).context("encoding release record")?;
            self.ensure_store_container().await?;
            let content = RequestContent::from(bytes);
            blob.upload(content, None).await.context("uploading release record")?;
            Ok(())
        }
        .boxed()
    }
}

impl Client {
    /// Create the store container when absent; an existing one is fine.
    async fn ensure_store_container(&self) -> Result<()> {
        match self.service.blob_container_client(STORE_CONTAINER).create(None).await {
            Ok(_) => Ok(()),
            Err(err) if err.http_status() == Some(StatusCode::Conflict) => Ok(()),
            Err(err) => Err(err).context("creating plugin store container"),
        }
    }
}

fn content_name(digest: &str) -> String {
    format!("content/{digest}")
}

fn release_name(registry: &str, package: &str, version: &str) -> String {
    format!("releases/{registry}/{package}-{version}.json")
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

/// Read a blob's bytes; any 404 (blob or container) is an absent entry.
async fn read_optional(blob: &azure_storage_blob::BlobClient) -> Result<Option<Vec<u8>>> {
    match blob.download(None).await {
        Ok(response) => {
            let bytes = response.body.collect().await.context("reading store entry body")?;
            Ok(Some(bytes.to_vec()))
        }
        Err(err) if err.http_status() == Some(StatusCode::NotFound) => Ok(None),
        Err(err) => Err(err).context("reading store entry"),
    }
}
