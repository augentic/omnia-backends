//! [`ContentStore`] and [`ReleaseStore`] backed by a dedicated Azure Blob
//! container.
//!
//! The store owns the `omnia-plugins` container — a name the impl chooses,
//! never an operator input — so guest `wasi:blobstore` containers map
//! elsewhere by construction. Content blobs are keyed by their
//! `sha256:<hex>` digest and shared across registries; release records are
//! scoped per registry. Writes are verify-before-persist; a blob PUT is
//! atomic on the service side.

use anyhow::{Context as _, Result, bail};
use azure_core::http::{RequestContent, StatusCode};
use futures::FutureExt as _;
use futures::future::BoxFuture;
use omnia_plugin::{ContentStore, ReleaseStore, sha256_digest};

use crate::Client;

/// The container the plugin store writes; disjoint from guest containers
/// because the store names it itself.
const STORE_CONTAINER: &str = "omnia-plugins";

impl ContentStore for Client {
    fn content<'a>(&'a self, digest: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
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
}

impl ReleaseStore for Client {
    fn release<'a>(
        &'a self, registry: &'a str, package: &'a str, version: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>>> {
        tracing::trace!("getting plugin release: {package}@{version} from {registry}");
        let blob =
            self.service.blob_client(STORE_CONTAINER, &release_name(registry, package, version));

        async move {
            let Some(bytes) = read_optional(&blob).await? else {
                return Ok(None);
            };
            let digest = String::from_utf8(bytes).context("decoding release record")?;
            Ok(Some(digest))
        }
        .boxed()
    }

    fn put_release<'a>(
        &'a self, registry: &'a str, package: &'a str, version: &'a str, digest: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        tracing::trace!("putting plugin release: {package}@{version} in {registry}");
        let blob =
            self.service.blob_client(STORE_CONTAINER, &release_name(registry, package, version));

        async move {
            self.ensure_store_container().await?;
            let content = RequestContent::from(digest.as_bytes().to_vec());
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
    format!("releases/{registry}/{package}-{version}")
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
