//! [`PackageStore`] backed by a dedicated Azure Blob container.
//!
//! The store owns the `omnia-plugins` container — a name the impl chooses,
//! never an operator input, and one the `wasi:blobstore` view refuses — so
//! no guest container is it. One blob per release, named as omnia's
//! `FsStore` files it (`namespace_name@version.wasm`); a blob already there
//! is never replaced, so a stored release is final until removed.

use anyhow::{Context as _, Result};
use azure_core::http::{RequestContent, StatusCode};
use azure_storage_blob::models::BlobClientUploadOptions;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use omnia_plugin::{PackageStore, Reference};

use crate::{Client, STORE_CONTAINER};

impl PackageStore for Client {
    fn get<'a>(&'a self, reference: &'a Reference) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        tracing::trace!("getting package: {reference}");
        let blob = self.service.blob_client(STORE_CONTAINER, &reference.file_name());

        async move { read_optional(&blob).await }.boxed()
    }

    fn put<'a>(&'a self, reference: &'a Reference, bytes: &'a [u8]) -> BoxFuture<'a, Result<()>> {
        tracing::trace!("putting package: {reference}");
        let blob = self.service.blob_client(STORE_CONTAINER, &reference.file_name());

        async move {
            self.ensure_store_container().await?;
            let content = RequestContent::from(bytes.to_vec());
            let create_only = BlobClientUploadOptions::default().if_not_exists();
            match blob.upload(content, Some(create_only)).await {
                Ok(_) => Ok(()),
                // a release already stored stays as it is: the service answers
                // `If-None-Match: *` with 409 `BlobAlreadyExists`, and its
                // conditional-header contract documents 412 for the same case
                Err(err)
                    if matches!(
                        err.http_status(),
                        Some(StatusCode::Conflict | StatusCode::PreconditionFailed)
                    ) =>
                {
                    Ok(())
                }
                Err(err) => Err(err).context("uploading package"),
            }
        }
        .boxed()
    }

    fn describe(&self, reference: &Reference) -> String {
        format!("the `{STORE_CONTAINER}` container holds no `{}`", reference.file_name())
    }
}

impl Client {
    async fn ensure_store_container(&self) -> Result<()> {
        match self.service.blob_container_client(STORE_CONTAINER).create(None).await {
            Ok(_) => Ok(()),
            Err(err) if err.http_status() == Some(StatusCode::Conflict) => Ok(()),
            Err(err) => Err(err).context("creating the package store container"),
        }
    }
}

// A 404 for the container, not just the blob, is an absent entry too.
async fn read_optional(blob: &azure_storage_blob::BlobClient) -> Result<Option<Vec<u8>>> {
    match blob.download(None).await {
        Ok(response) => {
            let bytes = response.body.collect().await.context("reading the stored package")?;
            Ok(Some(bytes.to_vec()))
        }
        Err(err) if err.http_status() == Some(StatusCode::NotFound) => Ok(None),
        Err(err) => Err(err).context("reading the stored package"),
    }
}
