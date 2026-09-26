use std::fmt::Debug;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use azure_core::http::RequestContent;
use azure_storage_blob::BlobServiceClient;
use azure_storage_blob::models::{
    BlobClientDownloadOptions, BlobClientGetPropertiesResultHeaders,
    BlobContainerClientCreateOptions, BlobContainerClientDeleteOptions, HttpRange,
};
use futures::{FutureExt, TryStreamExt};
use omnia_wasi_blobstore::{
    Bytes, Container, ContainerMetadata, FutureResult, ObjectMetadata, WasiBlobstoreCtx,
};

use crate::Client;

impl WasiBlobstoreCtx for Client {
    fn create_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("creating container: {name}");
        let service = Arc::clone(&self.service);

        async move {
            let container_client = service.blob_container_client(&name);
            container_client
                .create(Option::<BlobContainerClientCreateOptions<'_>>::None)
                .await
                .context("creating container")?;

            let created_at = now_unix_secs();

            Ok(Arc::new(AzureBlobContainer {
                name,
                service,
                created_at,
            }) as Arc<dyn Container>)
        }
        .boxed()
    }

    fn get_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("getting container: {name}");
        let service = Arc::clone(&self.service);

        async move {
            Ok(Arc::new(AzureBlobContainer {
                name,
                service,
                created_at: 0,
            }) as Arc<dyn Container>)
        }
        .boxed()
    }

    fn delete_container(&self, name: String) -> FutureResult<()> {
        tracing::trace!("deleting container: {name}");
        let service = Arc::clone(&self.service);

        async move {
            service
                .blob_container_client(&name)
                .delete(Option::<BlobContainerClientDeleteOptions<'_>>::None)
                .await
                .context("deleting container")?;
            Ok(())
        }
        .boxed()
    }

    fn container_exists(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of container: {name}");
        let service = Arc::clone(&self.service);

        async move {
            service
                .blob_container_client(&name)
                .exists()
                .await
                .context("checking container existence")
        }
        .boxed()
    }
}

struct AzureBlobContainer {
    name: String,
    service: Arc<BlobServiceClient>,
    created_at: u64,
}

impl Debug for AzureBlobContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobContainer").field("name", &self.name).finish_non_exhaustive()
    }
}

impl Container for AzureBlobContainer {
    fn name(&self) -> anyhow::Result<String> {
        tracing::trace!("getting container name");
        Ok(self.name.clone())
    }

    fn info(&self) -> anyhow::Result<ContainerMetadata> {
        tracing::trace!("getting container info");
        Ok(ContainerMetadata {
            name: self.name.clone(),
            created_at: self.created_at,
        })
    }

    fn get_data(&self, name: String, start: u64, end: u64) -> FutureResult<Option<Bytes>> {
        tracing::trace!("getting object data: {name}");
        let blob_client = self.service.blob_client(&self.name, &name);

        async move {
            let response = blob_client
                .download(range_options(start, end)?)
                .await
                .context("downloading blob")?;
            let data = response.body.collect().await.context("reading blob body")?;
            Ok(Some(data))
        }
        .boxed()
    }

    fn write_data(&self, name: String, data: Bytes) -> FutureResult<()> {
        tracing::trace!("writing object data: {name}");
        let blob_client = self.service.blob_client(&self.name, &name);

        async move {
            // the SDK's `RequestContent` only converts from `Vec<u8>`
            let content = RequestContent::from(data.to_vec());
            blob_client.upload(content, None).await.context("uploading blob")?;
            Ok(())
        }
        .boxed()
    }

    fn list_objects(&self) -> FutureResult<Vec<String>> {
        tracing::trace!("listing objects");
        let container_client = self.service.blob_container_client(&self.name);

        async move {
            let pager = container_client.list_blobs(None).context("listing blobs")?;

            let items: Vec<_> = pager.try_collect().await.context("paginating blob list")?;
            let names = items.into_iter().filter_map(|item| item.name).collect();

            Ok(names)
        }
        .boxed()
    }

    fn delete_object(&self, name: String) -> FutureResult<()> {
        tracing::trace!("deleting object: {name}");
        let blob_client = self.service.blob_client(&self.name, &name);

        async move {
            blob_client.delete(None).await.context("deleting blob")?;
            Ok(())
        }
        .boxed()
    }

    fn has_object(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of object: {name}");
        let blob_client = self.service.blob_client(&self.name, &name);

        async move { blob_client.exists().await.context("checking blob existence") }.boxed()
    }

    fn object_info(&self, name: String) -> FutureResult<ObjectMetadata> {
        tracing::trace!("getting object info: {name}");
        let blob_client = self.service.blob_client(&self.name, &name);
        let container_name = self.name.clone();

        async move {
            let response =
                blob_client.get_properties(None).await.context("getting blob properties")?;

            let size = response.content_length().ok().flatten().unwrap_or(0);

            let created_at = response
                .creation_time()
                .ok()
                .flatten()
                .map_or(0, |t| u64::try_from(t.unix_timestamp()).unwrap_or(0));

            Ok(ObjectMetadata {
                name,
                container: container_name,
                size,
                created_at,
            })
        }
        .boxed()
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

// Download options with an HTTP `Range` header for partial reads.
fn range_options(
    start: u64, end: u64,
) -> anyhow::Result<Option<BlobClientDownloadOptions<'static>>> {
    if start == 0 && (end == 0 || end == u64::MAX) {
        return Ok(None);
    }

    let unbounded = end == 0 || end == u64::MAX;

    if !unbounded && end < start {
        anyhow::bail!("invalid byte range: end ({end}) < start ({start})");
    }

    let range = if unbounded {
        HttpRange::from_offset(start)
    } else {
        HttpRange::new(start, end - start + 1)
    };

    Ok(Some(BlobClientDownloadOptions {
        range: Some(range),
        ..Default::default()
    }))
}

// Only `range_options` is pure enough to unit-test; the list and metadata
// mappings are proven against the real service in `tests/live.rs`.
#[cfg(test)]
mod tests {
    use azure_storage_blob::models::HttpRange;

    use super::*;

    #[test]
    fn range_full_zero_zero() {
        assert!(range_options(0, 0).unwrap().is_none());
    }

    #[test]
    fn range_full_zero_max() {
        assert!(range_options(0, u64::MAX).unwrap().is_none());
    }

    #[test]
    fn range_offset_unbounded() {
        let opts = range_options(100, u64::MAX).unwrap().expect("should produce options");
        assert_eq!(opts.range, Some(HttpRange::from_offset(100)));
    }

    #[test]
    fn range_offset_zero() {
        let opts = range_options(100, 0).unwrap().expect("should produce options");
        assert_eq!(opts.range, Some(HttpRange::from_offset(100)));
    }

    #[test]
    fn range_bounded() {
        let opts = range_options(10, 99).unwrap().expect("should produce options");
        assert_eq!(opts.range, Some(HttpRange::new(10, 90)));
    }

    #[test]
    fn range_single_byte() {
        let opts = range_options(5, 5).unwrap().expect("should produce options");
        assert_eq!(opts.range, Some(HttpRange::new(5, 1)));
    }

    #[test]
    fn range_end_before_start() {
        let err = range_options(10, 5).unwrap_err();
        assert!(err.to_string().contains("end (5) < start (10)"));
    }
}
