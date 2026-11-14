use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use azure_core::error::ErrorKind;
use azure_core::http::RequestContent;
use azure_core::time::OffsetDateTime;
use azure_storage_blob::BlobServiceClient;
use azure_storage_blob::models::{
    BlobClientDownloadOptions, BlobClientGetPropertiesResultHeaders,
    BlobContainerClientCreateOptions, BlobContainerClientDeleteOptions,
    BlobContainerClientListBlobsOptions, HttpRange, StorageErrorCode,
};
use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use omnia::FutureResult;
use omnia_wasi_blobstore::{
    BlobStoreClient, Container, ContainerMetadata, Error, ObjectMetadata, Page,
};

use crate::{Client, STORE_CONTAINER};

impl BlobStoreClient for Client {
    fn create_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("creating container: {name}");
        let service = Arc::clone(&self.service);

        async move {
            ensure_not_reserved(&name)?;
            let created = service
                .blob_container_client(&name)
                .create(Option::<BlobContainerClientCreateOptions<'_>>::None)
                .await;
            if let Err(err) = created
                && storage_code(&err) != Some(StorageErrorCode::ContainerAlreadyExists)
            {
                return Err(err).context("creating container");
            }

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
            ensure_not_reserved(&name)?;
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
            ensure_not_reserved(&name)?;
            let deleted = service
                .blob_container_client(&name)
                .delete(Option::<BlobContainerClientDeleteOptions<'_>>::None)
                .await;
            match deleted {
                Err(err) if storage_code(&err) != Some(StorageErrorCode::ContainerNotFound) => {
                    Err(err).context("deleting container")
                }
                _ => Ok(()),
            }
        }
        .boxed()
    }

    fn container_exists(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of container: {name}");
        let service = Arc::clone(&self.service);

        async move {
            ensure_not_reserved(&name)?;
            service
                .blob_container_client(&name)
                .exists()
                .await
                .context("checking container existence")
        }
        .boxed()
    }
}

// SECURITY: the store's container holds the releases the host loads, and the
// acquirer serves a stored one before any registry; a guest handle on it
// could plant, replace, or delete a release, so the blobstore view never
// opens it. `copy_object` and `move_object` reach containers through
// `get_container`, so the four entry points cover every guest path.
fn ensure_not_reserved(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        name != STORE_CONTAINER,
        "container `{name}` is reserved for the package store"
    );
    Ok(())
}

// a failed request names what it found absent, a blob or its container, in
// its storage error code
fn storage_code(err: &azure_core::Error) -> Option<StorageErrorCode> {
    match err.kind() {
        ErrorKind::HttpResponse {
            error_code: Some(code),
            ..
        } => code.parse().ok(),
        _ => None,
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
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

    fn get_data(&self, name: String, range: Range<u64>) -> FutureResult<Option<Bytes>> {
        tracing::trace!("getting object data: {name}");

        // an HTTP range cannot be empty, so an empty read only asks whether the blob exists
        if range.is_empty() {
            let exists = self.has_object(name);
            return async move { Ok(exists.await?.then(Bytes::new)) }.boxed();
        }

        let blob_client = self.service.blob_client(&self.name, &name);
        let container = self.name.clone();

        async move {
            let options = BlobClientDownloadOptions {
                range: http_range(range),
                ..Default::default()
            };
            let response = match blob_client.download(Some(options)).await {
                Ok(response) => response,
                Err(err) if storage_code(&err) == Some(StorageErrorCode::BlobNotFound) => {
                    return Ok(None);
                }
                // the service refuses a range that starts past the end, which
                // the contract reads as empty
                Err(err) if storage_code(&err) == Some(StorageErrorCode::InvalidRange) => {
                    return Ok(Some(Bytes::new()));
                }
                Err(err) => return Err(fault(err, &container, "downloading blob")),
            };
            let data = response.body.collect().await.context("reading blob body")?;
            Ok(Some(data))
        }
        .boxed()
    }

    fn write_data(&self, name: String, data: Bytes) -> FutureResult<()> {
        tracing::trace!("writing object data: {name}");
        let blob_client = self.service.blob_client(&self.name, &name);
        let container = self.name.clone();

        async move {
            // the SDK's `RequestContent` only converts from `Vec<u8>`
            let content = RequestContent::from(data.to_vec());
            blob_client
                .upload(content, None)
                .await
                .map_err(|err| fault(err, &container, "uploading blob"))?;
            Ok(())
        }
        .boxed()
    }

    fn list_objects(&self, prefix: String, after: Option<String>) -> FutureResult<Page> {
        tracing::trace!("listing objects");
        let container_client = self.service.blob_container_client(&self.name);
        let container = self.name.clone();

        async move {
            let options = BlobContainerClientListBlobsOptions {
                prefix: Some(prefix),
                ..Default::default()
            };
            let pager = container_client.list_blobs(Some(options)).context("listing blobs")?;
            let items: Vec<_> = pager
                .try_collect()
                .await
                .map_err(|err| fault(err, &container, "paginating blob list"))?;

            let mut objects: Vec<_> = items
                .into_iter()
                .filter_map(|item| {
                    let properties = item.properties.unwrap_or_default();
                    Some(ObjectMetadata {
                        name: item.name?,
                        container: container.clone(),
                        created_at: properties.creation_time.map_or(0, unix_secs),
                        size: properties.content_length.unwrap_or(0),
                    })
                })
                .filter(|object| after.as_ref().is_none_or(|after| &object.name > after))
                .collect();
            objects.sort_unstable_by(|a, b| a.name.cmp(&b.name));
            Ok(Page { objects, next: None })
        }
        .boxed()
    }

    fn delete_object(&self, name: String) -> FutureResult<()> {
        tracing::trace!("deleting object: {name}");
        let blob_client = self.service.blob_client(&self.name, &name);
        let container = self.name.clone();

        async move {
            match blob_client.delete(None).await {
                Err(err) if storage_code(&err) != Some(StorageErrorCode::BlobNotFound) => {
                    Err(fault(err, &container, "deleting blob"))
                }
                _ => Ok(()),
            }
        }
        .boxed()
    }

    fn has_object(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of object: {name}");
        let blob_client = self.service.blob_client(&self.name, &name);
        let container = self.name.clone();

        async move {
            match blob_client.get_properties(None).await {
                Ok(_) => Ok(true),
                Err(err) if storage_code(&err) == Some(StorageErrorCode::BlobNotFound) => Ok(false),
                Err(err) => Err(fault(err, &container, "checking blob existence")),
            }
        }
        .boxed()
    }

    fn object_info(&self, name: String) -> FutureResult<Option<ObjectMetadata>> {
        tracing::trace!("getting object info: {name}");
        let blob_client = self.service.blob_client(&self.name, &name);
        let container_name = self.name.clone();

        async move {
            let response = match blob_client.get_properties(None).await {
                Ok(response) => response,
                Err(err) if storage_code(&err) == Some(StorageErrorCode::BlobNotFound) => {
                    return Ok(None);
                }
                Err(err) => return Err(fault(err, &container_name, "getting blob properties")),
            };
            Ok(Some(ObjectMetadata {
                name,
                container: container_name,
                size: response.content_length().ok().flatten().unwrap_or(0),
                created_at: response.creation_time().ok().flatten().map_or(0, unix_secs),
            }))
        }
        .boxed()
    }
}

// the whole blob takes no `Range` header, and a read to the end no last byte
fn http_range(range: Range<u64>) -> Option<HttpRange> {
    match range {
        Range {
            start: 0,
            end: u64::MAX,
        } => None,
        Range { start, end: u64::MAX } => Some(HttpRange::from_offset(start)),
        range => Some(range.into()),
    }
}

// a request in a container that does not exist fails as the container's
// absence, and any other failure keeps the request's context
fn fault(err: azure_core::Error, container: &str, action: &'static str) -> anyhow::Error {
    if storage_code(&err) == Some(StorageErrorCode::ContainerNotFound) {
        return Error::missing_container(container).into();
    }
    anyhow::Error::new(err).context(action)
}

fn unix_secs(time: OffsetDateTime) -> u64 {
    u64::try_from(time.unix_timestamp()).unwrap_or(0)
}
