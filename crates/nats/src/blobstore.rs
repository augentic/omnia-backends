use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, bail};
use async_nats::jetstream::ErrorCode;
use async_nats::jetstream::context::{GetStreamError, GetStreamErrorKind, ObjectStoreError};
use async_nats::jetstream::object_store::{
    Config, DeleteErrorKind, GetErrorKind, InfoErrorKind, ObjectInfo, ObjectStore,
};
use bytes::Bytes;
use chrono::Utc;
use futures::{FutureExt, StreamExt};
use omnia::FutureResult;
use omnia_wasi_blobstore::{
    BlobStoreClient, Container, ContainerMetadata, Error, ObjectMetadata, Page,
};
use tokio::io::AsyncReadExt;

use crate::Client;

impl BlobStoreClient for Client {
    fn create_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("creating container: {name}");
        let jetstream = self.jetstream.clone();

        async move {
            let created = jetstream
                .create_object_store(Config {
                    bucket: name.clone(),
                    ..Default::default()
                })
                .await;
            let store = match created {
                Ok(store) => store,
                Err(err) => match jetstream.get_object_store(&name).await {
                    Ok(store) => store,
                    Err(_) => return Err(err).context("creating object store"),
                },
            };
            let metadata = container_metadata(name);

            Ok(Arc::new(NatsContainer { metadata, store }) as Arc<dyn Container>)
        }
        .boxed()
    }

    fn get_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("getting container: {name}");
        let jetstream = self.jetstream.clone();

        async move {
            let store = match jetstream.get_object_store(&name).await {
                Ok(store) => store,
                Err(err) if stream_missing(&err) => bail!(Error::missing_container(&name)),
                Err(err) => return Err(err).context("getting object store"),
            };
            let metadata = container_metadata(name);

            Ok(Arc::new(NatsContainer { metadata, store }) as Arc<dyn Container>)
        }
        .boxed()
    }

    fn delete_container(&self, name: String) -> FutureResult<()> {
        tracing::trace!("deleting container: {name}");
        let jetstream = self.jetstream.clone();

        async move {
            match jetstream.delete_object_store(&name).await {
                Err(err) if !stream_missing(&err) => {
                    Err(err).context("issue deleting object store")
                }
                _ => Ok(()),
            }
        }
        .boxed()
    }

    fn container_exists(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of container: {name}");
        let jetstream = self.jetstream.clone();

        async move {
            match jetstream.get_object_store(&name).await {
                Ok(_) => Ok(true),
                Err(err) if stream_missing(&err) => Ok(false),
                Err(err) => Err(err).context("checking object store"),
            }
        }
        .boxed()
    }
}

// an object store is the stream `OBJ_<name>`, so a missing store is a
// stream the server does not find
fn stream_missing(err: &ObjectStoreError) -> bool {
    std::error::Error::source(err)
        .and_then(|source| source.downcast_ref::<GetStreamError>())
        .is_some_and(|err| {
            matches!(err.kind(), GetStreamErrorKind::JetStream(err)
                if err.error_code() == ErrorCode::STREAM_NOT_FOUND)
        })
}

fn container_metadata(name: String) -> ContainerMetadata {
    ContainerMetadata {
        name,
        created_at: u64::try_from(Utc::now().timestamp()).unwrap_or_default(),
    }
}

struct NatsContainer {
    metadata: ContainerMetadata,
    store: ObjectStore,
}

impl Debug for NatsContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsContainer").field("name", &self.metadata.name).finish_non_exhaustive()
    }
}

impl Container for NatsContainer {
    fn name(&self) -> anyhow::Result<String> {
        tracing::trace!("getting container name");
        Ok(self.metadata.name.clone())
    }

    fn info(&self) -> anyhow::Result<ContainerMetadata> {
        Ok(self.metadata.clone())
    }

    fn get_data(&self, name: String, range: Range<u64>) -> FutureResult<Option<Bytes>> {
        tracing::trace!("getting object data: {name}");
        let store = self.store.clone();

        async move {
            let mut object = match store.get(&name).await {
                Ok(object) => object,
                Err(err) if err.kind() == GetErrorKind::NotFound => return Ok(None),
                Err(err) => return Err(err).context("getting object"),
            };
            let mut bytes = Vec::new();
            object.read_to_end(&mut bytes).await?;

            let data = Bytes::from(bytes);
            let len = data.len();
            let clamp = |bound: u64| usize::try_from(bound).map_or(len, |bound| bound.min(len));
            let end = clamp(range.end);
            Ok(Some(data.slice(clamp(range.start).min(end)..end)))
        }
        .boxed()
    }

    fn write_data(&self, name: String, data: Bytes) -> FutureResult<()> {
        tracing::trace!("writing object data: {name}");
        let store = self.store.clone();

        async move {
            store.put(name.as_str(), &mut data.as_ref()).await.context("writing object")?;
            Ok(())
        }
        .boxed()
    }

    fn list_objects(&self, prefix: String, after: Option<String>) -> FutureResult<Page> {
        tracing::trace!("listing objects");
        let store = self.store.clone();
        let metadata = self.metadata.clone();

        async move {
            let mut list = store.list().await.context("listing objects")?;

            let mut objects = vec![];
            while let Some(n) = list.next().await {
                let Ok(info) = n else {
                    tracing::warn!("issue listing object");
                    continue;
                };
                if info.name.starts_with(&prefix)
                    && after.as_ref().is_none_or(|after| &info.name > after)
                {
                    objects.push(object_metadata(&metadata, info));
                }
            }

            objects.sort_unstable_by(|a, b| a.name.cmp(&b.name));
            Ok(Page { objects, next: None })
        }
        .boxed()
    }

    fn delete_object(&self, name: String) -> FutureResult<()> {
        let store = self.store.clone();

        async move {
            match store.delete(name).await {
                Err(err) if err.kind() != DeleteErrorKind::NotFound => {
                    Err(err).context("deleting object")
                }
                _ => Ok(()),
            }
        }
        .boxed()
    }

    fn has_object(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of object: {name}");
        let store = self.store.clone();

        async move {
            match store.info(&name).await {
                Ok(info) => Ok(!info.deleted),
                Err(err) if err.kind() == InfoErrorKind::NotFound => Ok(false),
                Err(err) => Err(err).context("checking object"),
            }
        }
        .boxed()
    }

    fn object_info(&self, name: String) -> FutureResult<Option<ObjectMetadata>> {
        tracing::trace!("getting object info: {name}");
        let store = self.store.clone();
        let metadata = self.metadata.clone();

        async move {
            match store.info(&name).await {
                Ok(info) if !info.deleted => Ok(Some(object_metadata(&metadata, info))),
                Ok(_) => Ok(None),
                Err(err) if err.kind() == InfoErrorKind::NotFound => Ok(None),
                Err(err) => Err(err).context("getting object info"),
            }
        }
        .boxed()
    }
}

// an object without a modification time takes its container's
fn object_metadata(container: &ContainerMetadata, info: ObjectInfo) -> ObjectMetadata {
    ObjectMetadata {
        name: info.name,
        container: container.name.clone(),
        created_at: info
            .modified
            .and_then(|time| u64::try_from(time.unix_timestamp()).ok())
            .unwrap_or(container.created_at),
        size: info.size as u64,
    }
}
