use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_nats::jetstream::object_store::{Config, GetErrorKind, InfoErrorKind, ObjectStore};
use chrono::Utc;
use futures::{FutureExt, StreamExt};
use omnia_wasi_blobstore::{
    Bytes, Container, ContainerMetadata, FutureResult, ObjectMetadata, WasiBlobstoreCtx,
};
use tokio::io::AsyncReadExt;

use crate::Client;

impl WasiBlobstoreCtx for Client {
    fn create_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("creating container: {name}");
        let jetstream = self.jetstream.clone();

        async move {
            let store = jetstream
                .create_object_store(Config {
                    bucket: name.clone(),
                    ..Default::default()
                })
                .await
                .context("creating object store")?;
            let metadata = container_metadata(name);

            Ok(Arc::new(NatsContainer { metadata, store }) as Arc<dyn Container>)
        }
        .boxed()
    }

    fn get_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("getting container: {name}");
        let jetstream = self.jetstream.clone();

        async move {
            let store = jetstream.get_object_store(&name).await.context("getting object store")?;
            let metadata = container_metadata(name);

            Ok(Arc::new(NatsContainer { metadata, store }) as Arc<dyn Container>)
        }
        .boxed()
    }

    fn delete_container(&self, name: String) -> FutureResult<()> {
        tracing::trace!("deleting container: {name}");
        let jetstream = self.jetstream.clone();

        async move {
            jetstream.delete_object_store(&name).await.context("issue deleting object store")?;
            Ok(())
        }
        .boxed()
    }

    fn container_exists(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of container: {name}");
        let jetstream = self.jetstream.clone();

        async move {
            let exists = jetstream.get_object_store(&name).await.is_ok();
            Ok(exists)
        }
        .boxed()
    }
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

    fn get_data(&self, name: String, start: u64, end: u64) -> FutureResult<Option<Bytes>> {
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
            slice_range(&Bytes::from(bytes), start, end).map(Some)
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

    fn list_objects(&self) -> FutureResult<Vec<String>> {
        tracing::trace!("listing objects");
        let store = self.store.clone();

        async move {
            let mut objects = store.list().await.context("listing objects")?;

            let mut names = vec![];
            while let Some(n) = objects.next().await {
                let Ok(obj_info) = n else {
                    tracing::warn!("issue listing object");
                    continue;
                };
                names.push(obj_info.name);
            }

            Ok(names)
        }
        .boxed()
    }

    fn delete_object(&self, name: String) -> FutureResult<()> {
        let store = self.store.clone();
        async move { store.delete(name).await.context("deleting object") }.boxed()
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

    fn object_info(&self, name: String) -> FutureResult<ObjectMetadata> {
        tracing::trace!("getting object info: {name}");
        let store = self.store.clone();
        let metadata = self.metadata.clone();

        async move {
            let info = store.info(&name).await.context("getting object info")?;
            let created_at = info
                .modified
                .and_then(|t| u64::try_from(t.unix_timestamp()).ok())
                .unwrap_or(metadata.created_at);

            Ok(ObjectMetadata {
                container: metadata.name.clone(),
                name: info.name,
                size: info.size as u64,
                created_at,
            })
        }
        .boxed()
    }
}

fn slice_range(data: &Bytes, start: u64, end: u64) -> anyhow::Result<Bytes> {
    let unbounded = end == 0 || end == u64::MAX;
    if !unbounded && end < start {
        return Err(anyhow!("invalid byte range: end ({end}) < start ({start})"));
    }
    let len = data.len();
    let clamp = |bound: u64| usize::try_from(bound).map_or(len, |bound| bound.min(len));
    let from = clamp(start);
    let to = if unbounded { len } else { clamp(end.saturating_add(1)) };
    Ok(data.slice(from..to))
}

// Byte-range slicing for `get_data`; JetStream object store calls are covered
// by `tests/live.rs`.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_range_bounds() {
        let data = Bytes::from_static(b"hello world");
        assert_eq!(slice_range(&data, 0, 0).unwrap(), Bytes::from_static(b"hello world"));
        assert_eq!(slice_range(&data, 0, u64::MAX).unwrap(), Bytes::from_static(b"hello world"));
        assert_eq!(slice_range(&data, 6, 0).unwrap(), Bytes::from_static(b"world"));
        assert_eq!(slice_range(&data, 0, 4).unwrap(), Bytes::from_static(b"hello"));
        assert_eq!(slice_range(&data, 3, 3).unwrap(), Bytes::from_static(b"l"));
        assert_eq!(slice_range(&data, 6, 100).unwrap(), Bytes::from_static(b"world"));
        slice_range(&data, 5, 2).unwrap_err();
    }
}
