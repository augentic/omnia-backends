use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use bytes::Bytes;
use chrono::Utc;
use futures::{FutureExt, StreamExt};
use mongodb::Collection;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{self, Binary};
use mongodb::error::ErrorKind;
use omnia::FutureResult;
use omnia_wasi_blobstore::{BlobStoreClient, Container, ContainerMetadata, Error, ObjectMetadata};
use serde::{Deserialize, Serialize};

use crate::Client;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Blob {
    name: String,
    data: Binary,
    size: u64,
    created_at: u64,
}

impl BlobStoreClient for Client {
    fn create_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("creating container: {name}");
        let client = self.0.clone();

        async move {
            let db = client.default_database().ok_or_else(|| anyhow!("No default database"))?;
            if let Err(err) = db.create_collection(&name).await
                && !already_exists(&err)
            {
                return Err(err).context("creating container");
            }
            let collection = db.collection::<Blob>(&name);
            Ok(Arc::new(MongoDbContainer { name, collection }) as Arc<dyn Container>)
        }
        .boxed()
    }

    fn get_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("getting container: {name}");
        let client = self.0.clone();
        let exists = self.container_exists(name.clone());

        async move {
            if !exists.await? {
                anyhow::bail!(Error::missing_container(&name));
            }
            let db = client.default_database().ok_or_else(|| anyhow!("No default database"))?;
            let collection = db.collection::<Blob>(&name);
            Ok(Arc::new(MongoDbContainer { name, collection }) as Arc<dyn Container>)
        }
        .boxed()
    }

    fn delete_container(&self, name: String) -> FutureResult<()> {
        tracing::trace!("deleting container: {name}");
        let client = self.0.clone();

        async move {
            let db = client.default_database().ok_or_else(|| anyhow!("No default database"))?;
            db.collection::<Blob>(&name).drop().await.context("deleting container")
        }
        .boxed()
    }

    fn container_exists(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of container: {name}");
        let client = self.0.clone();

        async move {
            let db = client.default_database().ok_or_else(|| anyhow!("No default database"))?;
            let names = db
                .list_collection_names()
                .filter(bson::doc! { "name": name })
                .await
                .context("checking container existence")?;
            Ok(!names.is_empty())
        }
        .boxed()
    }
}

// `NamespaceExists`: the collection is already there
fn already_exists(err: &mongodb::error::Error) -> bool {
    matches!(err.kind.as_ref(), ErrorKind::Command(command) if command.code == 48)
}

#[derive(Debug)]
struct MongoDbContainer {
    name: String,
    collection: Collection<Blob>,
}

impl Container for MongoDbContainer {
    fn name(&self) -> anyhow::Result<String> {
        tracing::trace!("getting container name");
        Ok(self.name.clone())
    }

    fn info(&self) -> anyhow::Result<ContainerMetadata> {
        tracing::trace!("getting container info");
        Ok(ContainerMetadata {
            name: self.name.clone(),
            created_at: 0,
        })
    }

    fn get_data(&self, name: String, start: u64, end: u64) -> FutureResult<Option<Bytes>> {
        tracing::trace!("getting object data: {name}");
        let collection = self.collection.clone();

        async move {
            let Some(blob) = collection.find_one(bson::doc! { "name": name }).await? else {
                return Ok(None);
            };
            let sliced = slice_range(Bytes::from(blob.data.bytes), start, end)?;
            Ok(Some(sliced))
        }
        .boxed()
    }

    fn write_data(&self, name: String, data: Bytes) -> FutureResult<()> {
        tracing::trace!("writing object data: {name}");
        let collection = self.collection.clone();

        async move {
            let size = data.len() as u64;
            let blob = Blob {
                name: name.clone(),
                data: Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: data.to_vec(),
                },
                size,
                created_at: u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default(),
            };
            collection
                .replace_one(bson::doc! { "name": name }, blob)
                .upsert(true)
                .await
                .context("writing object")?;

            Ok(())
        }
        .boxed()
    }

    fn list_objects(&self) -> FutureResult<Vec<String>> {
        tracing::trace!("listing objects");
        let collection = self.collection.clone();

        async move {
            let mut list = collection.find(bson::doc! {}).await?;
            let mut names = vec![];

            while let Some(n) = list.next().await {
                match n {
                    Ok(blob) => names.push(blob.name),
                    Err(e) => tracing::warn!("issue listing object: {e}"),
                }
            }

            Ok(names)
        }
        .boxed()
    }

    fn delete_object(&self, name: String) -> FutureResult<()> {
        let collection = self.collection.clone();

        async move {
            collection.delete_one(bson::doc! { "name": name }).await.context("deleting object")?;
            Ok(())
        }
        .boxed()
    }

    fn has_object(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of object: {name}");
        let collection = self.collection.clone();

        async move { Ok(collection.find_one(bson::doc! { "name": name }).await?.is_some()) }.boxed()
    }

    fn object_info(&self, name: String) -> FutureResult<ObjectMetadata> {
        tracing::trace!("getting object info: {name}");
        let collection = self.collection.clone();

        async move {
            let Some(blob) = collection.find_one(bson::doc! { "name": name }).await? else {
                return Err(anyhow!("Object not found"));
            };

            Ok(ObjectMetadata {
                name: blob.name,
                container: collection.name().to_string(),
                size: blob.size,
                created_at: blob.created_at,
            })
        }
        .boxed()
    }
}

fn slice_range(data: Bytes, start: u64, end: u64) -> anyhow::Result<Bytes> {
    if start == 0 && (end == 0 || end == u64::MAX) {
        return Ok(data);
    }
    let unbounded = end == 0 || end == u64::MAX;
    if !unbounded && end < start {
        anyhow::bail!("invalid byte range: end ({end}) < start ({start})");
    }
    let len = data.len();
    let start_idx = usize::try_from(start).unwrap_or(len).min(len);
    let end_idx = if unbounded {
        len
    } else {
        usize::try_from(end).map_or(len, |e| e.saturating_add(1).min(len))
    };
    Ok(data.slice(start_idx..end_idx))
}
