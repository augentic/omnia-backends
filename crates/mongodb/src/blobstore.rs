use std::fmt::Debug;
use std::ops::Range;
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
use omnia_wasi_blobstore::{
    BlobStoreClient, Container, ContainerMetadata, Error, ObjectMetadata, Page,
};
use serde::{Deserialize, Serialize};

use crate::Client;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Blob {
    name: String,
    data: Binary,
    size: u64,
    created_at: u64,
}

// a `Blob` read without its data
#[derive(Debug, Deserialize)]
struct BlobInfo {
    name: String,
    size: u64,
    created_at: u64,
}

impl BlobInfo {
    fn metadata(self, container: String) -> ObjectMetadata {
        ObjectMetadata {
            name: self.name,
            container,
            created_at: self.created_at,
            size: self.size,
        }
    }
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

    fn get_data(&self, name: String, range: Range<u64>) -> FutureResult<Option<Bytes>> {
        tracing::trace!("getting object data: {name}");
        let collection = self.collection.clone();

        async move {
            let Some(blob) = collection.find_one(bson::doc! { "name": name }).await? else {
                return Ok(None);
            };
            let data = Bytes::from(blob.data.bytes);
            let len = data.len();
            let clamp = |bound: u64| usize::try_from(bound).map_or(len, |bound| bound.min(len));
            let end = clamp(range.end);
            Ok(Some(data.slice(clamp(range.start).min(end)..end)))
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

    fn list_objects(&self, prefix: String, after: Option<String>) -> FutureResult<Page> {
        tracing::trace!("listing objects");
        let collection = self.collection.clone_with_type::<BlobInfo>();
        let container = self.name.clone();

        async move {
            let mut list = collection
                .find(bson::doc! {})
                .projection(bson::doc! { "data": 0 })
                .await
                .context("listing objects")?;
            let mut objects = vec![];

            while let Some(n) = list.next().await {
                let info = match n {
                    Ok(info) => info,
                    Err(e) => {
                        tracing::warn!("issue listing object: {e}");
                        continue;
                    }
                };
                if info.name.starts_with(&prefix)
                    && after.as_ref().is_none_or(|after| &info.name > after)
                {
                    objects.push(info.metadata(container.clone()));
                }
            }

            objects.sort_unstable_by(|a, b| a.name.cmp(&b.name));
            Ok(Page { objects, next: None })
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

    fn object_info(&self, name: String) -> FutureResult<Option<ObjectMetadata>> {
        tracing::trace!("getting object info: {name}");
        let collection = self.collection.clone_with_type::<BlobInfo>();
        let container = self.name.clone();

        async move {
            let info = collection
                .find_one(bson::doc! { "name": name })
                .projection(bson::doc! { "data": 0 })
                .await
                .context("getting object info")?;
            Ok(info.map(|info| info.metadata(container)))
        }
        .boxed()
    }
}
