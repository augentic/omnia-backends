//! `wasi-blobstore` backed by a local directory tree.
//!
//! A container is a directory only `create_container` makes; writes replace files atomically.

use std::io::Write as _;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use anyhow::{Context as _, Result, bail};
use bytes::Bytes;
use omnia::FutureResult;
use omnia_wasi_blobstore::{
    BlobStoreClient, Container, ContainerMetadata, Error, ObjectMetadata, Page,
};

use crate::{Client, blocking, collect, segment_ok};

impl BlobStoreClient for Client {
    fn create_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("creating container: {name}");
        let root = self.root.join("blobstore");

        blocking(move || {
            let dir = container_dir(&root, &name)?;
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("creating container `{name}`"))?;
            Ok(Arc::new(FsContainer { name, dir }) as Arc<dyn Container>)
        })
    }

    fn get_container(&self, name: String) -> FutureResult<Arc<dyn Container>> {
        tracing::trace!("getting container: {name}");
        let root = self.root.join("blobstore");

        blocking(move || {
            let dir = container_dir(&root, &name)?;
            if !dir.is_dir() {
                bail!(Error::missing_container(&name));
            }
            Ok(Arc::new(FsContainer { name, dir }) as Arc<dyn Container>)
        })
    }

    fn delete_container(&self, name: String) -> FutureResult<()> {
        tracing::trace!("deleting container: {name}");
        let root = self.root.join("blobstore");

        blocking(move || {
            let dir = container_dir(&root, &name)?;
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err).with_context(|| format!("deleting container `{name}`")),
            }
        })
    }

    fn container_exists(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of container: {name}");
        let root = self.root.join("blobstore");

        blocking(move || Ok(container_dir(&root, &name)?.is_dir()))
    }
}

#[derive(Debug)]
struct FsContainer {
    name: String,
    dir: PathBuf,
}

impl Container for FsContainer {
    fn name(&self) -> Result<String> {
        Ok(self.name.clone())
    }

    fn info(&self) -> Result<ContainerMetadata> {
        let meta = std::fs::metadata(&self.dir)
            .with_context(|| format!("reading container `{}`", self.name))?;
        Ok(ContainerMetadata {
            name: self.name.clone(),
            created_at: created_secs(&meta),
        })
    }

    fn get_data(&self, name: String, range: Range<u64>) -> FutureResult<Option<Bytes>> {
        tracing::trace!("getting object: {name} from container: {}", self.name);
        let dir = self.dir.clone();

        blocking(move || {
            let path = object_path(&dir, &name)?;
            let data = match std::fs::read(&path) {
                Ok(data) => Bytes::from(data),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(err) => return Err(err).with_context(|| format!("reading object `{name}`")),
            };

            let len = data.len();
            let clamp = |bound: u64| usize::try_from(bound).map_or(len, |bound| bound.min(len));
            let end = clamp(range.end);
            Ok(Some(data.slice(clamp(range.start).min(end)..end)))
        })
    }

    fn write_data(&self, name: String, data: Bytes) -> FutureResult<()> {
        tracing::trace!("writing object: {name} to container: {}", self.name);
        let container = self.name.clone();
        let dir = self.dir.clone();

        blocking(move || {
            if !dir.is_dir() {
                bail!(Error::missing_container(&container));
            }
            let path = object_path(&dir, &name)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("creating parents for object `{name}`"))?;
            }
            let mut temp = tempfile::NamedTempFile::new_in(&dir)
                .with_context(|| format!("creating temp file for object `{name}`"))?;
            temp.write_all(&data).with_context(|| format!("writing object `{name}`"))?;
            temp.as_file().sync_all().with_context(|| format!("syncing object `{name}`"))?;
            temp.persist(&path).with_context(|| format!("persisting object `{name}`"))?;
            Ok(())
        })
    }

    fn list_objects(&self, prefix: String, after: Option<String>) -> FutureResult<Page> {
        tracing::trace!("listing objects in container: {}", self.name);
        let dir = self.dir.clone();
        let container = self.name.clone();

        blocking(move || {
            let mut names = collect(&dir, "")?;
            names.retain(|name| {
                name.starts_with(&prefix) && after.as_ref().is_none_or(|after| name > after)
            });
            names.sort_unstable();
            let objects = names
                .into_iter()
                .map(|name| stat(&dir, &container, name))
                .filter_map(Result::transpose)
                .collect::<Result<_>>()?;
            Ok(Page { objects, next: None })
        })
    }

    fn delete_object(&self, name: String) -> FutureResult<()> {
        tracing::trace!("deleting object: {name} from container: {}", self.name);
        let dir = self.dir.clone();

        blocking(move || {
            let path = object_path(&dir, &name)?;
            match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err).with_context(|| format!("deleting object `{name}`")),
            }
        })
    }

    fn has_object(&self, name: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of object: {name} in container: {}", self.name);
        let dir = self.dir.clone();

        blocking(move || Ok(object_path(&dir, &name)?.is_file()))
    }

    fn object_info(&self, name: String) -> FutureResult<Option<ObjectMetadata>> {
        tracing::trace!("getting info for object: {name} in container: {}", self.name);
        let dir = self.dir.clone();
        let container = self.name.clone();

        blocking(move || stat(&dir, &container, name))
    }
}

fn container_dir(root: &Path, name: &str) -> Result<PathBuf> {
    if segment_ok(name) {
        Ok(root.join(name))
    } else {
        bail!("container name `{name}` must be a plain directory name");
    }
}

fn object_path(dir: &Path, name: &str) -> Result<PathBuf> {
    if !name.is_empty() && name.split('/').all(segment_ok) {
        Ok(dir.join(name))
    } else {
        bail!("object name `{name}` must be a container-relative path");
    }
}

// a name that is not a file, such as one deleted since it was listed, is absent
fn stat(dir: &Path, container: &str, name: String) -> Result<Option<ObjectMetadata>> {
    let path = object_path(dir, &name)?;
    let meta = match std::fs::metadata(&path) {
        Ok(meta) if meta.is_file() => meta,
        Ok(_) => return Ok(None),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("reading object `{name}`")),
    };
    Ok(Some(ObjectMetadata {
        name,
        container: container.to_owned(),
        created_at: created_secs(&meta),
        size: meta.len(),
    }))
}

fn created_secs(meta: &std::fs::Metadata) -> u64 {
    meta.created()
        .or_else(|_err| meta.modified())
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |elapsed| elapsed.as_secs())
}
