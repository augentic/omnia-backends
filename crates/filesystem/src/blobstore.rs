//! `wasi-blobstore` implementation over a local directory tree.
//!
//! Containers map to subdirectories of the `blobstore/` subtree of the
//! client root; object names may contain `/` and map to nested paths
//! beneath their container directory, so clients encode their own
//! sharding in names. Writes are temp-file + atomic-rename: an object
//! is either fully visible or absent, never torn, and concurrent
//! same-name writes are benign (last rename wins).

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use anyhow::{Context as _, Result, anyhow, bail};
use omnia_wasi_blobstore::{
    Bytes, Container, ContainerMetadata, FutureResult, ObjectMetadata, WasiBlobstoreCtx,
};

use crate::{Client, blocking, collect, segment_ok};

impl WasiBlobstoreCtx for Client {
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
                bail!("container not found: {name}");
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

    fn get_data(&self, name: String, start: u64, end: u64) -> FutureResult<Option<Bytes>> {
        tracing::trace!("getting object: {name} from container: {}", self.name);
        let dir = self.dir.clone();

        blocking(move || {
            let path = object_path(&dir, &name)?;
            let data = match std::fs::read(&path) {
                Ok(data) => Bytes::from(data),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(err) => return Err(err).with_context(|| format!("reading object `{name}`")),
            };

            let unbounded = end == 0 || end == u64::MAX;
            if !unbounded && end < start {
                return Err(anyhow!("invalid byte range: end ({end}) < start ({start})"));
            }
            let len = data.len() as u64;
            let from = start.min(len);
            let to = if unbounded { len } else { end.saturating_add(1).min(len) };
            #[allow(clippy::cast_possible_truncation)]
            let range = from as usize..to as usize;
            Ok(Some(data.slice(range)))
        })
    }

    fn write_data(&self, name: String, data: Bytes) -> FutureResult<()> {
        tracing::trace!("writing object: {name} to container: {}", self.name);
        let dir = self.dir.clone();

        blocking(move || {
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

    fn list_objects(&self) -> FutureResult<Vec<String>> {
        tracing::trace!("listing objects in container: {}", self.name);
        let dir = self.dir.clone();

        blocking(move || {
            let mut names = Vec::new();
            collect(&dir, "", &mut names)?;
            Ok(names)
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

    fn object_info(&self, name: String) -> FutureResult<ObjectMetadata> {
        tracing::trace!("getting info for object: {name} in container: {}", self.name);
        let dir = self.dir.clone();
        let container = self.name.clone();

        blocking(move || {
            let path = object_path(&dir, &name)?;
            let meta = match std::fs::metadata(&path) {
                Ok(meta) => meta,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    bail!("object not found: {name}");
                }
                Err(err) => return Err(err).with_context(|| format!("reading object `{name}`")),
            };
            Ok(ObjectMetadata {
                name,
                container,
                created_at: created_secs(&meta),
                size: meta.len(),
            })
        })
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

fn created_secs(meta: &std::fs::Metadata) -> u64 {
    meta.created()
        .or_else(|_err| meta.modified())
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |elapsed| elapsed.as_secs())
}
