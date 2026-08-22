//! `wasi-keyvalue` backed by a local directory tree.
//!
//! Writes are atomic and serialized per key.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context as _, Result, anyhow, bail};
use omnia_wasi_keyvalue::{Bucket, Cas, FutureResult, WasiKeyValueCtx};

use crate::{Client, blocking, collect, segment_ok};

type KeyLocks = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;
pub type LockRegistry = Arc<Mutex<HashMap<String, KeyLocks>>>;

impl WasiKeyValueCtx for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening bucket: {identifier}");
        let root = self.root.join("keyvalue");

        let locks = {
            let mut buckets = self.locks.lock().expect("bucket lock registry poisoned");
            Arc::clone(buckets.entry(identifier.clone()).or_default())
        };

        blocking(move || {
            let dir = bucket_dir(&root, &identifier)?;
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("creating bucket `{identifier}`"))?;
            Ok(Arc::new(FsBucket {
                name: identifier,
                dir,
                locks,
            }) as Arc<dyn Bucket>)
        })
    }
}

#[derive(Debug)]
struct FsBucket {
    name: String,
    dir: PathBuf,
    locks: KeyLocks,
}

impl FsBucket {
    fn key_lock(&self, key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().expect("key lock registry poisoned");
        Arc::clone(locks.entry(key.to_string()).or_default())
    }
}

impl Bucket for FsBucket {
    fn get(&self, key: String) -> FutureResult<Option<Vec<u8>>> {
        tracing::trace!("getting key: {key} from bucket: {}", self.name);
        let dir = self.dir.clone();

        blocking(move || read_value(&key_path(&dir, &key)?, &key))
    }

    fn set(&self, key: String, value: Vec<u8>) -> FutureResult<()> {
        tracing::trace!("setting key: {key} in bucket: {}", self.name);
        let dir = self.dir.clone();
        let lock = self.key_lock(&key);

        blocking(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            write_value(&dir, &key, &value)
        })
    }

    fn delete(&self, key: String) -> FutureResult<()> {
        tracing::trace!("deleting key: {key} from bucket: {}", self.name);
        let dir = self.dir.clone();
        let lock = self.key_lock(&key);

        blocking(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            let path = key_path(&dir, &key)?;
            match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err).with_context(|| format!("deleting key `{key}`")),
            }
        })
    }

    fn exists(&self, key: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of key: {key} in bucket: {}", self.name);
        let dir = self.dir.clone();

        blocking(move || Ok(key_path(&dir, &key)?.is_file()))
    }

    fn keys(&self) -> FutureResult<Vec<String>> {
        tracing::trace!("listing keys in bucket: {}", self.name);
        let dir = self.dir.clone();

        blocking(move || {
            let mut names = Vec::new();
            collect(&dir, "", &mut names)?;
            Ok(names)
        })
    }

    fn increment(&self, key: String, delta: i64) -> FutureResult<i64> {
        tracing::trace!("incrementing key: {key} in bucket: {}", self.name);
        let dir = self.dir.clone();
        let lock = self.key_lock(&key);

        blocking(move || {
            let path = key_path(&dir, &key)?;
            let _guard = lock.lock().expect("key lock poisoned");

            let base = match read_value(&path, &key)? {
                None => 0,
                Some(value) => decode_counter(&key, &value)?,
            };
            let incremented = base
                .checked_add(delta)
                .with_context(|| format!("incrementing `{key}` by {delta} overflows i64"))?;
            write_value(&dir, &key, &incremented.to_be_bytes())?;
            Ok(incremented)
        })
    }

    fn swap(&self, cas: Cas, value: Vec<u8>) -> FutureResult<Result<(), Cas>> {
        tracing::trace!("swapping key: {} in bucket: {}", cas.key, self.name);
        let dir = self.dir.clone();
        let lock = self.key_lock(&cas.key);

        blocking(move || {
            let path = key_path(&dir, &cas.key)?;
            let _guard = lock.lock().expect("key lock poisoned");

            let observed = read_value(&path, &cas.key)?;
            if observed == cas.current {
                write_value(&dir, &cas.key, &value)?;
                Ok(Ok(()))
            } else {
                Ok(Err(Cas {
                    current: observed,
                    ..cas
                }))
            }
        })
    }
}

fn read_value(path: &Path, key: &str) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(data) => Ok(Some(data)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("reading key `{key}`")),
    }
}

fn write_value(dir: &Path, key: &str, value: &[u8]) -> Result<()> {
    let path = key_path(dir, key)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating parents for key `{key}`"))?;
    }
    let mut temp = tempfile::NamedTempFile::new_in(dir)
        .with_context(|| format!("creating temp file for key `{key}`"))?;
    temp.write_all(value).with_context(|| format!("writing key `{key}`"))?;
    temp.as_file().sync_all().with_context(|| format!("syncing key `{key}`"))?;
    temp.persist(&path).with_context(|| format!("persisting key `{key}`"))?;
    Ok(())
}

// `increment` stores counters as big-endian `i64` values.
fn decode_counter(key: &str, value: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_len_mismatch| {
        anyhow!("value at `{key}` is {} bytes, not an 8-byte big-endian integer", value.len())
    })?;
    Ok(i64::from_be_bytes(bytes))
}

fn bucket_dir(root: &Path, name: &str) -> Result<PathBuf> {
    if segment_ok(name) {
        Ok(root.join(name))
    } else {
        bail!("bucket name `{name}` must be a plain directory name");
    }
}

fn key_path(dir: &Path, key: &str) -> Result<PathBuf> {
    if !key.is_empty() && key.split('/').all(segment_ok) {
        Ok(dir.join(key))
    } else {
        bail!("key `{key}` must be a bucket-relative path");
    }
}
