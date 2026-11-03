//! `wasi-keyvalue` backed by a local directory tree.
//!
//! Writes are atomic and serialized per key. Raw value bytes live under
//! `<root>/keyvalue/<bucket>/<key>`; optional per-key expiry timestamps live in
//! the parallel `<root>/keyvalue-ttl/<bucket>/<key>` tree so stored values stay
//! byte-transparent while TTLs survive across handles.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, anyhow, bail};
use omnia_backend::FutureResult;
use omnia_backend::keyvalue::{Bucket, KeyValueClient};

use crate::{Client, blocking, collect, segment_ok};

type KeyLocks = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;
pub type LockRegistry = Arc<Mutex<HashMap<String, KeyLocks>>>;

impl KeyValueClient for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening bucket: {identifier}");
        let kv_root = self.root.join("keyvalue");
        let ttl_root = self.root.join("keyvalue-ttl");

        let locks = {
            let mut buckets = self.locks.lock().expect("bucket lock registry poisoned");
            Arc::clone(buckets.entry(identifier.clone()).or_default())
        };

        blocking(move || {
            let dir = bucket_dir(&kv_root, &identifier)?;
            let ttl_dir = bucket_dir(&ttl_root, &identifier)?;
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("creating bucket `{identifier}`"))?;

            Ok(Arc::new(FsBucket {
                name: identifier,
                dir,
                ttl_dir,
                locks,
            }) as Arc<dyn Bucket>)
        })
    }
}

#[derive(Debug)]
struct FsBucket {
    name: String,
    dir: PathBuf,
    ttl_dir: PathBuf,
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
        let ttl_dir = self.ttl_dir.clone();
        let lock = self.key_lock(&key);

        blocking(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            read_live(&dir, &ttl_dir, &key)
        })
    }

    fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> FutureResult<()> {
        tracing::trace!("setting key: {key} in bucket: {}", self.name);
        let dir = self.dir.clone();
        let ttl_dir = self.ttl_dir.clone();
        let lock = self.key_lock(&key);

        blocking(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            write_value(&dir, &key, &value)?;
            write_ttl(&ttl_dir, &key, ttl)
        })
    }

    fn delete(&self, key: String) -> FutureResult<()> {
        tracing::trace!("deleting key: {key} from bucket: {}", self.name);
        let dir = self.dir.clone();
        let ttl_dir = self.ttl_dir.clone();
        let lock = self.key_lock(&key);

        blocking(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            remove_entry(&dir, &ttl_dir, &key)
        })
    }

    fn exists(&self, key: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of key: {key} in bucket: {}", self.name);
        let dir = self.dir.clone();
        let ttl_dir = self.ttl_dir.clone();
        let lock = self.key_lock(&key);

        blocking(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            Ok(read_live(&dir, &ttl_dir, &key)?.is_some())
        })
    }

    fn keys(&self) -> FutureResult<Vec<String>> {
        tracing::trace!("listing keys in bucket: {}", self.name);
        let dir = self.dir.clone();
        let ttl_dir = self.ttl_dir.clone();

        blocking(move || {
            let candidates = collect(&dir, "")?;
            let mut live = Vec::with_capacity(candidates.len());
            for key in candidates {
                if is_expired(&ttl_dir, &key)? {
                    let _ = remove_entry(&dir, &ttl_dir, &key);
                } else {
                    live.push(key);
                }
            }
            Ok(live)
        })
    }

    fn increment(&self, key: String, delta: i64) -> FutureResult<i64> {
        tracing::trace!("incrementing key: {key} in bucket: {}", self.name);
        let dir = self.dir.clone();
        let ttl_dir = self.ttl_dir.clone();
        let lock = self.key_lock(&key);

        blocking(move || {
            let _guard = lock.lock().expect("key lock poisoned");

            let base = match read_live(&dir, &ttl_dir, &key)? {
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

    fn cas(
        &self, key: String, expected: Option<Vec<u8>>, value: Vec<u8>,
    ) -> FutureResult<Result<(), Option<Vec<u8>>>> {
        tracing::trace!("swapping key: {key} in bucket: {}", self.name);
        let dir = self.dir.clone();
        let ttl_dir = self.ttl_dir.clone();
        let lock = self.key_lock(&key);

        blocking(move || {
            let _guard = lock.lock().expect("key lock poisoned");

            let observed = read_live(&dir, &ttl_dir, &key)?;
            if observed == expected {
                write_value(&dir, &key, &value)?;
                Ok(Ok(()))
            } else {
                Ok(Err(observed))
            }
        })
    }
}

fn now_millis() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
}

fn is_expired(ttl_dir: &Path, key: &str) -> Result<bool> {
    let path = key_path(ttl_dir, key)?;
    match std::fs::read(&path) {
        Ok(bytes) => {
            let Ok(arr) = <[u8; 16]>::try_from(bytes.as_slice()) else {
                return Ok(false);
            };
            Ok(now_millis() >= u128::from_be_bytes(arr))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| format!("reading ttl for key `{key}`")),
    }
}

fn write_ttl(ttl_dir: &Path, key: &str, ttl: Option<Duration>) -> Result<()> {
    match ttl {
        Some(ttl) => {
            let expires_at = now_millis().saturating_add(ttl.as_millis());
            std::fs::create_dir_all(ttl_dir)
                .with_context(|| format!("creating ttl dir for key `{key}`"))?;
            write_value(ttl_dir, key, &expires_at.to_be_bytes())
        }
        None => remove_file(&key_path(ttl_dir, key)?, key),
    }
}

fn remove_entry(dir: &Path, ttl_dir: &Path, key: &str) -> Result<()> {
    let path = key_path(dir, key)?;
    let ttl_path = key_path(ttl_dir, key)?;
    let _ = remove_file(&ttl_path, key);
    remove_file(&path, key)
}

fn remove_file(path: &Path, key: &str) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("deleting key `{key}`")),
    }
}

fn read_live(dir: &Path, ttl_dir: &Path, key: &str) -> Result<Option<Vec<u8>>> {
    let path = key_path(dir, key)?;
    if is_expired(ttl_dir, key)? {
        remove_entry(dir, ttl_dir, key)?;
        return Ok(None);
    }
    read_value(&path, key)
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
