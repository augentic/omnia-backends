//! `omnia:keyvalue` backed by a local directory tree.
//!
//! Writes are atomic and serialized per key. A value file holds only the
//! caller's bytes. An entry's deadline lives at the same path in a parallel
//! `keyvalue-ttl` tree.

use std::collections::{HashMap, HashSet};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, ensure};
use futures::FutureExt as _;
use omnia_wasi_keyvalue::{Bucket, Error, FutureResult, KeyValueClient};

use crate::{Client, blocking, collect, segment_ok};

type KeyLocks = Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>;
pub type LockRegistry = Arc<Mutex<HashMap<String, KeyLocks>>>;

impl KeyValueClient for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening bucket: {identifier}");
        if !self.buckets.contains(&identifier) {
            let err = Error::NotFound(format!("bucket `{identifier}` is not declared"));
            return async move { Err(err.into()) }.boxed();
        }

        let dir = self.root.join("keyvalue").join(&identifier);
        let ttl_dir = self.root.join("keyvalue-ttl").join(&identifier);
        let locks = {
            let mut buckets = self.locks.lock().expect("bucket lock registry poisoned");
            Arc::clone(buckets.entry(identifier.clone()).or_default())
        };

        on_disk(move || {
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

// A declared name holds only ASCII letters, digits, `_`, and `-`, the NATS
// bucket rule, so a declaration moves between backends unchanged.
pub fn declare(names: Vec<String>) -> Result<HashSet<String>> {
    for name in &names {
        let valid = !name.is_empty()
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'));
        ensure!(valid, "bucket name `{name}` must hold only ASCII letters, digits, `_`, and `-`");
    }
    Ok(names.into_iter().collect())
}

#[derive(Clone, Debug)]
struct FsBucket {
    name: String,
    dir: PathBuf,
    ttl_dir: PathBuf,
    locks: KeyLocks,
}

impl FsBucket {
    fn key_lock(&self, key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().expect("key lock registry poisoned");
        if let Some(existing) = locks.get(key).and_then(Weak::upgrade) {
            return existing;
        }
        locks.retain(|_, weak| weak.strong_count() > 0);
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key.to_string(), Arc::downgrade(&lock));
        lock
    }

    // The caller holds the key's lock.
    fn read_live(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = key_path(&self.dir, key)?;
        if self.expire(key)? {
            return Ok(None);
        }
        read(&path, key)
    }

    // Removes an entry whose deadline has passed, reporting whether it did.
    // The caller holds the key's lock.
    fn expire(&self, key: &str) -> Result<bool> {
        let expired = self.deadline(key)?.is_some_and(|deadline| deadline <= now_millis());
        if expired {
            self.remove(key)?;
        }
        Ok(expired)
    }

    // An unreadable deadline keeps the entry rather than losing it.
    fn deadline(&self, key: &str) -> Result<Option<u64>> {
        let Some(bytes) = read(&key_path(&self.ttl_dir, key)?, key)? else {
            return Ok(None);
        };
        Ok(std::str::from_utf8(&bytes).ok().and_then(|text| text.parse().ok()))
    }

    // Writes the entry's deadline as decimal unix milliseconds, or clears it.
    fn set_deadline(&self, key: &str, deadline: Option<u64>) -> Result<()> {
        match deadline {
            Some(deadline) => write(&self.ttl_dir, key, deadline.to_string().as_bytes()),
            None => remove(&key_path(&self.ttl_dir, key)?, key),
        }
    }

    fn remove(&self, key: &str) -> Result<()> {
        remove(&key_path(&self.ttl_dir, key)?, key)?;
        remove(&key_path(&self.dir, key)?, key)
    }
}

impl Bucket for FsBucket {
    fn get(&self, key: String) -> FutureResult<Option<Vec<u8>>> {
        tracing::trace!("getting key: {key} from bucket: {}", self.name);
        let bucket = self.clone();
        let lock = self.key_lock(&key);

        on_disk(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            bucket.read_live(&key)
        })
    }

    fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> FutureResult<()> {
        tracing::trace!("setting key: {key} in bucket: {}", self.name);
        let bucket = self.clone();
        let lock = self.key_lock(&key);

        on_disk(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            bucket.set_deadline(&key, deadline(ttl))?;
            write(&bucket.dir, &key, &value)
        })
    }

    fn delete(&self, key: String) -> FutureResult<()> {
        tracing::trace!("deleting key: {key} from bucket: {}", self.name);
        let bucket = self.clone();
        let lock = self.key_lock(&key);

        on_disk(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            bucket.remove(&key)
        })
    }

    fn exists(&self, key: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of key: {key} in bucket: {}", self.name);
        let bucket = self.clone();
        let lock = self.key_lock(&key);

        on_disk(move || {
            let _guard = lock.lock().expect("key lock poisoned");
            let path = key_path(&bucket.dir, &key)?;
            Ok(!bucket.expire(&key)? && path.is_file())
        })
    }

    fn keys(&self) -> FutureResult<Vec<String>> {
        tracing::trace!("listing keys in bucket: {}", self.name);
        let bucket = self.clone();

        on_disk(move || {
            let mut live = Vec::new();
            for key in collect(&bucket.dir, "")? {
                let Ok(path) = key_path(&bucket.dir, &key) else {
                    continue;
                };
                let lock = bucket.key_lock(&key);
                let _guard = lock.lock().expect("key lock poisoned");
                if !bucket.expire(&key)? && path.is_file() {
                    live.push(key);
                }
            }
            Ok(live)
        })
    }

    fn increment(&self, key: String, delta: i64, ttl: Option<Duration>) -> FutureResult<i64> {
        tracing::trace!("incrementing key: {key} in bucket: {}", self.name);
        let bucket = self.clone();
        let lock = self.key_lock(&key);

        on_disk(move || {
            let _guard = lock.lock().expect("key lock poisoned");

            let existing = bucket.read_live(&key)?;
            let incremented = add(&key, existing.as_deref(), delta)?;
            if existing.is_none() {
                bucket.set_deadline(&key, deadline(ttl))?;
            }
            write(&bucket.dir, &key, &incremented.to_be_bytes())?;
            Ok(incremented)
        })
    }

    fn compare_and_swap(
        &self, key: String, current: Option<Vec<u8>>, new: Vec<u8>, ttl: Option<Duration>,
    ) -> FutureResult<std::result::Result<(), Option<Vec<u8>>>> {
        tracing::trace!("swapping key: {key} in bucket: {}", self.name);
        let bucket = self.clone();
        let lock = self.key_lock(&key);

        on_disk(move || {
            let _guard = lock.lock().expect("key lock poisoned");

            let observed = bucket.read_live(&key)?;
            if observed != current {
                return Ok(Err(observed));
            }
            if observed.is_none() {
                bucket.set_deadline(&key, deadline(ttl))?;
            }
            write(&bucket.dir, &key, &new)?;
            Ok(Ok(()))
        })
    }
}

// Runs a task off the async runtime. A permission-denied I/O error anywhere
// in a failure's chain becomes `denied`.
fn on_disk<T: Send + 'static>(
    task: impl FnOnce() -> Result<T> + Send + 'static,
) -> FutureResult<T> {
    blocking(move || {
        task().map_err(|err| {
            let denied = err
                .downcast_ref::<io::Error>()
                .is_some_and(|io| io.kind() == io::ErrorKind::PermissionDenied);
            if denied { Error::Denied(format!("{err:#}")).into() } else { err }
        })
    })
}

// The deadline a TTL sets, in unix milliseconds rounded up, so an entry
// never expires early. A TTL past what a `u64` holds is stored without one.
fn deadline(ttl: Option<Duration>) -> Option<u64> {
    let millis = u64::try_from(ttl?.as_nanos().div_ceil(1_000_000)).ok()?;
    now_millis().checked_add(millis)
}

fn now_millis() -> u64 {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
}

// Counters are 8-byte big-endian `i64`s.
fn add(key: &str, current: Option<&[u8]>, delta: i64) -> Result<i64> {
    let base = match current {
        None => 0,
        Some(value) => {
            let bytes: [u8; 8] = value.try_into().map_err(|_len_mismatch| {
                let len = value.len();
                Error::Invalid(format!(
                    "`{key}` holds {len} bytes, not an 8-byte big-endian counter"
                ))
            })?;
            i64::from_be_bytes(bytes)
        }
    };
    let added = base.checked_add(delta);
    added.ok_or_else(|| Error::Invalid(format!("incrementing `{key}` by {delta} overflows")).into())
}

fn read(path: &Path, key: &str) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(data) => Ok(Some(data)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("reading `{key}`")),
    }
}

fn write(dir: &Path, key: &str, value: &[u8]) -> Result<()> {
    let path = key_path(dir, key)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("creating parents for `{key}`"))?;
    }
    let mut temp = tempfile::NamedTempFile::new_in(dir)
        .with_context(|| format!("creating temp file for `{key}`"))?;
    temp.write_all(value).with_context(|| format!("writing `{key}`"))?;
    temp.as_file().sync_all().with_context(|| format!("syncing `{key}`"))?;
    temp.persist(&path).map_err(|err| err.error).with_context(|| format!("persisting `{key}`"))?;
    Ok(())
}

fn remove(path: &Path, key: &str) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("deleting `{key}`")),
    }
}

fn key_path(dir: &Path, key: &str) -> Result<PathBuf> {
    if !key.is_empty() && key.split('/').all(segment_ok) {
        Ok(dir.join(key))
    } else {
        Err(Error::Invalid(format!("key `{key}` must be a bucket-relative path")).into())
    }
}
