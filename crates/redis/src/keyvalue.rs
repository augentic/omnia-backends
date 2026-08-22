//! Key-value implementation for the Redis backend.
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Context, bail};
use futures::FutureExt;
use omnia_wasi_keyvalue::{Bucket, Cas, FutureResult, WasiKeyValueCtx};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::Client;

const TTL_DAY: u64 = 24 * 60 * 60; // 1 day

/// Server-side compare-and-set: the compare and the write are one atomic
/// step. `WATCH`/`MULTI` is not an option on a multiplexed connection.
/// Returns `{swapped, present, current}`.
const SWAP: &str = r"
local current = redis.call('GET', KEYS[1])
local matches
if ARGV[1] == '1' then
  matches = current ~= false and current == ARGV[2]
else
  matches = current == false
end
if matches then
  redis.call('SET', KEYS[1], ARGV[3], 'EX', ARGV[4])
  return {1, 0, ''}
end
if current == false then
  return {0, 0, ''}
end
return {0, 1, current}
";

/// `wasi-keyvalue` implementation backed by Redis.
impl WasiKeyValueCtx for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening redis bucket: {}", identifier);
        let conn = self.0.clone();

        async move {
            let bucket = RedisBucket {
                identifier,
                conn: Conn(conn.clone()),
            };
            Ok(Arc::new(bucket) as Arc<dyn Bucket>)
        }
        .boxed()
    }
}

/// Wrapper around [`ConnectionManager`] to implement [`Debug`].
pub struct Conn(ConnectionManager);

impl Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager").finish_non_exhaustive()
    }
}

/// A key-value bucket backed by Redis, namespaced by identifier.
#[derive(Debug)]
pub struct RedisBucket {
    /// Bucket identifier used as key prefix.
    pub identifier: String,
    /// Redis connection.
    pub conn: Conn,
}

impl Bucket for RedisBucket {
    fn get(&self, key: String) -> FutureResult<Option<Vec<u8>>> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();
        async move {
            conn.get(key.clone()).await.with_context(|| format!("failed to get value for {key}"))
        }
        .boxed()
    }

    fn set(&self, key: String, value: Vec<u8>) -> FutureResult<()> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();

        async move {
            conn.set_ex(&key, value, TTL_DAY)
                .await
                .with_context(|| format!("failed to set value for {key}"))
        }
        .boxed()
    }

    fn delete(&self, key: String) -> FutureResult<()> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();
        async move {
            conn.del(key.clone()).await.with_context(|| format!("failed to delete value for {key}"))
        }
        .boxed()
    }

    fn exists(&self, key: String) -> FutureResult<bool> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();
        async move {
            conn.exists(key.clone())
                .await
                .with_context(|| format!("failed to check existence of key {key}"))
        }
        .boxed()
    }

    fn keys(&self) -> FutureResult<Vec<String>> {
        let mut conn = self.conn.0.clone();
        let pattern = format!("{}:*", self.identifier);
        async move {
            conn.keys(pattern.clone())
                .await
                .with_context(|| format!("failed to list keys for {pattern}"))
        }
        .boxed()
    }

    fn increment(&self, key: String, delta: i64) -> FutureResult<i64> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();
        async move {
            conn.incr(key.clone(), delta)
                .await
                .with_context(|| format!("failed to increment {key}"))
        }
        .boxed()
    }

    fn swap(&self, cas: Cas, value: Vec<u8>) -> FutureResult<Result<(), Cas>> {
        let key = format!("{}:{}", self.identifier, cas.key);
        let mut conn = self.conn.0.clone();

        async move {
            let expected = cas.current.clone().unwrap_or_default();
            let has_expected = i64::from(cas.current.is_some());

            let (swapped, present, observed): (i64, i64, Vec<u8>) = redis::Script::new(SWAP)
                .key(&key)
                .arg(has_expected)
                .arg(expected)
                .arg(&value)
                .arg(TTL_DAY)
                .invoke_async(&mut conn)
                .await
                .with_context(|| format!("failed to swap {key}"))?;

            match (swapped, present) {
                (1, _) => Ok(Ok(())),
                // Stale snapshot: refresh the handle at the observed value so
                // the caller can retry, as the WIT contract requires.
                (0, 1) => Ok(Err(Cas {
                    current: Some(observed),
                    ..cas
                })),
                (0, 0) => Ok(Err(Cas { current: None, ..cas })),
                other => bail!("unexpected swap response for {key}: {other:?}"),
            }
        }
        .boxed()
    }
}
