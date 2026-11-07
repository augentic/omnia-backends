use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use futures::FutureExt;
use omnia_backend::FutureResult;
use omnia_backend::keyvalue::{Bucket, KeyValueClient};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::Client;

// Server-side compare-and-set: the compare and the write are one atomic
// step, preserving any remaining TTL via `KEEPTTL`. `WATCH`/`MULTI` is
// not an option on a multiplexed connection. Returns `{swapped, present, current}`.
const CAS: &str = r"
local current = redis.call('GET', KEYS[1])
local matches
if ARGV[1] == '1' then
  matches = current ~= false and current == ARGV[2]
else
  matches = current == false
end
if matches then
  redis.call('SET', KEYS[1], ARGV[3], 'KEEPTTL')
  return {1, 0, ''}
end
if current == false then
  return {0, 0, ''}
end
return {0, 1, current}
";

impl KeyValueClient for Client {
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

// `ConnectionManager` has no `Debug`, and the bucket derives it.
struct Conn(ConnectionManager);

impl Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager").finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct RedisBucket {
    identifier: String,
    conn: Conn,
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

    fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> FutureResult<()> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();

        async move {
            match ttl {
                Some(ttl) => conn.set_ex(&key, value, ttl.as_secs().max(1)).await,
                None => conn.set(&key, value).await,
            }
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
        let identifier = self.identifier.clone();
        let pattern = format!("{identifier}:*");
        async move {
            let mut cursor: u64 = 0;
            let mut keys = Vec::new();
            loop {
                let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                    .cursor_arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .query_async(&mut conn)
                    .await
                    .with_context(|| format!("failed to list keys for {pattern}"))?;
                for raw_key in batch {
                    if let Some(key) = strip_bucket_prefix(&identifier, &raw_key) {
                        keys.push(key);
                    }
                }
                if next_cursor == 0 {
                    break;
                }
                cursor = next_cursor;
            }
            Ok(keys)
        }
        .boxed()
    }

    fn increment(&self, key: String, delta: i64) -> FutureResult<i64> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();
        async move {
            conn.incr(&key, delta).await.with_context(|| format!("failed to increment {key}"))
        }
        .boxed()
    }

    fn cas(
        &self, key: String, expected: Option<Vec<u8>>, value: Vec<u8>,
    ) -> FutureResult<Result<(), Option<Vec<u8>>>> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();

        async move {
            let has_expected = i64::from(expected.is_some());
            let expected = expected.unwrap_or_default();

            let (swapped, present, observed): (i64, i64, Vec<u8>) = redis::Script::new(CAS)
                .key(&key)
                .arg(has_expected)
                .arg(expected)
                .arg(&value)
                .invoke_async(&mut conn)
                .await
                .with_context(|| format!("failed to swap {key}"))?;

            match (swapped, present) {
                (1, _) => Ok(Ok(())),
                (0, 1) => Ok(Err(Some(observed))),
                (0, 0) => Ok(Err(None)),
                other => bail!("unexpected swap response for {key}: {other:?}"),
            }
        }
        .boxed()
    }
}

fn strip_bucket_prefix(identifier: &str, raw_key: &str) -> Option<String> {
    raw_key.strip_prefix(identifier).and_then(|rest| rest.strip_prefix(':')).map(ToOwned::to_owned)
}
