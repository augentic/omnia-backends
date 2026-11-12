use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, ensure};
use futures::FutureExt;
use omnia_wasi_keyvalue::{Bucket, Error, FutureResult, KeyValueClient};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, ErrorKind, RedisError, ServerErrorKind};

use crate::Client;

impl KeyValueClient for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening redis bucket: {identifier}");
        let bucket = if self.buckets.contains(&identifier) {
            let conn = Conn(self.conn.clone());
            Ok(Arc::new(RedisBucket { identifier, conn }) as Arc<dyn Bucket>)
        } else {
            Err(Error::NotFound(format!("bucket `{identifier}` is not declared")).into())
        };
        async move { bucket }.boxed()
    }
}

// A declared name holds only ASCII letters, digits, `_`, and `-`: the NATS
// bucket rule, so a declaration moves between backends unchanged, and no
// bucket's `name:` prefix can reach into another's keys.
pub fn declare(names: Vec<String>) -> Result<HashSet<String>> {
    for name in &names {
        let valid = !name.is_empty()
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'));
        ensure!(valid, "bucket name `{name}` must hold only ASCII letters, digits, `_`, and `-`");
    }
    Ok(names.into_iter().collect())
}

// `ConnectionManager` has no `Debug`, and the bucket derives it.
struct Conn(ConnectionManager);

impl Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager").finish_non_exhaustive()
    }
}

// Server-side compare-and-set: the compare and the write are one atomic
// step. `WATCH`/`MULTI` is not an option on a multiplexed connection. A
// swap that creates the key applies the TTL in `ARGV[4]`, and one that
// replaces a value keeps its TTL. Returns `{swapped, present, current}`.
const COMPARE_AND_SWAP: &str = r"
local current = redis.call('GET', KEYS[1])
local matches
if ARGV[1] == '1' then
  matches = current ~= false and current == ARGV[2]
else
  matches = current == false
end
if not matches then
  if current == false then
    return {0, 0, ''}
  end
  return {0, 1, current}
end
if current ~= false then
  redis.call('SET', KEYS[1], ARGV[3], 'KEEPTTL')
elseif ARGV[4] ~= '' then
  redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
else
  redis.call('SET', KEYS[1], ARGV[3])
end
return {1, 0, ''}
";

// `INCRBY` keeps a key's TTL, so creating the counter at zero with the TTL
// in `ARGV[2]` first applies it only to a counter the call creates.
const INCREMENT: &str = r"
if ARGV[2] ~= '' then
  redis.call('SET', KEYS[1], '0', 'PX', ARGV[2], 'NX')
end
local counted = redis.pcall('INCRBY', KEYS[1], ARGV[1])
if type(counted) == 'table' and counted.err then
  return redis.error_reply('NOTCOUNTER ' .. counted.err)
end
return counted
";

// the error code `INCREMENT` replies with for a value that is not a counter
const NOT_A_COUNTER: &str = "NOTCOUNTER";

#[derive(Debug)]
struct RedisBucket {
    identifier: String,
    conn: Conn,
}

impl RedisBucket {
    fn key(&self, key: &str) -> String {
        format!("{}:{key}", self.identifier)
    }
}

impl Bucket for RedisBucket {
    fn get(&self, key: String) -> FutureResult<Option<Vec<u8>>> {
        let key = self.key(&key);
        let mut conn = self.conn.0.clone();
        async move { conn.get(&key).await.map_err(|err| classify(err, format!("getting `{key}`"))) }
            .boxed()
    }

    fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> FutureResult<()> {
        let key = self.key(&key);
        let mut conn = self.conn.0.clone();

        async move {
            let mut set = redis::cmd("SET");
            set.arg(&key).arg(value);
            if let Some(millis) = px(ttl) {
                set.arg("PX").arg(millis);
            }
            set.query_async(&mut conn)
                .await
                .map_err(|err| classify(err, format!("setting `{key}`")))
        }
        .boxed()
    }

    fn delete(&self, key: String) -> FutureResult<()> {
        let key = self.key(&key);
        let mut conn = self.conn.0.clone();
        async move { conn.del(&key).await.map_err(|err| classify(err, format!("deleting `{key}`"))) }
            .boxed()
    }

    fn exists(&self, key: String) -> FutureResult<bool> {
        let key = self.key(&key);
        let mut conn = self.conn.0.clone();
        async move {
            conn.exists(&key).await.map_err(|err| classify(err, format!("checking `{key}`")))
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
                    .map_err(|err| classify(err, format!("listing keys for {pattern}")))?;
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

    fn increment(&self, key: String, delta: i64, ttl: Option<Duration>) -> FutureResult<i64> {
        let key = self.key(&key);
        let mut conn = self.conn.0.clone();
        async move {
            redis::Script::new(INCREMENT)
                .key(&key)
                .arg(delta)
                .arg(px(ttl).map_or_else(String::new, |millis| millis.to_string()))
                .invoke_async(&mut conn)
                .await
                .map_err(|err| classify(err, format!("incrementing `{key}` by {delta}")))
        }
        .boxed()
    }

    fn compare_and_swap(
        &self, key: String, current: Option<Vec<u8>>, new: Vec<u8>, ttl: Option<Duration>,
    ) -> FutureResult<Result<(), Option<Vec<u8>>>> {
        let key = self.key(&key);
        let mut conn = self.conn.0.clone();

        async move {
            let present = i64::from(current.is_some());
            let (swapped, found, observed): (i64, i64, Vec<u8>) =
                redis::Script::new(COMPARE_AND_SWAP)
                    .key(&key)
                    .arg(present)
                    .arg(current.unwrap_or_default())
                    .arg(new)
                    .arg(px(ttl).map_or_else(String::new, |millis| millis.to_string()))
                    .invoke_async(&mut conn)
                    .await
                    .map_err(|err| classify(err, format!("swapping `{key}`")))?;

            match (swapped, found) {
                (1, _) => Ok(Ok(())),
                (_, 1) => Ok(Err(Some(observed))),
                _ => Ok(Err(None)),
            }
        }
        .boxed()
    }
}

// `PX` fails once the TTL overflows when added to the server's clock
const MAX_PX: u64 = i64::MAX.unsigned_abs() / 2;

// Milliseconds for `PX`, rounded up so an entry never expires early. A TTL
// too long for `PX` is stored without one.
fn px(ttl: Option<Duration>) -> Option<u64> {
    let millis = u64::try_from(ttl?.as_nanos().div_ceil(1_000_000)).ok()?;
    (millis <= MAX_PX).then_some(millis)
}

// The kind a guest sees for a Redis failure, where an unrecognised one stays
// `other`. A failed authentication asks for a reconnect, so `denied` is
// matched before `unavailable`.
fn classify(err: RedisError, context: String) -> anyhow::Error {
    let message = format!("{context}: {err}");
    let unavailable = err.is_io_error()
        || err.is_timeout()
        || err.is_connection_dropped()
        || err.is_unrecoverable_error()
        || matches!(
            err.kind(),
            ErrorKind::Server(
                ServerErrorKind::BusyLoading
                    | ServerErrorKind::TryAgain
                    | ServerErrorKind::ClusterDown
                    | ServerErrorKind::MasterDown
            )
        );

    let typed = if err.kind() == ErrorKind::AuthenticationFailed
        || matches!(err.code(), Some("NOAUTH" | "WRONGPASS" | "NOPERM"))
    {
        Error::Denied(message)
    } else if err.code() == Some(NOT_A_COUNTER) {
        Error::Invalid(message)
    } else if unavailable {
        Error::Unavailable(message)
    } else {
        return anyhow::Error::new(err).context(context);
    };
    typed.into()
}

fn strip_bucket_prefix(identifier: &str, raw_key: &str) -> Option<String> {
    raw_key.strip_prefix(identifier).and_then(|rest| rest.strip_prefix(':')).map(ToOwned::to_owned)
}
