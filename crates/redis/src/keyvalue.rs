use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, ensure};
use futures::FutureExt;
use omnia_wasi_keyvalue::{
    Bucket, Entry, Error, FutureResult, KeyValueClient, Lifetime, Precondition, Revision,
};
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

// Server-side conditional write: the compare and the write are one atomic
// step. `WATCH`/`MULTI` is not an option on a multiplexed connection.
// `ARGV[1]` is `any`, `absent`, or `holds`, which writes only while the key
// holds `ARGV[2]`. A `keep` in `ARGV[4]` leaves an existing key its TTL, and
// otherwise the write applies the TTL in `ARGV[5]`. Returns
// `{written, present, current}`.
const WRITE: &str = r"
local current = redis.call('GET', KEYS[1])
local holds = ARGV[1] == 'any'
  or (ARGV[1] == 'absent' and current == false)
  or (ARGV[1] == 'holds' and current == ARGV[2])
if not holds then
  if current == false then
    return {0, 0, ''}
  end
  return {0, 1, current}
end
if ARGV[4] == 'keep' and current ~= false then
  redis.call('SET', KEYS[1], ARGV[3], 'KEEPTTL')
elseif ARGV[5] ~= '' then
  redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[5])
else
  redis.call('SET', KEYS[1], ARGV[3])
end
return {1, 0, ''}
";

// Server-side conditional delete, of a key only while it holds `ARGV[1]`.
// Returns `{deleted, present, current}`.
const DELETE: &str = r"
local current = redis.call('GET', KEYS[1])
if current == false then
  return {0, 0, ''}
end
if current ~= ARGV[1] then
  return {0, 1, current}
end
redis.call('DEL', KEYS[1])
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
    fn get(&self, key: String) -> FutureResult<Option<Entry>> {
        let key = self.key(&key);
        let mut conn = self.conn.0.clone();
        async move {
            let value: Option<Vec<u8>> =
                conn.get(&key).await.map_err(|err| classify(err, format!("getting `{key}`")))?;
            Ok(value.map(entry))
        }
        .boxed()
    }

    fn set(
        &self, key: String, value: Vec<u8>, lifetime: Lifetime, when: Option<Precondition>,
    ) -> FutureResult<Result<Revision, Option<Entry>>> {
        let key = self.key(&key);
        let mut conn = self.conn.0.clone();

        async move {
            let (condition, expected) = match when {
                None => ("any", Vec::new()),
                Some(Precondition::Absent) => ("absent", Vec::new()),
                Some(Precondition::Matches(revision)) => {
                    match named(&mut conn, &key, &revision).await? {
                        Ok(expected) => ("holds", expected),
                        Err(found) => return Ok(Err(found)),
                    }
                }
            };
            let (keep, ttl) = match lifetime {
                Lifetime::Replace(ttl) => ("replace", ttl),
                Lifetime::Keep(ttl) => ("keep", ttl),
            };

            let revision = Revision::of(&value);
            let reply = redis::Script::new(WRITE)
                .key(&key)
                .arg(condition)
                .arg(expected)
                .arg(value)
                .arg(keep)
                .arg(px(ttl).map_or_else(String::new, |millis| millis.to_string()))
                .invoke_async(&mut conn)
                .await
                .map_err(|err| classify(err, format!("setting `{key}`")))?;
            Ok(outcome(reply).map(|()| revision))
        }
        .boxed()
    }

    fn delete(
        &self, key: String, when: Option<Revision>,
    ) -> FutureResult<Result<(), Option<Entry>>> {
        let key = self.key(&key);
        let mut conn = self.conn.0.clone();

        async move {
            let Some(revision) = when else {
                return conn
                    .del(&key)
                    .await
                    .map(|()| Ok(()))
                    .map_err(|err| classify(err, format!("deleting `{key}`")));
            };
            let expected = match named(&mut conn, &key, &revision).await? {
                Ok(expected) => expected,
                Err(found) => return Ok(Err(found)),
            };

            let reply = redis::Script::new(DELETE)
                .key(&key)
                .arg(expected)
                .invoke_async(&mut conn)
                .await
                .map_err(|err| classify(err, format!("deleting `{key}`")))?;
            Ok(outcome(reply))
        }
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
}

// The bytes at `key` while they have `revision`, or the entry found in their
// place. A revision is a digest of the bytes, so the scripts compare the
// bytes it names.
async fn named(
    conn: &mut ConnectionManager, key: &str, revision: &Revision,
) -> Result<Result<Vec<u8>, Option<Entry>>> {
    let current: Option<Vec<u8>> =
        conn.get(key).await.map_err(|err| classify(err, format!("getting `{key}`")))?;
    Ok(match current {
        Some(current) if Revision::of(&current) == *revision => Ok(current),
        found => Err(found.map(entry)),
    })
}

// A script's `{done, present, current}` reply, as the entry found in place
// of the one the call expected.
fn outcome((done, present, current): (i64, i64, Vec<u8>)) -> Result<(), Option<Entry>> {
    match (done, present) {
        (1, _) => Ok(()),
        (_, 1) => Err(Some(entry(current))),
        _ => Err(None),
    }
}

fn entry(value: Vec<u8>) -> Entry {
    Entry {
        revision: Revision::of(&value),
        value,
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
