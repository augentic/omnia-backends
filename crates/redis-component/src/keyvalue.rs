//! The keyvalue commands over Redis: one RESP2 command per call, the
//! compare-and-swap through [`CAS_SCRIPT`].

use std::future::{Future, ready};

use omnia_wasi_keyvalue::backend::{Context, Error, KeyValue, Links};

use crate::resp::{Command, Frame};
use crate::script::CAS_SCRIPT;

const SCAN_COUNT: &[u8] = b"1000";

/// Redis as a keyvalue package.
///
/// A bucket is a key prefix (`{bucket}:{key}`), `set` takes a millisecond
/// TTL (`SET … PX`), `cas` is one `EVAL` of [`CAS_SCRIPT`], and `increment`
/// is `GET` and `cas` until the swap lands. The commands run over any
/// [`Links`] carrying a [`Command`] to a [`Frame`]: the package's pool, or a
/// scripted link under test.
#[derive(Clone, Copy, Debug, Default)]
pub struct Redis;

// A package runs inside one single-threaded component instance, and `Links`
// promises no `Send` future, so none of these are.
#[expect(clippy::future_not_send, reason = "a package's futures stay on its one thread")]
impl<L> KeyValue<L> for Redis
where
    L: Links<Request = Command, Reply = Frame>,
{
    fn open_bucket(
        _context: Context<'_, L>, bucket: &str,
    ) -> impl Future<Output = Result<(), Error>> {
        ready(if bucket.is_empty() { Err(Error::NoSuchStore) } else { Ok(()) })
    }

    async fn get(
        context: Context<'_, L>, bucket: &str, key: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        fetch(context, &scoped(bucket, key)).await
    }

    async fn set(
        context: Context<'_, L>, bucket: &str, key: &str, value: Vec<u8>, ttl_ms: Option<u64>,
    ) -> Result<(), Error> {
        let key = scoped(bucket, key);
        // Redis refuses a zero expiry; the shortest it accepts is one millisecond
        let command = ttl_ms.map_or_else(
            || Command::new([b"SET".as_slice(), key.as_bytes(), &value]),
            |ms| {
                let ms = ms.max(1).to_string();
                Command::new([b"SET", key.as_bytes(), &value, b"PX", ms.as_bytes()])
            },
        );
        match context.round_trip(command).await? {
            Frame::Simple(_) => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn delete(context: Context<'_, L>, bucket: &str, key: &str) -> Result<(), Error> {
        let key = scoped(bucket, key);
        context
            .round_trip(Command::new([b"DEL".as_slice(), key.as_bytes()]))
            .await?
            .into_integer()
            .map_err(unexpected)?;
        Ok(())
    }

    async fn exists(context: Context<'_, L>, bucket: &str, key: &str) -> Result<bool, Error> {
        let key = scoped(bucket, key);
        let count = context
            .round_trip(Command::new([b"EXISTS".as_slice(), key.as_bytes()]))
            .await?
            .into_integer()
            .map_err(unexpected)?;
        Ok(count > 0)
    }

    async fn keys(context: Context<'_, L>, bucket: &str) -> Result<Vec<String>, Error> {
        let prefix = scoped(bucket, "");
        let pattern = format!("{prefix}*");
        let mut cursor = b"0".to_vec();
        let mut keys = Vec::new();
        loop {
            let scan = Command::new([
                b"SCAN".as_slice(),
                &cursor,
                b"MATCH",
                pattern.as_bytes(),
                b"COUNT",
                SCAN_COUNT,
            ]);
            let mut parts =
                context.round_trip(scan).await?.into_array().map_err(unexpected)?.into_iter();
            let (Some(next), Some(batch)) = (parts.next(), parts.next()) else {
                return Err(Error::Other("SCAN replied without a cursor and a batch".to_owned()));
            };
            cursor = next
                .into_bulk()
                .map_err(unexpected)?
                .ok_or_else(|| Error::Other("SCAN replied with a null cursor".to_owned()))?;
            for item in batch.into_array().map_err(unexpected)? {
                let key = item.into_bulk().map_err(unexpected)?.unwrap_or_default();
                let key = String::from_utf8(key)
                    .map_err(|_bytes| Error::Other("a key is not UTF-8".to_owned()))?;
                if let Some(bare) = key.strip_prefix(&prefix) {
                    keys.push(bare.to_owned());
                }
            }
            if cursor == b"0" {
                return Ok(keys);
            }
        }
    }

    async fn increment(
        context: Context<'_, L>, bucket: &str, key: &str, delta: i64,
    ) -> Result<i64, Error> {
        let key = scoped(bucket, key);
        loop {
            let current = fetch(context, &key).await?;
            let value = match &current {
                Some(bytes) => {
                    i64::from_be_bytes(bytes.as_slice().try_into().map_err(|_length| {
                        Error::Other("the value is not an 8-byte integer".to_owned())
                    })?)
                }
                None => 0,
            };
            let next = value
                .checked_add(delta)
                .ok_or_else(|| Error::Other("the increment overflows".to_owned()))?;
            if swap(context, &key, current.as_deref(), &next.to_be_bytes()).await?.is_ok() {
                return Ok(next);
            }
        }
    }

    async fn cas(
        context: Context<'_, L>, bucket: &str, key: &str, expected: Option<Vec<u8>>, value: Vec<u8>,
    ) -> Result<Result<(), Option<Vec<u8>>>, Error> {
        swap(context, &scoped(bucket, key), expected.as_deref(), &value).await
    }
}

fn scoped(bucket: &str, key: &str) -> String {
    format!("{bucket}:{key}")
}

#[expect(clippy::future_not_send, reason = "a package's futures stay on its one thread")]
async fn fetch<L>(context: Context<'_, L>, key: &str) -> Result<Option<Vec<u8>>, Error>
where
    L: Links<Request = Command, Reply = Frame>,
{
    context
        .round_trip(Command::new([b"GET".as_slice(), key.as_bytes()]))
        .await?
        .into_bulk()
        .map_err(unexpected)
}

#[expect(clippy::future_not_send, reason = "a package's futures stay on its one thread")]
async fn swap<L>(
    context: Context<'_, L>, key: &str, expected: Option<&[u8]>, value: &[u8],
) -> Result<Result<(), Option<Vec<u8>>>, Error>
where
    L: Links<Request = Command, Reply = Frame>,
{
    let has_expected: &[u8] = if expected.is_some() { b"1" } else { b"0" };
    let eval = Command::new([
        b"EVAL",
        CAS_SCRIPT.as_bytes(),
        b"1",
        key.as_bytes(),
        has_expected,
        expected.unwrap_or_default(),
        value,
    ]);
    let mut parts = context.round_trip(eval).await?.into_array().map_err(unexpected)?.into_iter();
    let (Some(swapped), Some(present), Some(observed)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(Error::Other("the CAS script replied without a triple".to_owned()));
    };
    let swapped = swapped.into_integer().map_err(unexpected)?;
    let present = present.into_integer().map_err(unexpected)?;
    let observed = observed.into_bulk().map_err(unexpected)?;
    Ok(match (swapped, present) {
        (1, 0) => Ok(()),
        (0, 0) => Err(None),
        (0, 1) => Err(Some(observed.unwrap_or_default())),
        _ => unreachable!("the CAS script reports a swap, an absent key, or the value observed"),
    })
}

#[expect(clippy::needless_pass_by_value, reason = "`map_err` hands over the owned frame")]
fn unexpected(frame: Frame) -> Error {
    Error::Other(format!("unexpected reply: {frame:?}"))
}

// The unit tier: the command mapping over a scripted link, for the branches
// the guest scenarios over the fake never take. The e2e suite in
// `tests/keyvalue.rs` covers the rest.
#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    struct Script(Mutex<VecDeque<(Command, Frame)>>);

    impl Script {
        fn new<const N: usize>(exchanges: [(Command, Frame); N]) -> Self {
            Self(Mutex::new(exchanges.into()))
        }

        fn finish(self) {
            let left = self.0.into_inner().expect("script lock");
            assert!(left.is_empty(), "scripted exchanges were left unconsumed");
        }
    }

    impl Links for Script {
        type Reply = Frame;
        type Request = Command;

        fn round_trip(&self, request: Command) -> impl Future<Output = Result<Frame, Error>> {
            let next = self.0.lock().expect("script lock").pop_front();
            let (expected, reply) = next.expect("an unscripted round trip");
            assert_eq!(request, expected, "the round trip is not the one scripted");
            ready(Ok(reply))
        }
    }

    fn command<const N: usize>(args: [&[u8]; N]) -> Command {
        Command::new(args)
    }

    fn ok() -> Frame {
        Frame::Simple("OK".to_owned())
    }

    fn cas_reply(swapped: i64, present: i64, observed: &[u8]) -> Frame {
        Frame::Array(Some(vec![
            Frame::Integer(swapped),
            Frame::Integer(present),
            Frame::Bulk(Some(observed.to_vec())),
        ]))
    }

    fn scan_reply(cursor: &[u8], keys: &[&[u8]]) -> Frame {
        let keys = keys.iter().map(|key| Frame::Bulk(Some(key.to_vec()))).collect();
        Frame::Array(Some(vec![Frame::Bulk(Some(cursor.to_vec())), Frame::Array(Some(keys))]))
    }

    #[tokio::test]
    async fn empty_bucket() {
        let script = Script::new([]);
        let refused = Redis::open_bucket(Context::new(&script), "").await;
        assert_eq!(refused, Err(Error::NoSuchStore));
        assert_eq!(Redis::open_bucket(Context::new(&script), "b").await, Ok(()));
        script.finish();
    }

    #[tokio::test]
    async fn set_with_zero_ttl() {
        let script = Script::new([(command([b"SET", b"b:k", b"v", b"PX", b"1"]), ok())]);
        Redis::set(Context::new(&script), "b", "k", b"v".to_vec(), Some(0)).await.expect("set");
        script.finish();
    }

    #[tokio::test]
    async fn keys_across_cursor_pages() {
        let script = Script::new([
            (
                command([b"SCAN", b"0", b"MATCH", b"b:*", b"COUNT", b"1000"]),
                scan_reply(b"17", &[b"b:a", b"other"]),
            ),
            (
                command([b"SCAN", b"17", b"MATCH", b"b:*", b"COUNT", b"1000"]),
                scan_reply(b"0", &[b"b:c"]),
            ),
        ]);
        let keys = Redis::keys(Context::new(&script), "b").await.expect("keys");
        assert_eq!(keys, ["a", "c"]);
        script.finish();
    }

    #[tokio::test]
    async fn increment_under_a_concurrent_swap() {
        let script = Script::new([
            (command([b"GET", b"b:n"]), Frame::Bulk(Some(37_i64.to_be_bytes().to_vec()))),
            (
                command([
                    b"EVAL",
                    CAS_SCRIPT.as_bytes(),
                    b"1",
                    b"b:n",
                    b"1",
                    &37_i64.to_be_bytes(),
                    &42_i64.to_be_bytes(),
                ]),
                cas_reply(0, 1, &40_i64.to_be_bytes()),
            ),
            (command([b"GET", b"b:n"]), Frame::Bulk(Some(40_i64.to_be_bytes().to_vec()))),
            (
                command([
                    b"EVAL",
                    CAS_SCRIPT.as_bytes(),
                    b"1",
                    b"b:n",
                    b"1",
                    &40_i64.to_be_bytes(),
                    &45_i64.to_be_bytes(),
                ]),
                cas_reply(1, 0, b""),
            ),
        ]);
        let value = Redis::increment(Context::new(&script), "b", "n", 5).await.expect("increment");
        assert_eq!(value, 45);
        script.finish();
    }

    #[tokio::test]
    async fn increment_over_a_non_integer() {
        let script = Script::new([(command([b"GET", b"b:n"]), Frame::Bulk(Some(b"abc".to_vec())))]);
        let refused = Redis::increment(Context::new(&script), "b", "n", 1).await;
        assert_eq!(refused, Err(Error::Other("the value is not an 8-byte integer".to_owned())));
        script.finish();
    }

    #[tokio::test]
    async fn cas_against_an_absent_key() {
        let script = Script::new([(
            command([b"EVAL", CAS_SCRIPT.as_bytes(), b"1", b"b:k", b"1", b"old", b"new"]),
            cas_reply(0, 0, b""),
        )]);
        let outcome =
            Redis::cas(Context::new(&script), "b", "k", Some(b"old".to_vec()), b"new".to_vec())
                .await
                .expect("cas");
        assert_eq!(outcome, Err(None));
        script.finish();
    }

    #[tokio::test]
    async fn unexpected_reply_shape() {
        let script = Script::new([(command([b"EXISTS", b"b:k"]), ok())]);
        let refused = Redis::exists(Context::new(&script), "b", "k").await;
        assert_eq!(refused, Err(Error::Other("unexpected reply: Simple(\"OK\")".to_owned())));
        script.finish();
    }
}
