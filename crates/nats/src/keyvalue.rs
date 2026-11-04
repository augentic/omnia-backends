use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail, ensure};
use async_nats::header::{
    HeaderMap, HeaderValue, NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, NATS_MARKER_REASON,
    NATS_MESSAGE_TTL,
};
use async_nats::jetstream;
use async_nats::jetstream::context::{KeyValueErrorKind, PublishError, PublishErrorKind};
use async_nats::jetstream::kv::{self, Config};
use async_nats::jetstream::message::StreamMessage;
use async_nats::jetstream::stream::{DirectGetErrorKind, LastRawMessageErrorKind};
use futures::TryStreamExt;
use futures::future::FutureExt;
use omnia_backend::FutureResult;
use omnia_backend::keyvalue::{Bucket, KeyValueClient, add_i64};

use crate::Client;

const MAX_INCREMENT_RETRIES: usize = 64;

// How long the marker left where an entry expired stays visible to watchers.
// Setting it is what turns per-message TTL on for the bucket; a read treats
// the marker, and its absence, alike as the key being gone.
const MARKER_TTL: Duration = Duration::from_secs(60);

impl KeyValueClient for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening bucket: {identifier}");
        let jetstream = self.jetstream.clone();
        let max_age = self.kv_max_age.unwrap_or_default();

        async move {
            let store = match jetstream.get_key_value(&identifier).await {
                Ok(store) => store,
                Err(err) if err.kind() == KeyValueErrorKind::GetBucket => jetstream
                    .create_key_value(Config {
                        bucket: identifier.clone(),
                        history: 1,
                        max_age,
                        max_bytes: 100 * 1024 * 1024,
                        limit_markers: Some(MARKER_TTL),
                        ..Config::default()
                    })
                    .await
                    .context("failed to create bucket")?,
                Err(err) => return Err(err).context("failed to open bucket"),
            };

            Ok(Arc::new(KvBucket { jetstream, store }) as Arc<dyn Bucket>)
        }
        .boxed()
    }
}

#[derive(Clone, Debug)]
struct KvBucket {
    jetstream: jetstream::Context,
    store: kv::Store,
}

// What the server holds for a key: the revision a write is conditioned on
// (`0` when nothing was ever written), the value when that revision is a
// live put, and the TTL still to run on it.
struct Snapshot {
    revision: u64,
    value: Option<Vec<u8>>,
    ttl: Option<Duration>,
}

impl KvBucket {
    async fn snapshot(&self, key: &str) -> Result<Snapshot> {
        validate_key(key)?;
        let subject = format!("{}{key}", self.store.prefix);
        let Some(message) =
            self.last_message(&subject).await.with_context(|| format!("reading `{key}`"))?
        else {
            return Ok(Snapshot {
                revision: 0,
                value: None,
                ttl: None,
            });
        };

        // a marker, or a put past its TTL the server has not swept yet, reads as absent
        let ttl = remaining_ttl(&message);
        let live = is_put(&message.headers) && !ttl.is_some_and(|ttl| ttl.is_zero());
        Ok(Snapshot {
            revision: message.sequence,
            value: live.then(|| message.payload.to_vec()),
            ttl: ttl.filter(|_| live),
        })
    }

    async fn last_message(&self, subject: &str) -> Result<Option<StreamMessage>> {
        if self.store.stream.cached_info().config.allow_direct {
            match self.store.stream.direct_get_last_for_subject(subject).await {
                Ok(message) => Ok(Some(message)),
                Err(err) if err.kind() == DirectGetErrorKind::NotFound => Ok(None),
                Err(err) => Err(anyhow::Error::new(err)),
            }
        } else {
            match self.store.stream.get_last_raw_message_by_subject(subject).await {
                Ok(message) => Ok(Some(message)),
                Err(err) if err.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(None),
                Err(err) => Err(anyhow::Error::new(err)),
            }
        }
    }

    // Every write is the one publish `Store::put` makes, plus the headers the
    // store's own API cannot send together: the revision the write expects
    // and the TTL the server expires the entry by.
    async fn publish(
        &self, key: &str, value: Vec<u8>, revision: Option<u64>, ttl: Option<Duration>,
    ) -> Result<(), PublishError> {
        let mut headers = HeaderMap::new();
        if let Some(revision) = revision {
            headers.insert(NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, HeaderValue::from(revision));
        }
        if let Some(ttl) = ttl {
            headers.insert(NATS_MESSAGE_TTL, HeaderValue::from(ttl.as_secs().max(1)));
        }
        let prefix = self.store.put_prefix.as_deref().unwrap_or(&self.store.prefix);
        self.jetstream
            .publish_with_headers(format!("{prefix}{key}"), headers, value.into())
            .await?
            .await?;
        Ok(())
    }

    // A write conditioned on `revision`; `false` when the key moved on first.
    async fn update(
        &self, key: &str, value: Vec<u8>, revision: u64, ttl: Option<Duration>,
    ) -> Result<bool> {
        validate_key(key)?;
        match self.publish(key, value, Some(revision), ttl).await {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == PublishErrorKind::WrongLastSequence => Ok(false),
            Err(err) => Err(anyhow::Error::new(err).context(format!("writing `{key}`"))),
        }
    }
}

impl Bucket for KvBucket {
    fn get(&self, key: String) -> FutureResult<Option<Vec<u8>>> {
        tracing::trace!("getting key: {key}");
        let bucket = self.clone();
        async move { Ok(bucket.snapshot(&key).await?.value) }.boxed()
    }

    fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> FutureResult<()> {
        tracing::trace!("setting key: {key}");
        let bucket = self.clone();
        async move {
            validate_key(&key)?;
            bucket.publish(&key, value, None, ttl).await.with_context(|| format!("writing `{key}`"))
        }
        .boxed()
    }

    fn delete(&self, key: String) -> FutureResult<()> {
        tracing::trace!("deleting key: {key}");
        let store = self.store.clone();
        async move {
            store.delete(&key).await.with_context(|| format!("deleting `{key}`"))?;
            Ok(())
        }
        .boxed()
    }

    fn exists(&self, key: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of key: {key}");
        let bucket = self.clone();
        async move { Ok(bucket.snapshot(&key).await?.value.is_some()) }.boxed()
    }

    fn keys(&self) -> FutureResult<Vec<String>> {
        tracing::trace!("listing keys");
        let store = self.store.clone();
        async move {
            let keys = store.keys().await.context("listing keys")?;
            keys.try_collect().await.context("listing keys")
        }
        .boxed()
    }

    fn increment(&self, key: String, delta: i64) -> FutureResult<i64> {
        tracing::trace!("incrementing key: {key}");
        let bucket = self.clone();
        async move {
            // JetStream KV has no native increment; loop a revision-conditioned
            // read-modify-write, retrying only on lost races.
            for _ in 0..MAX_INCREMENT_RETRIES {
                let snapshot = bucket.snapshot(&key).await?;
                let incremented = add_i64(snapshot.value.as_deref(), delta)
                    .with_context(|| format!("incrementing `{key}`"))?;
                let payload = incremented.to_be_bytes().to_vec();
                if bucket.update(&key, payload, snapshot.revision, snapshot.ttl).await? {
                    return Ok(incremented);
                }
            }
            bail!("incrementing `{key}` exceeded {MAX_INCREMENT_RETRIES} CAS retries")
        }
        .boxed()
    }

    fn cas(
        &self, key: String, expected: Option<Vec<u8>>, value: Vec<u8>,
    ) -> FutureResult<std::result::Result<(), Option<Vec<u8>>>> {
        tracing::trace!("swapping key: {key}");
        let bucket = self.clone();
        async move {
            let snapshot = bucket.snapshot(&key).await?;
            if snapshot.value != expected {
                return Ok(Err(snapshot.value));
            }
            if bucket.update(&key, value, snapshot.revision, snapshot.ttl).await? {
                return Ok(Ok(()));
            }
            // lost the race between snapshot and write: the conflict carries what won
            Ok(Err(bucket.snapshot(&key).await?.value))
        }
        .boxed()
    }
}

fn is_put(headers: &HeaderMap) -> bool {
    headers.get(NATS_MARKER_REASON).is_none()
        && headers.get("KV-Operation").is_none_or(|op| op.as_str() == "PUT")
}

// The TTL a put was stored with, less the time it has already run; `None`
// for an entry stored without one.
fn remaining_ttl(message: &StreamMessage) -> Option<Duration> {
    let ttl: u64 = message.headers.get(NATS_MESSAGE_TTL)?.as_str().parse().ok()?;
    let stored = SystemTime::from(message.time);
    let elapsed = SystemTime::now().duration_since(stored).unwrap_or_default();
    Some(Duration::from_secs(ttl).saturating_sub(elapsed))
}

// JetStream KV keys: non-empty, no leading or trailing `.`, and only
// `[-/_=.a-zA-Z0-9]` so subject wildcards (`*`, `>`, whitespace) cannot
// reach the stream.
fn validate_key(key: &str) -> Result<()> {
    let valid = !key.is_empty()
        && !key.starts_with('.')
        && !key.ends_with('.')
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'/' | b'_' | b'=' | b'.'));
    ensure!(
        valid,
        "invalid key `{key}`: cannot be empty, start/end with `.`, or contain characters outside [-/_=.a-zA-Z0-9]"
    );
    Ok(())
}

// Key validation for JetStream KV subjects; live round-trips are in `tests/live.rs`.
#[cfg(test)]
mod tests {
    use super::validate_key;

    #[test]
    fn valid_keys() {
        for key in ["k", "conformance/get-set-delete", "a.b.c", "user_1=active-2"] {
            validate_key(key).expect(key);
        }
    }

    #[test]
    fn invalid_keys() {
        for key in ["", ".leading", "trailing.", "wild*card", "wild>card", "with space", "a:b"] {
            validate_key(key).expect_err(key);
        }
    }
}
