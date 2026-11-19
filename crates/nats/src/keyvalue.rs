use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use async_nats::header::{
    HeaderValue, NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, NATS_MARKER_REASON, NATS_MESSAGE_TTL,
};
use async_nats::jetstream::context::{
    CreateKeyValueErrorKind, KeyValueErrorKind, PublishError, PublishErrorKind,
};
use async_nats::jetstream::kv::{self, Config};
use async_nats::jetstream::message::StreamMessage;
use async_nats::jetstream::stream::{DirectGetErrorKind, LastRawMessageErrorKind};
use async_nats::{HeaderMap, jetstream};
use futures::TryStreamExt;
use futures::future::FutureExt;
use omnia_wasi_keyvalue::{
    Bucket, Entry, Error, FutureResult, KeyValueClient, Lifetime, Precondition, Revision,
};

use crate::Client;

impl KeyValueClient for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening bucket: {identifier}");
        let buckets = self.kv_buckets.clone();
        let jetstream = self.jetstream.clone();
        let max_age = self.kv_max_age.unwrap_or_default();

        async move {
            let bucket = buckets.open(&jetstream, identifier, max_age).await?;
            Ok(Arc::new(bucket) as Arc<dyn Bucket>)
        }
        .boxed()
    }
}

// How long the marker an expiry leaves stays visible to watchers. Setting
// it is what turns per-message TTL on for a bucket.
const MARKER_TTL: Duration = Duration::from_secs(60);

// Bounds the conditioned writes a lost race repeats.
const MAX_RETRIES: usize = 64;

// The header that carries an entry's deadline, in unix milliseconds.
const EXPIRES: &str = "Omnia-Expires";

const KV_OPERATION: &str = "KV-Operation";

// The buckets a deployment declares, each opened once and then reused.
#[derive(Clone, Debug)]
pub struct Buckets {
    declared: Arc<HashSet<String>>,
    open: Arc<Mutex<HashMap<String, KvBucket>>>,
}

impl Buckets {
    // A declared name holds only ASCII letters, digits, `_`, and `-`, the
    // JetStream bucket rule.
    pub fn declare(names: Vec<String>) -> Result<Self> {
        for name in &names {
            let valid = !name.is_empty()
                && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'));
            ensure!(
                valid,
                "bucket name `{name}` must hold only ASCII letters, digits, `_`, and `-`"
            );
        }
        Ok(Self {
            declared: Arc::new(names.into_iter().collect()),
            open: Arc::default(),
        })
    }

    async fn open(
        &self, jetstream: &jetstream::Context, identifier: String, max_age: Duration,
    ) -> Result<KvBucket> {
        if !self.declared.contains(&identifier) {
            return Err(Error::NotFound(format!("bucket `{identifier}` is not declared")).into());
        }
        let opened = self.open.lock().expect("open buckets poisoned").get(&identifier).cloned();
        if let Some(bucket) = opened {
            return Ok(bucket);
        }

        let store = match jetstream.get_key_value(&identifier).await {
            Ok(store) => store,
            Err(err) if err.kind() == KeyValueErrorKind::GetBucket => {
                create(jetstream, &identifier, max_age).await?
            }
            Err(err) => return Err(err).with_context(|| format!("opening bucket `{identifier}`")),
        };
        let ttl = store.stream.cached_info().config.allow_message_ttl
            || enable_ttl(jetstream, &store).await;

        let bucket = KvBucket {
            jetstream: jetstream.clone(),
            store,
            ttl,
        };
        self.open.lock().expect("open buckets poisoned").insert(identifier, bucket.clone());
        Ok(bucket)
    }
}

// A server older than 2.11 has no per-message TTL, so it gets a bucket
// without one.
async fn create(
    jetstream: &jetstream::Context, identifier: &str, max_age: Duration,
) -> Result<kv::Store> {
    let config = Config {
        bucket: identifier.to_owned(),
        history: 1,
        max_age,
        max_bytes: 100 * 1024 * 1024,
        limit_markers: Some(MARKER_TTL),
        ..Config::default()
    };
    match jetstream.create_key_value(config.clone()).await {
        Err(err) if err.kind() == CreateKeyValueErrorKind::LimitMarkersNotSupported => {
            tracing::warn!("bucket `{identifier}` is created without per-message TTL: {err}");
            let config = Config {
                limit_markers: None,
                ..config
            };
            jetstream.create_key_value(config).await
        }
        created => created,
    }
    .with_context(|| format!("creating bucket `{identifier}`"))
}

// Turns per-message TTL on for a bucket made without it, which the server
// cannot undo. A refusal leaves a bucket that takes no write with a TTL.
async fn enable_ttl(jetstream: &jetstream::Context, store: &kv::Store) -> bool {
    let mut config = store.stream.cached_info().config.clone();
    config.allow_message_ttl = true;
    config.subject_delete_marker_ttl = Some(MARKER_TTL);

    match jetstream.update_stream(&config).await {
        Ok(info) if info.config.allow_message_ttl => true,
        Ok(_info) => {
            tracing::warn!("bucket `{}` keeps no per-message TTL", store.name);
            false
        }
        Err(err) => {
            tracing::warn!("bucket `{}` keeps no per-message TTL: {err}", store.name);
            false
        }
    }
}

#[derive(Clone, Debug)]
pub struct KvBucket {
    jetstream: jetstream::Context,
    store: kv::Store,
    // whether the stream expires a message by its `Nats-TTL` header
    ttl: bool,
}

// What the stream holds for a key.
struct Snapshot {
    // the sequence a conditioned write expects, `0` for an empty subject
    revision: u64,
    // the value, while the last message is a put short of its deadline
    value: Option<Vec<u8>>,
    // that value's deadline, in unix milliseconds
    deadline: Option<u64>,
}

impl Snapshot {
    // A write that keeps the entry's time keeps a live entry's deadline, and
    // applies the call's TTL only to an entry it creates.
    fn deadline_for(&self, ttl: Option<Duration>) -> Option<u64> {
        if self.value.is_some() { self.deadline } else { deadline(ttl) }
    }

    // A live entry's revision is the sequence of the message that holds it.
    fn current(&self) -> Option<Revision> {
        self.value.is_some().then(|| revision(self.revision))
    }

    fn entry(self) -> Option<Entry> {
        let revision = revision(self.revision);
        self.value.map(|value| Entry { value, revision })
    }
}

fn revision(sequence: u64) -> Revision {
    Revision::from(sequence.to_string())
}

impl KvBucket {
    async fn snapshot(&self, key: &str) -> Result<Snapshot> {
        validate_key(key)?;
        let Some(message) = self.last_message(key).await? else {
            return Ok(Snapshot {
                revision: 0,
                value: None,
                deadline: None,
            });
        };

        let deadline = message.headers.get(EXPIRES).and_then(|value| value.as_str().parse().ok());
        let live =
            is_put(&message.headers) && deadline.is_none_or(|deadline| now_millis() < deadline);
        Ok(Snapshot {
            revision: message.sequence,
            value: live.then(|| message.payload.to_vec()),
            deadline: deadline.filter(|_| live),
        })
    }

    async fn last_message(&self, key: &str) -> Result<Option<StreamMessage>> {
        let subject = format!("{}{key}", self.store.prefix);
        let context = || format!("reading `{key}`");

        if self.store.stream.cached_info().config.allow_direct {
            match self.store.stream.direct_get_last_for_subject(&subject).await {
                Ok(message) => Ok(Some(message)),
                Err(err) if err.kind() == DirectGetErrorKind::NotFound => Ok(None),
                Err(err) => {
                    let unavailable = matches!(
                        err.kind(),
                        DirectGetErrorKind::TimedOut | DirectGetErrorKind::Request
                    );
                    Err(failure(err, unavailable, context()))
                }
            }
        } else {
            match self.store.stream.get_last_raw_message_by_subject(&subject).await {
                Ok(message) => Ok(Some(message)),
                Err(err) if err.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(None),
                Err(err) => Err(failure(err, false, context())),
            }
        }
    }

    // Every write is one publish to the subject `kv::Store::put` writes to,
    // with headers the store's own API cannot send together. It returns the
    // sequence written.
    async fn publish(
        &self, key: &str, payload: Vec<u8>, headers: HeaderMap,
    ) -> Result<u64, PublishError> {
        let prefix = self.store.put_prefix.as_deref().unwrap_or(&self.store.prefix);
        let ack = self
            .jetstream
            .publish_with_headers(format!("{prefix}{key}"), headers, payload.into())
            .await?
            .await?;
        Ok(ack.sequence)
    }

    // A put conditioned on `revision` when one is given, returning the
    // sequence written, or `None` when the key moved on first. A deadline
    // sets the server's TTL, in whole seconds rounded up, beside the
    // deadline a read checks.
    async fn put(
        &self, key: &str, value: Vec<u8>, revision: Option<u64>, deadline: Option<u64>,
    ) -> Result<Option<u64>> {
        let mut headers = HeaderMap::new();
        if let Some(revision) = revision {
            headers.insert(NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, HeaderValue::from(revision));
        }
        if let Some(deadline) = deadline {
            if !self.ttl {
                let message = format!("bucket `{}` has no per-message TTL", self.store.name);
                return Err(Error::Unsupported(message).into());
            }
            let seconds = deadline.saturating_sub(now_millis()).div_ceil(1000).max(1);
            headers.insert(NATS_MESSAGE_TTL, HeaderValue::from(seconds));
            headers.insert(EXPIRES, HeaderValue::from(deadline));
        }

        match self.publish(key, value, headers).await {
            Ok(sequence) => Ok(Some(sequence)),
            Err(err) if err.kind() == PublishErrorKind::WrongLastSequence => Ok(None),
            Err(err) => Err(publish_failure(err, format!("writing `{key}`"))),
        }
    }

    // A delete marker conditioned on `revision` when one is given, where
    // `false` means the key moved on first.
    async fn erase(&self, key: &str, revision: Option<u64>) -> Result<bool> {
        let mut headers = HeaderMap::new();
        headers.insert(KV_OPERATION, "DEL");
        if let Some(revision) = revision {
            headers.insert(NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, HeaderValue::from(revision));
        }

        match self.publish(key, Vec::new(), headers).await {
            Ok(_sequence) => Ok(true),
            Err(err) if err.kind() == PublishErrorKind::WrongLastSequence => Ok(false),
            Err(err) => Err(publish_failure(err, format!("deleting `{key}`"))),
        }
    }
}

impl Bucket for KvBucket {
    fn get(&self, key: String) -> FutureResult<Option<Entry>> {
        tracing::trace!("getting key: {key}");
        let bucket = self.clone();
        async move { Ok(bucket.snapshot(&key).await?.entry()) }.boxed()
    }

    fn set(
        &self, key: String, value: Vec<u8>, lifetime: Lifetime, when: Option<Precondition>,
    ) -> FutureResult<std::result::Result<Revision, Option<Entry>>> {
        tracing::trace!("setting key: {key}");
        let bucket = self.clone();
        async move {
            validate_key(&key)?;

            // a write that neither checks nor keeps the entry reads nothing
            if when.is_none()
                && let Lifetime::Replace(ttl) = lifetime
            {
                let written = bucket.put(&key, value, None, deadline(ttl)).await?;
                let sequence = written.context("an unconditioned write lost a race")?;
                return Ok(Ok(revision(sequence)));
            }

            for _ in 0..MAX_RETRIES {
                let snapshot = bucket.snapshot(&key).await?;
                if when.as_ref().is_some_and(|when| !when.holds(snapshot.current().as_ref())) {
                    return Ok(Err(snapshot.entry()));
                }
                let deadline = match lifetime {
                    Lifetime::Replace(ttl) => deadline(ttl),
                    Lifetime::Keep(ttl) => snapshot.deadline_for(ttl),
                };
                let written =
                    bucket.put(&key, value.clone(), Some(snapshot.revision), deadline).await?;
                if let Some(sequence) = written {
                    return Ok(Ok(revision(sequence)));
                }
            }
            let message = format!("setting `{key}` lost {MAX_RETRIES} races");
            Err(Error::Unavailable(message).into())
        }
        .boxed()
    }

    fn delete(
        &self, key: String, when: Option<Revision>,
    ) -> FutureResult<std::result::Result<(), Option<Entry>>> {
        tracing::trace!("deleting key: {key}");
        let bucket = self.clone();
        async move {
            validate_key(&key)?;
            let Some(when) = when else {
                bucket.erase(&key, None).await?;
                return Ok(Ok(()));
            };

            for _ in 0..MAX_RETRIES {
                let snapshot = bucket.snapshot(&key).await?;
                if snapshot.current().as_ref() != Some(&when) {
                    return Ok(Err(snapshot.entry()));
                }
                if bucket.erase(&key, Some(snapshot.revision)).await? {
                    return Ok(Ok(()));
                }
            }
            let message = format!("deleting `{key}` lost {MAX_RETRIES} races");
            Err(Error::Unavailable(message).into())
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

    fn increment(&self, key: String, delta: i64, ttl: Option<Duration>) -> FutureResult<i64> {
        tracing::trace!("incrementing key: {key}");
        let bucket = self.clone();
        async move {
            for _ in 0..MAX_RETRIES {
                let snapshot = bucket.snapshot(&key).await?;
                let incremented = add(&key, snapshot.value.as_deref(), delta)?;
                let payload = incremented.to_string().into_bytes();
                let deadline = snapshot.deadline_for(ttl);
                if bucket.put(&key, payload, Some(snapshot.revision), deadline).await?.is_some() {
                    return Ok(incremented);
                }
            }
            let message = format!("incrementing `{key}` lost {MAX_RETRIES} races");
            Err(Error::Unavailable(message).into())
        }
        .boxed()
    }
}

// A TTL longer than the server holds, in nanoseconds as an `i64`, is
// stored without one.
const MAX_TTL: Duration = Duration::from_secs(i64::MAX.unsigned_abs() / 1_000_000_000);

// The deadline a TTL sets, in unix milliseconds rounded up, so an entry
// never expires early.
fn deadline(ttl: Option<Duration>) -> Option<u64> {
    let ttl = ttl.filter(|ttl| *ttl <= MAX_TTL)?;
    let millis = u64::try_from(ttl.as_nanos().div_ceil(1_000_000)).ok()?;
    now_millis().checked_add(millis)
}

fn now_millis() -> u64 {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
}

// a put, rather than a delete, a purge, or the marker an expiry leaves
fn is_put(headers: &HeaderMap) -> bool {
    headers.get(NATS_MARKER_REASON).is_none()
        && headers.get(KV_OPERATION).is_none_or(|op| op.as_str() == "PUT")
}

// JetStream KV keys: non-empty, no leading or trailing `.`, and only
// `[-/_=.a-zA-Z0-9]`, so no subject wildcard reaches the stream.
fn validate_key(key: &str) -> Result<()> {
    let valid = !key.is_empty()
        && !key.starts_with('.')
        && !key.ends_with('.')
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'/' | b'_' | b'=' | b'.'));
    if valid {
        return Ok(());
    }
    let message = format!(
        "key `{key}` must be non-empty, neither start nor end with `.`, and hold only `[-/_=.a-zA-Z0-9]`"
    );
    Err(Error::Invalid(message).into())
}

fn add(key: &str, current: Option<&[u8]>, delta: i64) -> Result<i64> {
    let base = match current {
        None => 0,
        Some(value) => counter(value)
            .ok_or_else(|| Error::Invalid(format!("`{key}` does not hold a decimal counter")))?,
    };
    let added = base.checked_add(delta);
    added.ok_or_else(|| Error::Invalid(format!("incrementing `{key}` by {delta} overflows")).into())
}

// A counter is decimal text: `0`, or an optional `-` and digits that do not
// start with `0`.
fn counter(value: &[u8]) -> Option<i64> {
    let digits = value.strip_prefix(b"-").unwrap_or(value);
    let canonical = value == b"0"
        || (matches!(digits.first(), Some(b'1'..=b'9')) && digits.iter().all(u8::is_ascii_digit));
    if !canonical {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn publish_failure(err: PublishError, context: String) -> anyhow::Error {
    let unavailable = matches!(
        err.kind(),
        PublishErrorKind::TimedOut
            | PublishErrorKind::BrokenPipe
            | PublishErrorKind::StreamNotFound
    );
    failure(err, unavailable, context)
}

// `unavailable` when the server did not answer; any other failure stays `other`
fn failure(
    err: impl std::error::Error + Send + Sync + 'static, unavailable: bool, context: String,
) -> anyhow::Error {
    if unavailable {
        Error::Unavailable(format!("{context}: {err}")).into()
    } else {
        anyhow::Error::new(err).context(context)
    }
}
