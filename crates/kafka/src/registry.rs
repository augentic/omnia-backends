use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result};
use futures::FutureExt as _;
use futures::future::{BoxFuture, Shared};
use http::StatusCode;
use jsonschema::Validator;
use schema_registry_client::rest::apis::Error as SchemaRegistryError;
use schema_registry_client::rest::client_config::ClientConfig as RegistryConfig;
use schema_registry_client::rest::schema_registry_client::{Client, SchemaRegistryClient};
use serde_json::Value;
use tracing::instrument;

use crate::RegistryOptions;

// A transport failure (a refused or reset connection, DNS, a timeout) is
// retried twice after a jittered wait of up to 100 ms, then 200 ms, as
// Node's registry client retries three times within 5 s. A hung registry
// fails a fetch after three request timeouts. The client does not retry on
// an HTTP status: a 429 or 5xx response fails the fetch at once.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const RETRIES: u32 = 2;
const RETRY_WAIT_MS: u32 = 100;
const RETRY_MAX_WAIT_MS: u32 = 1000;

// The Confluent wire format's magic byte.
const BIG_ENDIAN: u8 = 0;

// Decode looks a schema up by the id in the record's header, encode by the
// topic's subject. Both are kept for the life of the process: an id always
// names the same schema, and a subject's latest is read once, as Node does.
#[derive(Clone)]
pub struct Registry {
    client: SchemaRegistryClient,
    by_id: Cache<i32, Arc<Schema>>,
    by_subject: Cache<String, Option<Arc<Schema>>>,
    // topics that have already logged an unframed payload
    unframed: Arc<Mutex<HashSet<String>>>,
}

struct Schema {
    id: i32,
    validator: Validator,
}

impl Schema {
    fn compile(id: i32, text: &str) -> Result<Self, Failure> {
        let json: Value = serde_json::from_str(text)
            .map_err(|e| Failure::Invalid(format!("schema {id} is not JSON: {e}")))?;
        let validator = jsonschema::validator_for(&json)
            .map_err(|e| Failure::Invalid(format!("schema {id} does not compile: {e}")))?;
        Ok(Self { id, validator })
    }

    fn check(&self, payload: &[u8]) -> Result<(), Rejection> {
        let value: Value = serde_json::from_slice(payload).map_err(|e| {
            Rejection::new(
                Reason::InvalidPayload,
                format!("not JSON under schema {}: {e}", self.id),
            )
        })?;
        let errors: Vec<String> =
            self.validator.iter_errors(&value).map(|e| e.to_string()).collect();
        if errors.is_empty() {
            return Ok(());
        }
        Err(Rejection::new(
            Reason::InvalidPayload,
            format!("fails schema {}: {}", self.id, errors.join("; ")),
        ))
    }
}

impl Registry {
    pub fn new(options: RegistryOptions) -> Result<Self> {
        let mut config = RegistryConfig::new(vec![options.url]);
        config.basic_auth = Some((options.api_key, Some(options.api_secret)));
        config.max_retries = RETRIES;
        config.retries_wait_ms = RETRY_WAIT_MS;
        config.retries_max_wait_ms = RETRY_MAX_WAIT_MS;
        config.client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("issue creating schema registry client")?;

        Ok(Self {
            client: SchemaRegistryClient::new(config),
            by_id: Cache::default(),
            by_subject: Cache::default(),
            unframed: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    // Validate `buffer` against the topic's latest schema and frame it. A
    // topic with no subject sends it as it came; anything else that stops
    // the send is a `Rejection` for the guest.
    #[instrument(skip(self, buffer))]
    pub async fn encode(&self, topic: &str, buffer: Vec<u8>) -> Result<Vec<u8>, Rejection> {
        let Some(schema) = self.latest(topic).await? else {
            return Ok(buffer);
        };
        if let Err(rejection) = schema.check(&buffer) {
            tracing::error!("send to {topic} not encoded: {rejection}");
            return Err(rejection);
        }
        Ok(Payload::encode(schema.id, buffer))
    }

    // Unwrap a framed payload and validate it against the schema its header
    // names. An unframed payload comes back as it came; a framed one the
    // guest must not see is a `Rejection` naming why.
    #[instrument(skip(self, buffer))]
    pub async fn decode(&self, topic: &str, buffer: &[u8]) -> Result<Vec<u8>, Rejection> {
        let Some(framed) = Payload::decode(buffer) else {
            self.warn_unframed(topic);
            return Ok(buffer.to_vec());
        };
        let schema = self.by_id(framed.registry_id).await?;
        schema.check(framed.data)?;
        Ok(framed.data.to_vec())
    }

    async fn by_id(&self, id: i32) -> Result<Arc<Schema>, Failure> {
        let client = self.client.clone();
        self.by_id
            .get_or_fetch(id, async move {
                let fetched = match client.get_by_subject_and_id(None, id, None).await {
                    Ok(schema) => Schema::compile(id, &schema.schema),
                    Err(error) => Err(Failure::from(error)),
                };
                if let Err(failure) = &fetched {
                    tracing::error!("schema {id} fetch failed: {failure}");
                }
                fetched.map(Arc::new)
            })
            .await
    }

    async fn latest(&self, topic: &str) -> Result<Option<Arc<Schema>>, Failure> {
        let client = self.client.clone();
        let subject = format!("{topic}-value");
        self.by_subject
            .get_or_fetch(subject.clone(), async move {
                let fetched = match client.get_latest_version(&subject, None).await {
                    Ok(registered) => match (registered.id, registered.schema.as_deref()) {
                        (Some(id), Some(text)) => {
                            Schema::compile(id, text).map(|schema| Some(Arc::new(schema)))
                        }
                        _ => Err(Failure::Invalid(format!("{subject} has no schema id or text"))),
                    },
                    Err(error) => Err(Failure::from(error)),
                };
                match &fetched {
                    Err(Failure::Unknown) => {
                        tracing::warn!("{subject} has no schema; sending unframed");
                        return Ok(None);
                    }
                    Err(failure) => tracing::error!("{subject} fetch failed: {failure}"),
                    Ok(_) => {}
                }
                fetched
            })
            .await
    }

    // One warning per topic. The backend cannot tell an unframed topic from
    // a framed topic with a producer that skips the header.
    fn warn_unframed(&self, topic: &str) {
        let mut seen = self.unframed.lock().expect("unframed topics");
        if seen.insert(topic.to_owned()) {
            tracing::warn!("unframed payload on {topic}; delivering the bytes unchanged");
        }
    }
}

// --- Failures ---

// Why a fetch produced no schema. Shared by every waiter on the fetch, so
// it owns its text.
#[derive(Clone, Debug)]
enum Failure {
    // the registry could not be reached, or answered with an error
    Unavailable(String),
    // 404: the id or subject is not registered
    Unknown,
    // the registry answered with a schema the backend cannot validate with
    Invalid(String),
}

impl From<SchemaRegistryError> for Failure {
    fn from(error: SchemaRegistryError) -> Self {
        match error {
            SchemaRegistryError::ResponseError(response)
                if response.status == StatusCode::NOT_FOUND =>
            {
                Self::Unknown
            }
            SchemaRegistryError::ResponseError(response) => {
                Self::Unavailable(format!("{}: {}", response.status, response.content.trim()))
            }
            other => Self::Unavailable(other.to_string()),
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(detail) => write!(f, "registry unavailable: {detail}"),
            Self::Unknown => f.write_str("not registered"),
            Self::Invalid(detail) => write!(f, "invalid schema: {detail}"),
        }
    }
}

// Why a record was dropped or a send refused; `reason` is the counter's
// attribute, `detail` the log line's.
#[derive(Debug)]
pub struct Rejection {
    reason: Reason,
    detail: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    RegistryUnavailable,
    UnknownSchema,
    InvalidSchema,
    InvalidPayload,
}

impl Rejection {
    const fn new(reason: Reason, detail: String) -> Self {
        Self { reason, detail }
    }

    pub const fn reason(&self) -> &'static str {
        match self.reason {
            Reason::RegistryUnavailable => "registry_unavailable",
            Reason::UnknownSchema => "unknown_schema",
            Reason::InvalidSchema => "invalid_schema",
            Reason::InvalidPayload => "invalid_payload",
        }
    }
}

impl From<Failure> for Rejection {
    fn from(failure: Failure) -> Self {
        let detail = failure.to_string();
        let reason = match failure {
            Failure::Unavailable(_) => Reason::RegistryUnavailable,
            Failure::Unknown => Reason::UnknownSchema,
            Failure::Invalid(_) => Reason::InvalidSchema,
        };
        Self { reason, detail }
    }
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.detail, self.reason())
    }
}

impl std::error::Error for Rejection {}

// --- Cache ---

type Fetch<V> = Shared<BoxFuture<'static, Result<V, Failure>>>;

// Ready values and in-flight fetches by key. One fetch per key at a time:
// every caller that misses while it runs waits on it, a success is kept for
// good, and a failure fails every waiter and is forgotten, so the next
// caller fetches afresh. `tokio::sync::OnceCell` would hand a failure to the
// next waiter to retry instead.
struct Cache<K, V>(Arc<Mutex<HashMap<K, Entry<V>>>>);

enum Entry<V> {
    Ready(V),
    Fetching(Fetch<V>),
}

impl<K, V> Clone for Cache<K, V> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<K, V> Default for Cache<K, V> {
    fn default() -> Self {
        Self(Arc::default())
    }
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone + Send + Sync + 'static,
{
    async fn get_or_fetch(
        &self, key: K, fetch: impl Future<Output = Result<V, Failure>> + Send + 'static,
    ) -> Result<V, Failure> {
        let fetch = {
            let mut entries = self.0.lock().expect("schema cache");
            match entries.get(&key) {
                Some(Entry::Ready(value)) => return Ok(value.clone()),
                Some(Entry::Fetching(fetch)) => fetch.clone(),
                None => {
                    let fetch = fetch.boxed().shared();
                    entries.insert(key.clone(), Entry::Fetching(fetch.clone()));
                    fetch
                }
            }
        };

        let outcome = fetch.clone().await;

        // every waiter settles the entry, so a caller dropped mid-fetch
        // leaves nothing behind for the next one to find
        let mut entries = self.0.lock().expect("schema cache");
        if let Some(Entry::Fetching(current)) = entries.get(&key)
            && current.ptr_eq(&fetch)
        {
            match &outcome {
                Ok(value) => {
                    entries.insert(key, Entry::Ready(value.clone()));
                }
                Err(_) => {
                    entries.remove(&key);
                }
            }
        }
        outcome
    }
}

// --- Wire format ---

// The Confluent wire format: magic byte, big-endian schema id, data.
pub struct Payload<'a> {
    registry_id: i32,
    data: &'a [u8],
}

impl Payload<'_> {
    #[must_use]
    pub fn encode(registry_id: i32, payload: Vec<u8>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4 + payload.len());
        buf.push(BIG_ENDIAN);
        buf.extend(&registry_id.to_be_bytes());
        buf.extend(payload);
        buf
    }

    // `None` for a buffer without the header: too short, or another magic byte.
    // `0x00` is the Confluent magic byte. No UTF-8 JSON starts with it, so a
    // miss here is an unframed payload, not a corrupt one.
    pub fn decode(buffer: &[u8]) -> Option<Payload<'_>> {
        let ([magic_byte, id @ ..], data) = buffer.split_first_chunk::<5>()?;
        if *magic_byte != BIG_ENDIAN {
            return None;
        }
        Some(Payload {
            registry_id: i32::from_be_bytes(*id),
            data,
        })
    }
}

// The wire format codec and the single-flight cache over a scripted fetch;
// a real registry answering them is `tests/live.rs`' registry tests.
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;

    use super::*;

    #[test]
    fn encode_decode() {
        let registry_id = i32::from_be_bytes([0xAA, 0xBB, 0xCC, 0xDD]);
        let payload = b"hello world".to_vec();

        let encoded = Payload::encode(registry_id, payload.clone());

        // layout: [magic byte][registry id, 4 bytes BE][payload]
        assert_eq!(encoded[0], BIG_ENDIAN, "magic byte mismatch");
        assert_eq!(&encoded[1..5], &[0xAA, 0xBB, 0xCC, 0xDD], "registry id mismatch");
        assert_eq!(&encoded[5..], &payload, "payload mismatch");

        let decoded = Payload::decode(&encoded).expect("decode failed");

        assert_eq!(decoded.registry_id, registry_id);
        assert_eq!(decoded.data, payload.as_slice());
    }

    #[test]
    fn wrong_magic_byte() {
        let mut encoded = Payload::encode(7, b"hello world".to_vec());
        encoded[0] = 1;

        assert!(Payload::decode(&encoded).is_none());
    }

    #[test]
    fn unframed_payload_is_not_decoded() {
        assert!(Payload::decode(br#"{"stationId":"1"}"#).is_none());
        assert!(Payload::decode(b"hello").is_none());
        assert!(Payload::decode(b"hi").is_none());
        assert!(Payload::decode(&[1]).is_none());
        assert!(Payload::decode(&[1, 0, 0, 0, 9]).is_none());
        assert!(Payload::decode(&[1, 2, 3, 4, 5, 6]).is_none());
    }

    #[test]
    fn payload_is_checked_against_the_named_schema() {
        let schema = Schema::compile(9, r#"{"type":"object","required":["id"]}"#).expect("schema");
        schema.check(br#"{"id":1}"#).expect("a valid payload");

        let rejected = schema.check(br#"{"name":"x"}"#).expect_err("a missing field");
        assert_eq!(rejected.reason(), "invalid_payload");
        assert!(rejected.detail.contains("schema 9"), "{rejected}");

        let rejected = schema.check(b"not json").expect_err("not JSON");
        assert_eq!(rejected.reason(), "invalid_payload");
    }

    #[test]
    fn an_unusable_schema_is_invalid() {
        assert!(matches!(Schema::compile(1, "nope"), Err(Failure::Invalid(_))));
        assert!(matches!(Schema::compile(2, r#"{"type":"record"}"#), Err(Failure::Invalid(_))));
    }

    type Fetched = Result<u32, Failure>;

    // A fetch the test resolves by hand, counting how many times it was
    // started.
    struct Scripted {
        cache: Cache<&'static str, u32>,
        started: Arc<AtomicUsize>,
    }

    impl Scripted {
        fn new() -> Self {
            Self {
                cache: Cache::default(),
                started: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get(&self, key: &'static str) -> (oneshot::Sender<Fetched>, JoinHandle<Fetched>) {
            let (resolve, resolved) = oneshot::channel();
            let started = Arc::clone(&self.started);
            let cache = self.cache.clone();
            let task = tokio::spawn(async move {
                cache
                    .get_or_fetch(key, async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        resolved.await.expect("the test resolves the fetch")
                    })
                    .await
            });
            (resolve, task)
        }

        fn started(&self) -> usize {
            self.started.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn concurrent_waiters_share_one_fetch() {
        let scripted = Scripted::new();
        let (first, waiting_first) = scripted.get("a");
        tokio::task::yield_now().await;
        let (_second, waiting_second) = scripted.get("a");
        tokio::task::yield_now().await;
        assert_eq!(scripted.started(), 1);

        first.send(Ok(7)).expect("the fetch is awaited");
        assert_eq!(waiting_first.await.expect("joins").expect("fetched"), 7);
        assert_eq!(waiting_second.await.expect("joins").expect("fetched"), 7);

        let (_third, hit) = scripted.get("a");
        assert_eq!(hit.await.expect("joins").expect("cached"), 7);
        assert_eq!(scripted.started(), 1);
    }

    #[tokio::test]
    async fn a_failed_fetch_fails_every_waiter_and_is_not_cached() {
        let scripted = Scripted::new();
        let (first, waiting_first) = scripted.get("a");
        tokio::task::yield_now().await;
        let (_second, waiting_second) = scripted.get("a");
        tokio::task::yield_now().await;

        first.send(Err(Failure::Unknown)).expect("the fetch is awaited");
        assert!(matches!(waiting_first.await.expect("joins"), Err(Failure::Unknown)));
        assert!(matches!(waiting_second.await.expect("joins"), Err(Failure::Unknown)));

        let (retry, waiting_retry) = scripted.get("a");
        tokio::task::yield_now().await;
        assert_eq!(scripted.started(), 2, "a failure is fetched again");
        retry.send(Ok(8)).expect("the fetch is awaited");
        assert_eq!(waiting_retry.await.expect("joins").expect("fetched"), 8);
    }

    #[tokio::test]
    async fn a_caller_dropped_mid_fetch_leaves_the_fetch_to_the_next() {
        let scripted = Scripted::new();
        let (first, waiting_first) = scripted.get("a");
        tokio::task::yield_now().await;
        waiting_first.abort();
        let _ = waiting_first.await;

        let (_second, waiting_second) = scripted.get("a");
        tokio::task::yield_now().await;
        assert_eq!(scripted.started(), 1, "the in-flight fetch is joined, not restarted");

        first.send(Ok(9)).expect("the fetch is awaited");
        assert_eq!(waiting_second.await.expect("joins").expect("fetched"), 9);
        let (_third, hit) = scripted.get("a");
        assert_eq!(hit.await.expect("joins").expect("cached"), 9);
        assert_eq!(scripted.started(), 1);
    }
}
