use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
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

impl Registry {
    pub fn new(options: RegistryOptions) -> Result<Self> {
        ensure!(
            options.api_key.is_some() || options.api_secret.is_none(),
            "KAFKA_REGISTRY_API_SECRET requires KAFKA_REGISTRY_API_KEY"
        );
        let mut config = RegistryConfig::new(vec![options.url]);
        config.basic_auth = options.api_key.map(|key| (key, options.api_secret));
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    RegistryUnavailable,
    UnknownSchema,
    InvalidSchema,
    InvalidPayload,
}

// --- Cache ---

type Fetch<V> = Shared<BoxFuture<'static, Result<V, Failure>>>;

// Ready values and in-flight fetches by key. One fetch per key at a time:
// every caller that misses while it runs waits on it, a success is kept for
// good, and a failure fails every waiter and is forgotten, so the next
// caller fetches afresh. `tokio::sync::OnceCell` would hand a failure to the
// next waiter to retry instead.
struct Cache<K, V>(Arc<Mutex<HashMap<K, Entry<V>>>>);

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

enum Entry<V> {
    Ready(V),
    Fetching(Fetch<V>),
}

// --- Wire format ---

// The Confluent wire format's magic byte.
const MAGIC_BYTE: u8 = 0;

// The Confluent wire format: magic byte, big-endian schema id, data.
struct Payload<'a> {
    registry_id: i32,
    data: &'a [u8],
}

impl Payload<'_> {
    #[must_use]
    fn encode(registry_id: i32, payload: Vec<u8>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4 + payload.len());
        buf.push(MAGIC_BYTE);
        buf.extend(&registry_id.to_be_bytes());
        buf.extend(payload);
        buf
    }

    // `None` for a buffer without the header: too short, or another magic byte.
    // `0x00` is the Confluent magic byte. No UTF-8 JSON starts with it, so a
    // miss here is an unframed payload, not a corrupt one.
    fn decode(buffer: &[u8]) -> Option<Payload<'_>> {
        let ([magic_byte, id @ ..], data) = buffer.split_first_chunk::<5>()?;
        if *magic_byte != MAGIC_BYTE {
            return None;
        }
        Some(Payload {
            registry_id: i32::from_be_bytes(*id),
            data,
        })
    }
}
