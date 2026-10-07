use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{Result, anyhow};
use http::StatusCode;
use jsonschema::validate;
use schema_registry_client::rest::apis::Error as SchemaRegistryError;
use schema_registry_client::rest::client_config::ClientConfig as RegistryConfig;
use schema_registry_client::rest::schema_registry_client::{Client, SchemaRegistryClient};
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time;
use tracing::instrument;

use crate::RegistryOptions;

type SchemaMap = HashMap<String, Option<(i32, Value)>>;

#[derive(Clone)]
pub struct Registry {
    client: Option<SchemaRegistryClient>,
    schemas: Arc<Mutex<SchemaMap>>,
    // topics that have already logged an unframed payload
    unframed: Arc<StdMutex<HashSet<String>>>,
}

// The Confluent wire format's magic byte.
const BIG_ENDIAN: u8 = 0;

impl Registry {
    #[must_use]
    pub fn new(options: RegistryOptions) -> Self {
        let mut config = RegistryConfig::new(vec![options.url.clone()]);
        config.basic_auth = Some((options.api_key, Some(options.api_secret)));

        let sr_client = Self {
            client: Some(SchemaRegistryClient::new(config)),
            schemas: Arc::new(Mutex::new(HashMap::new())),
            unframed: Arc::new(StdMutex::new(HashSet::new())),
        };
        sr_client.start_cache_cleaner(options.cache_ttl_secs);

        sr_client
    }

    // Validate `buffer` against the topic's schema and frame it with the wire
    // format header. It goes out as it came when the topic has no schema or
    // the payload does not validate: a send is never failed here.
    #[instrument(skip(self, buffer))]
    pub async fn encode(&self, topic: &str, buffer: Vec<u8>) -> Vec<u8> {
        if self.client.is_some() {
            match self.get_schema(topic).await {
                Ok(Some((id, schema))) => {
                    let payload: Value = match serde_json::from_slice(&buffer) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::debug!("invalid JSON on {topic}: {e:?}");
                            return buffer;
                        }
                    };

                    if let Err(e) = Self::validate(&schema, &payload) {
                        tracing::debug!("JSON validation failed on {topic}: {e}");
                        return buffer;
                    }

                    Payload::encode(id, buffer)
                }
                Ok(None) => buffer,
                Err(e) => {
                    tracing::debug!("failed to fetch schema for topic {topic}: {e}");
                    buffer
                }
            }
        } else {
            buffer
        }
    }

    // Unwrap a Confluent-framed payload and validate it. Anything else —
    // no schema, a fetch failure, an unframed body, a body that does not
    // decode — goes out as it came. A framed body that fails to decode is
    // counted; the guest never sees the stripped bytes. A header naming
    // another schema is logged and still decoded.
    #[instrument(skip(self, buffer))]
    pub async fn decode(&self, topic: &str, buffer: &[u8]) -> Vec<u8> {
        if self.client.is_none() {
            return buffer.to_vec();
        }

        let Some((id, schema)) = (match self.get_schema(topic).await {
            Ok(schema) => schema,
            Err(e) => {
                tracing::debug!("failed to fetch schema for {topic}: {e}");
                return buffer.to_vec();
            }
        }) else {
            return buffer.to_vec();
        };

        let Some(decoded) = Payload::decode(buffer) else {
            self.warn_unframed(topic);
            return buffer.to_vec();
        };
        if decoded.registry_id != id {
            tracing::warn!(
                "message on topic {topic} was written with schema {}, not the latest {id}",
                decoded.registry_id
            );
        }

        let payload: Value = match serde_json::from_slice(decoded.data) {
            Ok(value) => value,
            Err(e) => {
                tracing::debug!("invalid JSON on {topic}: {e:?}");
                tracing::info!(monotonic_counter.discarded_messages = 1, topic = %topic);
                return buffer.to_vec();
            }
        };

        if let Err(e) = Self::validate(&schema, &payload) {
            tracing::debug!("schema validation failed on {topic}: {e}");
            tracing::info!(monotonic_counter.discarded_messages = 1, topic = %topic);
            return buffer.to_vec();
        }

        decoded.data.to_vec()
    }

    // One warning per topic. A registered subject does not mean every
    // producer writes the Confluent header.
    fn warn_unframed(&self, topic: &str) {
        let mut seen = self.unframed.lock().expect("unframed topics");
        if seen.insert(topic.to_owned()) {
            tracing::warn!(
                "unframed payload on {topic}, which has a registered schema; delivering the bytes \
                 unchanged"
            );
        }
    }

    pub fn validate(schema: &Value, payload: &Value) -> Result<(), String> {
        validate(schema, payload).map_err(|e| format!("Validation error: {e}"))?;
        Ok(())
    }

    async fn get_schema(&self, topic: &str) -> Result<Option<(i32, Value)>> {
        {
            let schemas = self.schemas.lock().await;
            if let Some(schema_entry) = schemas.get(topic) {
                return Ok(schema_entry.clone());
            }
        }

        // the lock is not held across the registry call
        let fetched = self.fetch_schema(topic).await?;
        self.schemas.lock().await.insert(topic.to_owned(), fetched.clone());
        Ok(fetched)
    }

    async fn fetch_schema(&self, topic: &str) -> Result<Option<(i32, Value)>> {
        let sr =
            self.client.as_ref().ok_or_else(|| anyhow!("No schema registry client available"))?;
        let subject = format!("{topic}-value");
        let schema_response = match sr.get_latest_version(&subject, None).await {
            Ok(schema) => schema,
            Err(SchemaRegistryError::ResponseError(error))
                if error.status == StatusCode::NOT_FOUND =>
            {
                return Ok(None);
            }
            Err(SchemaRegistryError::ResponseError(error)) => {
                return Err(anyhow!("Error fetching schema for topic {topic}: {}", error.content));
            }
            Err(error) => {
                return Err(anyhow!("Error fetching schema for topic {topic}: {error:?}"));
            }
        };

        let schema_str =
            schema_response.schema.as_ref().ok_or_else(|| anyhow!("Schema string is missing"))?;
        let schema_json: Value =
            serde_json::from_str(schema_str).map_err(|e| anyhow!("Invalid schema JSON: {e:?}"))?;
        let registry_id =
            schema_response.id.ok_or_else(|| anyhow!("Registry ID missing for topic {topic}"))?;
        Ok(Some((registry_id, schema_json)))
    }

    // Drop the whole cache every `cache_ttl_secs`, so a schema re-registered
    // under a topic is picked up.
    fn start_cache_cleaner(&self, cache_ttl_secs: u64) {
        let schemas_clone = Arc::clone(&self.schemas);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(cache_ttl_secs));
            loop {
                interval.tick().await;
                schemas_clone.lock().await.clear();
                tracing::info!("Schema cache cleared");
            }
        });
    }
}

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

// The wire format codec alone; a real registry accepting the layout is
// `tests/live.rs::registry_wire_format`.
#[cfg(test)]
mod tests {
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
}
