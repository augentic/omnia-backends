use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_nats::jetstream::context::KeyValueErrorKind;
use async_nats::jetstream::kv::{self, Config};
use futures::TryStreamExt;
use futures::future::FutureExt;
use omnia_backend::FutureResult;
use omnia_backend::keyvalue::{Bucket, KeyValueClient};

use crate::Client;

const MAX_INCREMENT_RETRIES: usize = 64;

impl KeyValueClient for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening bucket: {identifier}");
        let jetstream = self.jetstream.clone();
        let max_age = self.kv_max_age.unwrap_or_default();
        let expiries = Arc::clone(&self.expiries);

        async move {
            let store = match jetstream.get_key_value(&identifier).await {
                Ok(store) => store,
                Err(err) if err.kind() == KeyValueErrorKind::GetBucket => jetstream
                    .create_key_value(Config {
                        bucket: identifier.clone(),
                        history: 1,
                        max_age,
                        max_bytes: 100 * 1024 * 1024,
                        ..Config::default()
                    })
                    .await
                    .context("failed to create bucket")?,
                Err(err) => return Err(err).context("failed to open bucket"),
            };

            Ok(Arc::new(KvBucket {
                name: identifier,
                store,
                expiries,
            }) as Arc<dyn Bucket>)
        }
        .boxed()
    }
}

#[derive(Debug)]
struct KvBucket {
    name: String,
    store: kv::Store,
    expiries: Arc<Mutex<HashMap<String, Instant>>>,
}

impl KvBucket {
    fn scoped(&self, key: &str) -> String {
        format!("{}:{key}", self.name)
    }

    fn set_expiry(&self, key: &str, ttl: Option<Duration>) {
        let mut expiries = self.expiries.lock().expect("expiries lock poisoned");
        let scoped = self.scoped(key);
        match ttl {
            Some(ttl) => {
                expiries.insert(scoped, Instant::now() + ttl);
            }
            None => {
                expiries.remove(&scoped);
            }
        }
    }

    fn take_if_expired(&self, key: &str) -> bool {
        let mut expiries = self.expiries.lock().expect("expiries lock poisoned");
        let scoped = self.scoped(key);
        if expiries.get(&scoped).is_some_and(|&at| Instant::now() >= at) {
            expiries.remove(&scoped);
            true
        } else {
            false
        }
    }
}

impl Bucket for KvBucket {
    fn get(&self, key: String) -> FutureResult<Option<Vec<u8>>> {
        tracing::trace!("getting key: {key}");
        let store = self.store.clone();
        let expired = self.take_if_expired(&key);

        async move {
            if expired {
                let _ = store.delete(&key).await;
                return Ok(None);
            }
            let entry = store.get(key).await.context("getting key")?;
            Ok(entry.map(Into::into))
        }
        .boxed()
    }

    fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> FutureResult<()> {
        tracing::trace!("setting key: {key}");
        let store = self.store.clone();
        self.set_expiry(&key, ttl);

        async move {
            store.put(key, value.into()).await.context("setting key")?;
            Ok(())
        }
        .boxed()
    }

    fn delete(&self, key: String) -> FutureResult<()> {
        tracing::trace!("deleting key: {key}");
        let store = self.store.clone();
        self.set_expiry(&key, None);

        async move {
            store.delete(key).await.context("deleting key")?;
            Ok(())
        }
        .boxed()
    }

    fn exists(&self, key: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of key: {key}");
        let store = self.store.clone();
        let expired = self.take_if_expired(&key);

        async move {
            if expired {
                let _ = store.delete(&key).await;
                return Ok(false);
            }
            let entry = store.get(key).await.context("checking key")?;
            Ok(entry.is_some())
        }
        .boxed()
    }

    fn keys(&self) -> FutureResult<Vec<String>> {
        let store = self.store.clone();
        let bucket_name = self.name.clone();
        let expiries = Arc::clone(&self.expiries);

        async move {
            tracing::trace!("listing keys");

            let key_results = store.keys().await.context("listing keys")?;
            let raw_keys: Vec<String> =
                key_results.try_filter_map(|k| async move { Ok(Some(k)) }).try_collect().await?;
            let now = Instant::now();
            let mut guard = expiries.lock().expect("expiries lock poisoned");
            let keys = raw_keys
                .into_iter()
                .filter(|k| {
                    let scoped = format!("{bucket_name}:{k}");
                    if guard.get(&scoped).is_some_and(|&at| now >= at) {
                        guard.remove(&scoped);
                        false
                    } else {
                        true
                    }
                })
                .collect();

            Ok(keys)
        }
        .boxed()
    }

    fn increment(&self, key: String, delta: i64) -> FutureResult<i64> {
        tracing::trace!("incrementing key: {key}");
        let store = self.store.clone();
        let expired = self.take_if_expired(&key);

        async move {
            if expired {
                let _ = store.delete(&key).await;
            }
            // JetStream KV has no native increment; loop a revision-conditioned
            // read-modify-write, retrying only on lost races.
            for _ in 0..MAX_INCREMENT_RETRIES {
                let live = live_entry(&store, &key).await?;
                let base = match &live {
                    None => 0,
                    Some(entry) => decode_counter(&key, entry.value.as_ref())?,
                };
                let incremented = base
                    .checked_add(delta)
                    .with_context(|| format!("incrementing `{key}` by {delta} overflows i64"))?;
                let payload = incremented.to_be_bytes().to_vec().into();

                match &live {
                    Some(entry) => match store.update(&key, payload, entry.revision).await {
                        Ok(_revision) => return Ok(incremented),
                        Err(err) if err.kind() == kv::UpdateErrorKind::WrongLastRevision => {}
                        Err(err) => return Err(err).context("updating counter"),
                    },
                    None => match store.create(&key, payload).await {
                        Ok(_revision) => return Ok(incremented),
                        Err(err) if err.kind() == kv::CreateErrorKind::AlreadyExists => {}
                        Err(err) => return Err(err).context("creating counter"),
                    },
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
        let store = self.store.clone();
        let expired = self.take_if_expired(&key);

        async move {
            if expired {
                let _ = store.delete(&key).await;
            }
            // Snapshot the live entry: value for the compare, revision for
            // the conditioned write.
            let live = live_entry(&store, &key).await?;
            let observed = live.as_ref().map(|entry| entry.value.to_vec());
            if observed != expected {
                return Ok(Err(observed));
            }

            let outcome = match &live {
                Some(entry) => {
                    store.update(&key, value.into(), entry.revision).await.map(|_rev| ()).map_err(
                        |err| {
                            let raced = err.kind() == kv::UpdateErrorKind::WrongLastRevision;
                            (raced, anyhow::Error::new(err))
                        },
                    )
                }
                // Absent-expected: `create` refuses if the key appeared meanwhile.
                None => store.create(&key, value.into()).await.map(|_rev| ()).map_err(|err| {
                    let raced = err.kind() == kv::CreateErrorKind::AlreadyExists;
                    (raced, anyhow::Error::new(err))
                }),
            };

            match outcome {
                Ok(()) => Ok(Ok(())),
                // Lost the race between snapshot and write: return the
                // observed value so the caller can retry.
                Err((true, _raced)) => {
                    let observed =
                        live_entry(&store, &key).await?.map(|entry| entry.value.to_vec());
                    Ok(Err(observed))
                }
                Err((false, err)) => Err(err.context(format!("swapping `{key}`"))),
            }
        }
        .boxed()
    }
}

// The live entry for `key`: delete and purge tombstones read as absent.
async fn live_entry(store: &kv::Store, key: &str) -> Result<Option<kv::Entry>> {
    let entry = store.entry(key).await.with_context(|| format!("reading `{key}`"))?;
    Ok(entry.filter(|entry| entry.operation == kv::Operation::Put))
}

// Counter values are 8-byte big-endian `i64`, matching the other backends.
fn decode_counter(key: &str, value: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_len_mismatch| {
        anyhow!("value at `{key}` is {} bytes, not an 8-byte big-endian integer", value.len())
    })?;
    Ok(i64::from_be_bytes(bytes))
}

// Counter wire-format decoding; JetStream KV operations are covered by
// `tests/live.rs::keyvalue_atomics`.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_counter_values() {
        assert_eq!(decode_counter("k", &42_i64.to_be_bytes()).unwrap(), 42);
        assert_eq!(decode_counter("k", &(-7_i64).to_be_bytes()).unwrap(), -7);
        assert_eq!(decode_counter("k", &i64::MAX.to_be_bytes()).unwrap(), i64::MAX);
        assert_eq!(decode_counter("k", &i64::MIN.to_be_bytes()).unwrap(), i64::MIN);

        let err = decode_counter("bad", &[1, 2, 3]).unwrap_err();
        assert!(err.to_string().contains("`bad` is 3 bytes"));
    }
}
