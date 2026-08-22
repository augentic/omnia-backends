use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use async_nats::jetstream::kv::Config;
use async_nats::jetstream::{self, kv};
use futures::TryStreamExt;
use futures::future::FutureExt;
use omnia_wasi_keyvalue::{Bucket, Cas, FutureResult, WasiKeyValueCtx};

use crate::Client;

/// `wasi-keyvalue` implementation backed by NATS JetStream KV store.
impl WasiKeyValueCtx for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening bucket: {identifier}");
        let client = self.inner.clone();

        async move {
            let jetstream = jetstream::new(client);
            let store = if let Ok(store) = jetstream.get_key_value(&identifier).await {
                store
            } else {
                let result = jetstream
                    .create_key_value(Config {
                        bucket: identifier,
                        history: 1,
                        max_age: Duration::from_mins(10),
                        max_bytes: 100 * 1024 * 1024, // 100 MiB
                        ..Config::default()
                    })
                    .await;

                result.context("failed to create bucket")?
            };

            Ok(Arc::new(KvBucket(store)) as Arc<dyn Bucket>)
        }
        .boxed()
    }
}

/// A key-value bucket backed by a NATS JetStream KV store.
#[derive(Debug)]
pub struct KvBucket(pub kv::Store);

impl Bucket for KvBucket {
    fn get(&self, key: String) -> FutureResult<Option<Vec<u8>>> {
        tracing::trace!("getting key: {key}");
        let store = self.0.clone();

        async move {
            let entry = store.get(key).await.context("getting key")?;
            Ok(entry.map(Into::into))
        }
        .boxed()
    }

    fn set(&self, key: String, value: Vec<u8>) -> FutureResult<()> {
        tracing::trace!("setting key: {key}");
        let store = self.0.clone();

        async move {
            store.put(key, value.into()).await.context("setting key")?;
            Ok(())
        }
        .boxed()
    }

    fn delete(&self, key: String) -> FutureResult<()> {
        tracing::trace!("deleting key: {key}");
        let store = self.0.clone();

        async move {
            store.delete(key).await.context("deleting key")?;
            Ok(())
        }
        .boxed()
    }

    fn exists(&self, key: String) -> FutureResult<bool> {
        tracing::trace!("checking existence of key: {key}");
        let store = self.0.clone();

        async move {
            let entry = store.get(key).await.context("checking key")?;
            Ok(entry.is_some())
        }
        .boxed()
    }

    fn keys(&self) -> FutureResult<Vec<String>> {
        let store = self.0.clone();

        async move {
            tracing::trace!("listing keys");

            let key_results = store.keys().await.context("listing keys")?;
            let keys =
                key_results.try_filter_map(|k| async move { Ok(Some(k)) }).try_collect().await?;

            Ok(keys)
        }
        .boxed()
    }

    fn increment(&self, key: String, delta: i64) -> FutureResult<i64> {
        tracing::trace!("incrementing key: {key}");
        let store = self.0.clone();

        async move {
            // JetStream KV has no native increment; loop a revision-conditioned
            // read-modify-write, retrying only on lost races.
            loop {
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
        }
        .boxed()
    }

    fn swap(&self, cas: Cas, value: Vec<u8>) -> FutureResult<std::result::Result<(), Cas>> {
        tracing::trace!("swapping key: {}", cas.key);
        let store = self.0.clone();

        async move {
            // Snapshot the live entry: value for the compare, revision for
            // the conditioned write.
            let live = live_entry(&store, &cas.key).await?;
            let observed = live.as_ref().map(|entry| entry.value.to_vec());
            if observed != cas.current {
                return Ok(Err(Cas {
                    current: observed,
                    ..cas
                }));
            }

            let outcome = match &live {
                Some(entry) => store
                    .update(&cas.key, value.into(), entry.revision)
                    .await
                    .map(|_rev| ())
                    .map_err(|err| {
                        let raced = err.kind() == kv::UpdateErrorKind::WrongLastRevision;
                        (raced, anyhow::Error::new(err))
                    }),
                // Absent-expected: `create` refuses if the key appeared meanwhile.
                None => store.create(&cas.key, value.into()).await.map(|_rev| ()).map_err(|err| {
                    let raced = err.kind() == kv::CreateErrorKind::AlreadyExists;
                    (raced, anyhow::Error::new(err))
                }),
            };

            match outcome {
                Ok(()) => Ok(Ok(())),
                // Lost the race between snapshot and write: refresh the
                // handle at the observed value so the caller can retry.
                Err((true, _raced)) => {
                    let observed =
                        live_entry(&store, &cas.key).await?.map(|entry| entry.value.to_vec());
                    Ok(Err(Cas {
                        current: observed,
                        ..cas
                    }))
                }
                Err((false, err)) => Err(err.context(format!("swapping `{}`", cas.key))),
            }
        }
        .boxed()
    }
}

/// The live entry for `key`: delete and purge tombstones read as absent.
async fn live_entry(store: &kv::Store, key: &str) -> Result<Option<kv::Entry>> {
    let entry = store.entry(key).await.with_context(|| format!("reading `{key}`"))?;
    Ok(entry.filter(|entry| entry.operation == kv::Operation::Put))
}

/// Counter values are 8-byte big-endian `i64`, matching the other backends.
fn decode_counter(key: &str, value: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_len_mismatch| {
        anyhow!("value at `{key}` is {} bytes, not an 8-byte big-endian integer", value.len())
    })?;
    Ok(i64::from_be_bytes(bytes))
}
