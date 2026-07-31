//! Key-value implementation for the Redis backend.
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Context;
use futures::FutureExt;
use omnia_wasi_keyvalue::{Bucket, FutureResult, WasiKeyValueCtx};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::Client;

const TTL_DAY: u64 = 24 * 60 * 60; // 1 day

/// `wasi-keyvalue` implementation backed by Redis.
impl WasiKeyValueCtx for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        tracing::trace!("opening redis bucket: {}", identifier);
        let conn = self.0.clone();

        async move {
            let bucket = RedisBucket {
                identifier,
                conn: Conn(conn.clone()),
            };
            Ok(Arc::new(bucket) as Arc<dyn Bucket>)
        }
        .boxed()
    }
}

/// Wrapper around [`ConnectionManager`] to implement [`Debug`].
pub struct Conn(ConnectionManager);

impl Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager").finish_non_exhaustive()
    }
}

/// A key-value bucket backed by Redis, namespaced by identifier.
#[derive(Debug)]
pub struct RedisBucket {
    /// Bucket identifier used as key prefix.
    pub identifier: String,
    /// Redis connection.
    pub conn: Conn,
}

impl Bucket for RedisBucket {
    fn get(&self, key: String) -> FutureResult<Option<Vec<u8>>> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();
        async move {
            conn.get(key.clone()).await.with_context(|| format!("failed to get value for {key}"))
        }
        .boxed()
    }

    fn set(&self, key: String, value: Vec<u8>) -> FutureResult<()> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();

        async move {
            conn.set_ex(&key, value, TTL_DAY)
                .await
                .with_context(|| format!("failed to set value for {key}"))
        }
        .boxed()
    }

    fn delete(&self, key: String) -> FutureResult<()> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();
        async move {
            conn.del(key.clone()).await.with_context(|| format!("failed to delete value for {key}"))
        }
        .boxed()
    }

    fn exists(&self, key: String) -> FutureResult<bool> {
        let key = format!("{}:{key}", self.identifier);
        let mut conn = self.conn.0.clone();
        async move {
            conn.exists(key.clone())
                .await
                .with_context(|| format!("failed to check existence of key {key}"))
        }
        .boxed()
    }

    fn keys(&self) -> FutureResult<Vec<String>> {
        let mut conn = self.conn.0.clone();
        let pattern = format!("{}:*", self.identifier);
        async move {
            conn.keys(pattern.clone())
                .await
                .with_context(|| format!("failed to list keys for {pattern}"))
        }
        .boxed()
    }
}
