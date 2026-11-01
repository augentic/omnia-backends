#![doc = include_str!("../README.md")]

mod sql;
mod types;

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use anyhow::{Context as _, Result};
use deadpool_postgres::{
    Manager, ManagerConfig, Pool, PoolConfig, QueueMode, RecyclingMethod, Runtime, Timeouts,
};
use omnia::Backend;
use rustls::crypto::{CryptoProvider, ring};
use rustls::{ClientConfig, RootCertStore};
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::instrument;
use webpki_roots::TLS_SERVER_ROOTS;

/// `PostgreSQL` backend client managing one or more named connection pools.
#[derive(Clone)]
pub struct Client(HashMap<String, Pool>);

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").field("pools", &self.0.keys()).finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let mut pools = HashMap::new();
        let mut tls: Option<MakeRustlsConnect> = None;

        for entry in std::iter::once(&options.default_pool).chain(&options.additional_pools) {
            let mut pg_config: tokio_postgres::Config =
                entry.uri.parse().context("parsing Postgres URI")?;
            pg_config.keepalives_idle(KEEPALIVE_IDLE);
            let dbname = pg_config.get_dbname().unwrap_or_default().to_owned();
            let use_tls = matches!(pg_config.get_ssl_mode(), SslMode::Require | SslMode::Prefer);

            let manager_config = ManagerConfig {
                recycling_method: RecyclingMethod::Verified,
            };
            let manager = if use_tls {
                let tls = tls.get_or_insert_with(tls_connector).clone();
                Manager::from_config(pg_config, tls, manager_config)
            } else {
                Manager::from_config(pg_config, tokio_postgres::NoTls, manager_config)
            };
            let pool = Pool::builder(manager)
                .config(PoolConfig {
                    max_size: entry.pool_size,
                    timeouts: Timeouts {
                        wait: Some(WAIT_TIMEOUT),
                        create: Some(CREATE_TIMEOUT),
                        recycle: Some(RECYCLE_TIMEOUT),
                    },
                    queue_mode: QueueMode::Lifo,
                })
                .runtime(Runtime::Tokio1)
                .build()
                .with_context(|| format!("failed to create postgres pool '{}'", entry.name))?;

            // check the pool is usable
            let _cnn = pool
                .get()
                .await
                .with_context(|| format!("failed to get connection from pool '{}'", entry.name))?;

            tracing::info!(
                "connected to Postgres database {dbname:?}, with pool name '{}', tls '{use_tls}'",
                entry.name
            );
            reap_idle(&pool);
            pools.insert(entry.name.clone(), pool);
        }

        Ok(Self(pools))
    }
}

// A connection idles no longer than a cloud NAT gateway tolerates (about four
// minutes on Azure), and a held one is probed well inside that window.
const REAP_INTERVAL: Duration = Duration::from_secs(30);
const MAX_IDLE: Duration = Duration::from_secs(60);
const KEEPALIVE_IDLE: Duration = Duration::from_secs(30);

// A checkout pays up to `RECYCLE_TIMEOUT` per unresponsive idle connection it
// discards, then `CREATE_TIMEOUT` for the replacement; a shorter recycle limit
// would close healthy connections on a slow round trip.
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const CREATE_TIMEOUT: Duration = Duration::from_secs(5);
const RECYCLE_TIMEOUT: Duration = Duration::from_secs(2);

// Holds a weak handle so the task ends once every `Client` clone is dropped.
fn reap_idle(pool: &Pool) {
    let weak = pool.weak();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(REAP_INTERVAL).await;
            let Some(pool) = weak.upgrade() else { break };
            pool.retain(|_, metrics| metrics.last_used() < MAX_IDLE);
        }
    });
}

// Another backend (e.g. omnia-wasi-http's default impl) may have installed
// the process crypto provider first; both install ring, so an already-set
// default is fine.
fn tls_connector() -> MakeRustlsConnect {
    if CryptoProvider::get_default().is_none() {
        let _ = ring::default_provider().install_default();
    }
    let mut roots = RootCertStore::empty();
    roots.extend(TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    MakeRustlsConnect::new(config)
}

/// A named connection pool entry.
#[derive(Clone)]
pub struct PoolEntry {
    /// Pool name (e.g. `"EVENTSTORE"`). Used as lookup key and env var suffix.
    pub name: String,
    /// `PostgreSQL` connection URI.
    pub uri: String,
    /// Maximum number of connections in the pool.
    pub pool_size: usize,
}

impl fmt::Debug for PoolEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolEntry")
            .field("name", &self.name)
            .field("uri", &"<redacted>")
            .field("pool_size", &self.pool_size)
            .finish()
    }
}

/// Connection options for the `PostgreSQL` backend.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Default connection pool (from `POSTGRES_URL`).
    pub default_pool: PoolEntry,
    /// Additional named pools discovered from `POSTGRES_POOLS`.
    pub additional_pools: Vec<PoolEntry>,
}

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        // default pool (required)
        let default_uri = std::env::var("POSTGRES_URL").context("POSTGRES_URL must be set");
        let default_size =
            std::env::var("POSTGRES_POOL_SIZE").unwrap_or_default().parse().unwrap_or(10);

        let default = PoolEntry {
            name: "default".to_ascii_uppercase(),
            uri: default_uri?,
            pool_size: default_size,
        };

        // optional extra pools: POSTGRES_POOLS=eventstore
        let extras = std::env::var("POSTGRES_POOLS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(|name| -> anyhow::Result<PoolEntry> {
                let name = name.to_ascii_uppercase();
                let uri_key = format!("POSTGRES_URL__{name}");
                let size_key = format!("POSTGRES_POOL_SIZE__{name}");

                let uri = std::env::var(&uri_key)
                    .with_context(|| format!("missing {uri_key} for pool {name}"))?;
                let pool_size = std::env::var(&size_key)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(default.pool_size);

                Ok(PoolEntry { name, uri, pool_size })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            default_pool: default,
            additional_pools: extras,
        })
    }
}
