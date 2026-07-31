#![doc = include_str!("../README.md")]

mod sql;
mod types;

use std::collections::HashMap;

use anyhow::{Context as _, Result, anyhow};
use deadpool_postgres::{Pool, PoolConfig, Runtime};
use omnia::Backend;
use rustls::crypto::ring;
use rustls::{ClientConfig, RootCertStore};
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::instrument;
use webpki_roots::TLS_SERVER_ROOTS;

/// Postgres client
#[derive(Clone, Debug)]
pub struct Client(HashMap<String, Pool>);

/// Postgres resource builder
impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    /// Connect to `PostgreSQL` with provided options and return a connection pool
    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let mut pools = HashMap::new();
        let runtime = Some(Runtime::Tokio1);
        let mut tls_factory: Option<MakeRustlsConnect> = None; // factory is cheaper to clone

        for entry in std::iter::once(&options.default_pool).chain(&options.additional_pools) {
            // deadpool parses `url` itself (via tokio_postgres); parse here only
            // to decide whether the connection needs TLS.
            let tokio: tokio_postgres::Config =
                entry.uri.parse().context("parsing Postgres URI")?;
            let use_tls = matches!(tokio.get_ssl_mode(), SslMode::Require | SslMode::Prefer);

            let mut pool_config = deadpool_postgres::Config::new();
            pool_config.url = Some(entry.uri.clone());
            pool_config.pool = Some(PoolConfig {
                max_size: entry.pool_size,
                ..PoolConfig::default()
            });

            let pool = if use_tls {
                let factory = if let Some(f) = &tls_factory {
                    f.clone()
                } else {
                    ring::default_provider()
                        .install_default()
                        .map_err(|_e| anyhow!("Failed to install rustls crypto provider"))?;

                    let mut cert_store = RootCertStore::empty();
                    cert_store.extend(TLS_SERVER_ROOTS.iter().cloned());

                    let client_config = ClientConfig::builder()
                        .with_root_certificates(cert_store)
                        .with_no_client_auth();

                    let factory = MakeRustlsConnect::new(client_config);
                    tls_factory = Some(factory.clone());

                    factory
                };

                pool_config
                    .create_pool(runtime, factory)
                    .context(format!("failed to create postgres pool: '{}'", entry.name))?
            } else {
                pool_config
                    .create_pool(runtime, tokio_postgres::NoTls)
                    .context(format!("failed to create postgres pool: '{}'", entry.name))?
            };

            // Check pool is usable
            let cnn = pool.get().await;
            if cnn.is_err() {
                return Err(anyhow!("failed to get connection from pool: {:?}", cnn.err()));
            }

            tracing::info!(
                "connected to Postgres database {:?}, with pool name '{}', tls '{}'",
                tokio.get_dbname().unwrap_or_default(),
                entry.name,
                use_tls
            );
            pools.insert(entry.name.clone(), pool);
        }

        Ok(Self(pools))
    }
}

/// A named connection pool entry.
#[derive(Debug, Clone)]
pub struct PoolEntry {
    /// Pool name (e.g. `"EVENTSTORE"`). Used as lookup key and env var suffix.
    pub name: String,
    /// `PostgreSQL` connection URI.
    pub uri: String,
    /// Maximum number of connections in the pool.
    pub pool_size: usize,
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    use super::PoolEntry;

    /// Connection options for the `PostgreSQL` backend.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Default connection pool (from `POSTGRES_URL`).
        pub default_pool: PoolEntry,
        /// Additional named pools discovered from `POSTGRES_POOLS`.
        pub additional_pools: Vec<PoolEntry>,
    }
}
pub use config::ConnectOptions;

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
        // Self::from_env().finalize().context("issue loading connection options")
    }
}
