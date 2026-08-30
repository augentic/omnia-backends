#![doc = include_str!("../README.md")]

mod compose;
mod store;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, anyhow, bail};
use futures::FutureExt as _;
use futures::future::BoxFuture;
use omnia::{Acquire, AcquireContext, AcquireError, Location};
use tokio::sync::Mutex;
use wasm_pkg_client::caching::CachingClient;
use wasm_pkg_client::{Client, Config, ContentStream, PackageRef, Registry, Release, Version};

pub use self::compose::{AcquireExt, Or};
pub use self::store::{ContentStore, sha256_digest};

/// Registry acquisition for omnia's plugin loader over [wasm-pkg-client].
///
/// Fetches exact package versions, optionally cached in a [`ContentStore`],
/// and verifies every result against the registry's content digest before
/// returning bytes.
///
/// Serves [`Location::Registry`] only; compose with `MountAcquire` through
/// [`AcquireExt::or`] for preopen-relative paths. The operator's own sha256
/// pin is verified host-side by the loader, after acquisition.
///
/// [wasm-pkg-client]: https://github.com/bytecodealliance/wasm-pkg-tools
pub struct RegistryAcquire {
    default_registry: String,
    config: Config,
    store: Option<ContentStore>,
    // One fetcher per effective registry: `Client` resolves endpoints from
    // its `Config`, not per call, so each endpoint override needs its own
    // client. All of them share one content store.
    fetchers: Mutex<HashMap<Registry, Arc<Fetcher>>>,
}

impl RegistryAcquire {
    /// Acquirer whose default endpoint is `default_registry`
    /// (a `Location::Registry(None)` load resolves there).
    ///
    /// Starts from an empty client configuration — no user-global wasm-pkg
    /// config file and no hard-coded fallback registries — so the compiled
    /// binary alone attests which endpoints the deployment may reach.
    /// Cacheless until [`cached`](Self::cached) attaches a store; an invalid
    /// registry name refuses as a typed failure at first use.
    #[must_use]
    pub fn new(default_registry: impl Into<String>) -> Self {
        Self {
            default_registry: default_registry.into(),
            config: Config::empty(),
            store: None,
            fetchers: Mutex::new(HashMap::new()),
        }
    }

    /// Replaces the client configuration (per-registry backend and
    /// credential settings). The acquirer's default registry and per-load
    /// overrides still take precedence over the configuration's own default.
    #[must_use]
    pub fn with_config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Attaches a content-addressed store; fetched content is persisted
    /// verify-before-persist and served from the store on later loads.
    #[must_use]
    pub fn cached(mut self, store: ContentStore) -> Self {
        self.store = Some(store);
        self
    }

    /// Attaches a [`ContentStore`] rooted at `root` (created lazily).
    #[must_use]
    pub fn cached_at(self, root: impl Into<PathBuf>) -> Self {
        self.cached(ContentStore::new(root))
    }

    async fn fetcher(&self, registry: Registry) -> Arc<Fetcher> {
        let mut fetchers = self.fetchers.lock().await;
        if let Some(fetcher) = fetchers.get(&registry) {
            return Arc::clone(fetcher);
        }
        let mut config = self.config.clone();
        config.set_default_registry(Some(registry.clone()));
        let client = Client::new(config);
        let fetcher = Arc::new(match &self.store {
            Some(store) => Fetcher::Cached(CachingClient::new(
                Some(client),
                store.clone().scoped_to(&registry),
            )),
            None => Fetcher::Direct(client),
        });
        fetchers.insert(registry, Arc::clone(&fetcher));
        fetcher
    }
}

impl Acquire for RegistryAcquire {
    fn acquire<'a>(
        &'a self, package: &'a str, from: &'a Location, _context: &'a AcquireContext,
    ) -> BoxFuture<'a, Result<Vec<u8>, AcquireError>> {
        async move {
            let Location::Registry(endpoint) = from else {
                return Err(AcquireError::Unsupported(format!(
                    "RegistryAcquire serves registry locations only; acquiring `{package}` \
                     from {from} requires a path acquirer such as MountAcquire"
                )));
            };
            let (package_ref, version) = parse_package(package).map_err(AcquireError::Failed)?;
            let registry = endpoint.as_deref().unwrap_or(&self.default_registry);
            let registry: Registry = registry.parse().map_err(|error| {
                AcquireError::Failed(anyhow!("registry `{registry}` is not a valid name: {error}"))
            })?;

            let fetcher = self.fetcher(registry).await;
            let release = fetcher.release(&package_ref, &version).await.map_err(|error| {
                AcquireError::Failed(
                    anyhow::Error::new(error).context(format!("resolving `{package}`")),
                )
            })?;
            let content = fetcher.content(&package_ref, &release).await.map_err(|error| {
                AcquireError::Failed(
                    anyhow::Error::new(error).context(format!("fetching `{package}`")),
                )
            })?;
            let bytes = store::collect(content).await.map_err(|error| {
                AcquireError::Failed(
                    anyhow::Error::new(error).context(format!("reading `{package}`")),
                )
            })?;

            // Self-verifying reads: store hits are otherwise unvalidated, so
            // a poisoned or truncated entry fails its hash here instead of
            // becoming code.
            let resolved = store::sha256_digest(&bytes);
            if resolved != release.content_digest {
                return Err(AcquireError::Failed(anyhow!(
                    "package `{package}` content hashes to {resolved}, not the registry \
                     digest {}",
                    release.content_digest
                )));
            }
            tracing::debug!(package, digest = %resolved, "package acquired");
            Ok(bytes)
        }
        .boxed()
    }
}

/// The per-registry client: caching when the acquirer holds a store, the
/// bare wasm-pkg client otherwise (every load reads fresh).
enum Fetcher {
    Cached(CachingClient<ContentStore>),
    Direct(Client),
}

impl Fetcher {
    async fn release(
        &self, package: &PackageRef, version: &Version,
    ) -> Result<Release, wasm_pkg_client::Error> {
        match self {
            Self::Cached(client) => client.get_release(package, version).await,
            Self::Direct(client) => client.get_release(package, version).await,
        }
    }

    async fn content(
        &self, package: &PackageRef, release: &Release,
    ) -> Result<ContentStream, wasm_pkg_client::Error> {
        match self {
            Self::Cached(client) => client.get_content(package, release).await,
            Self::Direct(client) => client.stream_content(package, release).await,
        }
    }
}

/// Split an exact `namespace:name@version` reference; remote lookup never
/// resolves "latest".
fn parse_package(package: &str) -> anyhow::Result<(PackageRef, Version)> {
    let Some((name, version)) = package.split_once('@') else {
        bail!("registry package `{package}` must pin an exact version (`namespace:name@version`)")
    };
    let package_ref = name.parse().with_context(|| {
        format!("package `{package}` is not a `namespace:name@version` reference")
    })?;
    let version = version
        .parse()
        .with_context(|| format!("package `{package}` does not pin an exact semver version"))?;
    Ok((package_ref, version))
}
