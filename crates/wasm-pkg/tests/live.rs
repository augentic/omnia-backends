//! Live OCI round-trip: publish a minimal component to a real registry and
//! acquire it back through `RegistryAcquire`.
//!
//! Run recipe (also in the README):
//!
//! ```sh
//! docker run --rm -p 5000:5000 distribution/distribution:edge
//! cargo nextest run -p omnia-wasm-pkg --run-ignored all
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use omnia::{Acquire as _, AcquireContext, Location, MountRegistry};
use omnia_wasm_pkg::RegistryAcquire;
use tempfile::TempDir;
use wasm_pkg_client::{Client, Config, Error, PublishOpts, Registry};

const REGISTRY: &str = "localhost:5000";
const PACKAGE: &str = "omnia-live:smoke@0.0.1";

#[derive(serde::Serialize)]
struct OciBackendConfig {
    protocol: String,
}

/// A plain-HTTP OCI backend for the local registry container.
fn plain_http_config() -> Result<Config> {
    let mut config = Config::empty();
    let registry: Registry = REGISTRY.parse()?;
    let backend = config.get_or_insert_registry_config_mut(&registry);
    backend.set_default_backend(Some("oci".into()));
    backend.set_backend_config(
        "oci",
        OciBackendConfig {
            protocol: "http".into(),
        },
    )?;
    Ok(config)
}

#[tokio::test]
#[ignore = "live: needs a reachable OCI registry at localhost:5000; run with --run-ignored all"]
async fn live_oci_publish_then_acquire() -> Result<()> {
    let bytes = wat::parse_str("(component)")?;

    let mut config = plain_http_config()?;
    config.set_default_registry(Some(REGISTRY.parse()?));
    let publisher = Client::new(config);
    let (name, version) = PACKAGE.split_once('@').expect("the test package pins a version");
    let publish = publisher
        .publish_release_data(
            Box::pin(std::io::Cursor::new(bytes.clone())),
            PublishOpts {
                package: Some((name.parse()?, version.parse()?)),
                ..PublishOpts::default()
            },
        )
        .await;
    match publish {
        // Re-runs republish the same content-addressed bytes; the registry
        // already holding the version is equivalent to a fresh publish.
        Ok(_) | Err(Error::VersionAlreadyExists(..)) => {}
        Err(error) => return Err(error.into()),
    }

    let store = TempDir::new()?;
    let acquirer =
        RegistryAcquire::new(REGISTRY).with_config(plain_http_config()?).cached_at(store.path());
    let context = AcquireContext {
        mounts: Arc::new(MountRegistry::open(Vec::new())?),
    };
    let fetched = acquirer
        .acquire(PACKAGE, &Location::Registry(None), &context)
        .await
        .map_err(|error| anyhow!("acquiring the published package: {error:?}"))?;
    assert_eq!(fetched, bytes, "the registry round-trips the exact bytes");

    let digest = omnia_wasm_pkg::sha256_digest(&bytes);
    let entry: PathBuf = store.path().join("content").join(digest.to_string());
    assert!(entry.exists(), "the fetch populates the digest-keyed store");
    Ok(())
}
