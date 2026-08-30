//! Acquisition over wasm-pkg-client's `local` backend: cache population and
//! hits, verify-before-persist, poisoned entries, endpoint overrides, and
//! acquirer composition — all offline.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt as _;
use omnia::{
    Acquire, AcquireContext, AcquireError, Location, MountAcquire, MountRegistry, ResolvedPreopen,
};
use omnia_wasm_pkg::{AcquireExt as _, ContentStore, RegistryAcquire, sha256_digest};
use tempfile::TempDir;
use wasm_pkg_client::caching::Cache as _;
use wasm_pkg_client::{Config, Registry};

const PACKAGE: &str = "test:adapter@1.0.0";
const DEFAULT_REGISTRY: &str = "registry.test";

#[derive(serde::Serialize)]
struct LocalBackendConfig {
    root: PathBuf,
}

/// Stage `bytes` as `package` in a local-backend registry rooted at `root`.
fn stage(root: &Path, package: &str, bytes: &[u8]) {
    let (name, version) = package.split_once('@').expect("test packages pin versions");
    let (namespace, name) = name.split_once(':').expect("test packages are namespaced");
    let dir = root.join(namespace).join(name);
    std::fs::create_dir_all(&dir).expect("creating package directory");
    std::fs::write(dir.join(format!("{version}.wasm")), bytes).expect("staging package");
}

/// Register a `local`-backend registry named `name` in `config`.
fn add_local_registry(config: &mut Config, name: &str, root: &Path) {
    let registry: Registry = name.parse().expect("test registry name parses");
    let backend = config.get_or_insert_registry_config_mut(&registry);
    backend.set_default_backend(Some("local".into()));
    backend
        .set_backend_config(
            "local",
            LocalBackendConfig {
                root: root.to_path_buf(),
            },
        )
        .expect("local backend config serializes");
}

/// A cacheless acquirer whose default registry is a local backend at `root`.
fn registry_acquirer(root: &Path) -> RegistryAcquire {
    let mut config = Config::empty();
    add_local_registry(&mut config, DEFAULT_REGISTRY, root);
    RegistryAcquire::new(DEFAULT_REGISTRY).with_config(config)
}

fn context() -> AcquireContext {
    AcquireContext {
        mounts: Arc::new(MountRegistry::open(Vec::new()).expect("opening no mounts")),
    }
}

async fn acquire(
    acquirer: &impl Acquire, package: &str, from: Location,
) -> Result<Vec<u8>, AcquireError> {
    acquirer.acquire(package, &from, &context()).await
}

/// The failure text of an [`AcquireError::Failed`], context chain included.
fn failure_text(error: &AcquireError) -> String {
    match error {
        AcquireError::Failed(error) => format!("{error:#}"),
        AcquireError::Unsupported(reason) => {
            panic!("expected a failure, got unsupported: {reason}")
        }
    }
}

#[tokio::test]
async fn registry_fetch_round_trips() {
    let registry = TempDir::new().expect("registry dir");
    stage(registry.path(), PACKAGE, b"component bytes");
    let store = TempDir::new().expect("store dir");
    let acquirer = registry_acquirer(registry.path()).cached_at(store.path());

    let bytes = acquire(&acquirer, PACKAGE, Location::Registry(None)).await.expect("acquires");
    assert_eq!(bytes, b"component bytes");
}

#[tokio::test]
async fn cache_miss_then_populates() {
    let registry = TempDir::new().expect("registry dir");
    stage(registry.path(), PACKAGE, b"component bytes");
    let store = TempDir::new().expect("store dir");
    let acquirer = registry_acquirer(registry.path()).cached_at(store.path());

    let digest = sha256_digest(b"component bytes");
    let entry = store.path().join("content").join(digest.to_string());
    assert!(!entry.exists(), "the store starts empty");
    acquire(&acquirer, PACKAGE, Location::Registry(None)).await.expect("acquires");
    assert!(entry.exists(), "the store gains the digest-keyed entry");
}

#[tokio::test]
async fn cache_hit_serves_after_source_removed() {
    let registry = TempDir::new().expect("registry dir");
    stage(registry.path(), PACKAGE, b"component bytes");
    let store = TempDir::new().expect("store dir");

    let warm = registry_acquirer(registry.path()).cached_at(store.path());
    acquire(&warm, PACKAGE, Location::Registry(None)).await.expect("acquires cold");

    std::fs::remove_file(registry.path().join("test").join("adapter").join("1.0.0.wasm"))
        .expect("removing the registry source");
    // A fresh acquirer over the same store: release record and content both
    // come from the warm cache — nothing reaches the registry.
    let offline = registry_acquirer(registry.path()).cached_at(store.path());
    let bytes = acquire(&offline, PACKAGE, Location::Registry(None)).await.expect("acquires warm");
    assert_eq!(bytes, b"component bytes");
}

#[tokio::test]
async fn verify_before_persist_refuses_mismatched_stream() {
    let root = TempDir::new().expect("store dir");
    let store = ContentStore::new(root.path());
    let digest = sha256_digest(b"expected bytes");
    let tampered = futures::stream::once(async {
        Ok::<_, wasm_pkg_client::Error>(bytes::Bytes::from_static(b"tampered bytes"))
    })
    .boxed();

    let error =
        store.put_data(digest.clone(), tampered).await.expect_err("mismatched bytes refused");
    assert!(error.to_string().contains("refusing to persist"), "typed refusal: {error}");
    let entry = root.path().join("content").join(digest.to_string());
    assert!(!entry.exists(), "nothing persisted under the digest key");
}

#[tokio::test]
async fn poisoned_cache_entry_refused() {
    let registry = TempDir::new().expect("registry dir");
    stage(registry.path(), PACKAGE, b"honest bytes");
    let store = TempDir::new().expect("store dir");
    let warm = registry_acquirer(registry.path()).cached_at(store.path());
    acquire(&warm, PACKAGE, Location::Registry(None)).await.expect("acquires cold");

    let entry = store.path().join("content").join(sha256_digest(b"honest bytes").to_string());
    std::fs::write(&entry, b"poison").expect("poisoning the store entry");

    let poisoned = registry_acquirer(registry.path()).cached_at(store.path());
    let error = acquire(&poisoned, PACKAGE, Location::Registry(None))
        .await
        .expect_err("a poisoned entry fails its hash");
    assert!(failure_text(&error).contains("hashes to"), "digest refusal: {error:?}");
}

#[tokio::test]
async fn registry_override_selects_endpoint() {
    let default_root = TempDir::new().expect("default registry dir");
    stage(default_root.path(), PACKAGE, b"default registry bytes");
    let override_root = TempDir::new().expect("override registry dir");
    stage(override_root.path(), PACKAGE, b"override registry bytes");

    let mut config = Config::empty();
    add_local_registry(&mut config, DEFAULT_REGISTRY, default_root.path());
    add_local_registry(&mut config, "override.test", override_root.path());
    let store = TempDir::new().expect("store dir");
    let acquirer =
        RegistryAcquire::new(DEFAULT_REGISTRY).with_config(config).cached_at(store.path());

    let default_bytes =
        acquire(&acquirer, PACKAGE, Location::Registry(None)).await.expect("default acquires");
    assert_eq!(default_bytes, b"default registry bytes");
    // Same package and version, same store: release records are scoped per
    // registry, so the override never answers from the default's record.
    let override_bytes =
        acquire(&acquirer, PACKAGE, Location::Registry(Some("override.test".into())))
            .await
            .expect("override acquires");
    assert_eq!(override_bytes, b"override registry bytes");
}

#[tokio::test]
async fn unversioned_and_missing_packages_refuse_typed() {
    let registry = TempDir::new().expect("registry dir");
    stage(registry.path(), PACKAGE, b"component bytes");
    let acquirer = registry_acquirer(registry.path());

    let unversioned = acquire(&acquirer, "test:adapter", Location::Registry(None))
        .await
        .expect_err("exact version is mandatory");
    assert!(failure_text(&unversioned).contains("exact version"), "refusal: {unversioned:?}");

    let missing = acquire(&acquirer, "test:absent@1.0.0", Location::Registry(None))
        .await
        .expect_err("an absent package fails");
    assert!(matches!(missing, AcquireError::Failed(_)), "typed failure: {missing:?}");
}

#[tokio::test]
async fn path_location_unsupported() {
    let registry = TempDir::new().expect("registry dir");
    let acquirer = registry_acquirer(registry.path());

    let error = acquire(&acquirer, PACKAGE, Location::Path("adapters/x.wasm".into()))
        .await
        .expect_err("paths are not served");
    assert!(matches!(error, AcquireError::Unsupported(_)), "typed refusal: {error:?}");
}

#[tokio::test]
async fn or_falls_through_on_unsupported() {
    let mount_root = TempDir::new().expect("mount dir");
    std::fs::write(mount_root.path().join("plugin.wasm"), b"mounted bytes")
        .expect("staging mounted component");
    let registry = TempDir::new().expect("registry dir");
    stage(registry.path(), PACKAGE, b"registry bytes");
    let composed = MountAcquire.or(registry_acquirer(registry.path()));
    let context = AcquireContext {
        mounts: Arc::new(
            MountRegistry::open(vec![ResolvedPreopen::new(
                ".".to_owned(),
                mount_root.path().to_path_buf(),
                false,
            )])
            .expect("opening mounts"),
        ),
    };

    let mounted = composed
        .acquire(PACKAGE, &Location::Path("plugin.wasm".into()), &context)
        .await
        .expect("mounts serve paths");
    assert_eq!(mounted, b"mounted bytes");
    let fetched = composed
        .acquire(PACKAGE, &Location::Registry(None), &context)
        .await
        .expect("the registry serves the fall-through");
    assert_eq!(fetched, b"registry bytes");
}

#[tokio::test]
async fn or_propagates_failures() {
    let empty = TempDir::new().expect("empty registry dir");
    let stocked = TempDir::new().expect("stocked registry dir");
    stage(stocked.path(), PACKAGE, b"reachable bytes");
    let composed = registry_acquirer(empty.path()).or(registry_acquirer(stocked.path()));

    let error = acquire(&composed, PACKAGE, Location::Registry(None))
        .await
        .expect_err("a failure never falls through");
    assert!(matches!(error, AcquireError::Failed(_)), "second never consulted: {error:?}");
}

#[tokio::test]
async fn or_reports_both_refusals() {
    let registry = TempDir::new().expect("registry dir");
    let composed = registry_acquirer(registry.path()).or(registry_acquirer(registry.path()));

    let error = acquire(&composed, PACKAGE, Location::Path("x.wasm".into()))
        .await
        .expect_err("neither serves paths");
    let AcquireError::Unsupported(reason) = error else {
        panic!("expected a combined unsupported refusal");
    };
    assert_eq!(reason.matches("registry locations only").count(), 2, "both refusals: {reason}");
}

#[tokio::test]
async fn cacheless_acquirer_fetches_fresh() {
    let registry = TempDir::new().expect("registry dir");
    stage(registry.path(), PACKAGE, b"first bytes");
    let acquirer = registry_acquirer(registry.path());

    let first = acquire(&acquirer, PACKAGE, Location::Registry(None)).await.expect("acquires");
    assert_eq!(first, b"first bytes");
    stage(registry.path(), PACKAGE, b"second bytes");
    let second = acquire(&acquirer, PACKAGE, Location::Registry(None)).await.expect("re-acquires");
    assert_eq!(second, b"second bytes", "nothing cached anywhere");
}
