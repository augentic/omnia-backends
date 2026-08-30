## 0.29.0

Unreleased

Pairs with omnia 0.35.x.

### Added

- `omnia-wasm-pkg`: registry acquirer batteries for omnia's plugin loader.
  `RegistryAcquire` implements the core `Acquire` trait over wasm-pkg-client —
  exact versions only (never "latest"), an empty base configuration so the
  compiled binary alone attests reachable endpoints, per-load registry
  overrides, and every result verified against the registry's content digest
  before it is returned. `ContentStore` is the optional digest-keyed
  content-addressed store behind wasm-pkg-client's `CachingClient`:
  verify-before-persist writes (temp-file plus atomic rename), per-registry
  release records, raw wasm only; a cacheless acquirer remains a valid
  deployment. `AcquireExt::or` composes acquirers by location kind
  (`MountAcquire.or(RegistryAcquire::new("omnia.host").cached_at(...))`) —
  unsupported locations fall through, real failures never do.

### Changed

---

Release notes for previous releases can be found on the respective release
branches of the repository.

<!-- ARCHIVE_START -->
* [0.29.x](https://github.com/augentic/omnia-backends/blob/release-0.29.0/RELEASES.md)
* [0.28.x](https://github.com/augentic/omnia-backends/blob/release-0.28.0/RELEASES.md)
* [0.27.x](https://github.com/augentic/omnia-backends/blob/release-0.27.0/RELEASES.md)
* [0.26.x](https://github.com/augentic/omnia-backends/blob/release-0.26.0/RELEASES.md)

- [0.25.x](https://github.com/augentic/omnia-backends/blob/release-0.25.0/RELEASES.md)
- [0.24.x](https://github.com/augentic/omnia-backends/blob/release-0.24.0/RELEASES.md)
- [0.23.x](https://github.com/augentic/omnia-backends/blob/release-0.23.0/RELEASES.md)
