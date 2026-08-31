## 0.29.0

Unreleased

Pairs with omnia 0.35.x.

### Added

- `omnia-filesystem` and `omnia-azure-blob` implement `omnia_plugin::ContentStore`
  and `omnia_plugin::ReleaseStore` (the `PluginStore` bound behind omnia's registry
  acquirer): content entries shared across registries, release records scoped
  per registry, verify-before-persist writes. Each impl owns a tree disjoint
  from guest storage by construction — a `plugins/` subtree beside
  `blobstore/` and `keyvalue/` on the filesystem, a dedicated `omnia-plugins`
  container on Azure.
### Changed

- `omnia-wasm-pkg` is deleted before it ever shipped: registry acquisition
  (`RegistryAcquire`, path/registry composition, the digest-verify story) was
  absorbed into omnia's `omnia-plugin` crate, re-exported from `omnia`. This
  repository's role in plugin loading is the store impls above.

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
