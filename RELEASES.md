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
- `omnia-cursor` pools its bridges: a completion leases one of
  `ConnectOptions::max_agents` slots (`CURSOR_MAX_AGENTS`, default 4, first
  come first served; the wait is `cursor_lease_wait_ms`) and runs its agent
  in a `cursor-sdk-bridge` process of its own, closed once the completion's
  teardown is done. The bridge reports its own exit: a process that dies
  mid-run fails that completion with the typed
  `cursor-sdk-bridge exited (…) during the run` (outcome `bridge_exit`,
  counter `cursor_bridge_exits`) instead of stalling it to the inactivity
  deadline, and the agent's teardown RPCs are skipped on a dead bridge.
  `ConnectOptions::bridge_bin` (`CURSOR_BRIDGE_BIN`) names the executable;
  `bridge_url` + `bridge_token` (`CURSOR_BRIDGE_URL`, `CURSOR_BRIDGE_TOKEN`)
  attach to a bridge another process manages instead of spawning. Spawn
  time is `cursor_bridge_spawn_ms`, and a spawn that exits before its ready
  line fails with the exit status and the tail of its stderr.

### Changed

- `omnia-cursor` runs one `cursor-sdk-bridge` process per live agent rather
  than one per client, so `Client::connect` spawns and closes a probe bridge
  (still failing fast on a missing or broken binary) and each completion
  pays a bridge spawn. `ConnectOptions` gains `max_agents`, `bridge_bin`,
  `bridge_url`, and `bridge_token`; struct literals must name them
  (`FromEnv` defaults are unchanged in effect).
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
