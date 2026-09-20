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
  counter `cursor_bridge_exits`), carrying the last lines it wrote to
  stderr, instead of stalling it to the inactivity deadline, and the
  agent's teardown RPCs are skipped on a dead bridge. No RPC waits on a
  bridge unbounded: `CreateAgent` and the head of `Send` fall under the
  inactivity window, the connect handshake and each teardown call have
  bounds of their own, so a bridge that is alive but silent gives its slot
  back. `CreateAgent` and the teardown run on tasks of their own, so a
  completion the guest drops mid-create or mid-teardown still closes and
  deletes its agent (an unanswered `CreateAgent` keeps its slot for one
  more window for the late id).
  `ConnectOptions::bridge_bin` (`CURSOR_BRIDGE_BIN`) names the
  executable; `bridge_url` + `bridge_token` (`CURSOR_BRIDGE_URL`,
  `CURSOR_BRIDGE_TOKEN`) attach to a loopback `http://` bridge another
  process manages instead of spawning — a non-loopback URL is rejected
  before any RPC, so the bearer token and API key never leave the host —
  and a request declaring function tools is rejected there (the callbacks
  would reach the bridge's owner, not this client).
  Spawn time is `cursor_bridge_spawn_ms`, spawn failures are
  `cursor_bridge_spawn_failures`, and a spawn that does not complete its
  handshake fails with the exit status, the failing step, and the tail of
  its stderr.

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
