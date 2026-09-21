## 0.29.0

Unreleased

Pairs with omnia 0.36.x.

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
  The last lines the process wrote to stderr are untrusted subprocess
  output: they are logged at DEBUG for operators and never reach a WARN
  event or the error a guest sees. No RPC waits on a bridge unbounded:
  `CreateAgent` and the head of `Send` fall under the
  inactivity window, the connect handshake and each teardown call have
  bounds of their own, so a bridge that is alive but silent gives its slot
  back. `CreateAgent` and the teardown run on tasks of their own, so a
  completion the guest drops mid-create or mid-teardown still closes and
  deletes its agent (an unanswered `CreateAgent` keeps its slot for one
  more window for the late id). The executable is `cursor-sdk-bridge` on
  `PATH`, and a ready line naming a non-loopback URL is rejected before
  any RPC, so the bearer token and API key never leave the host.
  Spawn time is `cursor_bridge_spawn_ms`, spawn failures are
  `cursor_bridge_spawn_failures`, and a spawn that does not complete its
  handshake fails with the exit status and the step that failed; its
  stderr tail, likewise, is at DEBUG only.
- `omnia-cursor` survives a bridge that dies under the opening of a
  completion: when `CreateAgent` or the opening `Send` fails because the
  process exited or its socket failed below Connect, and no candidate has
  yet reached the guest's `check`, the dead lease is released and the
  prompt goes once more on a fresh one — exactly one restart per
  `complete`, logged at WARN (`completion restarting on a fresh bridge`)
  and counted as `cursor_bridge_restarts`; the second attempt's result is
  final. A loss after a candidate, a deadline, an abort, a Connect or
  end-stream error, and a lease failure stand as they are. Failures below
  Connect are now the typed `TransportError` (outcome `transport`),
  distinct from a Connect error and from an end-stream error; `Failure`,
  `Exit`, and `TransportError` are public so callers match on types rather
  than messages, and `WasiModelCtx::complete` documents them. The bridge
  exit WARN names the process (`pid`, `uptime_ms`, `status_text` such as
  `signal: 9 (SIGKILL)`) and the run on it (`run_in_flight`, `silent_ms`),
  and the spawned bridge's `CURSOR_SDK_BRIDGE_LOG` passthrough is
  documented. The `expect_error` guest scenario gains a `check` flag for a
  failure that must strike after a candidate reached the guest.
- `omnia-genai` gains `ConnectOptions::endpoint` (`GENAI_ENDPOINT`): a base
  URL every request goes to in place of the provider's own — the
  self-hosted-gateway option. The request keeps the shape of the provider
  the model id routes to and auth is still that provider's key from the
  environment; a missing trailing slash is supplied, and a URL that is not
  `http://` or `https://` is rejected at `connect`.
- `omnia-cursor` and `omnia-genai` are tested end to end from real guest
  components: a `crates/test-programs` crate compiles the shared
  `omnia:model` scenarios (echo, the three `check` outcomes, tool
  round-trips, fan-out, abandoned fan-out, an expected failure) for
  `wasm32-wasip2`, and each backend runs them through `omnia_test::host`
  against a protocol-faithful fake — `fake-cursor-sdk-bridge` (behind the
  crate's `fake-bridge` feature; linked onto the test process's `PATH` as
  `cursor-sdk-bridge` and spawned per lease exactly as the real one is,
  with a fault matrix from a hung handshake to a `SIGKILL` mid-run, and a
  check that the ready line's bearer token never reaches a log) for
  cursor, an `OpenAI`-compatible chat-completions endpoint behind
  `GENAI_ENDPOINT` for genai. The scripted-server unit tests both crates
  carried are retired in their favour. Nothing on `omnia-cursor`'s public
  surface exists for the tests' sake: a lease fully released is observed
  as its process gone. The cursor live tier gains `stress_fanout` (the
  four-way fan-out twenty times over).

### Changed

- `omnia-cursor` runs one `cursor-sdk-bridge` process per live agent rather
  than one per client, so `Client::connect` spawns and closes a probe bridge
  (still failing fast on a missing or broken binary) and each completion
  pays a bridge spawn. `ConnectOptions` gains `max_agents`; struct
  literals must name it (`FromEnv` defaults are unchanged in effect).
- `omnia-genai`'s `ConnectOptions` gains `endpoint`; struct literals must
  name it (`None` keeps the provider's own endpoint, as before).
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
