## 0.31.0

Unreleased

### Added

### Changed

- `omnia-cursor` no longer attaches to a bridge another process manages.
  `ConnectOptions` drops `bridge_bin`, `bridge_url`, and `bridge_token`;
  the executable is `cursor-sdk-bridge` on `PATH`, and a ready line naming
  a non-loopback URL is rejected before any RPC, so the bearer token and
  API key never leave the host. Struct literals name `max_agents` only.
- The cursor end-to-end fake is linked onto the test process's `PATH` as
  `cursor-sdk-bridge` and spawned per lease. A lease fully released is
  observed as its process gone; `Client::idle_slots` and `upstream_tripwire`
  are gone.
- `omnia-cursor` spawns each bridge as the leader of its own process group
  (via `process-wrap`), so the kill after an unanswered `Shutdown` reaches
  the agent processes the bridge forks, and whatever a bridge left in its
  group when it exited is swept as the exit is seen. A graceful exit is
  bounded once — 5s for the `Shutdown` RPC and the exit together — rather
  than once each.
- `omnia-cursor` has no `fake-bridge` feature. The fake bridge binary and
  the `model` and `bridge` suites build on every `cargo nextest run -p
  omnia-cursor`; `libc` is a dev-dependency, and `http-body-util`'s `channel`
  (the fake's run stream, no crate of its own) is always on.
- `omnia-cursor` decodes an integral `google.protobuf.Struct` number (proto3
  JSON prints `3.0` as `3`) as a JSON integer, so a guest tool's arguments
  arrive as the tool declared them rather than as `3.0`. The fake bridge
  shares the crate's callback codec instead of carrying a copy.
- The `omnia-cursor` bridge-exit WARN carries `pid`, `uptime_ms`, and
  `status`; `run_in_flight` and `silent_ms` are gone from it. The completion
  that lost its run logs `run lost with its process` at INFO with the `pid`
  and its own `silent_ms`.

---

Release notes for previous releases can be found on the respective release
branches of the repository.

<!-- ARCHIVE_START -->
* [0.31.x](https://github.com/augentic/omnia-backends/blob/release-0.31.0/RELEASES.md)
* [0.30.x](https://github.com/augentic/omnia-backends/blob/release-0.30.0/RELEASES.md)
* [0.28.x](https://github.com/augentic/omnia-backends/blob/release-0.28.0/RELEASES.md)
* [0.27.x](https://github.com/augentic/omnia-backends/blob/release-0.27.0/RELEASES.md)
* [0.26.x](https://github.com/augentic/omnia-backends/blob/release-0.26.0/RELEASES.md)

- [0.25.x](https://github.com/augentic/omnia-backends/blob/release-0.25.0/RELEASES.md)
- [0.24.x](https://github.com/augentic/omnia-backends/blob/release-0.24.0/RELEASES.md)
- [0.23.x](https://github.com/augentic/omnia-backends/blob/release-0.23.0/RELEASES.md)
