## 0.31.0

Unreleased

### Added

- `omnia-cursor`'s `complete` span carries the request's schema name as
  `label` when the format is a schema — the guest's own name for the
  question — so a host log tells one completion from another by what was
  asked, not by sequence number alone.
- `omnia-cursor` reads `cursor-sdk-bridge`'s own tool-call frames — the
  tool's `name` and `args` at the top of the payload, a custom tool as
  `mcp` with its name and arguments beneath — beside the CLI stream's
  nested `<name>ToolCall` shape it already read. `tool_calls` on the
  `in progress` line and `tool_turns` on `send answered` count again, and
  the `Transcript` a completion returns is populated. Each call is logged
  at DEBUG as it starts (`tool call`, with `tool` and a compact `subject`:
  a path, a pattern, or the first line of a command) and completes
  (`tool call completed`, with `result_bytes`).
- The `in progress` heartbeat and `Failure::Inactive` name what a silent
  run is `waiting` on — `the opening frame`, ``tool `<name>` ``, or `the
  model`; `Failure::Inactive` gains a `waiting` field.
- A run cut short — at a deadline, aborted, or dropped — is cancelled and
  its bill asked back through `SdkAgentService/GetUsage` before the
  `completion` event, so the event's token counts cover it whenever the
  bridge can report them; when it cannot, a DEBUG line says why.

### Changed

- `omnia-cursor` connects without a `CURSOR_API_KEY` and fails the first
  completion instead, with the same message the connect once gave — so a
  process whose verbs never complete runs keyless. Nothing spawns at
  connect either way.
- `omnia-cursor` grants a lent workspace a read-only tool allowlist —
  `read`, `glob`, `grep`, `ls`, and `mcp`, the channel custom tools
  arrive over — where it granted the agent's full default toolset. No
  shell, write, or delete: a lent tree is inspected, never changed. The
  guest's function tools are unaffected, since they arrive as `mcp`
  frames.
- `omnia-cursor`'s inactivity window (`CURSOR_INACTIVITY_SECS`) bounds a
  run only while it waits on the bridge — for the stream's opening frame,
  or for a tool call the agent started to complete. Once the agent has
  every tool result and the model is composing, the window stands down and
  the absolute cap (`CURSOR_TIMEOUT_SECS`) alone ends the wait: the stream
  carries no model text while it reasons, so a long answer was being cut
  at the window for taking longer than 120s to think.

- `omnia-opentelemetry` reads its collector endpoint from
  `OTEL_EXPORTER_OTLP_ENDPOINT`, OpenTelemetry's own variable and the one
  the omnia host exports its spans to, where it read `OTEL_GRPC_URL`; one
  setting names the collector for host and guest telemetry alike. The
  `http://localhost:4317` default is unchanged.
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
