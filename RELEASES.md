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
- `omnia-cursor`'s `completion` line carries the bill in full and where
  the wait went. `cache_read_tokens`, `cache_write_tokens`, and
  `total_tokens` sit beside `input_tokens` and `output_tokens` — the
  bridge's `usage` frame had carried all three unread. The bridge's total
  is input plus output plus cache reads, so the cache reads are context
  beyond `input_tokens` rather than a part of it, and a completion's
  context is `input_tokens + cache_read_tokens`; `reasoning_tokens` is
  among `output_tokens`. `send answered` and the settled-usage line carry
  the same counts, and `TokenUsage` gains the three fields, each absent
  when the bridge reports none. `opening_ms`, `tool_ms`, `model_ms`, and
  `check_ms` split `duration_ms` by what the completion waited on — the
  bridge before a round's first frame, the agent's creation included;
  tool calls outstanding; the model composing; the guest's check of each
  candidate — every round together, so whether a slow completion read too
  much or reasoned too long is read off one INFO line rather than a TRACE
  log. The four sum to `duration_ms` less the moments between them and,
  on a run cut short, the cancel and usage read-back before the line.
- A reasoning block's closing frame is a DEBUG `thinking` event carrying
  its `duration_ms` — read as the bridge spells an `int64`, a number or a
  string — so a `-v` run shows the long silences between tool calls for
  what they are; the text deltas before it stay TRACE `frame`s. The
  `completion` line sums them as `thinking_ms`, beside `model_ms` — the
  part of the model's wait it spent reasoning by its own report, every
  round together — so whether a slow completion thought or wrote is read
  off the one INFO line.
- Each tool call's arguments are logged whole at TRACE (`tool call args`,
  with `tool` and `args`) as the call starts, beneath the DEBUG `tool
  call` line's one-argument `subject`, so a `-vv` run reads what the
  agent asked of each tool without the payload head of every frame.
- The `subject` on `tool call` / `tool call completed` shows a path under
  the agent's `cwd` relative to it, so the eighty characters go on the
  part that tells one file from another rather than on the lent tree's
  absolute path; the transcript keeps the path as the agent spelled it.

### Changed

- `omnia-cursor`'s nudge for a reply that is not JSON names what the parser
  said of it — `Your last reply is not one well-formed JSON value (EOF while
  parsing an object at line 1 column 34), so it is not the answer. …` —
  where it told every such reply it "held no JSON value", so a truncated
  document is told where it broke. A truncated document is nudged at all
  only once omnia's `Format::candidate` passes over a bracketed block that
  does not parse whole rather than reading the values inside it; before
  that, `{"findings":[],"verdict":"pass"` reached the check as `[]`.
- `omnia-cursor` returns the answer before the teardown. `CloseAgent` and
  `DeleteAgent` run on a task of their own once the `completion` line is
  written, where the completion had awaited them — half a second to two
  per agent, paid on the guest's critical path — so a guest waits on the
  model and never on the bridge putting the agent away. The teardown
  still runs whole, bounded as before, and the slot the agent held
  reopens only once its process is gone.
- `omnia-cursor` reports the absolute cap (`CURSOR_TIMEOUT_SECS`) as the
  typed `omnia_wasi_model::Error::BudgetExhausted` — `Failure::into_error`
  wraps the failure in it, and the failure still downcasts beneath — so a
  guest sees `bad_request` and a retry-on-`bad_gateway` policy does not
  put once more a request that takes as long the second time. The
  inactivity window stays a backend failure: a run that fell silent may
  answer when put again. The message and `Outcome::Timeout` are unchanged.
- `omnia-cursor` closes a tool call the stream never terminated when the
  model next reasons or speaks: the model resumes only once every call it
  issued has answered, so a call still pending then was answered off the
  stream. The phase leaves `Tool` for `Model`, so the inactivity window
  bounds a thinking model as a thinking model again and `tool_ms` stops
  booking model time; the unterminated call's turn is not counted.
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
- A rejected candidate is logged at DEBUG by its `round` and
  `candidate_bytes`, in `omnia-cursor` and `omnia-genai` alike; the
  correction the guest's check minted — the rejected answer echoed whole
  with the findings after it — is the TRACE `correction turn`, where it had
  been the DEBUG line's `correction` field, so `-v` no longer carries
  every rejected candidate in full and the findings are read where the
  guest logs them.

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
