# omnia-cursor

[![crates.io](https://img.shields.io/crates/v/omnia-cursor.svg)](https://crates.io/crates/omnia-cursor)
[![docs.rs](https://docs.rs/omnia-cursor/badge.svg)](https://docs.rs/omnia-cursor)

Cursor model backend for the Omnia WASI runtime, implementing the
`omnia:model/completion` boundary (`wasi-model`) through
[`cursor-sdk-bridge`](https://github.com/cursor/sdk-bridge) — a local process
wrapping Cursor's SDK behind Connect RPCs.

Each completion creates a fresh bridge-managed agent that owns its own tool
loop and edits the lent working tree directly, then returns its answer
through the same boundary as `omnia-genai`. Guest-declared function
tools round-trip through the session exactly as genai's do: they are declared
as SDK custom tools at `CreateAgent`, and when the agent calls one the bridge
POSTs `CallCustomTool` to this crate's loopback callback endpoint, which routes
it into the completion's session via `ToolHost::call_tool` — so the guest's
tool closure answers, under the host's declared-name check, budget, size cap,
and per-call timeout. `Tool::Mcp` grants pass inline as the agent's
`mcp_servers`; nothing is written into the workspace. The guest only ever
sees the answer string; the model id, the API key, and the bridge protocol
stay inside this crate.

The request's `format` reaches the agent as a final-answer instruction in
the prompt — steering only; nothing here validates the answer. When the
request sets `check`, the agent's answer is offered to the guest through
`ToolHost::check`: `Ok` ends the completion, `Err(correction)` sends the
guest's correction verbatim as the next prompt on the same agent — its
session already carries the prompt and the rejected answer, so the
provider's prompt cache stays warm. Two rounds are allowed; a rejection of
the second fails the completion with the typed `budget-exhausted` carrying
that correction. Agent scope is strictly one `complete` call: agents are
never reused across completions. Each is closed and then deleted against
the create-time workspace (a missing agent is already gone); `Drop` only
retries that cleanup if `complete` never ran.

MSRV: Rust 1.97

## Requirements

The [`cursor-sdk-bridge`](https://github.com/cursor/sdk-bridge) executable
must be on `PATH`, and `CURSOR_API_KEY`
must be set — the bridge protocol authenticates every agent with an explicit
key, so a prior `cursor-agent login` no longer suffices. The key is read from
the environment per completion; it is never stored on `Client` /
`ConnectOptions`, logged, or recorded into fixtures.

Each live agent runs in its own bridge process, spawned for the completion
as the leader of its own process group and shut down after it: a graceful
`Shutdown` RPC with 5s for the exit it asks for, then a kill of the whole
group, which reaches the agent processes the bridge forks; whatever a bridge
left in its group when it exited on its own is swept as the exit is seen,
so nothing of a slot's process outlives it. A bridge that
crashes fails the completion running on it with the typed
`cursor-sdk-bridge exited (…)` (metric outcome `bridge_exit`)
rather than as the next completion's stall. The exit is logged at WARN with
the process's `pid`, its `uptime_ms`, and the `status` (`signal: 9 (SIGKILL)`,
`exit status: 7`), and counted as `cursor_bridge_exits`; the completion that
lost its run to it logs `run lost with its process` at INFO under the same
`pid`, with how long that run's stream had been `silent_ms`. The last lines
the process wrote to stderr are logged at DEBUG only — they are untrusted
subprocess output and never reach WARN or the error a guest sees.

A bridge lost under the *opening* of a completion is not the prompt's
doing, so the completion restarts once: when `CreateAgent` or the opening
`Send` fails because the process exited (`Failure::BridgeExited`) or its
socket failed below Connect (`TransportError` — the request could not be
sent, the stream reset, or it ended mid-frame), and no candidate has yet
reached the guest, the dead lease is released, a fresh one is taken (with
one slot that means waiting for the dead process to be reaped), and the
original prompt is sent again with fresh deadlines. The restart is logged at
WARN (`completion restarting on a fresh bridge`, with the first attempt's
`outcome`, `error`, and `pid` when the process is known) and counted as
`cursor_bridge_restarts`; the second attempt's result is final, whatever it
is. Nothing else restarts: a failure after the guest's `check` has seen a
candidate (the guest would be offered a candidate twice), an inactivity or
cap deadline, a guest abort, `budget-exhausted`, a Connect or end-stream
error (the bridge answered), a lease that could not be taken, or a request
that could not be shaped all stand as they are. Each attempt is its own
agent on its own bridge, so the `completion started` / `completion` INFO
lines and the `cursor_completions` counter are per attempt: a restarted
call produces two pairs inside one `complete` span, the first ending
`bridge_exit` or `transport`, with the restart WARN between them, and
`attempts` on those lines still counts the sends on that one agent. A
follow-up worth doing when the bridge exposes it: resuming a run in flight
(`ObserveRun` / `WaitLiveRun`) on the new process instead of re-sending
the prompt. A bridge
that stays alive but stops answering is bounded too: no call waits on it
longer than the inactivity window, and the teardown calls after a
completion are bounded at a few seconds each, so a silent bridge frees its
slot instead of holding it. `CreateAgent` and the teardown run on tasks of
their own rather than on the completion future, so a completion the guest
drops mid-create still closes and deletes the id that arrives, and one
dropped mid-teardown still finishes it; an unanswered `CreateAgent` keeps
its slot for one more window for that late id, then gives it up.
`Client::connect()` still fails fast when the
binary is missing or broken — it binds the loopback callback endpoint, then
spawns and closes one probe bridge — and every spawn passes a private
`--state-root` so no durable agent state lands in `~/.cursor`, registers
with the callback endpoint under its own bearer token (agent ids are each
process's own to choose, so a callback routes by the token that carries it
as well as the id it names, and the token is revoked once the process is
gone), parses the bridge's stderr discovery line, and verifies the endpoint
with `Ping`/`GetVersion` (`sdk.v1`). A spawn that never completes that handshake fails with the
process's exit status and the step that failed; its stderr tail is at DEBUG.

## Configuration

The working tree is lent per completion through the guest's
`grants.workspace`: the runtime preopens the configured `[[mount]]`, the
guest lends that descriptor, and the host resolves it to a node-local path
exposed on the tool host (`ToolHost::local_path`). The agent runs there with
its default built-in toolset, honoring the tree's own project settings and
nothing from the host user. Without a lent workspace the completion still
runs — in a private empty directory with every built-in tool disabled — so
function-tool-only (references-style) completions work like genai's.

The model id is taken from each request (`request.model`); an unset value
falls back to `CURSOR_MODEL`, else `auto` (Cursor's server-side selection).
The request's `generation` controls (temperature, max tokens, effort, …)
are ignored: `CreateAgent` has no sampling knobs. Each `Send` — the opening
prompt, and a check's correction if any — is bounded twice: an inactivity window
(`CURSOR_INACTIVITY_SECS`, default 120s) cancels a run whose stream has gone
silent (keepalive frames do not count), while the absolute wall-clock cap
(`CURSOR_TIMEOUT_SECS`, default 600s) backstops a run that streams forever.
A completion that is corrected therefore gets a fresh inactivity window and
a fresh cap on the second send. The two errors are distinct
(`inactive for Ns` vs `timed out after Ns (absolute cap …)`).

Concurrency is bounded by `CURSOR_MAX_AGENTS` (default 4): that many agents
live at once, each in its own bridge process, and a further completion
waits its turn (first come, first served; the wait is recorded as
`cursor_lease_wait_ms`, apart from the completion's own duration, which
starts once the slot is held). The bridge executable is `cursor-sdk-bridge`,
resolved on `PATH`.

`Client::connect()` / `FromEnv` reads the optional `CURSOR_TIMEOUT_SECS`,
`CURSOR_INACTIVITY_SECS`, `CURSOR_MODEL`, and `CURSOR_MAX_AGENTS`; callers
that need different bounds, a default model, or another pool shape pass
`ConnectOptions` to `connect_with`. A spawned bridge inherits the host's
environment (bar the `GIT_*` identity variables, which would point the
agent at the host's repository), so the bridge's own `CURSOR_SDK_BRIDGE_LOG`
passes straight through: set it on the host process to have every spawned
bridge log its RPCs to stderr, where this crate records them at DEBUG.

A caller that needs to tell failures apart matches on the types `complete`'s
error downcasts to — `Failure::{Run, Timeout, Inactive, Aborted, BridgeExited}`,
`TransportError`, and `omnia_wasi_model::Error::BudgetExhausted` — rather
than on message text; `Exit` is the process status a `BridgeExited` carries,
and `RunStatus` the terminal status a `Run` ended in.

MCP servers are supplied per-request: a prompt's `mcp` grant carries the
endpoint `url` directly, passed inline through `CreateAgent`'s `mcp_servers`.
The grant's `tools` allowlist is advisory — it is named in the prompt hint
but not enforced by a filtering proxy.

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_cursor::Client as Cursor;
use omnia_wasi_model::WasiModel;

omnia::runtime!({
    hosts: {
        WasiModel: Cursor,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::Backend;
use omnia_cursor::{Client, ConnectOptions};

// CURSOR_TIMEOUT_SECS / CURSOR_INACTIVITY_SECS / CURSOR_MODEL /
// CURSOR_MAX_AGENTS when set; else a 600s cap, a 120s inactivity window, a
// Cursor-chosen model, and up to four agents, each in its own
// `cursor-sdk-bridge` process.
let client = Client::connect().await?;

// Explicit bounds, default model, and pool shape for long-running judgment
// legs.
let client = Client::connect_with(ConnectOptions {
    timeout_secs: 1800,
    inactivity_secs: 120,
    model: "composer-2".into(),
    max_agents: 2,
}).await?;
```

## End-to-end example

The full guest + runtime demo lives in [`examples/cursor`](../../examples/cursor). The guest declares a function tool and answers each session `tool-call` with a `ToolResult`.

## Tests

Three tiers. The first two run on every `cargo nextest run -p omnia-cursor`
with no bridge installed and no key; the third is the real bridge, by hand.

**Tier 1 — the fake bridge.** [`tests/support/fake_bridge`](tests/support/fake_bridge)
is a protocol-faithful `cursor-sdk-bridge`: bearer-checked `sdk.v1` Connect
RPCs, `agent-<n>` ids counted per process (so two processes hand out the
same id, as the real one does), `Send` as an enveloped run stream, and
`CallCustomTool` posted back to this crate's own callback endpoint. It is
built as the `fake-cursor-sdk-bridge` binary alongside the suites and
linked onto the test process's `PATH` as `cursor-sdk-bridge`, so the
client finds it exactly as a deployment finds the real one — nothing on
`Client` or `ConnectOptions` exists for the tests' sake. The client starts
one process per lease and every process appends to one JSONL log the test
folds back into per-process histories; a test can park a request at any
point of an agent's life and release it, the parks and releases going
through files in the fake's home. A scripted `Config` decides the reply
(`Echo`, `Replies`, `Tool`, `Paced`) and the faults (hang or park at a
point, exit or `SIGKILL` on the nth call, a missing, refused, or
non-loopback ready line, a reset stream, an empty id, a failing close, a
forked child left running past the process's own exit), each fault aimed
at one spawned process or all of them.

**Tier 2 — guests through the runtime.** Every scenario is a guest
component from [`crates/test-programs`](../test-programs) run through
`omnia_test::host::Deployment` over an `omnia_cursor::Client`, so what is
asserted is the guest-visible contract; the test then asserts the fake's
per-agent RPC sequence and that every process the client spawned is gone
again (a slot reopens only once its process is). [`tests/model.rs`](tests/model.rs)
is one row per guest program (`foreach_model!` fails to compile when a
program has no row): echo, the three `check` outcomes, tool round-trips in
every callback codec, a repairable and an undeclared tool, fan-out over
four processes, tool fan-out where every process calls its agent `agent-1`,
and a fan-out whose losers are dropped mid-run.
[`tests/bridge.rs`](tests/bridge.rs) is the lifecycle and fault matrix:
option validation, a completion dropped at every point it can be waiting
(handshake, pre-ready, create, pre-stream, teardown, and still queued for a
slot), pooling and the late-create reap, the inactivity and cap deadlines,
process death before the ready line, the restart matrix (a bridge killed on
the opening `Send` or exited on `CreateAgent` restarts once on a fresh
process that answers; killed twice fails with the typed exit; killed after
the guest's `check` has seen a candidate is not restarted; a run that stalls
mid-stream is cancelled, not restarted; a stream reset restarts on a fresh
process once the reset one has been asked to go, and reset twice fails with
the typed transport error), the children a bridge forked swept with it
whether it was killed mid-run, exited on `Shutdown`, or was dropped before
its ready line, a bridge that hangs on `Ping`, `CloseAgent`, or `Shutdown`
(the last killed as a group after the one bound, its forked child with
it), the callback endpoint's rejections
and token revocation, and a check that the ready line and its token never
reach a log. Guests are
compiled by the `test-programs` build script; there is no separate
`--target` build to run.

**Tier 3 — the real bridge.** [`tests/live.rs`](tests/live.rs) drives real
completions through the `wasi-model` boundary: the plain acceptance run, a
function-tool round-trip with a lent workspace, a no-workspace function-tool
run, an in-process MCP grant, the guest `check` loop, a four-way fan-out
that holds four bridge processes at once and then sees every one of them
gone, `stress_fanout`, that fan-out twenty times over (a bridge that exits
under load fails its completion with `cursor-sdk-bridge exited`), and
`bridge_killed_mid_run_recovers`, which `kill -9`s a real bridge under its
opening run and sees the completion answer from the restart. The rows that
watch the spawned processes read their pids from the client's own
`cursor-sdk-bridge spawned` events through the process's tracing
subscriber, so they run one per process, as nextest does; "gone" is the
whole process group each bridge led, so a `cursor-agent` left behind by a
bridge is a failure here, not just the bridge itself. All are
`#[ignore]`d so they never spawn a process in CI; run them with
`cursor-sdk-bridge` installed:

```bash
CURSOR_API_KEY=... \
  cargo nextest run -p omnia-cursor --run-ignored all
```

Add `CURSOR_SDK_BRIDGE_LOG=1` when a live failure needs the bridge's
per-RPC stderr; the client logs the tail of it at DEBUG.

## License

MIT OR Apache-2.0
