# omnia-cursor

[![crates.io](https://img.shields.io/crates/v/omnia-cursor.svg)](https://crates.io/crates/omnia-cursor)
[![docs.rs](https://docs.rs/omnia-cursor/badge.svg)](https://docs.rs/omnia-cursor)

Cursor model backend for the Omnia WASI runtime, implementing the
`omnia:model/completion` boundary (`wasi-model`) through
[`cursor-sdk-bridge`](https://github.com/cursor/sdk-bridge) — a local process
wrapping Cursor's SDK behind Connect RPCs.

Each completion creates a fresh bridge-managed agent that owns its own tool
loop and edits the lent working tree directly, then returns a validated
answer through the same boundary as `omnia-genai`. Guest-declared function
tools round-trip through the session exactly as genai's do: they are declared
as SDK custom tools at `CreateAgent`, and when the agent calls one the bridge
POSTs `CallCustomTool` to this crate's loopback callback server, which routes
it into the completion's session via `ToolHost::call_tool` — so the guest's
tool closure answers, under the host's declared-name check, budget, size cap,
and per-call timeout. `Tool::Mcp` grants pass inline as the agent's
`mcp_servers`; nothing is written into the workspace. The guest only ever
sees the validated answer string; the model id, the API key, and the bridge
protocol stay inside this crate.

When the first attempt's answer fails the format gate, the second (and last)
attempt sends only the format-repair instruction on the same agent — its
session already carries the prompt and the failed answer, so the provider's
prompt cache stays warm. Agent scope is strictly one `complete` call: agents
are never reused across completions, and each is deleted afterwards.

MSRV: Rust 1.97

## Requirements

The [`cursor-sdk-bridge`](https://github.com/cursor/sdk-bridge) executable
must be on `PATH` (or named via `CURSOR_SDK_BRIDGE_BIN`), and `CURSOR_API_KEY`
must be set — the bridge protocol authenticates every agent with an explicit
key, so a prior `cursor-agent login` no longer suffices. The key is read from
the environment per completion; it is never stored on `Client` /
`ConnectOptions`, logged, or recorded into fixtures.

`Client::connect()` spawns one bridge process (fail-fast if it is missing or
broken): it passes a private `--state-root` so no durable agent state lands
in `~/.cursor`, registers the loopback callback endpoint with a fresh bearer
token, parses the bridge's stderr discovery line, and verifies the endpoint
with `Ping`/`GetVersion` (`sdk.v1`). Dropping the last `Client` clone shuts
the bridge down (graceful `Shutdown` RPC, then kill).

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
Each run is bounded twice: an inactivity window (`CURSOR_INACTIVITY_SECS`,
default 120s) cancels a run whose stream has gone silent (keepalive frames do
not count), while the absolute wall-clock cap (`CURSOR_TIMEOUT_SECS`, default
600s) backstops a run that streams forever. The two errors are distinct
(`inactive for Ns` vs `timed out after Ns (absolute cap …)`).
`Client::connect()` / `FromEnv` reads the optional `CURSOR_TIMEOUT_SECS`,
`CURSOR_INACTIVITY_SECS`, and `CURSOR_MODEL`; callers that need different
bounds or a default model pass `ConnectOptions` to `connect_with`.

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

// CURSOR_TIMEOUT_SECS / CURSOR_INACTIVITY_SECS / CURSOR_MODEL when set;
// else a 600s cap, a 120s inactivity window, and Cursor-chosen model.
let client = Client::connect().await?;

// Explicit bounds and default model for long-running judgment legs.
let client = Client::connect_with(ConnectOptions {
    timeout_secs: 1800,
    inactivity_secs: 120,
    model: Some("composer-2".into()),
}).await?;
```

## End-to-end example

The full guest + runtime demo lives in [`examples/cursor`](../../examples/cursor). It composes the `ask` guest (calls `create`) with the omnia [`mcp`](https://github.com/augentic/omnia/tree/main/examples/mcp) docs guest under one HTTP server.

## Live tests

[`tests/live.rs`](tests/live.rs) drives real completions through the
`wasi-model` boundary: the plain acceptance run, a function-tool round-trip
proving the custom-tool callback chain, a no-workspace run, and an in-process
MCP grant. All are `#[ignore]`d so they never spawn a process in CI; run them
with `cursor-sdk-bridge` installed:

```bash
CURSOR_API_KEY=... \
  cargo nextest run -p omnia-cursor --run-ignored all
```

## License

MIT OR Apache-2.0
