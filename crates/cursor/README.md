# omnia-cursor

[![crates.io](https://img.shields.io/crates/v/omnia-cursor.svg)](https://crates.io/crates/omnia-cursor)
[![docs.rs](https://docs.rs/omnia-cursor/badge.svg)](https://docs.rs/omnia-cursor)

Spawned-agent model backend for the Omnia WASI runtime, implementing the
`omnia:model/completion` boundary (`wasi-model`).

Each completion launches a fresh, context-free [`cursor-agent`](https://cursor.com/docs/cli/headless)
session that owns its own tool loop and edits the lent working tree directly,
then returns a validated answer through the same boundary as `omnia-genai`. The
guest only ever sees the validated answer string; the model id, the API key, and
the agent protocol stay inside this crate.

When the first attempt's answer fails the format gate, the second (and last)
attempt resumes that attempt's session (`--resume=<session_id>`, captured from
the stream-json `init` event) and sends only the format-repair instruction —
the session already carries the prompt and the failed answer, so the provider's
prompt cache stays warm. Session scope is strictly one `complete` call: a
session is never reused across completions. If no session id was observed, the
repair falls back to a cold spawn whose prompt keeps the original as a
byte-identical prefix with the failed answer and repair instruction appended.

MSRV: Rust 1.97

## Requirements

The [`cursor-agent`](https://cursor.com/docs/cli) CLI must be installed and on
`PATH` (validated at `connect`), and authenticated via `CURSOR_API_KEY` or a
prior `cursor-agent login`. The child inherits `CURSOR_API_KEY` from the
environment. Every spawn sets `AGENT_CLI_CREDENTIAL_STORE=memory` so the child
never reads or writes the operator's macOS keychain or `~/.cursor/auth.json`.
The key is never stored on `Client` / `ConnectOptions`, logged, or recorded
into fixtures.

## Configuration

The working tree is lent per completion through the guest's `grants.workspace`:
the runtime preopens the configured `[[mount]]`, the guest lends that
descriptor, and the host resolves it to a node-local path exposed on the tool
host (`ToolHost::local_path`). A completion with no lent workspace yields
`error::backend("no local tree on this node")`, preserving the capability
signal.

The model id is taken from each request (`request.model`); an unset value and
no `CURSOR_MODEL` lets `cursor-agent` choose. Each spawn is bounded twice: an
inactivity window (`CURSOR_INACTIVITY_SECS`, default 120s) kills an agent that
has stopped emitting stream-json events, while the absolute wall-clock cap
(`CURSOR_TIMEOUT_SECS`, default 600s) backstops an agent that streams forever.
A stalled agent dies fast; one that is still streaming survives up to the cap.
The two kill errors are distinct (`inactive for Ns` vs `timed out after Ns
(absolute cap …)`). `Client::connect()` / `FromEnv` reads the optional
`CURSOR_TIMEOUT_SECS`, `CURSOR_INACTIVITY_SECS`, and `CURSOR_MODEL`; callers
that need different bounds or a default model pass `ConnectOptions` to
`connect_with`. MCP servers are supplied per-request: a prompt's `mcp` grant
carries the endpoint `url` directly (merged into `<workspace>/.cursor/mcp.json`
for the spawn). `cursor-agent` discovers that file from the git toplevel of
`--workspace`, so a lent tree that is not already a root is `git init`'d first.
Host `GIT_*` identity vars are stripped from the spawn so that discovery cannot
skip the workspace.

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
// else a 600s cap, a 120s inactivity window, and an agent-chosen model.
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

[`tests/live.rs`](tests/live.rs) drives a real completion through the `wasi-model`
boundary (including an in-process MCP grant). Both tests are `#[ignore]`d so they
never spawn a process in CI; run them with an installed, authenticated
`cursor-agent`:

```bash
CURSOR_API_KEY=... \
  cargo nextest run -p omnia-cursor --run-ignored all
```

## License

MIT OR Apache-2.0
