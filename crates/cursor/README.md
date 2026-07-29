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

MSRV: Rust 1.95

## Requirements

The [`cursor-agent`](https://cursor.com/docs/cli) CLI must be installed and on
`PATH` (validated at `connect`), and authenticated via `CURSOR_API_KEY` or a
prior `cursor-agent login`. The key is read by the spawned process and is never
captured, logged, or recorded into fixtures.

## Configuration

The working tree is lent per completion through the guest's `grants.workspace`:
the runtime preopens the configured `[[mount]]`, the guest lends that
descriptor, and the host resolves it to a node-local path exposed on the tool
host (`ToolHost::local_path`). A completion with no lent workspace yields
`error::backend("no local tree on this node")`, preserving the capability
signal.

The model id is taken from each request (`request.model`); an unset value and
no `CURSOR_MODEL` lets `cursor-agent` choose. Each spawn is bounded by
`ConnectOptions.timeout_secs` (default 600s). `Client::connect()` / `FromEnv`
reads optional `CURSOR_TIMEOUT_SECS` and `CURSOR_MODEL`; callers that need a
different ceiling or default model pass
`ConnectOptions { timeout_secs: n, model: Some(…) }` to `connect_with`. MCP
servers are supplied per-request: a prompt's `mcp` grant carries the endpoint
`url` directly (merged into `<workspace>/.cursor/mcp.json` for the spawn).

## Usage

```rust,ignore
use omnia::Backend;
use omnia_cursor::{Client, ConnectOptions};

// CURSOR_TIMEOUT_SECS / CURSOR_MODEL when set; else 600s and agent-chosen model.
let client = Client::connect().await?;

// Explicit ceiling and default model for long-running judgment legs.
let client = Client::connect_with(ConnectOptions {
    timeout_secs: 300,
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
