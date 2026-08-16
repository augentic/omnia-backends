# Cursor Example

Live model completion via `[omnia-cursor](../../crates/cursor)`: the guest calls `create` once (command mode) with a `docs` MCP grant. The same guest also exports `wasi:http` serving a small read-only MCP documentation server, so the spawned `cursor-agent` answers the prompt by calling back into the guest's own MCP tools.

Requires a sibling `[omnia](https://github.com/augentic/omnia)` checkout (this workspace patches the `omnia` crates to `../omnia/crates/*`) and `[cursor-agent](https://cursor.com/docs/cli)` on `PATH`, authenticated via `CURSOR_API_KEY` or a prior `cursor-agent login`.

## Build and run

```bash
# build the guest
cargo build -p examples --example cursor-wasm --target wasm32-wasip2

# create the working tree the mount lends
mkdir -p examples/cursor/workspace

# set Cursor API key
export CURSOR_API_KEY=<cursor API key>
export RUST_LOG=info,omnia_cursor=debug,cursor_wasm=debug,opentelemetry_sdk=off

# run the host (no config)
cargo run --example cursor -- run ./target/wasm32-wasip2/debug/examples/cursor_wasm.wasm --mount path=examples/cursor/workspace,name=.,writable

# run the host (with config)
cargo run --example cursor -- run --config examples/cursor/config.toml
```

The guest's `docs` MCP grant carries its endpoint URL directly (`http://localhost:8080/mcp` in `guest.rs`), pointing back at the guest's own HTTP export — as the sole HTTP-exporting guest it is the catch-all route, and the MCP router matches every path. The mount (`--mount`, or `[[mount]]` in `config.toml`) preopens `examples/cursor/workspace` as the tree named `.`; the guest lends it through `grants.workspace` and the cursor backend resolves it to the working tree the spawned agent runs in.

## Test

The run command drives `wasi:cli/run` once, which calls `create` and prints the answer — expect the widget lifecycle stages, sourced from the MCP docs tools.

## MCP servers

MCP wiring is opt-in per completion and spans two layers:

1. **Prompt grant** — the guest names a server in `tools` and supplies its endpoint `url` (here, `docs` → `http://localhost:8080/mcp` in `guest.rs`). Only granted servers are wired into the spawned `cursor-agent`.
2. **HTTP serving** — that endpoint must resolve to a running MCP server. Here it is the same guest's `wasi:http` export (`omnia_guest::mcp::router`); it could equally be a separate guest behind a `[[route.http]]` prefix, or any external server.

When a completion runs, `omnia-cursor` reads the grant's `url`, merges the entry into `<workspace>/.cursor/mcp.json` for the spawn, and passes `--approve-mcps`. `cursor-agent` has no `--mcp-config` flag; it discovers servers from the git toplevel of `--workspace` (or `~/.cursor/mcp.json`), so the backend `git init`s the workspace when it is not already a root and
strips host `GIT_*` identity vars from the spawn so discovery cannot skip it.