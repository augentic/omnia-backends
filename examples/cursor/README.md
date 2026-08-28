# Cursor Example

Live model completion via `[omnia-cursor](../../crates/cursor)`: the guest calls `create` once (command mode) with a `widget_lifecycle` function tool. When the bridge-managed Cursor agent calls that tool, the backend routes it through `ToolHost::call_tool` into the session; the guest writes a `ToolResult` and the agent answers from that extra information.

Requires a sibling `[omnia](https://github.com/augentic/omnia)` checkout (this workspace patches the `omnia` crates to `../omnia/crates/*`), `[cursor-sdk-bridge](https://github.com/cursor/sdk-bridge)` on `PATH`, and `CURSOR_API_KEY` set. Install the bridge from the [Cursor SDK bridge docs](https://cursor.com/docs/sdk/bridge).

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

The guest declares `widget_lifecycle` in `request.tools`. Each `tool-call` on the session is answered with a `ToolResult` whose `output` is the ordered widget stages. The mount (`--mount`, or `[[mount]]` in `config.toml`) preopens `examples/cursor/workspace` as the tree named `.`; the guest lends it through `grants.workspace` and the cursor backend resolves it to the working tree the agent runs in.

## Test

The run command drives `wasi:cli/run` once, which opens a completion session, answers the function-tool call, and prints the answer — expect the widget lifecycle stages (`draft`, `assembled`, `shipped`), sourced from the `ToolResult`.
