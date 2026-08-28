# Cursor Example

Live model completion via `[omnia-cursor](../../crates/cursor)`: the guest calls `create` with a `lifecycle` function tool.

## Prerequisites

[cursor-sdk-bridge](https://github.com/cursor/sdk-bridge) on `PATH`, and a `CURSOR_API_KEY`.

Install notes are in the [bridge docs](https://cursor.com/docs/sdk/bridge). To put the latest standalone binary on `PATH` via `~/.local/bin` (darwin/arm64; swap the asset suffix for `linux-x64`, `darwin-x64`, …):

```bash
# download and install
curl -fsSL -o /tmp/cursor-sdk-bridge.tar.gz \
  https://github.com/cursor/sdk-bridge/releases/latest/download/cursor-sdk-bridge-standalone-darwin-arm64.tar.gz \
  && tar -xzf /tmp/cursor-sdk-bridge.tar.gz -C /tmp \
  && install /tmp/bin/cursor-sdk-bridge ~/.local/bin/cursor-sdk-bridge

# verify
cursor-sdk-bridge --help
```



## Build and run

```bash
# build the guest
cargo build -p examples --example cursor-wasm --target wasm32-wasip2

# create the working tree the mount lends
mkdir -p examples/cursor/workspace

# set Cursor API key
export CURSOR_API_KEY=<cursor API key>
export RUST_LOG=info,omnia_cursor=debug,opentelemetry_sdk=off

# run the host (no config)
cargo run --example cursor -- run ./target/wasm32-wasip2/debug/examples/cursor_wasm.wasm --mount path=examples/cursor/workspace,name=.,writable

# run the host (with config)
cargo run --example cursor -- run --config examples/cursor/config.toml
```

## Host-to-guest tool calls

`wasi-model` implements guest-defined tools using two streams rather than direct callbacks. The host sends `ToolCall` values to the guest through the session’s `calls` stream, while the guest returns corresponding `ToolResult` values through a second stream it creates.

To open a completion session, the guest creates the result stream, retains its writable end, and passes the readable end to `create`. The host returns a `reply` future and the `calls` stream. While awaiting the reply, the guest handles each tool call and writes a result with the same correlation ID, allowing the host to resume the completion.

The mount (`--mount`, or `[[mount]]` in `config.toml`) preopens `examples/cursor/workspace` as the tree named `.`; the guest lends it through `grants.workspace` and the cursor backend resolves it to the working tree the agent runs in.

## Test

The run command drives `wasi:cli/run` once, which opens a completion session, answers the function-tool call, and prints the answer — expect a `tool call: widget_lifecycle` line, then the widget lifecycle stages (`draft`, `assembled`, `shipped`), sourced from the `ToolResult`.