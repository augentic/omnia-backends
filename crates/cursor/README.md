# omnia-cursor

[![crates.io](https://img.shields.io/crates/v/omnia-cursor.svg)](https://crates.io/crates/omnia-cursor)
[![docs.rs](https://docs.rs/omnia-cursor/badge.svg)](https://docs.rs/omnia-cursor)

Cursor model backend for the Omnia WASI runtime, implementing the `omnia:model/completion` boundary (`wasi-model`) through [`cursor-sdk-bridge`](https://github.com/cursor/sdk-bridge), a local process that wraps Cursor's SDK behind Connect RPCs.

Each completion gets its own `cursor-sdk-bridge` process (a *worker*) and a fresh agent on it. The agent runs its own tool loop against the lent workspace and returns its answer through the same boundary `omnia-genai` uses. The guest only sees the answer; the model id, API key, and `sdk.v1` protocol stay inside this crate.

MSRV: Rust 1.99

## Requirements

- `cursor-sdk-bridge` on `PATH`.
- `CURSOR_API_KEY` set. `sdk.v1` authenticates every agent with an explicit key, so a prior `cursor-agent login` is not enough. The key is never logged, stored on `ConnectOptions`, or shown by `Client`'s `Debug`.

Connecting without a key succeeds; the first completion fails instead.

## Usage

Bind the backend in your host's `runtime!` map. The guest `.wasm` is untouched (see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

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

// Reads CURSOR_* from the environment, with the defaults below.
let client = Client::connect().await?;

// Or set the bounds, default model, and pool shape explicitly.
let client = Client::connect_with(ConnectOptions {
    timeout_secs: 1800,
    inactivity_secs: 120,
    model: "composer-2".into(),
    max_agents: 2,
    max_tool_calls: 128,
}).await?;
```

A full guest + runtime demo lives in [`examples/cursor`](../../examples/cursor).

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CURSOR_API_KEY` | — | Required for any completion |
| `CURSOR_MODEL` | `auto` | Model id when a request leaves `model` unset (`auto` is Cursor's server-side choice) |
| `CURSOR_TIMEOUT_SECS` | `600` | Absolute wall-clock cap on one agent run |
| `CURSOR_INACTIVITY_SECS` | `120` | Cancel a run whose stream has gone silent while waiting on the bridge |
| `CURSOR_MAX_AGENTS` | `8` | Agents live at once, each on its own worker; further completions queue |
| `CURSOR_MAX_TOOL_CALLS` | `128` | Guest tool calls one completion may make before it ends as `budget-exhausted` |
| `CURSOR_SHELL_ROOTS` | — | Host directories (absolute, `PATH`-separated) whose lends get the `shell` tool, under the bridge's sandbox: writes confined to the tree, network closed; needs Cursor's sandbox binary, and a proxy variable without credentials |

Workers are spawned from a fixed environment allowlist, never the host's whole environment: `PATH`, `HOME`, `TMPDIR`, `SHELL`, `USER`, `LOGNAME`, `TERM`, `LANG`, `TZ` and `LC_*`; the bridge's own `CURSOR_BACKEND_URL`, `CURSOR_WEBSITE_URL`, `CURSOR_DATA_DIR`, `CURSOR_RIPGREP_PATH`, `CURSOR_TREE_SITTER_VENDOR_DIR`, `CURSOR_SDK_BRIDGE_LOG` and `CURSOR_SDK_BRIDGE_DEBUG_STARTUP`; and the proxy and CA variables (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` in either case, `SSL_CERT_FILE`, `SSL_CERT_DIR`, `NODE_EXTRA_CA_CERTS`, `NODE_USE_SYSTEM_CA`). `CURSOR_API_KEY` rides on every RPC instead, and the credentials of any backend sharing the host process never reach a worker or a shell child the agent runs. The bridge hands its whole environment to each shell command, so with `CURSOR_SHELL_ROOTS` set a proxy URL that carries credentials (`http://user:secret@proxy`) is refused at connect: the proxy is unauthenticated or there is none. `CURSOR_SDK_BRIDGE_LOG` passes straight through and the worker's RPC log lands in this crate's DEBUG output.

The request's `generation` controls (temperature, max tokens, …) are ignored: `CreateAgent` has no sampling knobs.

## How a completion runs

**Workspace.** The guest lends a working tree through `grants.workspace`; the agent runs there with the read-only tools (`read`, `glob`, `grep`, `ls`, plus `mcp`). No edit or delete: the tree is written through the guest's tools. A lend is a preopen, not a trust decision, so the shell is the operator's to grant, tree by tree: a lend at or beneath one of the `CURSOR_SHELL_ROOTS` also gets `shell`, and the bridge's sandbox with it — writes confined to the tree, the network closed — so a command a prompt names, or a file in the tree injects, runs confined or not at all (the bridge refuses the completion where Cursor's sandbox binary is missing); a lend outside the roots keeps the read-only tools. The bridge reads the sandbox policy of every shell command from the agent's working directory alone, folding that directory's `.cursor/sandbox.json` into the operator's, and a lent tree is the guest's to write for as long as the run lasts; so a shell-bearing agent is not started in the tree but in a private directory of the backend's, created for the completion and removed with it, whose policy admits the tree as a write path (its `.git/hooks`, `.git/config` and the like kept read-only, as the bridge keeps them in a tree it runs commands in), holds the directory itself read-only, and says nothing of the network, which stays the operator's. Whatever the tree says of its own confinement is never read, before the run or during it, and no command rewrites what the directory says. The tree rides beside the working directory as a workspace root, so `read`, `glob`, `grep` and `ls` cover it, and the prompt opens by naming it as the project to work in. The lent tree's `.cursor/` is never a setting source: its rules, hooks and MCP servers stay out of the agent. Each shell command is bounded by the bridge, at the timeout the agent's call names (30 s when it names none); the inactivity window does not bound it, so a long check the agent asked for is not cut short by a limit it cannot see. Without a lent workspace the agent runs in a private empty directory with every built-in tool disabled, so function-tool-only completions still work. `read` takes absolute paths, so staying inside the tree is the prompt's to ask, not the toolset's to enforce.

**Tools.** Guest-declared function tools become SDK custom tools at `CreateAgent`. When the agent calls one, the worker POSTs to this crate's loopback callback endpoint, which routes it into the session via `ToolHost::call_tool` under the host's name check, budget, size cap, and per-call timeout. The budget is this backend's: `CURSOR_MAX_TOOL_CALLS` guest tool calls a completion, answered through `WasiModelCtx::limits`, in place of the host's default of 32, since an agent that lays a tree one file a call runs past that long before its wall-clock cap. `Tool::Mcp` grants pass inline as `mcp_servers`; the grant's `tools` allowlist is advisory.

**Format and check.** `format` reaches the agent as a final-answer instruction — steering only. With `check`, the answer is offered through `ToolHost::check`: `Ok` ends the completion, `Err(correction)` sends the correction as the next prompt on the same agent (its cache stays warm). Two rounds are allowed; a second rejection fails with the typed `budget-exhausted`.

**Lifecycle.** Agent scope is one `complete` call. The agent's whole life runs on its own task, so a completion the guest drops still cancels its run and tears its agent down. The answer is returned before teardown finishes; the pool slot reopens only once the process is gone.

## Timeouts and failure

Each `Send` (the opening prompt, and a correction if any) is bounded twice:

- **Inactivity window** — the stream went silent while waiting on the bridge (opening frame, or a tool call in flight). A `backend` failure a caller may retry. Stands down once the model is composing its answer, and while a shell command runs, since the bridge bounds that by the call's own timeout.
- **Absolute cap** — the completion's time budget. Reaches the guest as the typed `budget-exhausted`.

Workers are process-group leaders and are shut down after their completion: a graceful `Shutdown` with 5s grace, then a kill of the whole group. A worker that crashes fails its completion with the typed `cursor-sdk-bridge exited (…)` rather than stalling the next one.

A worker lost while a completion is *opening* (`CreateAgent` or the first `Send` fails with `WorkerExited` or `RpcError::Transport`, and no candidate has reached the guest) restarts the completion once on a fresh worker. Nothing else restarts.

Match failures on types, not messages: `Failure::{Run, Timeout, Inactive, Aborted, WorkerExited}`, `RpcError::{Connect, Transport}`, and `omnia_wasi_model::Error::BudgetExhausted`.

## Observability

Everything a completion logs sits under its `complete` span (`n`, `model`, `format`, and the schema name as `label` where there is one).

- `completion started` (DEBUG) / `completion` (INFO on an answer, WARN otherwise), with phase buckets `opening_ms`, `tool_ms`, `model_ms`, `check_ms`, `thinking_ms`, and token counts.
- `in progress` (INFO) every 15s while a run is open: `elapsed_s`, `frames`, `tool_calls`, `silent_s`, and what it is `waiting` on.
- `tool call` / `tool call completed` (DEBUG), arguments at TRACE.
- Worker exit at WARN with `pid`, `uptime_ms`, `status`; its stderr tail at DEBUG only, since it is untrusted subprocess output.

## Tests

Three tiers. The first two run on every `cargo nextest run -p omnia-cursor` with no `cursor-sdk-bridge` installed and no key.

**The fake bridge.** [`tests/support/fake_bridge`](tests/support/fake_bridge) is a protocol-faithful `cursor-sdk-bridge`, built as `fake-cursor-sdk-bridge` and linked onto the test's `PATH` as `cursor-sdk-bridge`, so the client finds it exactly as a deployment finds the real one. A scripted `Config` decides replies and faults (hangs, parks, exits, `SIGKILL`, broken ready lines, reset streams, forked children), each aimed at one spawned process or all.

**Guests through the runtime.** Every scenario is a guest from [`crates/test-programs`](../test-programs) run through `omnia_test::host::Deployment`. [`tests/model.rs`](tests/model.rs) is one row per guest program (the `omnia:model` contract); [`tests/worker.rs`](tests/worker.rs) is the lifecycle and fault matrix. Each test also asserts the fake's RPC sequence and that every spawned process is gone afterwards.

**The real bridge.** [`tests/live.rs`](tests/live.rs) drives real completions, including fan-out and a `kill -9` of a worker mid-run. All are `#[ignore]`d; run them by hand:

```bash
CURSOR_API_KEY=... \
  cargo nextest run -p omnia-cursor --run-ignored all
```

Add `CURSOR_SDK_BRIDGE_LOG=1` when a failure needs the worker's per-RPC stderr.

## License

MIT OR Apache-2.0
