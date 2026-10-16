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

// Or set the bounds, default model, pool shape, and shell roots explicitly.
let client = Client::connect_with(ConnectOptions {
    timeout_secs: 1800,
    inactivity_secs: 120,
    model: "composer-2".into(),
    backend_url: "https://api2.cursor.sh".into(),
    max_agents: 2,
    max_tool_calls: 128,
    shell_roots: vec!["/srv/checkouts".into()],
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
| `CURSOR_SHELL_ROOTS` | — | Directories (`:`-separated) a lend may sit beneath to run with the `shell` tool and carry a worker grant; a lend elsewhere reads alone |

The Cursor backend the relay forwards to is `ConnectOptions::backend_url`, `https://api2.cursor.sh` under `connect`, and set in code alone: a host's `CURSOR_BACKEND_URL` is Cursor's own variable, which the client neither reads nor lets reach a worker.

What a worker reaches beyond its lend is the request's to ask, per completion, through `grants.worker` of `omnia:model@0.2.0` — a `worker-grant` of `egress` hosts its commands may reach through the client's proxy (`*.example.com` naming every host under a domain), `read` roots it may read beside the system, each a directory beneath the operator's home spelled `~/<path>` (a toolchain, a package cache; one the home lacks is logged at WARN and granted nothing), and `env` variables set in its environment, a value's leading `~/` the operator's home. The grant is for a worker that runs commands, so one over a lend under no shell root is refused as `invalid-request` before anything is spawned, as is an entry the boundary refuses: a root that resolves outside the home or to a file, one that overlaps a credential store (`~/.ssh`, `~/.gnupg`, `~/.aws`, `~/.azure`, `~/.kube`, `~/.docker`, `~/.config/gh`, `~/.config/gcloud`, `~/.password-store`, `~/Library/Keychains`, at it, beneath it, or above it, as spelled or as either resolves), a variable the boundary sets itself (`HOME`, `TMPDIR`, the proxy variables), `CURSOR_API_KEY`, `CURSOR_BACKEND_URL`, `NODE_USE_SYSTEM_CA`, one the dynamic loader or the bridge's runtime reads (`LD_*`, `DYLD_*`, `BUN_OPTIONS`, `NODE_OPTIONS`), one the host passes through to the bridge (its own settings and the trust-store variables), or a `GIT_DIR` sibling. A grant may set `PATH` for the commands the worker runs; `cursor-sdk-bridge` itself is resolved against the host's `PATH`, so a grant never chooses the bridge. Nothing of the host's environment is the operator's to widen or narrow: the deployment decides where a shell may run, the adapter what its toolchain needs, and the boundary what no grant reads.

Workers run under [`omnia-airlock`](../airlock): in the lend as their working directory, in a session of their own, and with an environment built from nothing: `PATH`, `LANG`, `LC_*`, `TZ`, `TERM`, a private `HOME` and `TMPDIR`, the variables the bridge itself reads (`CURSOR_SDK_BRIDGE_LOG`, `CURSOR_DATA_DIR`, the certificate variables, …), the proxy variables naming the boundary's egress proxy, `CURSOR_BACKEND_URL` naming the client's relay, and the grant's `env` over them. The host's `CURSOR_API_KEY` rides the handshake alone, and no `GIT_*` variable crosses.

The bridge's API goes through a relay of the client's own, not the proxy: a plain-HTTP server at `http://[::1]:<port>`, the `::1` loopback address the boundary's `NO_PROXY` excepts, that forwards each call to `backend_url` over TLS with the host's own trust store and proxy settings, bodies streamed both ways, and calls under `/v1/` — the REST API the bridge validates a model against — to `api.cursor.com`. The bridge's runtime (Bun 1.3.9, in `cursor-sdk-bridge` 1.0.37) cannot stream a request body through a `CONNECT` proxy, so a bridge behind the proxy never completes a turn; behind the relay it speaks plain HTTP/1.1 to loopback, which it can. The relay is never addressed as `localhost` or `127.0.0.1`, since a backend spelled either way makes the bridge turn TLS verification off for itself and every command it runs.

The kernel boundary is always on: Seatbelt on macOS, Landlock and seccomp on Linux, and a connect refusal where neither runs (a Linux kernel before 6.7, or one without Yama); nothing in code or the environment turns it off. Inside, a worker reads the system, its lend, its scratch, and the grant's `read` roots; writes its lend and scratch; and reaches the network through the client's relay for the bridge's API and a `CONNECT` proxy bound for that boundary alone for everything else, which admits the grant's `egress` after vetting what each name resolves to (loopback, link-local, the cloud metadata addresses, and the host's own addresses refused). On macOS the kernel closes the network to loopback, so the relay and the proxy are the only routes out and no name resolves inside; `.git` and `.emery` beneath the lend are denied, and `ps` shows nothing of the host. On Linux the posture differs, and the differences are the operator's to weigh: Landlock ABI 4 fences TCP by port alone, so the ruleset handles no network right and egress is cooperative — the proxy variables, which the bridge, npm, git and curl honour, with datagram and raw sockets refused so the resolver's UDP path answers nothing — and a client that ignores the proxy and carries its own address reaches the network; `/proc` is readable whole, so the `environ`, `cmdline` and `maps` of other same-uid processes are readable inside (the host's own are closed, since it sets itself non-dumpable at connect; `mem` is closed by Yama); Landlock cannot deny beneath a grant, so `.git` and `.emery` beneath the lend are reachable; and `AF_UNIX` is ungoverned, so any host unix socket a command can path to is reachable. Same-uid loopback services are reachable from inside on both platforms, since the tree's own test loop needs loopback open. The full statement is the confine crate's [README](../confine/README.md).

The request's `generation` controls (temperature, max tokens, …) are ignored: `CreateAgent` has no sampling knobs.

## How a completion runs

**Workspace.** The guest lends a working tree through `grants.workspace`; the agent runs there with a read-only toolset (`read`, `glob`, `grep`, `ls`, plus `mcp`). A lend beneath one of `CURSOR_SHELL_ROOTS` adds `shell`, so the agent can run the tree's own build and tests; the bridge bounds each command and the inactivity window stands down while one runs. No edit or delete anywhere. Without a lent workspace the agent runs in a private empty directory with every built-in tool disabled, so function-tool-only completions still work. `read` takes absolute paths, and the toolset enforces no bound on them: the kernel holds every read to the lend, the system, and the grant's `read` roots.

**Tools.** Guest-declared function tools become SDK custom tools at `CreateAgent`. When the agent calls one, the worker POSTs to this crate's loopback callback endpoint, which routes it into the session via `ToolHost::call_tool` under the host's name check, budget, size cap, and per-call timeout. The budget is this backend's: `CURSOR_MAX_TOOL_CALLS` guest tool calls a completion, answered through `WasiModelCtx::limits`, in place of the host's default of 32, since an agent that lays a tree one file a call runs past that long before its wall-clock cap. `Tool::Mcp` grants pass inline as `mcp_servers`; the grant's `tools` allowlist is advisory.

**Format and check.** `format` reaches the agent as a final-answer instruction — steering only. With `check`, the answer is offered through `ToolHost::check`: `Ok` ends the completion, `Err(correction)` sends the correction as the next prompt on the same agent (its cache stays warm). Two rounds are allowed; a second rejection fails with the typed `budget-exhausted`.

**Lifecycle.** Agent scope is one `complete` call. The agent's whole life runs on its own task, so a completion the guest drops still cancels its run and tears its agent down. The answer is returned before teardown finishes; the pool slot reopens only once the process is gone.

## Timeouts and failure

Each `Send` (the opening prompt, and a correction if any) is bounded twice:

- **Inactivity window** — the stream went silent while waiting on the bridge (opening frame, or a tool call in flight). A `backend` failure a caller may retry. Stands down once the model is composing its answer.
- **Absolute cap** — the completion's time budget. Reaches the guest as the typed `budget-exhausted`.

Workers are session leaders and are shut down after their completion: a graceful `Shutdown` with 5s grace, then a kill of the whole session. A worker that crashes fails its completion with the typed `cursor-sdk-bridge exited (…)` rather than stalling the next one.

A worker lost while a completion is *opening* (`CreateAgent` or the first `Send` fails with `WorkerExited` or `RpcError::Transport`, and no candidate has reached the guest) restarts the completion once on a fresh worker. Nothing else restarts.

Match failures on types, not messages: `Failure::{Run, Timeout, Inactive, Aborted, WorkerExited}`, `RpcError::{Connect, Transport}`, and `omnia_wasi_model::Error::BudgetExhausted`.

## Observability

Everything a completion logs sits under its `complete` span (`n`, `model`, `format`, and the schema name as `label` where there is one).

- `completion started` (DEBUG) / `completion` (INFO on an answer, WARN otherwise), with phase buckets `opening_ms`, `tool_ms`, `model_ms`, `check_ms`, `thinking_ms`, and token counts.
- `in progress` (INFO) every 15s while a run is open: `elapsed_s`, `frames`, `tool_calls`, `silent_s`, and what it is `waiting` on.
- `tool call` / `tool call completed` (DEBUG) with a `subject`: a path, a pattern, or a command's first line, every `<cwd>/` in it spelled `./`. The arguments whole at TRACE under the target `omnia_cursor::tools`, so `RUST_LOG=omnia_cursor=debug,omnia_cursor::tools=trace` reads them without the stream's frames.
- Worker exit at WARN with `pid`, `uptime_ms`, `status`; its stderr tail at DEBUG only, since it is untrusted subprocess output.

## Tests

Three tiers. The first two run on every `cargo nextest run -p omnia-cursor` with no `cursor-sdk-bridge` installed and no key.

**The fake bridge.** [`tests/support/fake_bridge`](tests/support/fake_bridge) is a protocol-faithful `cursor-sdk-bridge`, built as `fake-cursor-sdk-bridge` and linked onto the test's `PATH` as `cursor-sdk-bridge`, so the client finds it exactly as a deployment finds the real one. It runs inside the kernel boundary as the real one does, so its script, log and releases live in a home every guest lends, where a confined process can read and append them; a completion lending nothing runs it in a private directory, where it finds no script and answers as an echo. A scripted `Config` decides replies and faults (hangs, parks, exits, `SIGKILL`, broken ready lines, reset streams, forked children), each aimed at one spawned process or all.

**Guests through the runtime.** Every scenario is a guest from [`crates/test-programs`](../test-programs) run through `omnia_test::host::Deployment`. [`tests/model.rs`](tests/model.rs) is one row per guest program (the `omnia:model` contract); [`tests/worker.rs`](tests/worker.rs) is the lifecycle and fault matrix, with the chamber rows: the shell on a shell root, and a worker grant laid against a scratch `HOME` — a `~/` root the fake reads inside beside one it cannot, a `~/` value expanded, a missing root skipped, and each refusal of the lay ending the completion unspawned. Each test also asserts the fake's RPC sequence and that every spawned process is gone afterwards.

**The relay.** [`tests/relay.rs`](tests/relay.rs) stands a fake backend behind the relay a fake-bridge completion was pointed at: path, query and end-to-end headers reach it, the connection-specific set does not, a request body arrives chunk by chunk and a response leaves the same way, a backend nothing listens at is `502`, and a worker grant setting `CURSOR_BACKEND_URL` is refused.

**The real bridge.** [`tests/live.rs`](tests/live.rs) drives real completions, including fan-out, a `kill -9` of a worker mid-run, a turn completed inside the kernel boundary with the bridge's API through the relay, and an `npm install` through the proxy over Node's bundled roots. All are `#[ignore]`d; run them by hand:

```bash
CURSOR_API_KEY=... \
  cargo nextest run -p omnia-cursor --run-ignored all
```

Add `CURSOR_SDK_BRIDGE_LOG=1` when a failure needs the worker's per-RPC stderr.

## License

MIT OR Apache-2.0
