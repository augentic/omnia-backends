# Architecture

This document describes how the crates in this repository plug production services into the [Omnia](https://github.com/augentic/omnia) WASI component runtime. For the runtime's own architecture (engine, registry, execution flow), see [Omnia's Architecture doc](https://github.com/augentic/omnia/blob/main/docs/Architecture.md).

## Where backends sit

Omnia is organized into three layers; this repository is the top one:

```text
┌─────────────────────────────────────────────────────────────────┐
│  Layer 3: Backends (this repo)                                  │
│  Concrete connections to external services                      │
│  redis, kafka, nats, postgres, mongodb, azure-*, genai, cursor  │
├─────────────────────────────────────────────────────────────────┤
│  Layer 2: WASI Interfaces (omnia: crates/wasi-*)                │
│  Abstract service capabilities defined by WIT interfaces        │
├─────────────────────────────────────────────────────────────────┤
│  Layer 1: Runtime core (omnia: crates/omnia)                    │
│  wasmtime engine, CLI, deployment registry, dispatch, traits    │
└─────────────────────────────────────────────────────────────────┘
```

Guests are compiled against Layer 2 interfaces only. A guest that calls `wasi:keyvalue` neither knows nor cares whether the host answers with Omnia's in-memory default or this repo's Redis client — swapping backends is a host-side, one-line change and never requires recompiling the guest.

## What a backend implements

Every backend crate implements two things from the `omnia` runtime:

1. **`omnia::Backend`** — connection management. `connect()` reads a `ConnectOptions` struct from environment variables (via `FromEnv`) and establishes the client:

```rust
pub trait Backend: Sized + Sync + Send {
    type ConnectOptions: FromEnv;

    /// Connect using options parsed from the environment.
    fn connect() -> impl Future<Output = Result<Self>>;

    /// Connect with explicit options.
    fn connect_with(options: Self::ConnectOptions) -> impl Future<Output = Result<Self>>;
}
```

2. **One or more `WasiXxxCtx` context traits** — the behavior behind a WASI interface. For example, `omnia-redis` implements `WasiKeyValueCtx`; `omnia-nats` implements `WasiMessagingCtx`, `WasiKeyValueCtx`, and `WasiBlobstoreCtx`.

A typical backend:

```rust
#[derive(Clone)]
pub struct Client(ConnectionManager);

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        // Connect to the service...
    }
}

impl WasiKeyValueCtx for Client {
    fn open_bucket(&self, identifier: String) -> FutureResult<Arc<dyn Bucket>> {
        // Provide keyvalue functionality via Redis...
    }
}
```

## Interface coverage

| Crate           | Service                 | Implements                              |
| --------------- | ----------------------- | --------------------------------------- |
| `redis`         | Redis                   | keyvalue                                |
| `nats`          | NATS / JetStream        | keyvalue, messaging, blobstore          |
| `kafka`         | Apache Kafka            | messaging                               |
| `mongodb`       | MongoDB                 | blobstore                               |
| `postgres`      | PostgreSQL              | sql                                     |
| `filesystem`    | Local filesystem        | keyvalue, blobstore, plugin store       |
| `azure-blob`    | Azure Blob Storage      | blobstore, plugin store                 |
| `azure-id`      | Azure Managed Identity  | identity                                |
| `azure-vault`   | Azure Key Vault         | vault                                   |
| `azure-table`   | Azure Table Storage     | docstore                                |
| `opentelemetry` | OTEL Collector          | otel                                    |
| `genai`         | LLM provider APIs       | model                                   |
| `cursor`        | `cursor-sdk-bridge`     | model                                   |

No backend here implements `wasi-http`, `wasi-config`, or `wasi-websocket`; those use Omnia's in-tree defaults.

### Plugin store

`filesystem` and `azure-blob` additionally implement `omnia_plugin::ContentStore` and `omnia_plugin::ReleaseStore` — the two halves of the `PluginStore` bound behind omnia's registry acquirer (the `runtime!` macro's plugin cache). Each impl owns a tree disjoint from guest storage by construction: `filesystem` writes a `plugins/` subtree beside `blobstore/` and `keyvalue/`; `azure-blob` writes a dedicated container it names itself (`omnia-plugins`). Content entries are shared across registries (the digest is the identity); release records are scoped per registry. Verification of served bytes lives in the acquirer — the content impls only refuse writes whose bytes do not hash to their digest key.

### Model backends

The two `wasi-model` backends serve `omnia:model/completion` requests and differ in execution model:

- **`genai`** calls provider APIs (OpenAI, Anthropic, Gemini, Groq, Ollama, ...) in-process via the [`genai`](https://crates.io/crates/genai) SDK, advertising the request's declared function tools — plus the host-injected `read`/`list` workspace tools when the guest lent a workspace — and driving the bounded session tool loop: `read`/`list` execute host-side through the `ToolHost` workspace capability, every other call goes through `ToolHost::call_tool`. Provider API keys are read from the environment at call time. MCP tool grants are rejected — use `cursor` for those.
- **`cursor`** spawns one [`cursor-sdk-bridge`](https://github.com/cursor/sdk-bridge) process per client and creates one bridge-managed agent per completion, running an agentic session inside the workspace the guest granted (or a private empty directory, tools-only, when none is lent). Guest-declared function tools are declared as SDK custom tools; the bridge calls them back on the backend's loopback `CallCustomTool` server, which routes into the session through `ToolHost::call_tool`. MCP server grants pass inline as the agent's `mcp_servers`. Requires `cursor-sdk-bridge` on `PATH` and `CURSOR_API_KEY`.

### Registry acquisition

Registry acquisition itself is not a backend: omnia's `RegistryAcquire` (the `omnia-plugin` crate, re-exported from `omnia`) fetches and verifies packages, compiled in at the composition root through the `runtime!` macro's `plugins:` block. This repository only supplies the `ContentStore` / `ReleaseStore` impls for it (see [Plugin store](#plugin-store) above).

## Wiring a backend into a host runtime

Backends slot into the `omnia::runtime!` host map in place of an in-tree default:

```rust
use omnia_redis::Client as Redis;
use omnia_wasi_http::{HttpDefault, WasiHttp};
use omnia_wasi_keyvalue::WasiKeyValue;
use omnia_wasi_otel::{OtelDefault, WasiOtel};

omnia::runtime!({
    hosts: {
        WasiHttp: HttpDefault,
        WasiOtel: OtelDefault,
        WasiKeyValue: Redis,
    }
});
```

At startup the generated code calls each backend's `connect()` (reading its environment variables), links every WASI interface into the shared linker, and starts the trigger servers. See the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md) for the full walk-through.

This workspace consumes the `omnia` runtime through `[patch.crates-io]` entries in the workspace `Cargo.toml` — pointed at the omnia git repository by default, or at a sibling checkout by switching the entries to `path = "../omnia/crates/<name>"` (which requires both repositories checked out side by side).

## Configuration

All backends configure themselves from environment variables through the `FromEnv` derive:

```rust
#[derive(Debug, Clone, FromEnv)]
pub struct ConnectOptions {
    #[env(from = "REDIS_URL", default = "redis://localhost:6379")]
    pub url: String,
    #[env(from = "REDIS_MAX_RETRIES", default = "3")]
    pub max_retries: usize,
}
```

Each crate's README documents its variable set.

## Testing

CI cannot stand up the real services, so the testing policy (see [AGENTS.md](../AGENTS.md)) splits coverage:

- **Unit tests** cover pure, service-free translation logic (OData filter building, Postgres type mapping, the Kafka partitioner, cursor prompt build/parse). These run in CI.
- **Live tests** (`tests/live.rs`, `#[ignore]`-gated) drive the backend's `WasiXxxCtx` implementation against the real service. Run them locally:

```bash
cargo nextest run -p <crate> --run-ignored all   # with the service + credentials available
```

## Adding a New Backend

1. Create `crates/<name>/`
2. Implement the `Backend` trait with a `FromEnv`-derived `ConnectOptions`
3. Implement the `WasiXxxCtx` context trait(s) for the interfaces it serves
4. Add `#[ignore]`-gated live tests in `tests/live.rs` and document the run recipe in the crate README
5. Add example(s) under `examples/` if the backend benefits from an end-to-end demo

## Related Documentation

- [Omnia Architecture](https://github.com/augentic/omnia/blob/main/docs/Architecture.md) — the runtime this repo plugs into
- [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md) — wiring and configuration
- [wasmtime Component Model](https://docs.wasmtime.dev/api/wasmtime/component/)
- [WIT Format](https://component-model.bytecodealliance.org/design/wit.html)
