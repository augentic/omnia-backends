# Architecture

This document describes how the crates in this repository plug production services into the [Omnia](https://github.com/augentic/omnia) WASI component runtime. For the runtime's own architecture (engine, registry, execution flow), see [Omnia's Architecture doc](https://github.com/augentic/omnia/blob/main/docs/Architecture.md).

## Where backends sit

Omnia is organized into three layers; this repository is the top one:

```text
┌─────────────────────────────────────────────────────────────────┐
│  Layer 3: Backends (this repo)                                  │
│  Concrete connections to external services                      │
│  redis, kafka, nats, postgres, mongodb, azure-*, genai, cursor, │
│  git                                                            │
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
| `filesystem`    | Local filesystem        | keyvalue, blobstore                     |
| `azure-blob`    | Azure Blob Storage      | blobstore, package store                |
| `azure-id`      | Azure Managed Identity  | identity                                |
| `azure-vault`   | Azure Key Vault         | vault                                   |
| `azure-table`   | Azure Table Storage     | docstore                                |
| `opentelemetry` | OTEL Collector          | otel                                    |
| `genai`         | LLM provider APIs       | model                                   |
| `cursor`        | `cursor-sdk-bridge`     | model                                   |
| `git`           | `git`                   | vcs                                     |

No backend here implements `wasi-http`, `wasi-config`, or `wasi-websocket`; those use Omnia's in-tree defaults. `wasi-vcs` has no in-tree default, so `git` is the one backend a deployment serving it names.

### Package store

`azure-blob` additionally implements `omnia_plugin::PackageStore` — the store omnia's registry acquirer reads before any registry and writes a fetched release to once (the `runtime!` macro's `plugins: { store: .. }` names omnia's own local `FsStore`; this impl is the remote counterpart an embedder selects through `Deployment::registry_source`). It owns a container disjoint from guest storage, named by the impl itself (`omnia-plugins`) and refused by name to the `wasi:blobstore` view so the store is its only writer, with one blob per release under the file name `FsStore` uses (`namespace_name@version.wasm`). A blob already there is never replaced: a stored release is final until it is deleted. Verification lives in the acquirer — a fetched release is hashed against the registry's digest before the write, and a served one against the load's pin.

### Model backends

The two `wasi-model` backends serve `omnia:model/completion` requests and differ in execution model:

- **`genai`** calls provider APIs (OpenAI, Anthropic, Gemini, Groq, Ollama, ...) in-process via the [`genai`](https://crates.io/crates/genai) SDK, advertising the request's declared function tools — plus the host-injected `read`/`list` workspace tools when the guest lent a workspace — and driving the bounded session tool loop: `read`/`list` execute host-side through the `ToolHost` workspace capability, every other call goes through `ToolHost::call_tool`. Provider API keys are read from the environment at call time. MCP tool grants are rejected — use `cursor` for those.
- **`cursor`** spawns one [`cursor-sdk-bridge`](https://github.com/cursor/sdk-bridge) process per live agent — one bridge-managed agent per completion, at most `CURSOR_MAX_AGENTS` at once, each process closed after its completion so a crash costs that completion alone — running an agentic session inside the workspace the guest granted (or a private empty directory, tools-only, when none is lent). Guest-declared function tools are declared as SDK custom tools; the bridge calls them back on the backend's loopback `CallCustomTool` server, which routes into the session through `ToolHost::call_tool`. MCP server grants pass inline as the agent's `mcp_servers`. Requires `cursor-sdk-bridge` on `PATH` and `CURSOR_API_KEY`.

### Version control

`git` serves `omnia:vcs` — the `store`, `workspace`, and `transport` interfaces — by running the operator's own `git` binary, one process per operation, in the directory the runtime opened for the guest's lent location: the child enters the open handle before it executes, so git works where the location resolved and never walks a path a guest could redirect meanwhile. Omnia's lend rule bounds every location to a preopen the guest holds, so a guest reaches no repository it was not granted; `GIT_DIR` and its siblings are scrubbed from each process's environment, so the operator's shell cannot redirect one. A lent repository's own configuration is a guest's to write, so every operation is held to host policy on its command line: hooks, the file-system monitor, the alternate-refs command, signing, and submodule recursion are forced off, the work tree is the place itself, a repository-scoped SSH command, credential prompt, credential helper, or clean, smudge, or merge driver gives way to the operator's own or to none, every guest string follows `--`, every process runs under `GIT_ALLOW_PROTOCOL`, a transport under the list for its kind, and a repository that sets how a transport runs — a URL rewrite, an HTTP setting, a proxy — is refused on every transport, since those are the host's to set and no command line puts them back. A merge policy is applied by the backend from the merge's three commits, over whatever the repository's attributes made of a path, and is never written into the repository. Connecting refuses a git older than 2.26, the first with `config --show-scope`. See [`crates/git/README.md`](../crates/git/README.md).

### Registry acquisition

Registry acquisition itself is not a backend: omnia's `RegistryClient` (the `omnia-plugin` crate, re-exported from `omnia`) fetches and verifies packages, compiled in at the composition root through the `runtime!` macro's `plugins:` block. This repository only supplies the Azure `PackageStore` impl for it (see [Package store](#package-store) above).

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

This workspace consumes the `omnia` runtime as published crates.io dependencies, declared once under `[workspace.dependencies]` in the workspace `Cargo.toml` (currently the 0.36.0 line). Each release of this repository pairs with one omnia line; see [RELEASES.md](../RELEASES.md).

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
