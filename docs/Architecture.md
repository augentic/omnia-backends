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

1. **`omnia::Connect`** — connection management. `connect()` reads a `ConnectOptions` struct from environment variables (via `FromEnv`) and establishes the client:

```rust
pub trait Connect: Sized + Sync + Send {
    type ConnectOptions: FromEnv;

    /// Connect using options parsed from the environment.
    fn connect() -> impl Future<Output = Result<Self>>;

    /// Connect with explicit options.
    fn connect_with(options: Self::ConnectOptions) -> impl Future<Output = Result<Self>>;
}
```

2. **One or more `XxxClient` contract traits** — the behavior behind a capability interface. For example, `omnia-redis` implements `KeyValueClient`; `omnia-nats` implements `MessagingClient`, `KeyValueClient`, and `BlobStoreClient`.

A typical backend:

```rust
#[derive(Clone)]
pub struct Client(ConnectionManager);

impl Connect for Client {
    type ConnectOptions = ConnectOptions;

    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        // Connect to the service...
    }
}

impl KeyValueClient for Client {
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
| `opentelemetry` | OTEL Collector          | otel (deprecated)                       |
| `genai`         | LLM provider APIs       | model                                   |
| `cursor`        | `cursor-sdk-bridge`     | model                                   |
| `git`           | `git`                   | vcs                                     |

No backend here implements `wasi-http`, `wasi-config`, or `wasi-websocket`; those use Omnia's in-tree defaults. `opentelemetry` is deprecated, since Omnia's `OtelDefault` sends guest telemetry to the collector the host's own goes to. `wasi-vcs` has no in-tree default, so `git` is the one backend a deployment serving it names.

### Package store

`azure-blob` additionally implements `omnia_plugin::PackageStore` — the store omnia's registry acquirer reads before any registry and writes a fetched release to once (the `runtime!` macro's `plugins: { store: .. }` names omnia's own local `FsStore`; this impl is the remote counterpart an embedder selects through `Deployment::registry_source`). It owns a container disjoint from guest storage, named by the impl itself (`omnia-plugins`) and refused by name to the `omnia:blobstore` view so the store is its only writer, with one blob per release under the file name `FsStore` uses (`namespace_name@version.wasm`). A blob already there is never replaced: a stored release is final until it is deleted. Verification lives in the acquirer — a fetched release is hashed against the registry's digest before the write, and a served one against the load's pin.

### Model backends

The two `wasi-model` backends serve `omnia:model/completion` requests and differ in execution model:

- **`genai`** calls provider APIs (OpenAI, Anthropic, Gemini, Groq, Ollama, ...) in-process via the [`genai`](https://crates.io/crates/genai) SDK, advertising the request's declared function tools — plus the host-injected `read`/`list` workspace tools when the guest lent a workspace — and driving the bounded session tool loop: `read`/`list` execute host-side through the `ToolHost` workspace capability, every other call goes through `ToolHost::call_tool`. Provider API keys are read from the environment at call time.
- **`cursor`** spawns one [`cursor-sdk-bridge`](https://github.com/cursor/sdk-bridge) process per live agent — one bridge-managed agent per completion, at most `CURSOR_MAX_AGENTS` at once, each process closed after its completion so a crash costs that completion alone — running an agentic session inside the workspace the guest granted (or a private empty directory, tools-only, when none is lent). Guest-declared function tools are declared as SDK custom tools; the bridge calls them back on the backend's loopback `CallCustomTool` server, which routes into the session through `ToolHost::call_tool`. Requires `cursor-sdk-bridge` on `PATH` and `CURSOR_API_KEY`. Every bridge runs under `omnia-sandbox`, below.

### Sandbox

`omnia-sandbox` is not a backend but the library a backend that runs a program on the host spawns it through, so a process in the model's trust domain — the bridge, and the shell, `node`, `npm` and `git` the agent runs through it — reaches what the host decided and nothing else. A `Sandbox` is connected once with the backend's `Options`: the `shell_roots` a workspace must sit beneath to run with a shell, and the host variables a process inherits and those refused; the kernel boundary is always on, and a host that cannot hold one refuses to connect. Each process is then prepared a `Session` at its workspace (or a private directory) under the completion's `Grant` — the `egress` hosts its commands may reach, the `read` roots beneath the operator's home it may read beside the system (a credential store among them, `~/.ssh`, `~/.aws` and their kin, refused by the crate's own rule whatever the deployment), and the `env` set for it, which the guest spells per completion as the `worker` grant of its lend and the backend refuses as `invalid` where the workspace runs no shell or an entry breaks a rule of the sandbox — and the session's `Command` runs it in the workspace's open directory handle, as a session leader, with an environment built from nothing — `PATH`, `LANG`, `LC_*`, `TZ`, `TERM`, a private `HOME` and `TMPDIR`, the proxy variables, the backend's `inherit`, and the grant's variables — behind the platform's kernel boundary: a Seatbelt profile on macOS, applied through `/usr/bin/sandbox-exec` with the network closed to loopback; a Landlock ruleset and two seccomp filters on Linux, built in the parent and applied in the child, with the host set non-dumpable. A session holds its workspace by the descriptor the backend lent as a `Lend`, never by a path, so a guest that moves the lend or lays a link at its name changes nothing a process reaches, and a lend the guest mounted read-only is read-only to every process. The one way out is a `CONNECT` proxy on loopback bound for that session alone, which admits the grant's egress and vets what every name resolves to, so a public name that resolves to loopback, a link-local or metadata address, or the host itself is refused. The two platforms differ in what the kernel can hold: Landlock ABI 4 fences TCP by port alone, so on Linux the ruleset handles no network right and egress is cooperative (the proxy variables, with datagram and raw sockets refused), `/proc` of other same-uid processes is readable, and nothing beneath a grant can be denied. A backend that runs a program of the operator's over data a guest can write, as `git` runs over a guest's mounts, runs each process through `Sandbox::command` under `Bounds` instead: no session, scratch, or proxy, the roots it reads and writes given as open descriptors, no network at all, and an environment the backend builds. The crate's [README](../crates/sandbox/README.md) states both postures in full.

### Version control

`git` serves `omnia:vcs` — the `store`, `workspace`, and `transport` interfaces — by running the operator's own `git` binary in the directory the runtime opened for the guest's lent location: the child enters the open handle before it executes, so git works where the location resolved and never walks a path a guest could redirect meanwhile. Every git run for a place runs under the `omnia-sandbox` kernel boundary, which holds it to the guest's mounts, the files the host's git needs, and the operation's own directory, with no network and an environment from a short list. Omnia's lend rule bounds every location to a preopen the guest holds, and the backend holds the repository's gitdir and common directory, and every local address a transport dials, beneath the guest's mounts too, so a guest reaches no repository it was not granted, whatever it lays as a link or a `.git` file. A network remote is dialled only from the backend's own repositories outside every mount: a mirror per URL, which a clone or fetch fills and then reads into the place from inside the boundary, and a staging per push, filled from the place over a pipe. `GIT_DIR` and its siblings are scrubbed from each process's environment, so the operator's shell cannot redirect one. A lent repository's own configuration is a guest's to write, so every operation is held to host policy on its command line and in its environment: hooks, the file-system monitor, the alternate-refs command, signing, submodule recursion, bundle URIs, and automatic garbage collection and maintenance are forced off, the work tree is the place itself and git's search for the repository stops there, the configuration is listed once per operation and every later process reads that listing from a frozen copy the backend owns rather than from the repository's own files, so nothing a guest writes meanwhile is read, a listing that fails refuses the operation, the SSH command, credential prompt, signing programs, and credential helpers are the operator's own or none, a clean, smudge, or merge driver the repository defined is emptied, a repository key no pin can name refuses the operation, a clone lays the place's repository from a repository the backend owns, every guest string follows `--`, every process runs under `GIT_ALLOW_PROTOCOL` and `GIT_NO_LAZY_FETCH`, no protocol outside a transport operation and a transport under the list for its kind, and under `LC_ALL=C`, so the typed errors are read from English whatever the operator's locale, the pack command a transport's far side runs is named on the command line rather than left to the repository, and a repository that sets how a transport runs — a URL rewrite, an HTTP setting, a proxy — is refused on every transport, since those are the host's to set and no command line puts them back. A merge policy is applied by the backend from the merge's three commits, over whatever the repository's attributes made of a path, and is never written into the repository. Connecting refuses a git older than 2.26, the first with `config --show-scope`, and a host that cannot enforce the kernel boundary unless the operator sets `GIT_UNBOUNDED`, which it logs. See [`crates/git/README.md`](../crates/git/README.md).

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

This workspace consumes the `omnia` runtime as published crates.io dependencies, declared once under `[workspace.dependencies]` in the workspace `Cargo.toml` (currently the 0.39.0 line). Each release of this repository pairs with one omnia line; see [RELEASES.md](../RELEASES.md).

## Configuration

Backends configure themselves from environment variables through omnia's `FromEnv` derive. A crate reaches the derive through `omnia`, so it names that root in `#[env(crate = omnia)]`:

```rust
use omnia::FromEnv;

#[derive(Debug, Clone, FromEnv)]
#[env(crate = omnia)]
pub struct ConnectOptions {
    #[env(from = "REDIS_URL", default = "redis://localhost:6379")]
    pub url: String,
    #[env(from = "REDIS_MAX_RETRIES", default = "3")]
    pub max_retries: usize,
}
```

The derive emits:

- `FromEnv`, which `Connect::connect()` calls;
- `with_env`, which applies the variables an operator sets over options built in code;
- `load_from`, which loads through a lookup in place of the environment;
- `Default`, where every field has a `default` or is an `Option`.

Each crate's README documents its variable set.

## Testing

CI cannot stand up the real services, so the testing policy (see [AGENTS.md](../AGENTS.md)) splits coverage:

- **Unit tests** cover pure, service-free translation logic (OData filter building, Postgres type mapping, the Kafka partitioner, cursor prompt build/parse). These run in CI.
- **Live tests** (`tests/live.rs`, `#[ignore]`-gated) drive the backend's `XxxClient` implementation against the real service. Run them locally:

```bash
cargo nextest run -p <crate> --run-ignored all   # with the service + credentials available
```

## Adding a New Backend

1. Create `crates/<name>/`
2. Implement the `Connect` trait with a `ConnectOptions` that derives `omnia::FromEnv` under `#[env(crate = omnia)]`
3. Implement the `XxxClient` contract trait(s) for the interfaces it serves
4. Add `#[ignore]`-gated live tests in `tests/live.rs` and document the run recipe in the crate README
5. Add example(s) under `examples/` if the backend benefits from an end-to-end demo

## Related Documentation

- [Omnia Architecture](https://github.com/augentic/omnia/blob/main/docs/Architecture.md) — the runtime this repo plugs into
- [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md) — wiring and configuration
- [wasmtime Component Model](https://docs.wasmtime.dev/api/wasmtime/component/)
- [WIT Format](https://component-model.bytecodealliance.org/design/wit.html)
