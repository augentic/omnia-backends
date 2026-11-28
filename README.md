# Omnia Backends

Production backend implementations for the [Omnia](https://github.com/augentic/omnia) WASI runtime. Each crate bridges a WASI interface to a concrete service (database, message broker, vault, model provider, etc.), so the same guest `.wasm` that runs against Omnia's in-memory defaults runs unchanged against real infrastructure.

> [!WARNING]
> **Pre-release, pre-hardening software.** These backends are usable today, but they are still under active development alongside Omnia. Expect breaking changes and check [`RELEASES.md`](RELEASES.md) before upgrading. *Production* here means a real service in place of an in-memory default, not a hardened release.
>
> Neither the backends nor the runtime has yet been hardened or independently audited for security, including the process sandbox in [`omnia-sandbox`](crates/sandbox) and the kernel-bounded `git` in [`omnia-git`](crates/git). Do not rely on any of them as the only isolation boundary around hostile code. Read Omnia's [security model](https://github.com/augentic/omnia/blob/main/docs/security-model.md) before running untrusted code.
>
> Report a vulnerability [privately through GitHub](https://github.com/augentic/omnia-backends/security/advisories/new), as the [security policy](https://github.com/augentic/.github/blob/main/SECURITY.md) describes. Until `1.0`, a confirmed vulnerability is tracked as a public `security` issue and fixed on `main`, with no published advisories, CVEs, or backports.

MSRV: Rust 1.100. Each release line pairs with a specific omnia line — the current 0.33.x line pairs with omnia 0.39.x (see [RELEASES.md](RELEASES.md)).

## Crates

| Crate                                         | WASI Interface                                      | Service                        |
| --------------------------------------------- | --------------------------------------------------- | ------------------------------ |
| [`omnia-azure-blob`](crates/azure-blob)       | `wasi-blobstore`                                    | Azure Blob Storage             |
| [`omnia-azure-id`](crates/azure-id)           | `wasi-identity`                                     | Azure Managed Identity         |
| [`omnia-azure-table`](crates/azure-table)     | `wasi-docstore`                                     | Azure Table Storage            |
| [`omnia-azure-vault`](crates/azure-vault)     | `wasi-vault`                                        | Azure Key Vault                |
| [`omnia-cursor`](crates/cursor)               | `wasi-model`                                        | `cursor-sdk-bridge`            |
| [`omnia-genai`](crates/genai)                 | `wasi-model`                                        | LLM provider APIs (OpenAI, Anthropic, Gemini, ...) |
| [`omnia-git`](crates/git)                     | `wasi-vcs`                                          | `git`                          |
| [`omnia-kafka`](crates/kafka)                 | `wasi-messaging`                                    | Apache Kafka                   |
| [`omnia-mongodb`](crates/mongodb)             | `wasi-blobstore`                                    | MongoDB                        |
| [`omnia-nats`](crates/nats)                   | `wasi-messaging`, `wasi-keyvalue`, `wasi-blobstore` | NATS / JetStream               |
| [`omnia-opentelemetry`](crates/opentelemetry) | `wasi-otel`                                         | Deprecated: wire omnia's `OtelDefault` |
| [`omnia-postgres`](crates/postgres)           | `wasi-sql`                                          | PostgreSQL                     |
| [`omnia-redis`](crates/redis)                 | `wasi-keyvalue`                                     | Redis                          |
| [`omnia-sandbox`](crates/sandbox)             | —                                                   | Seatbelt, Landlock and seccomp |

## Architecture

Every backend crate implements the `omnia::Connect` trait (connection management, configured from environment variables) plus the `XxxClient` contract trait for each WASI interface it serves. `omnia-sandbox` serves no interface: it is the library a backend that runs a program on the host (`omnia-cursor`) spawns it through, so the process runs in the lent workspace, with an environment built from nothing, behind the platform's kernel boundary, and reaches the network through a vetting proxy alone. `omnia-azure-blob` additionally implements `omnia_plugin::PackageStore` (the store omnia's registry acquirer reads before any registry and writes what it fetches to; registry acquisition itself lives in omnia, as does the local `FsStore`). Backends are wired into a host runtime through the `omnia::runtime!` macro and connect at startup. See [`docs/Architecture.md`](docs/Architecture.md) for details, and the Omnia repo's [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md) for wiring instructions and configuration.

## Testing

CI runs only pure, service-free unit tests. Each crate's real-service coverage is an `#[ignore]`-gated live test in `tests/live.rs`, run locally with the service and credentials available:

```bash
cargo nextest run -p <crate> --run-ignored all
```

Each crate's README documents its required environment variables.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

## Releases

See [RELEASES.md](RELEASES.md).
