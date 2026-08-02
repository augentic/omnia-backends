# Omnia Backends

Production backend implementations for the [Omnia](https://github.com/augentic/omnia) WASI runtime. Each crate bridges a WASI interface to a concrete service (database, message broker, vault, model provider, etc.), so the same guest `.wasm` that runs against Omnia's in-memory defaults runs unchanged against real infrastructure.

MSRV: Rust 1.97. Each release line pairs with a specific omnia line — the current 0.29.x line pairs with omnia 0.35.x (see [RELEASES.md](RELEASES.md)).

## Crates

| Crate                                         | WASI Interface                                      | Service                        |
| --------------------------------------------- | --------------------------------------------------- | ------------------------------ |
| [`omnia-azure-blob`](crates/azure-blob)       | `wasi-blobstore`                                    | Azure Blob Storage             |
| [`omnia-azure-id`](crates/azure-id)           | `wasi-identity`                                     | Azure Managed Identity         |
| [`omnia-azure-table`](crates/azure-table)     | `wasi-docstore`                                     | Azure Table Storage            |
| [`omnia-azure-vault`](crates/azure-vault)     | `wasi-vault`                                        | Azure Key Vault                |
| [`omnia-cursor`](crates/cursor)               | `wasi-model`                                        | `cursor-agent` CLI             |
| [`omnia-genai`](crates/genai)                 | `wasi-model`                                        | LLM provider APIs (OpenAI, Anthropic, Gemini, ...) |
| [`omnia-kafka`](crates/kafka)                 | `wasi-messaging`                                    | Apache Kafka                   |
| [`omnia-mongodb`](crates/mongodb)             | `wasi-blobstore`                                    | MongoDB                        |
| [`omnia-nats`](crates/nats)                   | `wasi-messaging`, `wasi-keyvalue`, `wasi-blobstore` | NATS / JetStream               |
| [`omnia-opentelemetry`](crates/opentelemetry) | `wasi-otel`                                         | OpenTelemetry Collector (gRPC) |
| [`omnia-postgres`](crates/postgres)           | `wasi-sql`                                          | PostgreSQL                     |
| [`omnia-redis`](crates/redis)                 | `wasi-keyvalue`                                     | Redis                          |

## Architecture

Every crate implements the `omnia::Backend` trait (connection management, configured from environment variables) plus the `WasiXxxCtx` context trait for each WASI interface it serves. Backends are wired into a host runtime through the `omnia::runtime!` macro and connect at startup. See [`docs/Architecture.md`](docs/Architecture.md) for details, and the Omnia repo's [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md) for wiring instructions and configuration.

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
