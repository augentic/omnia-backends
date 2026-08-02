# omnia-opentelemetry

[![crates.io](https://img.shields.io/crates/v/omnia-opentelemetry.svg)](https://crates.io/crates/omnia-opentelemetry)
[![docs.rs](https://docs.rs/omnia-opentelemetry/badge.svg)](https://docs.rs/omnia-opentelemetry)

OpenTelemetry gRPC backend for the Omnia WASI runtime, implementing the `wasi-otel` interface.

Exports traces and metrics to an OpenTelemetry Collector via gRPC using the OTLP protocol. Telemetry failures are logged but never propagated to application logic.

MSRV: Rust 1.97

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OTEL_GRPC_URL` | no | `http://localhost:4317` | Collector gRPC endpoint |

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_opentelemetry::Client as Otel;
use omnia_wasi_otel::WasiOtel;

omnia::runtime!({
    hosts: {
        WasiOtel: Otel,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Backend, FromEnv};
use omnia_opentelemetry::Client;

let options = omnia_opentelemetry::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-otel` boundary against a real
OTLP/gRPC collector. It is `#[ignore]`d so it never runs in CI; run it explicitly:

```bash
docker run -d --name otelcol -p 4317:4317 otel/opentelemetry-collector:latest

OTEL_GRPC_URL=http://localhost:4317 \
  cargo nextest run -p omnia-opentelemetry --run-ignored all
```

## License

MIT OR Apache-2.0
