# omnia-opentelemetry

[![crates.io](https://img.shields.io/crates/v/omnia-opentelemetry.svg)](https://crates.io/crates/omnia-opentelemetry)
[![docs.rs](https://docs.rs/omnia-opentelemetry/badge.svg)](https://docs.rs/omnia-opentelemetry)

**Deprecated.** Wire [`OtelDefault`](https://docs.rs/omnia-wasi-otel/latest/omnia_wasi_otel/struct.OtelDefault.html) from `omnia-wasi-otel` instead. It sends guest telemetry to the collector the omnia host's own telemetry goes to, as the host's `OTEL_EXPORTER_OTLP_*` variables configure it.

`Client` remains as `OtelDefault` under its old name:

- It dials no collector of its own, so with no `OTEL_EXPORTER_OTLP_ENDPOINT` set nothing is exported. It dialled `http://localhost:4317`.
- `ConnectOptions` is read and ignored, a `grpc_url` passed in code included.

MSRV: Rust 1.100

## Migration

Replace the backend in your host's `runtime!` map. The guest `.wasm` is untouched:

```rust,ignore
use omnia_wasi_otel::{OtelDefault, WasiOtel};

omnia::runtime!({
    hosts: {
        WasiOtel: OtelDefault,
    }
});
```

[`examples/otel`](../../examples/otel) runs an instrumented guest against a collector, Jaeger, and Prometheus.

## License

MIT OR Apache-2.0
