# OpenTelemetry Example

Guest spans and metrics reaching an external collector. The guest instruments one route through `tracing` and the OpenTelemetry API. The runtime wires `OtelDefault`, which sends the guest's telemetry to the collector the host's own goes to. [`compose.yaml`](compose.yaml) runs a collector that sends traces to Jaeger and converts the guest's delta series to cumulative before writing them to Prometheus.

## Prerequisites

- Docker with Compose 2.23.1 or later, for the collector configuration inlined in `compose.yaml`.

## Build and run

```bash
# start the collector, Jaeger, and Prometheus
docker compose -f examples/otel/compose.yaml up -d

# build the guest
cargo build -p examples --example otel-wasm --target wasm32-wasip3

# run the host
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export COMPONENT=otel
cargo run --example otel -- -v run ./target/wasm32-wasip3/debug/examples/otel_wasm.wasm
```

`COMPONENT` names the service, which is otherwise `examples`. `-v` raises the run's level to `info`, the level the spans are recorded at.

## Test

```bash
curl --header 'Content-Type: application/json' -d '{"text":"hello"}' http://localhost:8080
```

Send a few requests, then look for them:

- Jaeger, at <http://localhost:16686>, lists the `otel` service with one trace per request. The guest's `http_guest_handle` span sits under the host's `http-request` span, with `handler` below it and `parse` and `compose` below that.
- Prometheus, at <http://localhost:9090>, answers `{otel_scope_name="otel-example"}` with the guest's series: `requests_total`, `in_flight`, `name_length_bucket` with its `_sum` and `_count`, and `reply_size_bytes`. The counter and the histogram rise with each request. `in_flight` is back to 0 once a request ends, and `reply_size_bytes` holds the size of the last reply.
- `docker compose -f examples/otel/compose.yaml logs otelcol` shows a line for each batch the collector receives.

## Cleanup

```bash
docker compose -f examples/otel/compose.yaml down
```
