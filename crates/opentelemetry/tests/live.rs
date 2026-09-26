//! Live export smoke test for the OpenTelemetry backend, driven through the
//! `omnia:otel` host boundary (`WasiOtelCtx`).
//!
//! `#[ignore]`d so it never dials a collector in CI. Run against a reachable
//! OTLP/gRPC collector (`OTEL_EXPORTER_OTLP_ENDPOINT`, default
//! `http://localhost:4317`):
//! `cargo nextest run -p omnia-opentelemetry --run-ignored all`.

use anyhow::Result;
use omnia::Backend;
use omnia_opentelemetry::Client;
use omnia_wasi_otel::WasiOtelCtx;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs an OTLP/gRPC collector (OTEL_EXPORTER_OTLP_ENDPOINT); run with --run-ignored"]
async fn exports_metrics() -> Result<()> {
    let client = <Client as Backend>::connect().await?;

    // An empty batch is a valid no-op export: it proves the request crosses the
    // boundary and the gRPC channel is accepted.
    client.export_metrics(ExportMetricsServiceRequest::default()).await?;
    Ok(())
}
