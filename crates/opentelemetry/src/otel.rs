use futures::FutureExt;
use omnia::FutureResult;
use omnia_wasi_otel::WasiOtelCtx;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use crate::Client;

// Export failures are logged, never propagated: telemetry must not fail the
// application logic it observes.
impl WasiOtelCtx for Client {
    fn export_traces(&self, request: ExportTraceServiceRequest) -> FutureResult<()> {
        let mut client = self.traces_client.clone();

        async move {
            if let Err(e) = client.export(request).await {
                tracing::error!("failed to send traces via gRPC: {e}");
            }
            Ok(())
        }
        .boxed()
    }

    fn export_metrics(&self, request: ExportMetricsServiceRequest) -> FutureResult<()> {
        let mut client = self.metrics_client.clone();

        async move {
            if let Err(e) = client.export(request).await {
                tracing::error!("failed to send metrics via gRPC: {e}");
            }
            Ok(())
        }
        .boxed()
    }
}
