use omnia::FutureResult;
use omnia_wasi_otel::OtelClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use crate::Client;

impl OtelClient for Client {
    fn export_traces(&self, request: ExportTraceServiceRequest) -> FutureResult<()> {
        self.0.export_traces(request)
    }

    fn export_metrics(&self, request: ExportMetricsServiceRequest) -> FutureResult<()> {
        self.0.export_metrics(request)
    }
}
