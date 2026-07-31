#![doc = include_str!("../README.md")]

mod otel;

use std::fmt::Debug;

use anyhow::{Context, Result};
use omnia::Backend;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use tonic::transport::Channel;
use tracing::instrument;

/// OpenTelemetry gRPC backend client for exporting traces and metrics.
#[derive(Clone)]
pub struct Client {
    traces_client: TraceServiceClient<Channel>,
    metrics_client: MetricsServiceClient<Channel>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "OpenTelemetry::connect_with")]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        tracing::debug!("connecting to OpenTelemetry gRPC endpoint at: {}", options.grpc_url);

        let channel = Channel::from_shared(options.grpc_url)?
            .connect()
            .await
            .context("failed to connect to OpenTelemetry gRPC endpoint")?;

        let traces_client = TraceServiceClient::new(channel.clone());
        let metrics_client = MetricsServiceClient::new(channel);

        tracing::info!("connected to OpenTelemetry gRPC endpoint");
        Ok(Self {
            traces_client,
            metrics_client,
        })
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the OpenTelemetry backend.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// gRPC endpoint URL for the OpenTelemetry Collector.
        #[env(from = "OTEL_GRPC_URL", default = "http://localhost:4317")]
        pub grpc_url: String,
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
