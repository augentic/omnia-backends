#![doc = include_str!("../README.md")]
#![expect(deprecated, reason = "the crate implements the deprecated items it exports")]

mod otel;

use anyhow::Result;
use omnia::{Connect, FromEnv};
use omnia_wasi_otel::OtelDefault;

/// The `omnia:otel` backend this crate provided, now [`OtelDefault`] under its
/// old name.
///
/// It sends guest telemetry to the collector the host's own telemetry goes
/// to, as the host's `OTEL_EXPORTER_OTLP_*` variables configure it. It dials
/// no collector of its own, so with no endpoint set nothing is exported.
#[deprecated(
    since = "0.33.0",
    note = "wire `omnia_wasi_otel::OtelDefault`, which sends guest telemetry to the host's collector"
)]
#[derive(Clone, Debug)]
pub struct Client(OtelDefault);

impl Connect for Client {
    type ConnectOptions = ConnectOptions;

    async fn connect_with(_options: Self::ConnectOptions) -> Result<Self> {
        OtelDefault::connect().await.map(Self)
    }
}

/// Connection options [`Client`] reads from the environment and ignores.
///
/// The host's `OTEL_EXPORTER_OTLP_*` variables say where guest telemetry goes.
#[deprecated(
    since = "0.33.0",
    note = "ignored: the host's `OTEL_EXPORTER_OTLP_*` variables say where guest telemetry goes"
)]
#[derive(Debug, Clone, FromEnv)]
#[env(crate = omnia)]
pub struct ConnectOptions {
    /// Ignored, since guest telemetry goes to the host's collector.
    #[env(from = "OTEL_EXPORTER_OTLP_ENDPOINT", default = "http://localhost:4317")]
    pub grpc_url: String,
}
