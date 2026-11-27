//! OpenTelemetry example runtime.
//!
//! Serves the guest over HTTP and sends the guest's spans and metrics,
//! with the host's own, to the collector at `OTEL_EXPORTER_OTLP_ENDPOINT`.
//! See `README.md`.

cfg_select! {
    not(target_arch = "wasm32") => {
        use omnia_wasi_http::{HttpDefault, WasiHttp};
        use omnia_wasi_otel::{OtelDefault, WasiOtel};

        omnia::runtime!({
            hosts: {
                WasiHttp: HttpDefault,
                WasiOtel: OtelDefault,
            }
        });
    }
    _ => {
        fn main() {}
    }
}
