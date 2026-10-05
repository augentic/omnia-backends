//! Cursor example runtime.
//!
//! Command mode drives the guest's `wasi:cli/run` export once and exits
//! with its status. See `README.md`.

cfg_select! {
    not(target_arch = "wasm32") => {
        use omnia_cursor::Client as Cursor;
        use omnia_wasi_model::WasiModel;
        use omnia_wasi_otel::{OtelDefault, WasiOtel};

        omnia::runtime!({
            mode: command,
            hosts: {
                WasiOtel: OtelDefault,
                WasiModel: Cursor,
            }
        });
    }
    _ => {
        fn main() {}
    }
}
