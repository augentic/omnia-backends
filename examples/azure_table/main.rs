//! Azure Table document store desk test. See `README.md`.
//!
//! The wasm32-wasip2 lint pass builds every example in this package, so the
//! host-only program lives in `desk.rs` and this entry point is an empty
//! `main` on wasm32.

cfg_if::cfg_if! {
    if #[cfg(not(target_arch = "wasm32"))] {
        mod desk;
        use desk::main;
    } else {
        fn main() {}
    }
}
