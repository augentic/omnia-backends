//! Compiles this crate to the `wasm32-wasip2` package the e2e suites pin and
//! generates `gen.rs` with its path constant, `BACKEND_OMNIA_REDIS_COMPONENT`,
//! for `tests/support` to `include!`. The driver guests come from the
//! `test-programs` crate.
//!
//! The nested build compiles this same package for `wasm32`, running this
//! script again; `Components` is a no-op under that target, so the recursion
//! stops there.

fn main() {
    omnia_test::build::Components::in_workspace("../..")
        .packages(["omnia-redis-component"])
        .group("backend")
        // Inputs outside the nested build's dep-info.
        .track(["Cargo.lock", "crates/redis-component/Cargo.toml"])
        .build()
        .write_gen("gen.rs");
}
