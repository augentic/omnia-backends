//! Shared helpers for the Redis package's integration tests.
#![expect(dead_code, reason = "each suite uses its own subset")]

pub mod fake_redis;
pub mod harness;

// `BACKEND_OMNIA_REDIS_COMPONENT`, the package the crate's build script
// compiled for `wasm32-wasip2`.
include!(concat!(env!("OUT_DIR"), "/gen.rs"));
