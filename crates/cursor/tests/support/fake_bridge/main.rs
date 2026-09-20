//! `fake-cursor-sdk-bridge`: the fake as the process `omnia_cursor::Client`
//! spawns per lease. Everything is in `mod.rs`, shared with the in-process
//! mount the suites use for attach mode.

#![allow(dead_code, reason = "the in-process mount's API is the suites' side of the module")]

#[path = "mod.rs"]
mod fake_bridge;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    fake_bridge::run_spawned(std::env::args().collect()).await;
}
