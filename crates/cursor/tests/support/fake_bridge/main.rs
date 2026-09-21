//! `fake-cursor-sdk-bridge`: the fake as the process `omnia_cursor::Client`
//! spawns per lease, reached through a `cursor-sdk-bridge` link on `PATH`.
//! Everything is in `mod.rs`, shared with the suites' side.

#![allow(dead_code, reason = "the suites' side of the module is unused here")]

#[path = "mod.rs"]
mod fake_bridge;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    fake_bridge::run_spawned(std::env::args().collect()).await;
}
