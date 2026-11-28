//! Shared helpers for cursor backend integration tests.

#![allow(dead_code, unused_imports, reason = "each suite uses its own subset")]

pub mod fake_bridge;
pub mod harness;
pub mod tool_host;

pub use tool_host::{
    CHECK_WORD, TOOL_SENTINEL, checking_tool_host, lending_tool_host, no_tool_host,
};
