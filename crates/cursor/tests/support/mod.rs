//! Shared helpers for cursor backend integration tests.

#![allow(dead_code, unused_imports, reason = "each suite uses its own subset")]

pub mod fake_bridge;
pub mod harness;
pub mod local_path_tool_host;
pub mod mcp_server;

pub use local_path_tool_host::{
    CHECK_WORD, TOOL_SENTINEL, checking_tool_host, local_path_tool_host, no_tool_host,
};
pub use mcp_server::{SENTINEL, serve};
