//! Shared helpers for the genai backend's integration tests.
#![expect(dead_code, reason = "each suite uses its own subset")]

pub mod capture;
pub mod fake_openai;
pub mod harness;
