//! Shared helpers for the guest scenario programs in `programs/<capability>/`.

use omnia_sdk::model::{Function, Message, Role, Tool};
use schemars::JsonSchema;
use serde::Deserialize;

/// The typed answer the `check_*` scenarios ask for.
#[derive(Debug, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct Verdict {
    /// `pass` or `fail`.
    pub verdict: String,
    /// What was wrong, when `fail`.
    pub findings: Vec<String>,
}

impl Verdict {
    /// The check every `check_*` scenario applies: a `pass` with no findings.
    ///
    /// # Errors
    ///
    /// Returns the findings a candidate must resolve.
    pub fn passing(&self) -> Result<(), Vec<String>> {
        let mut findings = Vec::new();
        if self.verdict != "pass" {
            findings.push("verdict must be `pass`".to_owned());
        }
        if !self.findings.is_empty() {
            findings.push("findings must be empty".to_owned());
        }
        if findings.is_empty() { Ok(()) } else { Err(findings) }
    }
}

/// One user chat turn.
#[must_use]
pub fn user(content: &str) -> Message {
    Message {
        role: Role::User,
        content: content.to_owned(),
    }
}

/// A declared `lookup` function tool.
#[must_use]
pub fn lookup() -> Tool {
    Tool::Function(
        Function::builder().name("lookup").description("test lookup").parameters("{}").build(),
    )
}

/// The operator arguments the deployment passed, `argv[0]` excluded.
#[must_use]
pub fn arguments() -> Vec<String> {
    omnia_sdk::wasip3::cli::environment::get_arguments().into_iter().skip(1).collect()
}

/// How many completions a fan-out scenario holds pending at once: the first
/// operator argument.
///
/// # Panics
///
/// Panics when the argument is missing or not a number.
#[must_use]
pub fn fanout_width() -> usize {
    arguments()
        .first()
        .expect("the fan-out width is the first argument")
        .parse()
        .expect("the fan-out width is a number")
}

/// The user turn — and the answer an echoing backend gives — that names
/// completion `index` of a fan-out.
#[must_use]
pub fn seam(index: usize) -> String {
    format!("seam {index}")
}
