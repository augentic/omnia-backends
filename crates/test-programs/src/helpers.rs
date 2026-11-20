//! Shared helpers for the guest scenario programs in `programs/<capability>/`.

use std::fmt;

use omnia_sdk::model::{Message, Question, Tool};
use schemars::JsonSchema;
use serde::Deserialize;
use serde::de::DeserializeOwned;

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
    Message::user(content)
}

/// A declared `lookup` function tool.
#[must_use]
pub fn lookup() -> Tool {
    tool("lookup", "test lookup")
}

/// A declared function tool taking any arguments.
#[must_use]
pub fn tool(name: &str, description: &str) -> Tool {
    Tool {
        name: name.to_owned(),
        description: Some(description.to_owned()),
        input_schema: "{}".to_owned(),
    }
}

/// Assert `error` is the failure `code`, its description holding `needle`.
///
/// # Panics
///
/// Panics when the code differs or the description lacks the needle.
#[track_caller]
pub fn fails_with(error: impl Into<omnia_sdk::Error>, code: &str, needle: &str) {
    let error = error.into();
    assert!(
        error.code() == code && error.description().contains(needle),
        "expected `{code}` holding {needle:?}, got {error:?}"
    );
}

/// Assert `outcome` failed as `not-found`, naming exactly `name`.
///
/// # Panics
///
/// Panics when `outcome` succeeded, or failed another way.
#[track_caller]
pub fn not_found<T: fmt::Debug>(outcome: Result<T, impl Into<omnia_sdk::Error>>, name: &str) {
    let outcome: Result<T, omnia_sdk::Error> = outcome.map_err(Into::into);
    match outcome {
        Err(error) if error.code() == "not-found" && error.description() == name => {}
        outcome => panic!("expected `not-found` naming {name:?}, got {outcome:?}"),
    }
}

// The argument naming the workspace a scenario lends with its request.
const LEND: &str = "lend=";

/// The operator arguments the deployment passed, `argv[0]` and the `lend=`
/// argument excluded.
#[must_use]
pub fn arguments() -> Vec<String> {
    omnia_sdk::wasip3::cli::environment::get_arguments()
        .into_iter()
        .skip(1)
        .filter(|argument| !argument.starts_with(LEND))
        .collect()
}

/// The workspace a scenario lends with every request: the `lend=<path>`
/// argument, a path on the guest's preopens. `None` lends nothing.
#[must_use]
pub fn lend() -> Option<String> {
    omnia_sdk::wasip3::cli::environment::get_arguments()
        .into_iter()
        .find_map(|argument| argument.strip_prefix(LEND).map(ToOwned::to_owned))
}

/// `question` over the scenario's lend, when there is one.
#[must_use]
pub fn lent<T: JsonSchema + DeserializeOwned + Send>(question: Question<T>) -> Question<T> {
    match lend() {
        Some(lend) => question.workspace(lend),
        None => question,
    }
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
