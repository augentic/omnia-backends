//! [`Completion`] emits the start (DEBUG) and finish (INFO) events. [`Failure`]
//! lets [`outcome_of`] classify backend errors by type instead of message text.

use std::time::Instant;

use omnia_wasi_model::Usage;

use crate::model::options::Turn;

// One completion's start/finish events: the outcome and what it cost, on
// the `complete` span that names the model and format. Drop without
// `finish` records `outcome=abort` (a cancelled future).
pub struct Completion {
    started: Instant,
    attempts: u32,
    input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    emitted: bool,
}

impl From<&Turn> for Completion {
    // The start event: the clock runs from here.
    fn from(turn: &Turn) -> Self {
        tracing::debug!(prompt_bytes = turn.prompt_bytes, tools = turn.tools, "completion started");

        Self {
            started: Instant::now(),
            attempts: 0,
            input_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            emitted: false,
        }
    }
}

impl Completion {
    // Count a candidate answer as produced (tool rounds do not count).
    pub const fn attempt(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    // Note one answer round and add its tokens to the completion's bill.
    pub fn record(&mut self, result_len: usize, tool_turns: usize, usage: Option<&Usage>) {
        tracing::debug!(result_bytes = result_len, tool_turns, ?usage, "round answered");

        if let Some(usage) = usage {
            self.input_tokens += u64::from(usage.input_tokens);
            self.output_tokens += u64::from(usage.output_tokens);
            self.reasoning_tokens += u64::from(usage.reasoning_tokens.unwrap_or(0));
        }
    }

    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    // The one INFO line per completion. Consumes self so Drop does not emit
    // a second time.
    pub fn finish(mut self, outcome: &'static str) {
        self.emit(outcome);
    }

    fn emit(&mut self, outcome: &'static str) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        let duration_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        tracing::info!(
            outcome,
            attempts = self.attempts,
            duration_ms,
            input_tokens = self.input_tokens,
            output_tokens = self.output_tokens,
            reasoning_tokens = self.reasoning_tokens,
            "completion"
        );
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if !self.emitted {
            self.emit("abort");
        }
    }
}

// A completion failure this crate constructs. `outcome_of` downcasts this
// so the `outcome` field does not depend on message wording.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    // Round budget spent without the model producing a final text answer.
    #[error("no answer after {rounds} model round-trips")]
    Exhausted { rounds: usize },
}

impl Failure {
    pub const fn outcome(&self) -> &'static str {
        match self {
            Self::Exhausted { .. } => "exhausted",
        }
    }
}

// Classify a failed `complete`: a `Failure` by variant, the typed
// `budget-exhausted` a rejected check ends on, anything else `error`.
pub fn outcome_of(error: &anyhow::Error) -> &'static str {
    if let Some(failure) = error.downcast_ref::<Failure>() {
        return failure.outcome();
    }
    match error.downcast_ref::<omnia_wasi_model::Error>() {
        Some(omnia_wasi_model::Error::BudgetExhausted(_)) => "exhausted",
        _ => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::{Failure, outcome_of};

    #[test]
    fn classify_errors() {
        let exhausted: anyhow::Error = Failure::Exhausted { rounds: 8 }.into();
        assert_eq!(outcome_of(&exhausted), "exhausted");

        let rejected: anyhow::Error =
            omnia_wasi_model::Error::BudgetExhausted("say more".to_owned()).into();
        assert_eq!(outcome_of(&rejected), "exhausted");

        let other: anyhow::Error = omnia_wasi_model::Error::Backend("down".to_owned()).into();
        assert_eq!(outcome_of(&other), "error");

        assert_eq!(outcome_of(&anyhow::anyhow!("provider unreachable")), "error");
    }

    #[test]
    fn failure_budget() {
        assert_eq!(
            Failure::Exhausted { rounds: 8 }.to_string(),
            "no answer after 8 model round-trips"
        );
    }
}
