//! Completion telemetry and failure classification.
//!
//! [`Completion`] records lifecycle events and metrics. [`Failure`] lets
//! [`outcome_of`] classify backend errors by type instead of message text.

use std::time::Instant;

use omnia_wasi_model::Usage;

use crate::model::options::Turn;

// One completion's metric-bearing start/finish. Drop without [`Self::finish`]
// records `outcome=abort` (a cancelled future).
pub struct Completion {
    model: String,
    format: String,
    prompt_bytes: u64,
    started: Instant,
    attempts: u32,
    result_bytes: u64,
    tool_turns: u64,
    input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    emitted: bool,
}

impl Completion {
    // INFO that a completion is in flight (no metric prefixes — live tail).
    pub fn start(turn: &Turn) -> Self {
        let format = turn.format.to_string();

        tracing::info!(
            model = %turn.model,
            format = %format,
            prompt_bytes = turn.prompt_bytes,
            tools = turn.tools,
            "completion started"
        );

        Self {
            model: turn.model.clone(),
            format,
            prompt_bytes: turn.prompt_bytes,
            started: Instant::now(),
            attempts: 0,
            result_bytes: 0,
            tool_turns: 0,
            input_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            emitted: false,
        }
    }

    // Count a candidate answer as produced (tool rounds do not count).
    pub const fn new_attempt(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    /// Snapshot the last answer round (result size, tools, tokens).
    pub fn record(&mut self, result_len: usize, tool_turns: usize, usage: Option<&Usage>) {
        self.result_bytes = u64::try_from(result_len).unwrap_or(u64::MAX);
        self.tool_turns = u64::try_from(tool_turns).unwrap_or(u64::MAX);

        if let Some(usage) = usage {
            self.input_tokens = u64::from(usage.input_tokens);
            self.output_tokens = u64::from(usage.output_tokens);
            self.reasoning_tokens = u64::from(usage.reasoning_tokens.unwrap_or(0));
        }
    }

    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    // INFO + OTEL metric fields for this completion. Consumes self so Drop
    // does not emit a second time.
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
            model = %self.model,
            format = %self.format,
            outcome,
            attempts = self.attempts,
            histogram.genai_completion_duration_ms = duration_ms,
            histogram.genai_prompt_bytes = self.prompt_bytes,
            histogram.genai_result_bytes = self.result_bytes,
            histogram.genai_tool_turns = self.tool_turns,
            histogram.genai_input_tokens = self.input_tokens,
            histogram.genai_output_tokens = self.output_tokens,
            histogram.genai_reasoning_tokens = self.reasoning_tokens,
            monotonic_counter.genai_completions = 1_u64,
            monotonic_counter.genai_corrections = u64::from(outcome == "corrected"),
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

/// A completion failure this crate constructs. [`outcome_of`] downcasts this
/// so metric labels do not depend on message wording.
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

/// Classify a failed `complete`: a [`Failure`] by variant, the typed
/// `budget-exhausted` a rejected check ends on, anything else `error`.
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
