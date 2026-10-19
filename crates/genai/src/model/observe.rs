//! [`Completion`] emits the start (DEBUG) and finish events — the finish at
//! INFO for an answer, WARN for anything else. [`Failure`] lets
//! [`outcome_of`] classify backend errors by type instead of message text.

use std::time::{Duration, Instant};

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
    model_time: Duration,
    tool_time: Duration,
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
            model_time: Duration::ZERO,
            tool_time: Duration::ZERO,
            emitted: false,
        }
    }
}

impl Completion {
    // Count a candidate answer as produced (tool rounds do not count).
    pub const fn attempt(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    // Time spent waiting on the provider, summed over the rounds.
    pub const fn model_time(&mut self, elapsed: Duration) {
        self.model_time = self.model_time.saturating_add(elapsed);
    }

    // Time spent in tool rounds, the shell's commands among them.
    pub const fn tool_time(&mut self, elapsed: Duration) {
        self.tool_time = self.tool_time.saturating_add(elapsed);
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

    // The one line per completion: INFO for an answer, WARN for anything
    // else, so a bare `info` run sees failed or aborted completions too.
    // Consumes self so Drop does not emit a second time.
    pub fn finish(mut self, outcome: &'static str) {
        self.emit(outcome);
    }

    fn emit(&mut self, outcome: &'static str) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        let duration_ms = millis(self.started.elapsed());

        // an event's level is fixed at its callsite, so each level is its own event
        macro_rules! completion {
            ($level:ident) => {
                tracing::$level!(
                    outcome,
                    attempts = self.attempts,
                    duration_ms,
                    model_ms = millis(self.model_time),
                    tool_ms = millis(self.tool_time),
                    input_tokens = self.input_tokens,
                    output_tokens = self.output_tokens,
                    reasoning_tokens = self.reasoning_tokens,
                    "completion"
                )
            };
        }
        if matches!(outcome, "ok" | "corrected") {
            completion!(info);
        } else {
            completion!(warn);
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if !self.emitted {
            self.emit("abort");
        }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

// A completion failure this crate constructs. `outcome_of` downcasts this
// so the `outcome` field does not depend on message wording.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    // The configured round budget spent without the model producing a
    // final text answer.
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

// Error classification and failure display; completion events are exercised
// through the model e2e suite.
#[cfg(test)]
mod tests {
    use super::{Failure, outcome_of};

    #[test]
    fn classify_errors() {
        let exhausted: anyhow::Error = Failure::Exhausted { rounds: 128 }.into();
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
            Failure::Exhausted { rounds: 128 }.to_string(),
            "no answer after 128 model round-trips"
        );
    }
}
