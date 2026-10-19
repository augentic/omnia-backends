//! Completion telemetry: the one line per completion, and its bill.
//!
//! [`Completion`] emits the start event at DEBUG and the finish event at
//! INFO for an answer, WARN for anything else, carrying the outcome, the
//! bill, and how the wait split between the provider and the tools.

use std::time::{Duration, Instant};

use anyhow::Result;
use omnia_wasi_model::{Answer, Error, Usage};

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

impl Completion {
    // The start event: the clock runs from here.
    pub fn start(prompt_bytes: u64, tools: usize) -> Self {
        tracing::debug!(prompt_bytes, tools, "completion started");

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

    // Time spent waiting on the provider, summed over the rounds.
    pub const fn model_time(&mut self, elapsed: Duration) {
        self.model_time = self.model_time.saturating_add(elapsed);
    }

    // Time spent in tool rounds, the shell's commands among them.
    pub const fn tool_time(&mut self, elapsed: Duration) {
        self.tool_time = self.tool_time.saturating_add(elapsed);
    }

    // One candidate answered, its tokens on the bill; a tool round is no
    // candidate.
    pub fn answered(&mut self, result_len: usize, tool_turns: usize, usage: Option<&Usage>) {
        self.attempts = self.attempts.saturating_add(1);
        tracing::debug!(result_bytes = result_len, tool_turns, ?usage, "round answered");

        if let Some(usage) = usage {
            self.input_tokens += u64::from(usage.input_tokens);
            self.output_tokens += u64::from(usage.output_tokens);
            self.reasoning_tokens += u64::from(usage.reasoning_tokens.unwrap_or(0));
        }
    }

    // Candidates answered so far.
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    // The one line per completion: INFO for an answer, WARN for anything
    // else, so a bare `info` run sees failed or aborted completions too.
    // Emitted once, so a later `Drop` says nothing.
    pub fn finish(&mut self, result: &Result<Answer>) {
        let outcome = match result {
            Ok(_) if self.attempts > 1 => "corrected",
            Ok(_) => "ok",
            Err(error) => outcome_of(error),
        };
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
        self.emit("abort");
    }
}

pub fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

// A failed `complete` by its class: the typed `budget-exhausted` — the
// rounds or the candidates spent — is `exhausted`, anything else `error`.
fn outcome_of(error: &anyhow::Error) -> &'static str {
    match error.downcast_ref::<Error>() {
        Some(Error::BudgetExhausted(_)) => "exhausted",
        _ => "error",
    }
}

// The outcome a failure is classed under; the events themselves are the
// `model` suite's, over the fake provider.
#[cfg(test)]
mod tests {
    use super::{Error, outcome_of};

    #[test]
    fn classify_errors() {
        let exhausted: anyhow::Error = Error::BudgetExhausted("say more".to_owned()).into();
        assert_eq!(outcome_of(&exhausted), "exhausted");

        let backend: anyhow::Error = Error::Backend("down".to_owned()).into();
        assert_eq!(outcome_of(&backend), "error");

        assert_eq!(outcome_of(&anyhow::anyhow!("provider unreachable")), "error");
    }
}
