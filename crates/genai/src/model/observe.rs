//! Completion telemetry. [`Completion`] emits the start/finish INFO lines
//! and tracing-opentelemetry metric fields; [`Failure`] carries the
//! completion failures this crate constructs so [`outcome_of`] classifies
//! by type, not message wording.

use std::time::Instant;

use omnia_wasi_model::{Format, Usage};

use crate::model::options::Turn;

/// Format kind used as a low-cardinality metric label.
const fn format_name(format: &Format) -> &'static str {
    match format {
        Format::Text => "text",
        Format::Json => "json",
        Format::Schema(_) => "schema",
    }
}

/// One completion's metric-bearing start/finish. Drop without [`Self::finish`]
/// records `outcome=abort` (a cancelled future).
pub struct Completion {
    model: String,
    format: &'static str,
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
        let format = format_name(&turn.format);

        tracing::info!(
            model = %turn.model,
            format,
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

    // Count an answer-parse attempt as started (tool rounds do not count).
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
            format = self.format,
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
            monotonic_counter.genai_repairs = u64::from(outcome == "repair"),
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
#[derive(Debug)]
pub enum Failure {
    /// Round budget spent without the model producing a final text answer.
    Exhausted {
        /// The configured round budget.
        rounds: usize,
    },
    /// Format gate found no parseable answer on the final round.
    Invalid {
        /// The configured round budget.
        rounds: usize,
        /// Why the last answer was rejected.
        reason: String,
    },
}

impl Failure {
    pub const fn outcome(&self) -> &'static str {
        match self {
            Self::Exhausted { .. } => "exhausted",
            Self::Invalid { .. } => "invalid",
        }
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exhausted { rounds } => {
                write!(f, "no final answer after {rounds} model round-trips")
            }
            Self::Invalid { rounds, reason } => {
                write!(f, "no valid answer after {rounds} model round-trips: {reason}")
            }
        }
    }
}

impl std::error::Error for Failure {}

/// Classify a failed `complete` from a [`Failure`] when present.
pub fn outcome_of(error: &anyhow::Error) -> &'static str {
    error.downcast_ref::<Failure>().map_or("error", Failure::outcome)
}

// Deliberate unit tests: pure outcome classification (CI floor).
#[cfg(test)]
mod tests {
    use super::{Failure, outcome_of};

    #[test]
    fn classify_crate_errors() {
        let exhausted: anyhow::Error = Failure::Exhausted { rounds: 8 }.into();
        assert_eq!(outcome_of(&exhausted), "exhausted");

        let invalid: anyhow::Error = Failure::Invalid {
            rounds: 8,
            reason: "not json".to_owned(),
        }
        .into();
        assert_eq!(outcome_of(&invalid), "invalid");

        assert_eq!(outcome_of(&anyhow::anyhow!("provider unreachable")), "error");
    }

    #[test]
    fn failure_messages_name_the_budget() {
        assert_eq!(
            Failure::Exhausted { rounds: 8 }.to_string(),
            "no final answer after 8 model round-trips"
        );
        assert_eq!(
            Failure::Invalid {
                rounds: 8,
                reason: "not json".to_owned()
            }
            .to_string(),
            "no valid answer after 8 model round-trips: not json"
        );
    }
}
