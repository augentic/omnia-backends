//! Completion telemetry: the one line per completion, and its bill.
//!
//! [`Completion`] emits the start (DEBUG) and finish events — the finish at
//! INFO for an answer, WARN for anything else — carrying the outcome, the
//! bill, and how the wait split across the [`Phase`]s. The result's token
//! counts become the guest's [`Usage`] here too.

use std::time::Duration;

use omnia_wasi_model::Usage;
use tokio::time::Instant;

use super::options::Turn;
use super::stream::Phase;
use crate::elapsed_ms;
use crate::failure::Outcome;
use crate::protocol::TokenUsage;

// One completion's start/finish events: the outcome, what it cost, and
// where its time went, on the `complete` span that names the model and
// format. Drop without `finish` records `Outcome::Abort` (a cancelled
// future).
pub struct Completion {
    started: Instant,
    attempts: u32,
    // the guest's check rejected a candidate: an answer is then `Corrected`,
    // which a nudge — a send with no verdict behind it — never makes it
    corrected: bool,
    bill: Bill,
    // the wait booked per phase, every round of the completion together,
    // and the guest's check of each candidate between rounds
    opening: Duration,
    tool: Duration,
    model: Duration,
    check: Duration,
    // the reasoning time the model reported, a part of `model` the stream names
    thinking: Duration,
    emitted: bool,
}

impl From<&Turn> for Completion {
    fn from(turn: &Turn) -> Self {
        tracing::debug!(
            prompt_bytes = turn.prompt.text.len(),
            mcp = turn.agent.options.mcp_servers.len(),
            "completion started"
        );
        Self::new()
    }
}

impl Completion {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            attempts: 0,
            corrected: false,
            bill: Bill::default(),
            opening: Duration::ZERO,
            tool: Duration::ZERO,
            model: Duration::ZERO,
            check: Duration::ZERO,
            thinking: Duration::ZERO,
            emitted: false,
        }
    }

    // Count an attempt as started, including ones that later time out.
    pub const fn attempt(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    // The guest's check rejected a candidate and a correction follows.
    pub const fn rejected(&mut self) {
        self.corrected = true;
    }

    pub const fn corrected(&self) -> bool {
        self.corrected
    }

    pub fn record(&mut self, result_len: usize, tool_turns: usize, usage: Option<&TokenUsage>) {
        tracing::debug!(
            result_bytes = result_len,
            tool_turns,
            input_tokens = usage.map(|u| u.input_tokens),
            cache_read_tokens = usage.and_then(|u| u.cache_read_tokens),
            cache_write_tokens = usage.and_then(|u| u.cache_write_tokens),
            output_tokens = usage.map(|u| u.output_tokens),
            reasoning_tokens = usage.and_then(|u| u.reasoning_tokens),
            total_tokens = usage.and_then(|u| u.total_tokens),
            "send answered"
        );

        if let Some(usage) = usage {
            self.bill.add(&Bill::from(usage));
        }
    }

    // Raise the bill to the agent's total, read back after a run was cut
    // short: the total covers every send the agent answered too, so the
    // counts already booked from those are never added twice.
    pub fn settle(&mut self, total: &TokenUsage) {
        tracing::debug!(
            input_tokens = total.input_tokens,
            cache_read_tokens = total.cache_read_tokens,
            cache_write_tokens = total.cache_write_tokens,
            output_tokens = total.output_tokens,
            reasoning_tokens = total.reasoning_tokens,
            total_tokens = total.total_tokens,
            "usage settled for the cancelled run"
        );
        self.bill.raise_to(&Bill::from(total));
    }

    // Book a wait on `phase`: the bridge before its first frame — the
    // agent's creation included — a tool call outstanding, or the model
    // composing.
    pub const fn spent(&mut self, phase: &Phase, waited: Duration) {
        let bucket = match phase {
            Phase::Opening => &mut self.opening,
            Phase::Tool(_) | Phase::Shell => &mut self.tool,
            Phase::Model => &mut self.model,
        };
        *bucket = bucket.saturating_add(waited);
    }

    // Book the guest's check of a candidate, which no phase of the stream covers.
    pub const fn checked(&mut self, waited: Duration) {
        self.check = self.check.saturating_add(waited);
    }

    // Book the reasoning time a closing `thinking` frame reported.
    pub const fn thought(&mut self, reported: Duration) {
        self.thinking = self.thinking.saturating_add(reported);
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    // The one line per completion: INFO for an answer, WARN for anything
    // else, so a bare `info` run sees where its time went either way. It is
    // emitted once, so a later `Drop` says nothing.
    pub fn finish(&mut self, outcome: Outcome) {
        if self.emitted {
            return;
        }
        self.emitted = true;

        // an event's level is fixed at its callsite, so each level is its own event
        macro_rules! completion {
            ($level:ident) => {
                tracing::$level!(
                    outcome = outcome.as_str(),
                    attempts = self.attempts,
                    duration_ms = elapsed_ms(self.started),
                    opening_ms = millis(self.opening),
                    tool_ms = millis(self.tool),
                    model_ms = millis(self.model),
                    thinking_ms = millis(self.thinking),
                    check_ms = millis(self.check),
                    input_tokens = self.bill.input,
                    cache_read_tokens = self.bill.cache_read,
                    cache_write_tokens = self.bill.cache_write,
                    output_tokens = self.bill.output,
                    reasoning_tokens = self.bill.reasoning,
                    total_tokens = self.bill.total,
                    "completion"
                )
            };
        }
        if outcome.answered() { completion!(info) } else { completion!(warn) }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.finish(Outcome::Abort);
    }
}

// The tokens a completion has been billed so far, every send together. The
// total is the bridge's own sum, absent until a send reports one.
#[derive(Debug, Default, PartialEq, Eq)]
struct Bill {
    input: u64,
    cache_read: u64,
    cache_write: u64,
    output: u64,
    reasoning: u64,
    total: Option<u64>,
}

impl From<&TokenUsage> for Bill {
    fn from(usage: &TokenUsage) -> Self {
        Self {
            input: count(usage.input_tokens),
            cache_read: count(usage.cache_read_tokens.unwrap_or(0)),
            cache_write: count(usage.cache_write_tokens.unwrap_or(0)),
            output: count(usage.output_tokens),
            reasoning: count(usage.reasoning_tokens.unwrap_or(0)),
            total: usage.total_tokens.map(count),
        }
    }
}

impl Bill {
    const fn add(&mut self, other: &Self) {
        self.input = self.input.saturating_add(other.input);
        self.cache_read = self.cache_read.saturating_add(other.cache_read);
        self.cache_write = self.cache_write.saturating_add(other.cache_write);
        self.output = self.output.saturating_add(other.output);
        self.reasoning = self.reasoning.saturating_add(other.reasoning);
        self.total = match (self.total, other.total) {
            (None, None) => None,
            (Some(total), None) | (None, Some(total)) => Some(total),
            (Some(booked), Some(total)) => Some(booked.saturating_add(total)),
        };
    }

    fn raise_to(&mut self, total: &Self) {
        self.input = self.input.max(total.input);
        self.cache_read = self.cache_read.max(total.cache_read);
        self.cache_write = self.cache_write.max(total.cache_write);
        self.output = self.output.max(total.output);
        self.reasoning = self.reasoning.max(total.reasoning);
        self.total = self.total.max(total.total);
    }
}

// A wire count booked on the bill: negatives become 0.
fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl From<TokenUsage> for Usage {
    fn from(usage: TokenUsage) -> Self {
        Self {
            input_tokens: clamp_u32(usage.input_tokens),
            output_tokens: clamp_u32(usage.output_tokens),
            reasoning_tokens: usage.reasoning_tokens.map(clamp_u32),
        }
    }
}

// Wire counts are `i64`; negatives become 0, values above `u32::MAX` saturate.
fn clamp_u32(count: i64) -> u32 {
    if count.is_negative() { 0 } else { u32::try_from(count).unwrap_or(u32::MAX) }
}

// The completion's books and the token clamp alone; the events themselves
// are read off a capture in `tests/worker.rs`.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use omnia_wasi_model::Usage;

    use super::{Bill, Completion, Phase};
    use crate::failure::Outcome;
    use crate::protocol::TokenUsage;

    fn wire(input: i64, output: i64, reasoning: Option<i64>) -> TokenUsage {
        TokenUsage {
            input_tokens: input,
            output_tokens: output,
            reasoning_tokens: reasoning,
            ..TokenUsage::default()
        }
    }

    fn usage(input: i64, output: i64, reasoning: Option<i64>) -> Usage {
        Usage::from(wire(input, output, reasoning))
    }

    // Two sends add up; a settled total only raises what is already booked,
    // and a negative or absent wire count books nothing. The bridge's total
    // stays absent until a send reports one.
    #[test]
    fn bill() {
        let mut completion = Completion::new();
        completion.record(10, 0, Some(&wire(30, 10, None)));
        assert_eq!(completion.bill.total, None, "no send has reported a total");
        completion.record(
            10,
            1,
            Some(&TokenUsage {
                cache_read_tokens: Some(50),
                cache_write_tokens: Some(-1),
                total_tokens: Some(140),
                ..wire(70, 20, Some(15))
            }),
        );
        completion.record(10, 0, None);
        assert_eq!(
            completion.bill,
            Bill {
                input: 100,
                cache_read: 50,
                cache_write: 0,
                output: 30,
                reasoning: 15,
                total: Some(140),
            }
        );

        completion.settle(&TokenUsage {
            cache_read_tokens: Some(60),
            total_tokens: Some(190),
            ..wire(90, 40, Some(15))
        });
        assert_eq!(
            completion.bill,
            Bill {
                input: 100,
                cache_read: 60,
                cache_write: 0,
                output: 40,
                reasoning: 15,
                total: Some(190),
            },
            "the total raises a count, never lowers or doubles one"
        );
        completion.finish(Outcome::Ok);
    }

    // Every wait lands in its phase's bucket, tool calls together, the
    // guest's checks in their own, and the reported reasoning time in its own.
    #[test]
    fn phase_buckets() {
        let mut completion = Completion::new();
        completion.spent(&Phase::Opening, Duration::from_secs(2));
        completion.spent(&Phase::Tool("read".to_owned()), Duration::from_secs(3));
        completion.spent(&Phase::Model, Duration::from_secs(5));
        completion.spent(&Phase::Tool("grep".to_owned()), Duration::from_secs(1));
        completion.spent(&Phase::Opening, Duration::from_millis(500));
        completion.checked(Duration::from_millis(40));
        completion.checked(Duration::from_millis(60));
        completion.thought(Duration::from_millis(4200));
        completion.thought(Duration::from_millis(300));
        assert_eq!(completion.opening, Duration::from_millis(2500));
        assert_eq!(completion.tool, Duration::from_secs(4));
        assert_eq!(completion.model, Duration::from_secs(5));
        assert_eq!(completion.check, Duration::from_millis(100));
        assert_eq!(completion.thinking, Duration::from_millis(4500));
        completion.finish(Outcome::Ok);
    }

    #[test]
    fn token_counts() {
        let counts =
            |usage: Usage| (usage.input_tokens, usage.output_tokens, usage.reasoning_tokens);
        assert_eq!(counts(usage(-1, -1, Some(-1))), (0, 0, Some(0)));
        assert_eq!(
            counts(usage(i64::MAX, i64::MAX, Some(i64::MAX))),
            (u32::MAX, u32::MAX, Some(u32::MAX))
        );
        assert_eq!(counts(usage(7, 3, None)), (7, 3, None));
    }
}
