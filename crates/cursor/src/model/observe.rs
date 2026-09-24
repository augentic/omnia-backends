//! Stream parse plus completion telemetry.
//!
//! [`EventLog`] follows a run's `sdk_message` events to rebuild the tool
//! transcript and capture the run id and last status text. Payload shapes
//! mirror the public SDK — every field access is nullable and a malformed
//! event is skipped, never fatal. The result's token counts become the
//! guest's [`Usage`] here too. [`Completion`] emits the start (DEBUG) and
//! finish (INFO) events.

use std::collections::HashMap;

use omnia_wasi_model::{Format, ToolTurn, Transcript, Usage};
use serde_json::Value;
use tokio::time::Instant;

use crate::elapsed_ms;
use crate::failure::Outcome;
use crate::protocol::{RunStreamMessage, SdkMessage, TokenUsage};

/// One completion's start/finish events. Drop without [`Self::finish`]
/// records [`Outcome::Abort`] (a cancelled future).
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
    pub fn start(model: &str, format: &Format, prompt: &str, mcp_servers: usize) -> Self {
        let format = format.to_string();
        let prompt_bytes = len_u64(prompt.len());

        tracing::debug!(model, format, prompt_bytes, mcp = mcp_servers, "completion started");

        Self {
            model: model.to_owned(),
            format,
            prompt_bytes,
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

    // Count an attempt as started, including ones that later time out.
    pub const fn new_attempt(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    /// Snapshot the last successful send (result size, tools, tokens).
    pub fn record(&mut self, result_len: usize, tool_turns: usize, usage: Option<&Usage>) {
        self.result_bytes = len_u64(result_len);
        self.tool_turns = len_u64(tool_turns);

        if let Some(usage) = usage {
            self.input_tokens = u64::from(usage.input_tokens);
            self.output_tokens = u64::from(usage.output_tokens);
            self.reasoning_tokens = u64::from(usage.reasoning_tokens.unwrap_or(0));
        }
    }

    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    // The one INFO line per completion. It is emitted once, so a later
    // `Drop` says nothing.
    pub fn finish(&mut self, outcome: Outcome) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        tracing::info!(
            model = %self.model,
            format = %self.format,
            outcome = outcome.as_str(),
            attempts = self.attempts,
            duration_ms = elapsed_ms(self.started),
            prompt_bytes = self.prompt_bytes,
            result_bytes = self.result_bytes,
            tool_turns = self.tool_turns,
            input_tokens = self.input_tokens,
            output_tokens = self.output_tokens,
            reasoning_tokens = self.reasoning_tokens,
            "completion"
        );
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.finish(Outcome::Abort);
    }
}

/// Reconstructs the tool transcript and run metadata from the SDK stream.
#[derive(Default)]
pub struct EventLog {
    run_id: Option<String>,
    status_message: Option<String>,
    pending_tools: HashMap<String, PendingCall>,
    turns: Vec<ToolTurn>,
}

impl EventLog {
    /// Absorb one stream message: its event, and the run id its result names.
    pub fn observe_message(&mut self, message: &RunStreamMessage) {
        if let Some(event) = &message.sdk_message {
            self.observe(event);
        }
        if self.run_id.is_none()
            && let Some(result) = &message.result
            && !result.run_id.is_empty()
        {
            self.run_id = Some(result.run_id.clone());
        }
    }

    fn observe(&mut self, event: &SdkMessage) {
        let payload = &event.message;
        if self.run_id.is_none() {
            self.run_id = first_match(payload, &["run_id", "runId"]).map(ToOwned::to_owned);
        }

        match event.kind.as_str() {
            "tool_call" => self.tool_call(payload),
            "system" | "status" => {
                if let Some(message) = payload.get("message").and_then(Value::as_str) {
                    self.status_message = Some(message.to_owned());
                }
            }
            _ => {}
        }
    }

    /// The run id observed in the stream, for `CancelRun`.
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    /// The last `status`/`system` payload's message — the failure detail when
    /// a run ends in an error status.
    pub fn status_message(&self) -> Option<&str> {
        self.status_message.as_deref()
    }

    // The CLI stream spells the phase `subtype` (started/completed); the
    // SDK message type spells it `status` (running/completed/error).
    fn tool_call(&mut self, payload: &Value) {
        let phase = first_match(payload, &["subtype", "status"]);
        let call_id = first_match(payload, &["call_id", "callId", "tool_call_id", "toolCallId"]);
        let tool_call = payload.get("tool_call").or_else(|| payload.get("toolCall"));

        match phase {
            Some("started" | "running") => {
                if let (Some(call_id), Some(pending)) =
                    (call_id, tool_call.and_then(PendingCall::from_value))
                {
                    self.pending_tools.insert(call_id.to_owned(), pending);
                }
            }
            Some("completed" | "error") => {
                let Some(call_id) = call_id else {
                    return;
                };
                let Some(tool_call) = tool_call else {
                    return;
                };
                let PendingCall { tool, args } = self
                    .pending_tools
                    .remove(call_id)
                    .or_else(|| PendingCall::from_value(tool_call))
                    .unwrap_or_else(|| PendingCall {
                        tool: "unknown".to_owned(),
                        args: Value::Null,
                    });

                let result = tool_call
                    .as_object()
                    .and_then(|map| map.values().find_map(|value| value.get("result").cloned()))
                    .unwrap_or_default();

                self.turns.push(ToolTurn { tool, args, result });
            }
            _ => {}
        }
    }

    /// The reconstructed tool transcript, or `None` when no tool completed.
    pub fn finish(self) -> Option<Transcript> {
        if self.turns.is_empty() { None } else { Some(Transcript { turns: self.turns }) }
    }
}

/// A started tool call awaiting its completion event.
struct PendingCall {
    tool: String,
    args: Value,
}

impl PendingCall {
    fn from_value(tool_call: &Value) -> Option<Self> {
        tool_call.as_object()?.iter().find_map(|(key, value)| {
            let tool = key.strip_suffix("ToolCall")?;
            let args = value.get("args").cloned().unwrap_or_else(|| value.clone());
            Some(Self {
                tool: tool.to_owned(),
                args,
            })
        })
    }
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

// Find the first string under any of `keys`, tolerating both `snake_case`
// and `camelCase` spellings across `cursor-sdk-bridge` versions.
fn first_match<'a>(payload: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| payload.get(key).and_then(Value::as_str))
}

// Widen a byte or item count to an event field. `usize` never exceeds
// `u64` on a supported target, so nothing is lost.
fn len_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use omnia_wasi_model::Usage;
    use serde_json::{Value, json};

    use super::EventLog;
    use crate::protocol::{SdkMessage, TokenUsage};

    fn usage(input: i64, output: i64, reasoning: Option<i64>) -> Usage {
        Usage::from(TokenUsage {
            input_tokens: input,
            output_tokens: output,
            reasoning_tokens: reasoning,
        })
    }

    #[test]
    fn token_counts() {
        assert_eq!(
            usage(-1, -1, Some(-1)),
            Usage {
                input_tokens: 0,
                output_tokens: 0,
                reasoning_tokens: Some(0),
            }
        );
        let saturated = usage(i64::MAX, i64::MAX, Some(i64::MAX));
        assert_eq!(saturated.input_tokens, u32::MAX);
        assert_eq!(saturated.output_tokens, u32::MAX);
        assert_eq!(saturated.reasoning_tokens, Some(u32::MAX));
        assert_eq!(
            usage(7, 3, None),
            Usage {
                input_tokens: 7,
                output_tokens: 3,
                reasoning_tokens: None,
            }
        );
    }

    fn observe_all(events: &[Value]) -> EventLog {
        let mut log = EventLog::default();
        for event in events {
            let message: SdkMessage =
                serde_json::from_value(event.clone()).expect("test events are SdkMessages");
            log.observe(&message);
        }
        log
    }

    #[test]
    fn tool_calls() {
        let log = observe_all(&[
            json!({ "type": "system", "message": { "subtype": "init", "run_id": "r-1" } }),
            json!({ "type": "tool_call", "message": {
                "subtype": "started", "call_id": "c1",
                "tool_call": { "readToolCall": { "args": { "path": "README.md" } } },
            }}),
            json!({ "type": "tool_call", "message": {
                "subtype": "completed", "call_id": "c1",
                "tool_call": { "readToolCall": {
                    "args": { "path": "README.md" },
                    "result": { "success": { "content": "hi" } },
                }},
            }}),
        ]);
        assert_eq!(log.run_id(), Some("r-1"));
        let transcript = log.finish().expect("one completed tool turn");
        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(transcript.turns[0].tool, "read");
        assert_eq!(transcript.turns[0].args, json!({ "path": "README.md" }));
    }

    #[test]
    fn sdk_status_spelling() {
        let log = observe_all(&[
            json!({ "type": "tool_call", "message": {
                "status": "running", "toolCallId": "c1",
                "toolCall": { "lookupToolCall": { "args": { "q": "x" } } },
            }}),
            json!({ "type": "tool_call", "message": {
                "status": "completed", "toolCallId": "c1",
                "toolCall": { "lookupToolCall": { "args": { "q": "x" }, "result": { "hit": true } } },
            }}),
        ]);
        let transcript = log.finish().expect("the camelCase spelling still yields a turn");
        assert_eq!(transcript.turns[0].tool, "lookup");
        assert_eq!(transcript.turns[0].result, json!({ "hit": true }));
    }

    #[test]
    fn status_message() {
        let log = observe_all(&[
            json!({ "type": "status", "message": { "runId": "r-2", "message": "model overloaded" } }),
        ]);
        assert_eq!(log.run_id(), Some("r-2"));
        assert_eq!(log.status_message(), Some("model overloaded"));
    }

    #[test]
    fn garbled_payloads() {
        let log = observe_all(&[
            json!({ "type": "assistant", "message": null }),
            json!({ "type": "thinking", "message": { "subtype": "delta", "text": null } }),
            json!({ "type": "tool_call", "message": { "subtype": "completed" } }),
        ]);
        assert!(log.finish().is_none(), "nothing usable, nothing recorded");
    }
}
