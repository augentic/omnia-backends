//! Stream parse plus completion telemetry.
//!
//! [`EventLog`] follows a run's `sdk_message` events to rebuild the tool
//! transcript and capture the run id and last status text. Payload shapes
//! mirror the public SDK — every field access is nullable and a malformed
//! event is skipped, never fatal. [`Completion`] emits the start/finish INFO
//! lines and tracing-opentelemetry metric fields.

use std::collections::HashMap;
use std::time::Instant;

use omnia_wasi_model::{Format, ToolTurn, Transcript, Usage};
use serde_json::Value;

use crate::bridge::SdkMessage;
use crate::model::options::Turn;

/// Format kind used as a low-cardinality metric label.
pub const fn format_name(format: &Format) -> &'static str {
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
        let prompt_bytes = u64::try_from(turn.prompt.len()).unwrap_or(u64::MAX);

        tracing::info!(
            model = %turn.options.model.id,
            format,
            prompt_bytes,
            mcp = turn.options.mcp_servers.len(),
            "completion started"
        );

        Self {
            model: turn.options.model.id.clone(),
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
            histogram.cursor_completion_duration_ms = duration_ms,
            histogram.cursor_prompt_bytes = self.prompt_bytes,
            histogram.cursor_result_bytes = self.result_bytes,
            histogram.cursor_tool_turns = self.tool_turns,
            histogram.cursor_input_tokens = self.input_tokens,
            histogram.cursor_output_tokens = self.output_tokens,
            histogram.cursor_reasoning_tokens = self.reasoning_tokens,
            monotonic_counter.cursor_completions = 1_u64,
            monotonic_counter.cursor_repairs = u64::from(outcome == "repair"),
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

/// Classify a failed `complete` from the error strings this crate constructs.
pub fn outcome_of(error: &anyhow::Error) -> &'static str {
    let message = error.to_string();
    if message.contains("timed out after") {
        "timeout"
    } else if message.contains("inactive for") {
        "inactive"
    } else if message.contains("completion aborted:") {
        "abort"
    } else if message.contains("no valid answer after") {
        "invalid"
    } else {
        "error"
    }
}

/// The first string found under any of `keys`, tolerating both `snake_case`
/// and `camelCase` spellings across bridge versions.
fn string_field<'a>(payload: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| payload.get(key).and_then(Value::as_str))
}

/// A started tool call awaiting its completion event.
struct PendingCall {
    tool: String,
    args: Value,
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
    pub fn observe(&mut self, event: &SdkMessage) {
        let payload = &event.message;
        if self.run_id.is_none() {
            self.run_id = string_field(payload, &["run_id", "runId"]).map(ToOwned::to_owned);
        }

        match event.kind.as_str() {
            "tool_call" => self.tool_call(payload),
            "system" | "status" => {
                if let Some(message) = string_field(payload, &["message"]) {
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

    fn tool_call(&mut self, payload: &Value) {
        // The CLI stream spells the phase `subtype` (started/completed); the
        // SDK message type spells it `status` (running/completed/error).
        let phase = string_field(payload, &["subtype", "status"]);
        let call_id = string_field(payload, &["call_id", "callId", "tool_call_id", "toolCallId"]);
        let tool_call = payload.get("tool_call").or_else(|| payload.get("toolCall"));

        match phase {
            Some("started" | "running") => {
                if let (Some(call_id), Some(pending)) =
                    (call_id, tool_call.and_then(tool_call_identity))
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
                    .or_else(|| tool_call_identity(tool_call))
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

fn tool_call_identity(tool_call: &Value) -> Option<PendingCall> {
    tool_call.as_object()?.iter().find_map(|(key, value)| {
        let tool = key.strip_suffix("ToolCall")?;
        let args = value.get("args").cloned().unwrap_or_else(|| value.clone());
        Some(PendingCall {
            tool: tool.to_owned(),
            args,
        })
    })
}

// Deliberate unit tests: pure event-log logic (CI floor). The edge variants
// (mixed spellings, garbled payloads) cannot be induced deterministically
// from a real bridge; `tests/live.rs` is the acceptance gate proving a real
// run's stream parses end-to-end.
#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::EventLog;
    use crate::bridge::SdkMessage;

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
    fn tool_calls_rebuild_the_transcript() {
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
    fn sdk_status_spelling_is_understood() {
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
    fn status_message_is_kept_for_error_reporting() {
        let log = observe_all(&[
            json!({ "type": "status", "message": { "runId": "r-2", "message": "model overloaded" } }),
        ]);
        assert_eq!(log.run_id(), Some("r-2"));
        assert_eq!(log.status_message(), Some("model overloaded"));
    }

    #[test]
    fn garbled_payloads_are_skipped() {
        let log = observe_all(&[
            json!({ "type": "assistant", "message": null }),
            json!({ "type": "thinking", "message": { "subtype": "delta", "text": null } }),
            json!({ "type": "tool_call", "message": { "subtype": "completed" } }),
        ]);
        assert!(log.finish().is_none(), "nothing usable, nothing recorded");
    }

    #[test]
    fn outcome_of_classifies_crate_errors() {
        assert_eq!(
            super::outcome_of(&anyhow::anyhow!(
                "cursor run timed out after 600s (absolute cap exceeded while still active)"
            )),
            "timeout"
        );
        assert_eq!(
            super::outcome_of(&anyhow::anyhow!(
                "cursor run inactive for 120s (no stream events; inactivity limit 120s, \
                 absolute cap 600s)"
            )),
            "inactive"
        );
        assert_eq!(
            super::outcome_of(&anyhow::anyhow!("completion aborted: session closed")),
            "abort"
        );
        assert_eq!(
            super::outcome_of(&anyhow::anyhow!("no valid answer after 2 attempts: not json")),
            "invalid"
        );
        assert_eq!(super::outcome_of(&anyhow::anyhow!("bridge RPC failed")), "error");
    }
}
