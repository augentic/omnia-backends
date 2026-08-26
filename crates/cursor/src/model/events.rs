//! Observe the `sdk_message` events of one run: rebuild the tool transcript,
//! coalesce thinking deltas for DEBUG logs, and capture the run id and last
//! status text. Payload shapes mirror the public SDK's message types — an
//! external, versioned protocol — so every field access is nullable and a
//! malformed event is skipped, never fatal.

use std::collections::HashMap;

use omnia_wasi_model::{ToolTurn, Transcript};
use serde_json::Value;

use crate::bridge::SdkMessage;

pub const PROMPT_PREVIEW_CHARS: usize = 500;
const TEXT_PREVIEW_CHARS: usize = 300;
/// Coalesced thinking blocks stay readable; flush when a turn grows past this.
const THINKING_PREVIEW_CHARS: usize = 2_000;

/// Compact JSON when parseable; otherwise collapse whitespace so a log field
/// stays one line.
fn single_line(text: &str) -> String {
    serde_json::from_str::<Value>(text.trim()).map_or_else(
        |_| text.split_whitespace().collect::<Vec<_>>().join(" "),
        |value| value.to_string(),
    )
}

/// The first `max` characters of `text` on one line, ellipsized when longer.
pub fn preview(text: &str, max: usize) -> String {
    let collapsed = single_line(text);
    let mut chars = collapsed.chars();
    let head: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() { format!("{head}…") } else { head }
}

pub fn is_noisy_tool(name: &str) -> bool {
    matches!(
        name,
        "read"
            | "write"
            | "shell"
            | "grep"
            | "glob"
            | "edit"
            | "delete"
            | "listDir"
            | "searchReplace"
            | "ls"
            | "SemSearch"
            | "ReadLints"
            | "AwaitShell"
            | "TodoWrite"
    )
}

fn args_summary(args: &Value) -> String {
    for key in ["path", "url", "query"] {
        if let Some(value) = args.get(key).and_then(Value::as_str) {
            return preview(value, TEXT_PREVIEW_CHARS);
        }
    }
    preview(&args.to_string(), TEXT_PREVIEW_CHARS)
}

/// Coalesces thinking deltas into turn-sized blocks for DEBUG logs.
#[derive(Default)]
struct ThinkingBuf(String);

impl ThinkingBuf {
    /// Absorb one thinking event; yields a block once it completes or the
    /// buffer overflows.
    fn push(&mut self, subtype: Option<&str>, text: &str) -> Option<String> {
        match subtype {
            Some("completed") => self.take(),
            Some("delta") | None => {
                if text.is_empty() {
                    return None;
                }
                self.0.push_str(text);
                if self.0.chars().count() >= THINKING_PREVIEW_CHARS {
                    return self.take();
                }
                None
            }
            // Full-payload subtypes (e.g. `extended`): one shot.
            _ => {
                if !text.is_empty() {
                    self.0.push_str(text);
                }
                self.take()
            }
        }
    }

    fn take(&mut self) -> Option<String> {
        if self.0.is_empty() { None } else { Some(std::mem::take(&mut self.0)) }
    }
}

fn log_thinking(text: &str) {
    tracing::debug!(text = %preview(text, THINKING_PREVIEW_CHARS), "thinking");
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

#[derive(Default)]
pub struct EventLog {
    run_id: Option<String>,
    status_message: Option<String>,
    pending_tools: HashMap<String, PendingCall>,
    turns: Vec<ToolTurn>,
    thinking: ThinkingBuf,
}

impl EventLog {
    pub fn observe(&mut self, event: &SdkMessage) {
        let payload = &event.message;
        if self.run_id.is_none() {
            self.run_id = string_field(payload, &["run_id", "runId"]).map(ToOwned::to_owned);
        }

        match event.kind.as_str() {
            "assistant" => {
                self.flush_thinking();
                let text = assistant_text(payload);
                if !text.is_empty() {
                    tracing::debug!(text = %preview(&text, TEXT_PREVIEW_CHARS), "assistant text");
                }
            }
            "thinking" => {
                let subtype = string_field(payload, &["subtype"]);
                let text = string_field(payload, &["text"]).unwrap_or_default();
                if let Some(block) = self.thinking.push(subtype, text) {
                    log_thinking(&block);
                }
            }
            "tool_call" => {
                self.flush_thinking();
                self.tool_call(payload);
            }
            "system" | "status" => {
                if let Some(message) = string_field(payload, &["message"]) {
                    self.status_message = Some(message.to_owned());
                }
            }
            other => {
                tracing::trace!(kind = other, "other sdk message");
            }
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

    fn flush_thinking(&mut self) {
        if let Some(text) = self.thinking.take() {
            log_thinking(&text);
        }
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
                    if is_noisy_tool(&pending.tool) {
                        tracing::trace!(%call_id, tool = %pending.tool, "tool call started");
                    }
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

                if is_noisy_tool(&tool) {
                    tracing::trace!(%call_id, %tool, "tool call completed");
                } else {
                    tracing::debug!(%tool, args = %args_summary(&args), "tool");
                }

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
    pub fn finish(mut self) -> Option<Transcript> {
        self.flush_thinking();
        if self.turns.is_empty() { None } else { Some(Transcript { turns: self.turns }) }
    }
}

fn assistant_text(payload: &Value) -> String {
    let content = payload
        .get("message")
        .and_then(|message| message.get("content"))
        .or_else(|| payload.get("content"));
    let Some(parts) = content.and_then(Value::as_array) else {
        return String::new();
    };
    parts.iter().filter_map(|part| part.get("text").and_then(Value::as_str)).collect()
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
// (thinking deltas, mixed spellings, garbled payloads) cannot be induced
// deterministically from a real bridge; `tests/live.rs` is the acceptance
// gate proving a real run's stream parses end-to-end.
#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{EventLog, ThinkingBuf, preview, single_line};
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
    fn single_line_compacts_json() {
        let pretty = "{\n  \"verdict\": \"pass\"\n}";
        assert_eq!(single_line(pretty), r#"{"verdict":"pass"}"#);
    }

    #[test]
    fn single_line_collapses_non_json() {
        assert_eq!(single_line("hello\n  world"), "hello world");
    }

    #[test]
    fn preview_appends_ellipsis() {
        assert_eq!(preview("abcdef", 3), "abc…");
        assert_eq!(preview("ab", 3), "ab");
    }

    #[test]
    fn thinking_buf_coalesces_deltas() {
        let mut buf = ThinkingBuf::default();
        assert!(buf.push(Some("delta"), "line 22, the canc").is_none());
        assert!(buf.push(Some("delta"), "ellation constraint").is_none());
        assert_eq!(
            buf.push(Some("completed"), "").as_deref(),
            Some("line 22, the cancellation constraint")
        );
        assert!(buf.take().is_none(), "completed clears the buffer");
    }

    #[test]
    fn thinking_buf_extended_is_one_shot() {
        let mut buf = ThinkingBuf::default();
        assert_eq!(
            buf.push(Some("extended"), "weighing the verdict").as_deref(),
            Some("weighing the verdict")
        );
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
}
