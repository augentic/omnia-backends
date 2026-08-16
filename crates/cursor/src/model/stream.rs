use std::collections::HashMap;

use anyhow::{Result, bail};
use omnia_wasi_model::{ToolTurn, Transcript};
use serde::Deserialize;
use serde_json::Value;

use super::AgentOutput;

pub const PROMPT_PREVIEW_CHARS: usize = 500;
const TEXT_PREVIEW_CHARS: usize = 300;
/// Coalesced thinking blocks stay readable; flush when a turn grows past this.
const THINKING_PREVIEW_CHARS: usize = 2_000;

impl AgentOutput {
    pub fn log(&self, attempt: u32) {
        let (interesting_tools, noisy_tools) = self.tool_counts();
        tracing::debug!(
            attempt,
            result_len = self.result.len(),
            interesting_tools,
            noisy_tools,
            "answer"
        );
    }

    fn tool_counts(&self) -> (usize, usize) {
        let turns = self.transcript.as_ref().map_or(&[][..], |t| t.turns.as_slice());
        let noisy = turns.iter().filter(|turn| is_noisy_tool(&turn.tool)).count();
        (turns.len() - noisy, noisy)
    }
}

/// Compact JSON when parseable; otherwise collapse whitespace so a log field stays one line.
fn single_line(text: &str) -> String {
    serde_json::from_str::<Value>(text.trim()).map_or_else(
        |_| text.split_whitespace().collect::<Vec<_>>().join(" "),
        |value| value.to_string(),
    )
}

pub fn truncate(text: &str, max: usize) -> String {
    let collapsed = single_line(text);
    let mut chars = collapsed.chars();
    let head: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() { format!("{head}…") } else { head }
}

fn is_noisy_tool(name: &str) -> bool {
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
            return truncate(value, TEXT_PREVIEW_CHARS);
        }
    }
    truncate(&args.to_string(), TEXT_PREVIEW_CHARS)
}

// The subset of `cursor-agent` stream events the backend consumes. The fields
// stay nullable because this is an external, versioned protocol.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Event {
    System {
        session_id: Option<String>,
    },
    Result {
        is_error: Option<bool>,
        result: Option<String>,
        session_id: Option<String>,
    },
    ToolCall {
        subtype: String,
        call_id: Option<String>,
        tool_call: Option<Value>,
    },
    Assistant {
        message: Option<AssistantMessage>,
    },
    Thinking {
        subtype: Option<String>,
        text: Option<String>,
    },
    #[serde(other)]
    Other,
}

#[derive(Default, Deserialize)]
struct AssistantMessage {
    #[serde(default)]
    content: Vec<ContentPart>,
}

#[derive(Deserialize)]
struct ContentPart {
    text: Option<String>,
}

impl AssistantMessage {
    fn text(&self) -> String {
        self.content.iter().filter_map(|part| part.text.as_deref()).collect()
    }
}

/// Coalesces stream-json thinking deltas into turn-sized blocks for DEBUG logs.
#[derive(Default)]
struct ThinkingBuf(String);

impl ThinkingBuf {
    fn event(&mut self, subtype: Option<&str>, text: &str) -> Option<String> {
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
    tracing::debug!(text = %truncate(text, THINKING_PREVIEW_CHARS), "thinking");
}

#[derive(Default)]
pub struct OutputParser {
    result: Option<String>,
    session_id: Option<String>,
    pending_tools: HashMap<String, (String, Value)>,
    turns: Vec<ToolTurn>,
    thinking: ThinkingBuf,
}

impl OutputParser {
    pub fn line(&mut self, line: &str) -> Result<()> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(());
        }

        // One garbled line must not cost an otherwise-successful answer.
        let event = match serde_json::from_str::<Event>(line) {
            Ok(event) => event,
            Err(error) => {
                tracing::debug!(
                    %error,
                    line = %truncate(line, TEXT_PREVIEW_CHARS),
                    "skipping unparsable event"
                );
                return Ok(());
            }
        };

        match event {
            Event::System { session_id } => {
                self.session(session_id);
            }
            Event::Result {
                is_error,
                result,
                session_id,
            } => {
                self.flush_thinking();
                self.session(session_id);
                if is_error == Some(true) {
                    bail!(
                        "cursor-agent reported an error: {}",
                        result.as_deref().unwrap_or("<no detail>")
                    );
                }
                if result.is_some() {
                    self.result = result;
                }
            }
            Event::ToolCall {
                subtype,
                call_id,
                tool_call,
            } => {
                self.flush_thinking();
                self.tool_call(&subtype, call_id, tool_call);
            }
            Event::Assistant { message } => {
                self.flush_thinking();
                let text = message.as_ref().map(AssistantMessage::text).unwrap_or_default();
                if !text.is_empty() {
                    tracing::debug!(
                        text = %truncate(&text, TEXT_PREVIEW_CHARS),
                        "assistant text"
                    );
                }
            }
            Event::Thinking { subtype, text } => {
                if let Some(text) =
                    self.thinking.event(subtype.as_deref(), text.as_deref().unwrap_or_default())
                {
                    log_thinking(&text);
                }
            }
            Event::Other => {
                tracing::trace!(
                    line = %truncate(line, TEXT_PREVIEW_CHARS),
                    "other event"
                );
            }
        }
        Ok(())
    }

    fn flush_thinking(&mut self) {
        if let Some(text) = self.thinking.take() {
            log_thinking(&text);
        }
    }

    /// Keep the first `session_id` seen (the `init` event's; the terminal
    /// `result` event repeats it as a fallback).
    fn session(&mut self, session_id: Option<String>) {
        if self.session_id.is_none() {
            self.session_id = session_id;
        }
    }

    fn tool_call(&mut self, subtype: &str, call_id: Option<String>, tool_call: Option<Value>) {
        match subtype {
            "started" => {
                if let (Some(call_id), Some((tool, args))) =
                    (call_id, tool_call.as_ref().and_then(tool_call_identity))
                {
                    if is_noisy_tool(&tool) {
                        tracing::trace!(subtype, %call_id, %tool, "tool call");
                    }
                    self.pending_tools.insert(call_id, (tool, args));
                }
            }
            "completed" => {
                let Some(call_id) = call_id else {
                    return;
                };
                let Some(tool_call) = tool_call else {
                    return;
                };
                let (tool, args) = self
                    .pending_tools
                    .remove(&call_id)
                    .or_else(|| tool_call_identity(&tool_call))
                    .unwrap_or_else(|| ("unknown".to_owned(), Value::Null));

                if is_noisy_tool(&tool) {
                    tracing::trace!(subtype, %call_id, %tool, "tool call");
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

    /// The parsed output, or `None` when the stream ended without a terminal
    /// `result` event — the service dropped the session before the agent did
    /// any work, so the caller may cheaply re-spawn.
    pub fn finish(mut self) -> Option<AgentOutput> {
        self.flush_thinking();
        let result = self.result?;
        let transcript =
            if self.turns.is_empty() { None } else { Some(Transcript { turns: self.turns }) };
        Some(AgentOutput {
            result,
            transcript,
            session_id: self.session_id,
        })
    }
}

fn tool_call_identity(tool_call: &Value) -> Option<(String, Value)> {
    tool_call.as_object()?.iter().find_map(|(key, value)| {
        let tool = key.strip_suffix("ToolCall")?;
        let args = value.get("args").cloned().unwrap_or_else(|| value.clone());
        Some((tool.to_owned(), args))
    })
}

// Deliberate unit tests: pure stream-parse logic (CI floor). The edge variants
// (thinking deltas, session-id fallback, garbled lines) cannot be induced
// deterministically from a real agent; `tests/live.rs` is the acceptance gate
// proving a real cursor-agent stream parses end-to-end.
#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{AgentOutput, OutputParser, ThinkingBuf, single_line, truncate};

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
    fn truncate_appends_ellipsis() {
        assert_eq!(truncate("abcdef", 3), "abc…");
        assert_eq!(truncate("ab", 3), "ab");
    }

    #[test]
    fn thinking_buf_coalesces_deltas() {
        let mut buf = ThinkingBuf::default();
        assert!(buf.event(Some("delta"), "line 22, the canc").is_none());
        assert!(buf.event(Some("delta"), "ellation constraint").is_none());
        assert_eq!(
            buf.event(Some("completed"), "").as_deref(),
            Some("line 22, the cancellation constraint")
        );
        assert!(buf.take().is_none(), "completed clears the buffer");
    }

    #[test]
    fn thinking_buf_extended_is_one_shot() {
        let mut buf = ThinkingBuf::default();
        assert_eq!(
            buf.event(Some("extended"), "weighing the verdict").as_deref(),
            Some("weighing the verdict")
        );
    }

    #[test]
    fn thinking_buf_flushes_before_lost_tail() {
        let mut buf = ThinkingBuf::default();
        assert!(buf.event(Some("delta"), "partial thought").is_none());
        assert_eq!(buf.take().as_deref(), Some("partial thought"));
    }

    fn parse_output(stdout: &[u8]) -> anyhow::Result<Option<AgentOutput>> {
        let text = std::str::from_utf8(stdout).expect("test payloads are UTF-8");
        let mut parser = OutputParser::default();
        for line in text.lines() {
            parser.line(line)?;
        }
        Ok(parser.finish())
    }

    /// Parse a stream that must carry a terminal result event.
    fn parse_some(stdout: &[u8]) -> AgentOutput {
        parse_output(stdout).expect("parse stream").expect("stream has a result event")
    }

    #[test]
    fn parse_result_error() {
        let stdout = br#"{"type":"result","is_error":true,"result":"boom"}"#;
        let err = parse_output(stdout).expect_err("an agent error must surface");
        assert!(err.to_string().contains("cursor-agent reported an error"), "unexpected: {err}");
    }

    #[test]
    fn stream_without_result_event() {
        let stdout =
            br#"{"type":"system","subtype":"init","cwd":"/ws","session_id":"s","model":"m"}"#;
        let output = parse_output(stdout).expect("an empty spawn is not a parse error");
        assert!(output.is_none(), "no result event means no output: {output:?}");
    }

    #[test]
    fn nullable_event_fields_remain_compatible() {
        let stdout = br#"{"type":"assistant","message":null}
{"type":"thinking","subtype":"delta","text":null}
{"type":"result","is_error":null,"result":"ok"}"#;
        let output = parse_some(stdout);
        assert_eq!(output.result, "ok");
    }

    #[test]
    fn parse_stream_json() {
        let stdout = br#"{"type":"thinking","subtype":"extended","text":"weighing the verdict"}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"I'll read the README"}]}}
{"type":"tool_call","subtype":"started","call_id":"c1","tool_call":{"readToolCall":{"args":{"path":"README.md"}}}}
{"type":"tool_call","subtype":"completed","call_id":"c1","tool_call":{"readToolCall":{"args":{"path":"README.md"},"result":{"success":{"content":"hi"}}}}}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Deciding now"}]}}
{"type":"result","subtype":"success","is_error":false,"result":"{\"verdict\":\"pass\"}"}"#;
        let AgentOutput {
            result, transcript, ..
        } = parse_some(stdout);
        assert_eq!(result, r#"{"verdict":"pass"}"#);
        let transcript = transcript.expect("tool transcript");
        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(transcript.turns[0].tool, "read");
        assert_eq!(transcript.turns[0].args, json!({ "path": "README.md" }));
    }

    #[test]
    fn parse_session_id_from_init() {
        let stdout =
            br#"{"type":"system","subtype":"init","cwd":"/ws","session_id":"s-init","model":"m"}
{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s-later"}"#;
        let output = parse_some(stdout);
        assert_eq!(output.session_id.as_deref(), Some("s-init"), "the init event's id wins");
    }

    #[test]
    fn parse_session_id_from_result_fallback() {
        let stdout =
            br#"{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s-result"}"#;
        let output = parse_some(stdout);
        assert_eq!(output.session_id.as_deref(), Some("s-result"));
    }

    #[test]
    fn parse_without_session_id() {
        let stdout = br#"{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#;
        let output = parse_some(stdout);
        assert!(output.session_id.is_none());
    }

    #[test]
    fn parse_thinking_deltas_then_result() {
        let stdout = br#"{"type":"thinking","subtype":"delta","text":"line 22, the canc"}
{"type":"thinking","subtype":"delta","text":"ellation constraint"}
{"type":"thinking","subtype":"completed","text":""}
{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#;
        let AgentOutput {
            result, transcript, ..
        } = parse_some(stdout);
        assert_eq!(result, "ok");
        assert!(transcript.is_none());
    }

    #[test]
    fn assistant_prefix_reaches_result() {
        let stdout = br#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working on it"}]}}
{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#;
        let AgentOutput {
            result, transcript, ..
        } = parse_some(stdout);
        assert_eq!(result, "ok");
        assert!(transcript.is_none(), "no tool turns means no transcript");
    }

    #[test]
    fn skip_garbled_line() {
        let stdout =
            b"warning: not an event\n{\"type\":\"result\",\"is_error\":false,\"result\":\"ok\"}";
        let AgentOutput { result, .. } = parse_some(stdout);
        assert_eq!(result, "ok");
    }
}
