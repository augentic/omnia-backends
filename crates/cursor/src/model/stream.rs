use std::collections::HashMap;

use anyhow::{Result, bail};
use omnia_wasi_model::{ToolTurn, Transcript};
use serde::Deserialize;
use serde_json::Value;

use super::AgentOutput;

const TEXT_PREVIEW_CHARS: usize = 300;
/// Coalesced thinking blocks stay readable; flush when a turn grows past this.
const THINKING_PREVIEW_CHARS: usize = 2_000;

impl AgentOutput {
    pub(super) fn log(&self, attempt: u32) {
        let (interesting_tools, noisy_tools) = self.tool_counts();
        tracing::debug!(
            attempt,
            result_len = self.result.len(),
            interesting_tools,
            noisy_tools,
            "cursor-agent answer"
        );
    }

    fn tool_counts(&self) -> (usize, usize) {
        let turns = self.transcript.as_ref().map_or(&[][..], |t| t.turns.as_slice());
        let noisy = turns.iter().filter(|turn| is_noisy_tool(&turn.tool)).count();
        (turns.len() - noisy, noisy)
    }
}

/// Compact JSON when parseable; otherwise collapse whitespace so a log field stays one line.
pub(super) fn single_line(text: &str) -> String {
    serde_json::from_str::<Value>(text.trim()).map_or_else(
        |_| text.split_whitespace().collect::<Vec<_>>().join(" "),
        |value| value.to_string(),
    )
}

pub(super) fn truncate(text: &str, max: usize) -> String {
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
pub(super) struct ThinkingBuf(String);

impl ThinkingBuf {
    pub(super) fn event(&mut self, subtype: Option<&str>, text: &str) -> Option<String> {
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

    pub(super) fn take(&mut self) -> Option<String> {
        if self.0.is_empty() { None } else { Some(std::mem::take(&mut self.0)) }
    }
}

fn log_thinking(text: &str) {
    tracing::debug!(text = %truncate(text, THINKING_PREVIEW_CHARS), "cursor-agent thinking");
}

#[derive(Default)]
pub(super) struct OutputParser {
    result: Option<String>,
    session_id: Option<String>,
    pending_tools: HashMap<String, (String, Value)>,
    turns: Vec<ToolTurn>,
    thinking: ThinkingBuf,
}

impl OutputParser {
    pub(super) fn line(&mut self, line: &str) -> Result<()> {
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
                    "skipping unparsable cursor-agent event"
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
                        "cursor-agent assistant text"
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
                    "cursor-agent other event"
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
                        tracing::trace!(subtype, %call_id, %tool, "cursor-agent tool call");
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
                    tracing::trace!(subtype, %call_id, %tool, "cursor-agent tool call");
                } else {
                    tracing::debug!(%tool, args = %args_summary(&args), "cursor-agent tool");
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

    pub(super) fn finish(mut self) -> Result<AgentOutput> {
        self.flush_thinking();
        let Some(result) = self.result else {
            bail!("cursor-agent did not emit a terminal result event");
        };
        let transcript =
            if self.turns.is_empty() { None } else { Some(Transcript { turns: self.turns }) };
        Ok(AgentOutput {
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
