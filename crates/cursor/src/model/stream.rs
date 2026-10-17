//! What a run's stream says, read frame by frame.
//!
//! [`EventLog`] follows a run's `sdk_message` events to rebuild the tool
//! transcript, capture the run id and last status text, and name the
//! [`Phase`] the run is in — what a silent stream is waiting on. Payload
//! shapes mirror the public SDK in both its spellings (the nested
//! `tool_call.<name>ToolCall` of the CLI stream and the flat `name` + `args`
//! of `cursor-sdk-bridge`) — every field access is nullable and a malformed
//! event is skipped, never fatal.

use std::collections::HashMap;
use std::time::Duration;
use std::{fmt, mem};

use omnia_wasi_model::{ToolTurn, Transcript};
use serde_json::Value;

use crate::protocol::{RunStreamMessage, SdkMessage, lenient_i64};

// Reconstructs the tool transcript and run metadata from the SDK stream,
// and reports each tool call as the agent starts and completes it.
#[derive(Default)]
pub struct EventLog {
    // the agent's working directory, which its tools spell paths under
    cwd: Option<String>,
    run_id: Option<String>,
    status_message: Option<String>,
    frames: u64,
    started: usize,
    pending_tools: HashMap<String, PendingCall>,
    // the started call most recently left pending, for `Phase::Tool`
    last_pending: Option<String>,
    // reasoning time closing frames have reported since the last `thought`
    thinking: Duration,
    turns: Vec<ToolTurn>,
}

impl EventLog {
    pub fn new(cwd: String) -> Self {
        Self {
            cwd: Some(cwd),
            ..Self::default()
        }
    }

    pub fn observe_message(&mut self, message: &RunStreamMessage) {
        self.frames += 1;
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
            // a reasoning block ends with its duration; the text deltas before it stay frames
            "thinking" => {
                self.resumed();
                match thinking_duration(payload) {
                    Some(duration_ms) => {
                        self.thinking =
                            self.thinking.saturating_add(Duration::from_millis(duration_ms));
                        tracing::debug!(duration_ms, "thinking");
                    }
                    None => frame(event),
                }
            }
            "assistant" => {
                self.resumed();
                frame(event);
            }
            _ => frame(event),
        }
    }

    // The model speaks only once every tool call it issued has answered, so
    // a call still pending when it does was answered off the stream: the
    // bridge drops a terminal frame now and then, most often for one of
    // several calls started together. Its turn is lost; the phase is not.
    fn resumed(&mut self) {
        if self.pending_tools.is_empty() {
            return;
        }
        let unterminated: Vec<String> =
            self.pending_tools.drain().map(|(_, pending)| pending.tool).collect();
        self.last_pending = None;
        tracing::debug!(
            tools = ?unterminated,
            "the model resumed with tool calls still pending; closing them without a result"
        );
    }

    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    // The last `status`/`system` payload's message — the failure detail when
    // a run ends in an error status.
    pub fn status_message(&self) -> Option<&str> {
        self.status_message.as_deref()
    }

    // How many stream messages the run has produced, keepalives aside.
    pub const fn frames(&self) -> u64 {
        self.frames
    }

    // How many tool calls the agent has started, completed or not.
    pub const fn tool_calls(&self) -> usize {
        self.started
    }

    // The reasoning time reported since the last take, for the completion to book.
    pub fn thought(&mut self) -> Duration {
        mem::take(&mut self.thinking)
    }

    pub fn phase(&self) -> Phase {
        if self.frames == 0 {
            return Phase::Opening;
        }
        let pending = self
            .last_pending
            .as_deref()
            .and_then(|call_id| self.pending_tools.get(call_id))
            .or_else(|| self.pending_tools.values().next());
        pending.map_or(Phase::Model, PendingCall::phase)
    }

    // The CLI stream spells the phase `subtype` (started/completed); the
    // SDK message type spells it `status` (running/completed/error).
    fn tool_call(&mut self, payload: &Value) {
        let phase = first_match(payload, &["subtype", "status"]);
        let call_id = first_match(payload, &["call_id", "callId", "tool_call_id", "toolCallId"]);
        let nested = payload.get("tool_call").or_else(|| payload.get("toolCall"));
        let call = || nested.and_then(PendingCall::nested).or_else(|| PendingCall::flat(payload));

        match phase {
            Some("started" | "running") => {
                if let (Some(call_id), Some(pending)) = (call_id, call()) {
                    self.started += 1;
                    tracing::debug!(
                        tool = %pending.tool,
                        subject = pending.subject(self.cwd.as_deref()),
                        "tool call"
                    );

                    // the arguments whole, under a target of their own so a filter can
                    // open them without the stream's frames
                    tracing::trace!(
                        target: "omnia_cursor::tools",
                        tool = %pending.tool,
                        args = %pending.args,
                        "tool call args"
                    );

                    self.pending_tools.insert(call_id.to_owned(), pending);
                    self.last_pending = Some(call_id.to_owned());
                }
            }
            Some("completed" | "error") => {
                let Some(call_id) = call_id else {
                    return;
                };

                // a terminal frame ends the call with or without a result, or the phase stays on the tool
                let known = self.pending_tools.remove(call_id);
                let Some(result) = nested
                    .and_then(|tool_call| tool_call.as_object())
                    .and_then(|map| map.values().find_map(|value| value.get("result").cloned()))
                    .or_else(|| payload.get("result").cloned())
                else {
                    return;
                };
                let pending = known.or_else(call).unwrap_or_else(|| PendingCall {
                    tool: "unknown".to_owned(),
                    args: Value::Null,
                    shell: false,
                });

                tracing::debug!(
                    tool = %pending.tool,
                    subject = pending.subject(self.cwd.as_deref()),
                    result_bytes = result.to_string().len(),
                    "tool call completed"
                );
                let PendingCall { tool, args, .. } = pending;
                self.turns.push(ToolTurn { tool, args, result });
            }
            _ => {}
        }
    }

    pub fn finish(self) -> Option<Transcript> {
        if self.turns.is_empty() { None } else { Some(Transcript { turns: self.turns }) }
    }
}

// What a run's stream is waiting on, read off the frames so far: the
// stream has yet to carry a frame, a started tool call has yet to complete,
// the bridge's own shell is running a command, or the model has every
// result and is composing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Opening,
    Tool(String),
    Shell,
    Model,
}

impl Phase {
    // Whether the inactivity window bounds this phase: the bridge and the
    // tools it runs answer within it, while the model — whose text the
    // stream never carries — and a shell command — which the bridge bounds
    // by the call's own timeout — are bounded by the cap alone.
    pub const fn bounded(&self) -> bool {
        match self {
            Self::Opening | Self::Tool(_) => true,
            Self::Shell | Self::Model => false,
        }
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Opening => f.write_str("the opening frame"),
            Self::Tool(tool) => write!(f, "tool `{tool}`"),
            Self::Shell => f.write_str("the shell"),
            Self::Model => f.write_str("the model"),
        }
    }
}

// A started tool call awaiting its completion event.
struct PendingCall {
    tool: String,
    args: Value,
    // the bridge's own shell, not a guest tool of that name
    shell: bool,
}

impl PendingCall {
    // The bridge's shell, by the name it carries in the built-in position
    // of its frame. A guest tool rides under `mcp`, so one named `shell`
    // never reaches here.
    const SHELL: &str = "shell";

    // The CLI stream's shape: `tool_call: { <name>ToolCall: { args, result } }`.
    fn nested(tool_call: &Value) -> Option<Self> {
        tool_call.as_object()?.iter().find_map(|(key, value)| {
            let tool = key.strip_suffix("ToolCall")?;
            let args = value.get("args").cloned().unwrap_or_else(|| value.clone());
            Some(Self {
                tool: tool.to_owned(),
                args,
                shell: tool == Self::SHELL,
            })
        })
    }

    // `cursor-sdk-bridge`'s shape: `name` and `args` at the top of the
    // payload; a custom tool is `name: "mcp"` with its own name and
    // arguments under `args.toolName` and `args.args`.
    fn flat(payload: &Value) -> Option<Self> {
        let name = payload.get("name").and_then(Value::as_str)?;
        let args = payload.get("args").cloned().unwrap_or(Value::Null);
        let custom = args.get("toolName").and_then(Value::as_str).map(|tool| Self {
            tool: tool.to_owned(),
            args: args.get("args").cloned().unwrap_or(Value::Null),
            shell: false,
        });
        Some(custom.unwrap_or_else(|| Self {
            tool: name.to_owned(),
            args,
            shell: name == Self::SHELL,
        }))
    }

    fn phase(&self) -> Phase {
        if self.shell { Phase::Shell } else { Phase::Tool(self.tool.clone()) }
    }

    // The one argument worth a log line: a path, a pattern, or the first
    // line of a command, cut to a readable width. Every `<cwd>/` in it is
    // spelled `./`, in a command as in a path, so the width goes on the
    // part that tells one file from another and a command keeps its words.
    fn subject(&self, cwd: Option<&str>) -> Option<String> {
        const KEYS: &[&str] = &["path", "globPattern", "pattern", "command", "query", "url"];
        const WIDTH: usize = 80;
        let text = first_match(&self.args, KEYS)?;
        let line = text.lines().next().unwrap_or_default();
        let line = cwd
            .map(|cwd| cwd.trim_end_matches('/'))
            .filter(|root| !root.is_empty())
            .map_or_else(|| line.to_owned(), |root| line.replace(&format!("{root}/"), "./"));
        Some(match line.char_indices().nth(WIDTH) {
            Some((cut, _)) => format!("{}…", &line[..cut]),
            None => line,
        })
    }
}

// Any other frame, at TRACE with the head of its payload; the payload is
// rendered only when something would read it.
fn frame(event: &SdkMessage) {
    if !tracing::enabled!(tracing::Level::TRACE) {
        return;
    }
    let raw = event.message.to_string();
    let head: String = raw.chars().take(600).collect();
    tracing::trace!(kind = %event.kind, payload = %head, "frame");
}

// The `thinking_duration_ms` a reasoning block's closing frame carries, in
// either spelling and as a number or a string; a text delta carries none.
fn thinking_duration(payload: &Value) -> Option<u64> {
    ["thinking_duration_ms", "thinkingDurationMs"]
        .iter()
        .find_map(|key| payload.get(key).and_then(lenient_i64))
        .and_then(|duration| u64::try_from(duration).ok())
}

// Find the first string under any of `keys`, tolerating both `snake_case`
// and `camelCase` spellings across `cursor-sdk-bridge` versions.
fn first_match<'a>(payload: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| payload.get(key).and_then(Value::as_str))
}

// Stream parsing alone; a run's events reach the log through the fake in
// `tests/model.rs`.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Value, json};

    use super::{EventLog, Phase};
    use crate::protocol::RunStreamMessage;

    fn observe_all(events: &[Value]) -> EventLog {
        events.iter().fold(EventLog::default(), |mut log, event| {
            log.observe_message(&frame(event));
            log
        })
    }

    // The stream frame carrying one SDK event, as the bridge sends it.
    fn frame(event: &Value) -> RunStreamMessage {
        serde_json::from_value(json!({ "sdkMessage": event })).expect("test events are frames")
    }

    // `cursor-sdk-bridge`'s frame: the tool's name and arguments at the top
    // of the payload, and the result beside them once it completes.
    fn flat(name: &str, call_id: &str, args: &Value, result: Option<Value>) -> Value {
        let mut payload = json!({
            "agent_id": "a-1", "run_id": "run-1", "call_id": call_id,
            "name": name, "args": args,
            "status": if result.is_some() { "completed" } else { "running" },
        });
        if let Some(result) = result {
            payload["result"] = result;
        }
        json!({ "type": "tool_call", "message": payload })
    }

    // A terminal frame that names the call but carries no result.
    fn resultless(name: &str, call_id: &str, args: &Value, status: &str) -> Value {
        json!({ "type": "tool_call", "message": {
            "agent_id": "a-1", "run_id": "run-1", "call_id": call_id,
            "name": name, "args": args, "status": status,
        }})
    }

    #[test]
    fn bridge_frames() {
        let custom = json!({
            "providerIdentifier": "custom-user-tools", "toolName": "read_doc",
            "args": { "path": "references/ids.md" },
        });
        let glob = json!({ "globPattern": "**/*.ts" });
        let mut log = EventLog::new("/Users/me/project".to_owned());
        for event in [
            json!({ "type": "status", "message": { "run_id": "run-1", "status": "RUNNING" } }),
            json!({ "type": "thinking", "message": { "text": "", "thinking_duration_ms": 12 } }),
            flat("glob", "c1", &glob, None),
            flat(
                "glob",
                "c1",
                &glob,
                Some(json!({ "status": "success", "value": { "files": ["a.ts"] } })),
            ),
            flat("mcp", "c2", &custom, None),
            flat(
                "mcp",
                "c2",
                &custom,
                Some(json!({ "status": "success", "value": { "content": [] } })),
            ),
            flat("read", "c3", &json!({ "path": "/Users/me/project/src/a.ts" }), None),
            flat(
                "read",
                "c3",
                &json!({ "path": "/Users/me/project/src/a.ts" }),
                Some(json!({ "status": "success", "value": { "content": "" } })),
            ),
        ] {
            log.observe_message(&frame(&event));
        }
        assert_eq!(log.run_id(), Some("run-1"));
        assert_eq!(log.tool_calls(), 3);
        assert_eq!(log.frames(), 8, "a thinking frame is a frame, not a turn");
        let transcript = log.finish().expect("three completed tool turns");
        assert_eq!(transcript.turns.len(), 3);
        assert_eq!(
            transcript.turns[2].args,
            json!({ "path": "/Users/me/project/src/a.ts" }),
            "the transcript keeps the path as the agent spelled it"
        );
        assert_eq!(transcript.turns[0].tool, "glob");
        assert_eq!(transcript.turns[0].args, json!({ "globPattern": "**/*.ts" }));
        assert_eq!(transcript.turns[0].result["value"]["files"], json!(["a.ts"]));

        // a custom tool is named and argued by the call it wraps
        assert_eq!(transcript.turns[1].tool, "read_doc");
        assert_eq!(transcript.turns[1].args, json!({ "path": "references/ids.md" }));
    }

    #[test]
    fn phases() {
        let mut log = EventLog::default();
        assert_eq!(log.phase(), Phase::Opening);

        log.observe_message(&frame(
            &json!({ "type": "status", "message": { "status": "RUNNING" } }),
        ));
        assert_eq!(log.phase(), Phase::Model);

        let read = json!({ "path": "a.ts" });
        let grep = json!({ "pattern": "fn" });
        log.observe_message(&frame(&flat("read", "c1", &read, None)));
        log.observe_message(&frame(&flat("grep", "c2", &grep, None)));
        assert_eq!(log.phase(), Phase::Tool("grep".to_owned()), "the latest started call");

        let done = json!({ "status": "success", "value": {} });
        log.observe_message(&frame(&flat("grep", "c2", &grep, Some(done.clone()))));
        assert_eq!(log.phase(), Phase::Tool("read".to_owned()), "the one still pending");

        log.observe_message(&frame(&flat("read", "c1", &read, Some(done))));
        assert_eq!(log.phase(), Phase::Model);
        assert_eq!(log.frames(), 5);

        assert!(Phase::Opening.bounded());
        assert!(Phase::Tool("read".to_owned()).bounded());
        assert!(!Phase::Shell.bounded());
        assert!(!Phase::Model.bounded());
        assert_eq!(Phase::Tool("read".to_owned()).to_string(), "tool `read`");
    }

    // The bridge's shell is the one in the built-in position of a frame, in
    // either shape; a guest tool of the same name rides under `mcp` and is
    // a tool like any other, so a stall on it is the window's to end.
    #[test]
    fn shell_phase() {
        let mut log = EventLog::default();
        log.observe_message(&frame(&flat(
            "shell",
            "c1",
            &json!({ "command": "cargo test" }),
            None,
        )));
        assert_eq!(log.phase(), Phase::Shell);
        assert_eq!(log.phase().to_string(), "the shell");
        log.observe_message(&frame(&resultless("shell", "c1", &json!({}), "completed")));

        let custom = json!({ "providerIdentifier": "custom-user-tools", "toolName": "shell",
            "args": { "command": "cargo test" } });
        log.observe_message(&frame(&flat("mcp", "c2", &custom, None)));
        assert_eq!(log.phase(), Phase::Tool("shell".to_owned()));
        assert!(log.phase().bounded());
        log.observe_message(&frame(&resultless("mcp", "c2", &custom, "completed")));

        log.observe_message(&frame(&json!({ "type": "tool_call", "message": {
            "subtype": "started", "call_id": "c3",
            "tool_call": { "shellToolCall": { "args": { "command": "ls" } } },
        }})));
        assert_eq!(log.phase(), Phase::Shell);
    }

    // A terminal frame without a result still ends the call: the tool is
    // done, so the run waits on the model through the silent compose and the
    // inactivity window stands down instead of cutting it short.
    #[test]
    fn terminal_without_result() {
        let mut log = EventLog::default();
        let read = json!({ "path": "a.ts" });
        let grep = json!({ "pattern": "fn" });

        log.observe_message(&frame(&flat("read", "c1", &read, None)));
        log.observe_message(&frame(&resultless("read", "c1", &read, "completed")));
        assert_eq!(log.phase(), Phase::Model);

        log.observe_message(&frame(&flat("read", "c2", &read, None)));
        log.observe_message(&frame(&resultless("read", "c2", &read, "error")));
        assert_eq!(log.phase(), Phase::Model);

        log.observe_message(&frame(&flat("read", "c3", &read, None)));
        log.observe_message(&frame(&flat("grep", "c4", &grep, None)));
        log.observe_message(&frame(&resultless("read", "c3", &read, "completed")));
        assert_eq!(log.phase(), Phase::Tool("grep".to_owned()), "the one still pending");
        log.observe_message(&frame(&resultless("grep", "c4", &grep, "error")));
        assert_eq!(log.phase(), Phase::Model);

        log.observe_message(&frame(&json!({ "type": "tool_call", "message": {
            "subtype": "started", "call_id": "c5",
            "tool_call": { "readToolCall": { "args": &read } },
        }})));
        log.observe_message(&frame(&json!({ "type": "tool_call", "message": {
            "subtype": "completed", "call_id": "c5",
            "tool_call": { "readToolCall": { "args": &read } },
        }})));
        assert_eq!(log.phase(), Phase::Model);

        assert_eq!(log.tool_calls(), 5);
        assert!(log.finish().is_none(), "no result, no turn");
    }

    // Three reads started together, one terminal frame: the model's next
    // reasoning or text frame closes the two the bridge never terminated, so
    // the run waits on the model, not on a tool that has already answered.
    #[test]
    fn resumed_without_terminal() {
        let mut log = EventLog::default();
        let read = |file: &str| json!({ "path": file });
        let done = json!({ "status": "success", "value": {} });

        log.observe_message(&frame(&flat("read", "c1", &read("a.ts"), None)));
        log.observe_message(&frame(&flat("read", "c2", &read("b.ts"), None)));
        log.observe_message(&frame(&flat("read", "c3", &read("c.ts"), None)));
        log.observe_message(&frame(&flat("read", "c1", &read("a.ts"), Some(done))));
        assert_eq!(log.phase(), Phase::Tool("read".to_owned()), "two still pending");

        log.observe_message(&frame(&json!({ "type": "thinking", "message": { "text": "Now" } })));
        assert_eq!(log.phase(), Phase::Model, "the model resumed, so nothing is pending");

        log.observe_message(&frame(&flat("grep", "c4", &json!({ "pattern": "fn" }), None)));
        assert_eq!(log.phase(), Phase::Tool("grep".to_owned()), "a later call is pending again");
        log.observe_message(&frame(&json!({ "type": "assistant", "message": {
            "message": { "content": [{ "text": "The", "type": "text" }], "role": "assistant" },
        }})));
        assert_eq!(log.phase(), Phase::Model);

        assert_eq!(log.tool_calls(), 4);
        let transcript = log.finish().expect("the one terminated read is a turn");
        assert_eq!(transcript.turns.len(), 1, "a call closed without a result is no turn");
        assert_eq!(transcript.turns[0].args, read("a.ts"));
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
        assert_eq!(log.tool_calls(), 1);
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

    // The closing frame of a reasoning block carries the duration; the text
    // deltas before it carry none, and no thinking frame is a turn.
    #[test]
    fn thinking_frames() {
        assert_eq!(
            super::thinking_duration(&json!({ "text": "", "thinking_duration_ms": 4193 })),
            Some(4193)
        );
        assert_eq!(
            super::thinking_duration(&json!({ "text": "", "thinkingDurationMs": "4193" })),
            Some(4193),
            "an int64 spelled as a string, as the bridge spells its counts"
        );
        assert_eq!(super::thinking_duration(&json!({ "thinking_duration_ms": -1 })), None);
        assert_eq!(super::thinking_duration(&json!({ "text": "I will extract" })), None);

        let mut log = observe_all(&[
            json!({ "type": "thinking", "message": { "run_id": "run-1", "text": "I will" } }),
            json!({ "type": "thinking", "message": { "text": "", "thinking_duration_ms": 4193 } }),
            json!({ "type": "thinking", "message": { "text": "", "thinking_duration_ms": 7 } }),
        ]);
        assert_eq!(log.run_id(), Some("run-1"));
        assert_eq!(log.phase(), Phase::Model);

        // the closing frames' durations are taken once
        assert_eq!(log.thought(), Duration::from_millis(4200));
        assert_eq!(log.thought(), Duration::ZERO);
        assert!(log.finish().is_none());
    }

    // A path under the agent's `cwd` is shown relative to it, in a command's
    // words as in a path argument; anything else is shown as spelled, cut to
    // the width.
    #[test]
    fn subjects() {
        let cwd = "/Users/me/project";
        let subject = |args: Value, cwd: Option<&str>| {
            super::PendingCall {
                tool: "read".to_owned(),
                args,
                shell: false,
            }
            .subject(cwd)
        };
        assert_eq!(
            subject(json!({ "path": "/Users/me/project/src/orders.ts" }), Some(cwd)).as_deref(),
            Some("./src/orders.ts")
        );
        assert_eq!(
            subject(json!({ "command": "cd /Users/me/project/x && npm test" }), Some(cwd))
                .as_deref(),
            Some("cd ./x && npm test"),
            "a command keeps its words"
        );
        assert_eq!(
            subject(json!({ "command": "cp /Users/me/project/a /Users/me/project/b" }), Some(cwd))
                .as_deref(),
            Some("cp ./a ./b"),
            "every occurrence"
        );
        assert_eq!(
            subject(json!({ "path": "/Users/me/project" }), Some(cwd)).as_deref(),
            Some("/Users/me/project"),
            "the root itself is not cut to nothing"
        );
        assert_eq!(
            subject(json!({ "path": "/Users/me/projects/x.ts" }), Some(cwd)).as_deref(),
            Some("/Users/me/projects/x.ts"),
            "a sibling that shares the prefix is not a path beneath"
        );
        assert_eq!(
            subject(json!({ "path": "/Users/me/project/src/orders.ts" }), None).as_deref(),
            Some("/Users/me/project/src/orders.ts")
        );
        assert_eq!(
            subject(json!({ "command": "ls -la\nrm -rf /" }), Some(cwd)).as_deref(),
            Some("ls -la"),
            "the first line of a command"
        );
        let long = format!("{cwd}/{}", "a".repeat(100));
        let shown = subject(json!({ "path": long }), Some(cwd)).expect("a path");
        assert_eq!(shown.chars().count(), 81, "eighty characters and the ellipsis");
        assert!(shown.starts_with("./aaaa"), "the width is spent on the relative part: {shown}");
        let exact = "é".repeat(80);
        assert_eq!(
            subject(json!({ "path": exact.clone() }), None).as_deref(),
            Some(exact.as_str()),
            "a line of exactly the width is whole, counted in characters"
        );
        assert_eq!(subject(json!({ "other": 1 }), Some(cwd)), None);
    }
}
