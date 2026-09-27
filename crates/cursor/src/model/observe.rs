//! Stream parse plus completion telemetry.
//!
//! [`EventLog`] follows a run's `sdk_message` events to rebuild the tool
//! transcript, capture the run id and last status text, and name the
//! [`Phase`] the run is in — what a silent stream is waiting on. Payload
//! shapes mirror the public SDK in both its spellings (the nested
//! `tool_call.<name>ToolCall` of the CLI stream and the flat `name` + `args`
//! of `cursor-sdk-bridge`) — every field access is nullable and a malformed
//! event is skipped, never fatal. The result's token counts become the
//! guest's [`Usage`] here too. [`Completion`] emits the start (DEBUG) and
//! finish events — the finish at INFO for an answer, WARN for anything else.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use omnia_wasi_model::{ToolTurn, Transcript, Usage};
use serde_json::Value;
use tokio::time::Instant;

use super::options::Turn;
use crate::elapsed_ms;
use crate::failure::Outcome;
use crate::protocol::{RunStreamMessage, SdkMessage, TokenUsage};

// One completion's start/finish events: the outcome and what it cost, on
// the `complete` span that names the model and format. Drop without
// `finish` records `Outcome::Abort` (a cancelled future).
pub struct Completion {
    started: Instant,
    attempts: u32,
    input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    emitted: bool,
}

impl From<&Turn> for Completion {
    // The start event: the clock runs from here.
    fn from(turn: &Turn) -> Self {
        tracing::debug!(
            prompt_bytes = turn.prompt.text.len(),
            mcp = turn.agent.options.mcp_servers.len(),
            "completion started"
        );

        Self {
            started: Instant::now(),
            attempts: 0,
            input_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            emitted: false,
        }
    }
}

impl Completion {
    // Count an attempt as started, including ones that later time out.
    pub const fn attempt(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    // Note one answered send and add its tokens to the completion's bill.
    pub fn record(&mut self, result_len: usize, tool_turns: usize, usage: Option<&Usage>) {
        tracing::debug!(
            result_bytes = result_len,
            tool_turns,
            input_tokens = usage.map(|u| u.input_tokens),
            output_tokens = usage.map(|u| u.output_tokens),
            reasoning_tokens = usage.and_then(|u| u.reasoning_tokens),
            "send answered"
        );

        if let Some(usage) = usage {
            self.input_tokens += u64::from(usage.input_tokens);
            self.output_tokens += u64::from(usage.output_tokens);
            self.reasoning_tokens += u64::from(usage.reasoning_tokens.unwrap_or(0));
        }
    }

    // Raise the bill to the agent's total, read back after a run was cut
    // short: the total covers every send the agent answered too, so the
    // counts already booked from those are never added twice.
    pub fn settle(&mut self, total: &Usage) {
        tracing::debug!(
            input_tokens = total.input_tokens,
            output_tokens = total.output_tokens,
            reasoning_tokens = total.reasoning_tokens,
            "usage settled for the cancelled run"
        );
        self.input_tokens = self.input_tokens.max(u64::from(total.input_tokens));
        self.output_tokens = self.output_tokens.max(u64::from(total.output_tokens));
        self.reasoning_tokens =
            self.reasoning_tokens.max(u64::from(total.reasoning_tokens.unwrap_or(0)));
    }

    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    // How long the completion has run, across its attempts.
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

        // an event's level is fixed at its callsite, so each level is its own
        macro_rules! completion {
            ($level:ident) => {
                tracing::$level!(
                    outcome = outcome.as_str(),
                    attempts = self.attempts,
                    duration_ms = elapsed_ms(self.started),
                    input_tokens = self.input_tokens,
                    output_tokens = self.output_tokens,
                    reasoning_tokens = self.reasoning_tokens,
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

// Reconstructs the tool transcript and run metadata from the SDK stream,
// and reports each tool call as the agent starts and completes it.
#[derive(Default)]
pub struct EventLog {
    run_id: Option<String>,
    status_message: Option<String>,
    frames: u64,
    started: usize,
    pending_tools: HashMap<String, PendingCall>,
    // the started call most recently left pending, for `Phase::Tool`
    last_pending: Option<String>,
    turns: Vec<ToolTurn>,
}

impl EventLog {
    // Absorb one stream message: its event, and the run id its result names.
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
            _ => {
                let raw = payload.to_string();
                let head: String = raw.chars().take(600).collect();
                tracing::trace!(kind = %event.kind, payload = %head, "frame");
            }
        }
    }

    // The run id observed in the stream, for `CancelRun`.
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

    // What a silent stream is waiting on right now.
    pub fn phase(&self) -> Phase {
        if self.frames == 0 {
            return Phase::Opening;
        }
        let pending = self
            .last_pending
            .as_deref()
            .and_then(|call_id| self.pending_tools.get(call_id))
            .or_else(|| self.pending_tools.values().next());
        pending.map_or(Phase::Model, |call| Phase::Tool(call.tool.clone()))
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
                    tracing::debug!(tool = %pending.tool, subject = pending.subject(), "tool call");
                    self.pending_tools.insert(call_id.to_owned(), pending);
                    self.last_pending = Some(call_id.to_owned());
                }
            }
            Some("completed" | "error") => {
                let Some(call_id) = call_id else {
                    return;
                };
                let Some(result) = nested
                    .and_then(|tool_call| tool_call.as_object())
                    .and_then(|map| map.values().find_map(|value| value.get("result").cloned()))
                    .or_else(|| payload.get("result").cloned())
                else {
                    return;
                };
                let pending =
                    self.pending_tools.remove(call_id).or_else(call).unwrap_or_else(|| {
                        PendingCall {
                            tool: "unknown".to_owned(),
                            args: Value::Null,
                        }
                    });

                tracing::debug!(
                    tool = %pending.tool,
                    subject = pending.subject(),
                    result_bytes = result.to_string().len(),
                    "tool call completed"
                );
                let PendingCall { tool, args } = pending;
                self.turns.push(ToolTurn { tool, args, result });
            }
            _ => {}
        }
    }

    // The reconstructed tool transcript, or `None` when no tool completed.
    pub fn finish(self) -> Option<Transcript> {
        if self.turns.is_empty() { None } else { Some(Transcript { turns: self.turns }) }
    }
}

// What a run's stream is waiting on, read off the frames so far: the
// stream has yet to carry a frame, a started tool call has yet to complete,
// or the model has every result and is composing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Opening,
    Tool(String),
    Model,
}

impl Phase {
    // Whether the inactivity window bounds this phase: the bridge and the
    // tools it runs answer within it, while the model — whose text the
    // stream never carries — is bounded by the cap alone.
    pub const fn bounded(&self) -> bool {
        !matches!(self, Self::Model)
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Opening => f.write_str("the opening frame"),
            Self::Tool(tool) => write!(f, "tool `{tool}`"),
            Self::Model => f.write_str("the model"),
        }
    }
}

// A started tool call awaiting its completion event.
struct PendingCall {
    tool: String,
    args: Value,
}

impl PendingCall {
    // The CLI stream's shape: `tool_call: { <name>ToolCall: { args, result } }`.
    fn nested(tool_call: &Value) -> Option<Self> {
        tool_call.as_object()?.iter().find_map(|(key, value)| {
            let tool = key.strip_suffix("ToolCall")?;
            let args = value.get("args").cloned().unwrap_or_else(|| value.clone());
            Some(Self {
                tool: tool.to_owned(),
                args,
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
        });
        Some(custom.unwrap_or_else(|| Self {
            tool: name.to_owned(),
            args,
        }))
    }

    // The one argument worth a log line: a path, a pattern, or the first
    // line of a command, cut to a readable width.
    fn subject(&self) -> Option<String> {
        const KEYS: &[&str] = &["path", "globPattern", "pattern", "command", "query", "url"];
        const WIDTH: usize = 80;
        let text = first_match(&self.args, KEYS)?;
        let line = text.lines().next().unwrap_or_default();
        Some(if line.chars().count() > WIDTH {
            format!("{}…", line.chars().take(WIDTH).collect::<String>())
        } else {
            line.to_owned()
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
#[cfg(test)]
mod tests {
    use omnia_wasi_model::Usage;
    use serde_json::{Value, json};

    use super::{EventLog, Phase};
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
            observe_one(&mut log, event);
        }
        log
    }

    fn observe_one(log: &mut EventLog, event: &Value) {
        let message: SdkMessage =
            serde_json::from_value(event.clone()).expect("test events are SdkMessages");
        log.frames += 1;
        log.observe(&message);
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

    #[test]
    fn bridge_frames() {
        let custom = json!({
            "providerIdentifier": "custom-user-tools", "toolName": "read_doc",
            "args": { "path": "references/ids.md" },
        });
        let glob = json!({ "globPattern": "**/*.ts" });
        let log = observe_all(&[
            json!({ "type": "status", "message": { "run_id": "run-1", "status": "RUNNING" } }),
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
        ]);
        assert_eq!(log.run_id(), Some("run-1"));
        assert_eq!(log.tool_calls(), 2);
        let transcript = log.finish().expect("two completed tool turns");
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

        observe_one(&mut log, &json!({ "type": "status", "message": { "status": "RUNNING" } }));
        assert_eq!(log.phase(), Phase::Model);

        let read = json!({ "path": "a.ts" });
        let grep = json!({ "pattern": "fn" });
        observe_one(&mut log, &flat("read", "c1", &read, None));
        observe_one(&mut log, &flat("grep", "c2", &grep, None));
        assert_eq!(log.phase(), Phase::Tool("grep".to_owned()), "the latest started call");

        let done = json!({ "status": "success", "value": {} });
        observe_one(&mut log, &flat("grep", "c2", &grep, Some(done.clone())));
        assert_eq!(log.phase(), Phase::Tool("read".to_owned()), "the one still pending");

        observe_one(&mut log, &flat("read", "c1", &read, Some(done)));
        assert_eq!(log.phase(), Phase::Model);
        assert_eq!(log.frames(), 5);

        assert!(Phase::Opening.bounded());
        assert!(Phase::Tool("read".to_owned()).bounded());
        assert!(!Phase::Model.bounded());
        assert_eq!(Phase::Tool("read".to_owned()).to_string(), "tool `read`");
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
}
