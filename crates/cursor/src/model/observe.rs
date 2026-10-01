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
//! finish events — the finish at INFO for an answer, WARN for anything else,
//! carrying the bill and how the wait split across the phases.

use std::collections::HashMap;
use std::time::Duration;
use std::{fmt, mem};

use omnia_wasi_model::{ToolTurn, Transcript, Usage};
use serde_json::Value;
use tokio::time::Instant;

use super::options::Turn;
use crate::elapsed_ms;
use crate::failure::Outcome;
use crate::protocol::{RunStreamMessage, SdkMessage, TokenUsage, lenient_i64};

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
            Phase::Tool(_) => &mut self.tool,
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

impl Drop for Completion {
    fn drop(&mut self) {
        self.finish(Outcome::Abort);
    }
}

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
                    tracing::debug!(
                        tool = %pending.tool,
                        subject = pending.subject(self.cwd.as_deref()),
                        "tool call"
                    );
                    // the arguments whole, for a `-vv` run reading what the agent asked for
                    tracing::trace!(tool = %pending.tool, args = %pending.args, "tool call args");
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
                });

                tracing::debug!(
                    tool = %pending.tool,
                    subject = pending.subject(self.cwd.as_deref()),
                    result_bytes = result.to_string().len(),
                    "tool call completed"
                );
                let PendingCall { tool, args } = pending;
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
    // line of a command, cut to a readable width. A path the agent spells
    // under its `cwd` is shown relative to it, so the width goes on the
    // part that tells one file from another.
    fn subject(&self, cwd: Option<&str>) -> Option<String> {
        const KEYS: &[&str] = &["path", "globPattern", "pattern", "command", "query", "url"];
        const WIDTH: usize = 80;
        let text = first_match(&self.args, KEYS)?;
        let line = text.lines().next().unwrap_or_default();
        let line = cwd
            .and_then(|cwd| line.strip_prefix(cwd)?.strip_prefix('/'))
            .filter(|rest| !rest.is_empty())
            .unwrap_or(line);
        Some(if line.chars().count() > WIDTH {
            format!("{}…", line.chars().take(WIDTH).collect::<String>())
        } else {
            line.to_owned()
        })
    }
}

// Any other frame, at TRACE with the head of its payload.
fn frame(event: &SdkMessage) {
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

// Stream parsing, the token clamp, and the completion's books alone; a
// run's events reach the log through the fake in `tests/model.rs`.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use omnia_wasi_model::Usage;
    use serde_json::{Value, json};

    use super::{Bill, Completion, EventLog, Phase};
    use crate::failure::Outcome;
    use crate::protocol::{SdkMessage, TokenUsage};

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

    // Every wait lands in its phase's bucket, tool calls together, and the
    // guest's checks in their own.
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
        assert_eq!(completion.opening, Duration::from_millis(2500));
        assert_eq!(completion.tool, Duration::from_secs(4));
        assert_eq!(completion.model, Duration::from_secs(5));
        assert_eq!(completion.check, Duration::from_millis(100));
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
            observe_one(&mut log, &event);
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

    // A terminal frame without a result still ends the call: the tool is
    // done, so the run waits on the model through the silent compose and the
    // inactivity window stands down instead of cutting it short.
    #[test]
    fn terminal_without_result() {
        let mut log = EventLog::default();
        let read = json!({ "path": "a.ts" });
        let grep = json!({ "pattern": "fn" });

        observe_one(&mut log, &flat("read", "c1", &read, None));
        observe_one(&mut log, &resultless("read", "c1", &read, "completed"));
        assert_eq!(log.phase(), Phase::Model);

        observe_one(&mut log, &flat("read", "c2", &read, None));
        observe_one(&mut log, &resultless("read", "c2", &read, "error"));
        assert_eq!(log.phase(), Phase::Model);

        observe_one(&mut log, &flat("read", "c3", &read, None));
        observe_one(&mut log, &flat("grep", "c4", &grep, None));
        observe_one(&mut log, &resultless("read", "c3", &read, "completed"));
        assert_eq!(log.phase(), Phase::Tool("grep".to_owned()), "the one still pending");
        observe_one(&mut log, &resultless("grep", "c4", &grep, "error"));
        assert_eq!(log.phase(), Phase::Model);

        observe_one(
            &mut log,
            &json!({ "type": "tool_call", "message": {
                "subtype": "started", "call_id": "c5",
                "tool_call": { "readToolCall": { "args": &read } },
            }}),
        );
        observe_one(
            &mut log,
            &json!({ "type": "tool_call", "message": {
                "subtype": "completed", "call_id": "c5",
                "tool_call": { "readToolCall": { "args": &read } },
            }}),
        );
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

        observe_one(&mut log, &flat("read", "c1", &read("a.ts"), None));
        observe_one(&mut log, &flat("read", "c2", &read("b.ts"), None));
        observe_one(&mut log, &flat("read", "c3", &read("c.ts"), None));
        observe_one(&mut log, &flat("read", "c1", &read("a.ts"), Some(done)));
        assert_eq!(log.phase(), Phase::Tool("read".to_owned()), "two still pending");

        observe_one(&mut log, &json!({ "type": "thinking", "message": { "text": "Now" } }));
        assert_eq!(log.phase(), Phase::Model, "the model resumed, so nothing is pending");

        observe_one(&mut log, &flat("grep", "c4", &json!({ "pattern": "fn" }), None));
        assert_eq!(log.phase(), Phase::Tool("grep".to_owned()), "a later call is pending again");
        observe_one(
            &mut log,
            &json!({ "type": "assistant", "message": {
                "message": { "content": [{ "text": "The", "type": "text" }], "role": "assistant" },
            }}),
        );
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

        // the closing frames' durations are taken once, for the completion's `thinking_ms`
        assert_eq!(log.thought(), Duration::from_millis(4200));
        assert_eq!(log.thought(), Duration::ZERO);
        let mut completion = Completion::new();
        completion.thought(Duration::from_millis(4200));
        completion.thought(Duration::from_millis(300));
        assert_eq!(completion.thinking, Duration::from_millis(4500));
        completion.finish(Outcome::Ok);
        assert!(log.finish().is_none());
    }

    // A path under the agent's `cwd` is shown relative to it; anything else
    // is shown as spelled, cut to the width.
    #[test]
    fn subjects() {
        let cwd = "/Users/me/project";
        let subject = |args: Value, cwd: Option<&str>| {
            super::PendingCall {
                tool: "read".to_owned(),
                args,
            }
            .subject(cwd)
        };
        assert_eq!(
            subject(json!({ "path": "/Users/me/project/src/orders.ts" }), Some(cwd)).as_deref(),
            Some("src/orders.ts")
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
        assert!(shown.starts_with("aaaa"), "the width is spent on the relative part: {shown}");
        assert_eq!(subject(json!({ "other": 1 }), Some(cwd)), None);
    }
}
