//! Stream parse plus completion telemetry.
//!
//! [`EventLog`] follows a run's `sdk_message` events to rebuild the tool
//! transcript and capture the run id and last status text. Payload shapes
//! mirror the public SDK — every field access is nullable and a malformed
//! event is skipped, never fatal. [`Completion`] emits the start/finish INFO
//! lines and tracing-opentelemetry metric fields.

use std::collections::HashMap;

use omnia_wasi_model::{Format, ToolTurn, Transcript, Usage};
use serde_json::Value;
use tokio::time::Instant;

use crate::bridge::{Exit, SdkMessage, TransportError};
use crate::elapsed_ms;

/// How a completion this backend ran came to fail, by variant rather than
/// by message.
///
/// `complete`'s error downcasts to one of these — or to a
/// [`TransportError`], or to the typed `budget-exhausted` a rejected check
/// ends on.
#[derive(Debug)]
#[non_exhaustive]
pub enum Failure {
    /// Absolute wall-clock cap exceeded while the stream was still active.
    Timeout {
        /// The cap in seconds, from connect options.
        cap_secs: u64,
    },
    /// No stream events within the inactivity window.
    Inactive {
        /// Observed idle span in seconds.
        idle_secs: u64,
        /// Configured inactivity limit in seconds.
        inactivity_secs: u64,
        /// Configured absolute cap in seconds.
        cap_secs: u64,
    },
    /// Hard tool-host failure (or a closed abort channel).
    Aborted(String),
    /// The spawned bridge process exited while the completion was running
    /// on it.
    BridgeExited(Exit),
}

impl Failure {
    /// The `outcome` label the `cursor_completions` counter carries for
    /// this failure.
    pub const fn outcome(&self) -> &'static str {
        match self {
            Self::Timeout { .. } => "timeout",
            Self::Inactive { .. } => "inactive",
            Self::Aborted(_) => "abort",
            Self::BridgeExited(_) => "bridge_exit",
        }
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout { cap_secs } => write!(
                f,
                "cursor run timed out after {cap_secs}s (absolute cap exceeded while still active)"
            ),
            Self::Inactive {
                idle_secs,
                inactivity_secs,
                cap_secs,
            } => write!(
                f,
                "cursor run inactive for {idle_secs}s (no stream events; inactivity limit \
                 {inactivity_secs}s, absolute cap {cap_secs}s)"
            ),
            Self::Aborted(reason) => write!(f, "completion aborted: {reason}"),
            Self::BridgeExited(exit) => {
                write!(f, "cursor-sdk-bridge exited ({exit}) during the run")
            }
        }
    }
}

impl std::error::Error for Failure {}

/// One completion's metric-bearing start/finish. Drop without [`Self::finish`]
/// records `outcome=abort` (a cancelled future).
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
    // INFO that a completion is in flight (no metric prefixes — live tail).
    pub fn start(model: &str, format: &Format, prompt: &str, mcp_servers: usize) -> Self {
        let format = format.to_string();
        let prompt_bytes = len_u64(prompt.len());

        tracing::info!(model, format, prompt_bytes, mcp = mcp_servers, "completion started");

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

    // INFO + OTEL metric fields for this completion; emitted once, so a
    // later `Drop` says nothing.
    pub fn finish(&mut self, outcome: &'static str) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        let duration_ms = elapsed_ms(self.started);
        tracing::info!(
            model = %self.model,
            format = %self.format,
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
            monotonic_counter.cursor_corrections = u64::from(outcome == "corrected"),
            "completion"
        );
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.finish("abort");
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
    pub fn observe(&mut self, event: &SdkMessage) {
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

/// Classify a failed `complete`: a [`Failure`] by variant, a
/// [`TransportError`] as `transport`, the typed `budget-exhausted` a
/// rejected check ends on, anything else `error`.
pub fn outcome_of(error: &anyhow::Error) -> &'static str {
    if let Some(failure) = error.downcast_ref::<Failure>() {
        return failure.outcome();
    }
    if error.downcast_ref::<TransportError>().is_some() {
        return "transport";
    }
    match error.downcast_ref::<omnia_wasi_model::Error>() {
        Some(omnia_wasi_model::Error::BudgetExhausted(_)) => "exhausted",
        _ => "error",
    }
}

/// Whether the bridge, or the socket to it, was lost under the completion:
/// the process exited, or an RPC failed below Connect. Neither says anything
/// about the prompt, so a fresh bridge may be given it again; a Connect
/// error, an end-stream error, or a run that ended in a failing status is
/// the bridge answering, and is not.
pub fn lost_bridge(error: &anyhow::Error) -> bool {
    matches!(error.downcast_ref::<Failure>(), Some(Failure::BridgeExited(_)))
        || error.downcast_ref::<TransportError>().is_some()
}

// The first string found under any of `keys`, tolerating both `snake_case`
// and `camelCase` spellings across bridge versions.
fn first_match<'a>(payload: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| payload.get(key).and_then(Value::as_str))
}

// a byte or item count as a metric value; `usize` never exceeds `u64` on a
// supported target, so this is a lossless widening
fn len_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{EventLog, Failure};
    use crate::bridge::{Exit, SdkMessage, TransportError};

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

    // an exit whose status the wait never reported
    const EXITED: Exit = Exit { status: None, pid: 1 };

    #[test]
    fn classify_crate_errors() {
        let timeout: anyhow::Error = Failure::Timeout { cap_secs: 600 }.into();
        assert_eq!(super::outcome_of(&timeout), "timeout");

        let inactive: anyhow::Error = Failure::Inactive {
            idle_secs: 120,
            inactivity_secs: 120,
            cap_secs: 600,
        }
        .into();
        assert_eq!(super::outcome_of(&inactive), "inactive");

        let aborted: anyhow::Error = Failure::Aborted("session closed".to_owned()).into();
        assert_eq!(super::outcome_of(&aborted), "abort");

        let exited: anyhow::Error = Failure::BridgeExited(EXITED).into();
        assert_eq!(super::outcome_of(&exited), "bridge_exit");
        assert_eq!(exited.to_string(), "cursor-sdk-bridge exited (status unknown) during the run");

        let transport: anyhow::Error = TransportError::truncated("SdkAgentService/Send", 3).into();
        assert_eq!(super::outcome_of(&transport), "transport");

        let rejected: anyhow::Error =
            omnia_wasi_model::Error::BudgetExhausted("say more".to_owned()).into();
        assert_eq!(super::outcome_of(&rejected), "exhausted");

        assert_eq!(super::outcome_of(&anyhow::anyhow!("bridge RPC failed")), "error");
    }

    #[test]
    fn lost_bridge_classes() {
        let exited: anyhow::Error = Failure::BridgeExited(EXITED).into();
        assert!(super::lost_bridge(&exited));
        let reset = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
        let socket: anyhow::Error =
            TransportError::io("SdkAgentService/Send", "reading the stream", reset).into();
        assert!(super::lost_bridge(&socket));
        let torn: anyhow::Error = TransportError::truncated("SdkAgentService/Send", 3).into();
        assert!(super::lost_bridge(&torn));

        // The bridge answered, in one way or another.
        let inactive: anyhow::Error = Failure::Inactive {
            idle_secs: 120,
            inactivity_secs: 120,
            cap_secs: 600,
        }
        .into();
        assert!(!super::lost_bridge(&inactive));
        let timeout: anyhow::Error = Failure::Timeout { cap_secs: 600 }.into();
        assert!(!super::lost_bridge(&timeout));
        let aborted: anyhow::Error = Failure::Aborted("session closed".to_owned()).into();
        assert!(!super::lost_bridge(&aborted));
        let rejected: anyhow::Error =
            omnia_wasi_model::Error::BudgetExhausted("say more".to_owned()).into();
        assert!(!super::lost_bridge(&rejected));
        assert!(!super::lost_bridge(&anyhow::anyhow!(
            "bridge RPC `SdkAgentService/Send` failed (500 Internal Server Error, internal): boom"
        )));
        assert!(!super::lost_bridge(&anyhow::anyhow!("cursor run error: model overloaded")));
    }
}
