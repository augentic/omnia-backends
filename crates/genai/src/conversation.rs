//! Runs a bounded provider conversation with tools, structured output, and guest checks.
//!
//! Each provider reply consumes one round and has its own timeout. Rejected
//! candidates also consume the candidate budget.
//!
//! The loop applies these safeguards:
//!
//! - Invalid structured output gets one diagnostic nudge per candidate.
//! - Closing rounds that require an answer refuse tool calls.
//! - Token-truncated replies do not execute possibly incomplete tool calls.
//! - Tool results are capped while preserving their head and tail.
//! - Repeated failed calls terminate the completion.
//! - Transport failures are retried three times with backoff.
//!
//! Exhausted budgets and reply timeouts return the typed `budget-exhausted`
//! error.

use std::collections::HashMap;
use std::fmt;
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponse, ChatResponseFormat, StopReason, ToolCall,
    ToolChoice, ToolResponse,
};
use jsonschema::Validator;
use omnia_wasi_model::{Answer, Error, Format, ToolTurn, Transcript, Usage};
use serde_json::Value;
use tokio::time::timeout;

use crate::observe::{Completion, Phase, Timeout, to_usage};
use crate::request::Turn;
use crate::shell::truncate;
use crate::tools::Tools;

#[derive(Clone, Copy, Debug)]
pub struct Budgets {
    pub max_rounds: u32,
    pub max_candidates: u32,
    pub max_repeats: u32,
    pub timeout: Duration,
}

const REPEAT_NOTE_FROM: u32 = 3;
const MAX_RESULT_BYTES: usize = 64 * 1024;
const TRANSPORT_RETRY_WAITS: [Duration; 3] =
    [Duration::from_secs(1), Duration::from_secs(5), Duration::from_secs(15)];

pub struct Conversation {
    client: genai::Client,
    model: String,
    chat: ChatRequest,
    options: ChatOptions,
    format: Format,
    validator: Option<Validator>,
    check: bool,
    tools: Tools,
    budgets: Budgets,
    round: u32,
    candidates: u32,
    // consecutive failures per (tool, args) hash
    repeats: HashMap<u64, u32>,
    nudged: bool,
    transcript: Transcript,
    completion: Completion,
}

struct Reply {
    text: String,
    usage: Option<Usage>,
}

const REFUSED_IN_CLOSING: &str = "not run: your final answer is due now. Reply with only your \
                                  final answer as a single JSON value in the shape the prompt \
                                  asked for, and nothing else.";

impl Conversation {
    pub fn new(client: genai::Client, turn: Turn, tools: Tools, budgets: Budgets) -> Self {
        let advertised = turn.chat.tools.as_ref().map_or(0, Vec::len);
        let completion = Completion::start(turn.prompt_bytes, advertised);
        let Turn {
            model,
            chat,
            options,
            format,
            validator,
            check,
            ..
        } = turn;

        Self {
            client,
            model,
            chat,
            options,
            format,
            validator,
            check,
            tools,
            budgets,
            round: 0,
            candidates: 0,
            repeats: HashMap::new(),
            nudged: false,
            transcript: Transcript::default(),
            completion,
        }
    }

    pub async fn complete(mut self) -> Result<Answer> {
        let result = {
            let mut heartbeat = self.completion.heartbeat();
            let run = self.run();
            tokio::pin!(run);
            loop {
                tokio::select! {
                    result = &mut run => break result,
                    () = heartbeat.tick() => {}
                }
            }
        };
        self.completion.finish(&result);
        result
    }

    async fn run(&mut self) -> Result<Answer> {
        loop {
            // await next model reply within timeout
            let reply = match timeout(self.budgets.timeout, self.next_reply()).await {
                Ok(reply) => reply?,
                Err(_elapsed) => {
                    return Err(Timeout {
                        after: self.budgets.timeout,
                        waiting: self.completion.phase(),
                    }
                    .into_error());
                }
            };
            let Some(Reply { text, usage }) = reply else {
                bail!(Error::BudgetExhausted(format!(
                    "no answer after {} model round-trips",
                    self.budgets.max_rounds
                )));
            };

            let candidate = self.format.candidate(&text);
            if !self.check {
                return Ok(self.answer(candidate, usage));
            }

            // nudge once on malformed JSON or schema mismatch
            if !self.nudged
                && let Some((fault, nudge)) = self.off_shape(&candidate)
            {
                self.nudged = true;
                tracing::debug!(
                    round = self.round,
                    reply_bytes = candidate.len(),
                    %fault,
                    "the reply is off the shape asked for; nudging under JSON mode"
                );
                tracing::trace!(%nudge, "nudge turn");
                self.chat = std::mem::take(&mut self.chat)
                    .append_message(ChatMessage::assistant(candidate))
                    .append_message(ChatMessage::user(nudge));
                continue;
            }

            // validate candidate with guest check
            self.candidates += 1;
            self.completion.enter(Phase::Check);
            match self.tools.check(candidate.clone()).await? {
                Ok(()) => return Ok(self.answer(candidate, usage)),
                Err(correction)
                    if self.candidates >= self.budgets.max_candidates
                        || self.round == self.budgets.max_rounds =>
                {
                    bail!(Error::BudgetExhausted(correction));
                }
                Err(correction) => {
                    self.completion.rejected();
                    tracing::debug!(
                        round = self.round,
                        candidate_bytes = candidate.len(),
                        "check rejected the candidate"
                    );
                    tracing::trace!(%correction, "correction turn");
                    self.nudged = false;
                    self.chat = std::mem::take(&mut self.chat)
                        .append_message(ChatMessage::assistant(candidate))
                        .append_message(ChatMessage::user(correction));
                }
            }
        }
    }

    async fn next_reply(&mut self) -> Result<Option<Reply>> {
        while self.round < self.budgets.max_rounds {
            self.round += 1;
            self.completion.enter(Phase::Model);

            // execute provider round
            let closing = self.closing();
            let (chat, options) = self.round_request();
            let response = self.exec_chat(chat, &options).await?;

            // record round telemetry
            let cut = self.cut(&response);
            let text = response.first_text().map(ToOwned::to_owned);
            let usage = response.usage.clone();
            let tool_calls = response.into_tool_calls();
            let reply_bytes = text.as_deref().filter(|_| tool_calls.is_empty()).map(str::len);
            self.completion.answered(&usage, reply_bytes, tool_calls.len(), cut);

            // dispatch or refuse tool calls
            if !tool_calls.is_empty() {
                if closing {
                    self.refuse_closing(tool_calls);
                } else if cut {
                    self.refuse_cut(tool_calls);
                } else {
                    self.tool_round(tool_calls).await?;
                }
                continue;
            }

            let Some(text) = text else {
                bail!("genai returned neither content nor tool calls (model `{}`)", self.model);
            };
            return Ok(Some(Reply {
                text,
                usage: to_usage(&usage),
            }));
        }
        Ok(None)
    }

    async fn exec_chat(
        &mut self, chat: ChatRequest, options: &ChatOptions,
    ) -> Result<genai::chat::ChatResponse> {
        let mut waits = TRANSPORT_RETRY_WAITS.iter();
        loop {
            let error = match self.client.exec_chat(&self.model, chat.clone(), Some(options)).await
            {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };
            let transport = matches!(
                &error,
                genai::Error::WebModelCall { webc_error, .. }
                | genai::Error::WebAdapterCall { webc_error, .. }
                    if matches!(webc_error, genai::webc::Error::Reqwest(_))
            );
            let Some(wait) = transport.then(|| waits.next()).flatten() else {
                return Err(error)
                    .with_context(|| format!("genai exec_chat failed for model `{}`", self.model));
            };
            self.completion.retried();
            tracing::warn!(
                round = self.round,
                wait_ms = wait.as_millis(),
                "the request failed at the transport before any reply; putting it again"
            );
            tokio::time::sleep(*wait).await;
        }
    }

    fn round_request(&self) -> (ChatRequest, ChatOptions) {
        let chat = self.chat.clone();
        let mut options = self.options.clone();
        // Fall back to JSON mode after a nudge for adapters that ignore JsonSpec.
        if self.nudged {
            options = options.with_response_format(ChatResponseFormat::JsonMode);
        }
        // Keep tools declared to preserve prompt prefix cache while forbidding calls.
        if self.closing() && chat.tools.as_ref().is_some_and(|tools| !tools.is_empty()) {
            options = options.with_tool_choice(ToolChoice::None);
        }
        (chat, options)
    }

    const fn closing(&self) -> bool {
        self.nudged || self.round == self.budgets.max_rounds
    }

    // Also check token usage: vLLM reports `tool_calls` when it salvages a call from a truncated reply.
    fn cut(&self, response: &ChatResponse) -> bool {
        let by_reason = response.stop_reason.as_ref().is_some_and(StopReason::is_max_tokens);
        let by_bill = match (self.options.max_tokens, response.usage.completion_tokens) {
            (Some(bound), Some(spent)) => u32::try_from(spent).is_ok_and(|spent| spent >= bound),
            _ => false,
        };
        by_reason || by_bill
    }

    fn off_shape(&self, candidate: &str) -> Option<(String, String)> {
        if !matches!(self.format, Format::Json | Format::Schema(_)) {
            return None;
        }

        // parse JSON
        let value = match serde_json::from_str::<Value>(candidate) {
            Ok(value) => value,
            Err(fault) => {
                let fault = fault.to_string();
                let nudge = format!(
                    "Your last reply is not one well-formed JSON value ({fault}), so it is not \
                     the answer. Reply now with only your final answer as a single JSON value \
                     in the shape the prompt asked for, and nothing else."
                );
                return Some((fault, nudge));
            }
        };

        // validate against JSON schema, masking instance values in debug logs
        let validator = self.validator.as_ref()?;
        let mut masked = Vec::new();
        let mut detailed = Vec::new();
        for error in validator.iter_errors(&value) {
            let path = error.instance_path().to_string();
            masked.push(at_path(&path, error.masked()));
            detailed.push(at_path(&path, &error));
        }
        if detailed.is_empty() {
            return None;
        }
        let nudge = format!(
            "Your last reply is JSON but not in the shape the prompt asked for ({}), so it is \
             not the answer. Reply now with only your final answer as a single JSON value in \
             exactly that shape, and nothing else.",
            detailed.join("; ")
        );
        Some((masked.join("; "), nudge))
    }

    async fn tool_round(&mut self, tool_calls: Vec<ToolCall>) -> Result<()> {
        let mut chat = std::mem::take(&mut self.chat).append_message(tool_calls.clone());
        for call in tool_calls {
            self.completion.enter(Phase::of_tool(&call.fn_name));
            let result = match self.tools.call(&call).await? {
                Ok(result) => {
                    self.repeats.remove(&identity(&call));
                    result
                }
                Err(reason) => self.failed(&call, &reason)?,
            };
            let result = truncate(result, MAX_RESULT_BYTES);
            self.transcript.turns.push(ToolTurn {
                tool: call.fn_name,
                args: call.fn_arguments,
                result: Value::String(result.clone()),
            });
            chat = chat.append_message(ToolResponse::new(call.call_id, result));
        }
        self.chat = chat;
        Ok(())
    }

    fn failed(&mut self, call: &ToolCall, reason: &str) -> Result<String> {
        let count = self.repeats.entry(identity(call)).or_insert(0);
        *count += 1;
        let count = *count;
        if count >= self.budgets.max_repeats {
            tracing::debug!(
                round = self.round,
                tool = %call.fn_name,
                count,
                "the same call failed the same way up to the repeat budget"
            );
            bail!(Error::BudgetExhausted(format!(
                "tool `{}` failed {count} times with the same arguments: {reason}",
                call.fn_name
            )));
        }
        let result = format!("tool `{}` failed: {reason}", call.fn_name);
        if count < REPEAT_NOTE_FROM {
            return Ok(result);
        }
        Ok(format!(
            "{result}\n\nThis exact call has now failed {count} times, and the same call will \
             fail the same way. Change the arguments or the approach: after {} identical \
             failures the task ends.",
            self.budgets.max_repeats
        ))
    }

    fn refuse_closing(&mut self, tool_calls: Vec<ToolCall>) {
        tracing::debug!(
            round = self.round,
            tool_calls = tool_calls.len(),
            "tool calls in a closing round refused; the JSON answer is due"
        );
        self.refuse(tool_calls, REFUSED_IN_CLOSING);
    }

    fn refuse_cut(&mut self, tool_calls: Vec<ToolCall>) {
        let bound = self.options.max_tokens.unwrap_or_default();
        tracing::debug!(
            round = self.round,
            tool_calls = tool_calls.len(),
            bound,
            "the reply was cut at the token bound; its tool calls refused, not run"
        );
        let refusal = format!(
            "not run: your reply ran to the cap of {bound} tokens and was cut there, so the \
             arguments of this call did not arrive as you wrote them. Send smaller calls, fewer \
             files or edits a call and each shorter, so one reply stays within the cap."
        );
        self.refuse(tool_calls, &refusal);
    }

    fn refuse(&mut self, tool_calls: Vec<ToolCall>, refusal: &str) {
        let mut chat = std::mem::take(&mut self.chat).append_message(tool_calls.clone());
        for call in tool_calls {
            self.transcript.turns.push(ToolTurn {
                tool: call.fn_name,
                args: call.fn_arguments,
                result: Value::String(refusal.to_owned()),
            });
            chat = chat.append_message(ToolResponse::new(call.call_id, refusal));
        }
        self.chat = chat;
    }

    fn answer(&mut self, answer: String, usage: Option<Usage>) -> Answer {
        Answer {
            answer,
            usage,
            transcript: Some(std::mem::take(&mut self.transcript)),
        }
    }
}

fn at_path(path: &str, fault: impl fmt::Display) -> String {
    if path.is_empty() { fault.to_string() } else { format!("at `{path}`: {fault}") }
}

fn identity(call: &ToolCall) -> u64 {
    let mut hasher = DefaultHasher::new();
    call.fn_name.hash(&mut hasher);
    call.fn_arguments.to_string().hash(&mut hasher);
    hasher.finish()
}
