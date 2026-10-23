//! Runs a bounded provider conversation with tools, structured output, and guest checks.
//!
//! Each provider reply consumes one round and has its own timeout. Rejected
//! candidates also consume the candidate budget.
//!
//! The loop applies these safeguards:
//!
//! - Invalid structured output gets one diagnostic nudge per candidate.
//! - Checked requests reserve the final quarter of rounds for corrections.
//! - Rounds that require an answer refuse tool calls.
//! - Token-truncated replies do not execute possibly incomplete tool calls.
//! - Tool results are capped while preserving their head and tail.
//! - Repeated failed calls terminate the completion.
//! - Repeated unchanged results make the next round require an answer.
//! - Transport failures are retried three times with backoff.
//!
//! A correction resets tool-repeat tracking. Exhausted budgets and reply
//! timeouts return the typed `budget-exhausted` error.

use std::collections::HashMap;
use std::fmt;
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponse, ChatResponseFormat, ToolCall, ToolChoice,
    ToolResponse,
};
use jsonschema::Validator;
use omnia_wasi_model::{Answer, Error, Format, ToolTurn, Transcript, Usage};
use serde_json::Value;
use tokio::time::timeout;

use crate::observe::{Completion, Phase, Timeout, to_usage};
use crate::request::Turn;
use crate::shell::truncate;
use crate::tools::Tools;

// The budgets one completion runs within, from the connect options: the
// provider rounds it may make, the candidates it may offer the guest's
// check, the identical failed tool calls it may make, and the time one
// reply of the model may take.
#[derive(Clone, Copy, Debug)]
pub struct Budgets {
    pub max_rounds: u32,
    pub max_candidates: u32,
    pub max_repeats: u32,
    pub timeout: Duration,
}

impl Budgets {
    // The round from which the first candidate is due where the request
    // carries a check: the rounds before the corrections' share are spent,
    // and a budget too small to share is due at its cap alone.
    const fn due_at(&self) -> u32 {
        self.max_rounds - self.max_rounds / CORRECTION_SHARE
    }
}

// The share of the round budget kept for the check's corrections, one in
// this many: a candidate forced at the cap is refused with no round left to
// repair it in, so the first is made due while a quarter remain.
const CORRECTION_SHARE: u32 = 4;

// The identical call from which its result names the count, so the model
// is told it is repeating itself before the repeats end the completion or
// make its answer due.
const REPEAT_NOTE_FROM: u32 = 3;

// The most of one tool's result the model is handed, its head and its
// tail: the host's cap admits a mebibyte, which a context window of a
// quarter-million tokens cannot hold beside the prompt.
const MAX_RESULT_BYTES: usize = 64 * 1024;

// The waits before a request that failed at the transport — a connection
// the server closed as it was written on, a tunnel between — is put again,
// unchanged. A chat request applies nothing until its reply is read, so
// the same request is the same question; the last wait outlasts the ten
// seconds a tunnel takes to reopen. Past the list the failure is the
// completion's, `transport`, as before.
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
    // the failures so far of each call by its identity, a success of the
    // same call clearing it
    repeats: HashMap<u64, u32>,
    // the identical results so far of each succeeding call by its identity,
    // a correction clearing them
    echoes: HashMap<u64, u32>,
    nudged: bool,
    // why the model's text is due in the rounds to come, until it arrives
    due: Option<Due>,
    transcript: Transcript,
    completion: Completion,
}

// What makes the model's text due before the round budget does: a tool
// call is refused either way, and the rounds after a nudge ask for JSON
// mode in place of the schema besides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Due {
    // A reply off the shape asked for was nudged.
    Nudged,
    // One call returned the same result up to the repeat budget.
    Repeated,
}

// Why a round is one the model's text is due in, its tool calls refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Finishing {
    // A nudge, the repeat budget, or the last round of the budget.
    Closing,
    // No candidate has reached the check and the rounds left are kept for
    // its corrections.
    Reserved,
}

// The model's text once a round ends in one, with the round's counts.
struct Reply {
    text: String,
    usage: Option<Usage>,
}

// What the model is told of a tool call it makes where its JSON answer is
// due: the result of every such call, in place of running it.
const REFUSED_IN_CLOSING: &str = "not run: your final answer is due now. Reply with only your \
                                  final answer as a single JSON value in the shape the prompt \
                                  asked for, and nothing else.";

// The same, where what makes the answer due is the share of the rounds
// kept for the corrections.
const REFUSED_IN_RESERVE: &str = "not run: the rounds left are kept for correcting your answer, \
                                  so your final answer is due now. Reply with only your final \
                                  answer as a single JSON value in the shape the prompt asked \
                                  for, and nothing else.";

impl Conversation {
    // The completion starts here, once nothing is left to refuse: the start
    // event means a completion that will be billed.
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
            echoes: HashMap::new(),
            nudged: false,
            due: None,
            transcript: Transcript::default(),
            completion,
        }
    }

    pub async fn complete(mut self) -> Result<Answer> {
        // the run, with the heartbeat beside it
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
            // the model's next reply, under the timeout
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

            // a reply off the shape asked for is nudged once per candidate,
            // told what the parser or the schema found, and answered in the
            // closing rounds under JSON mode
            if !self.nudged
                && let Some((fault, nudge)) = self.off_shape(&candidate)
            {
                self.nudged = true;
                self.due = Some(Due::Nudged);
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

            // the guest's verdict on the candidate
            self.candidates += 1;
            self.completion.enter(Phase::Check);
            match self.tools.check(candidate.clone()).await? {
                Ok(()) => return Ok(self.answer(candidate, usage)),
                // The guest's correction is the model's next turn, verbatim;
                // with no candidate or round left it is the typed failure
                // the guest sees.
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
                    self.echoes.clear();
                    self.chat = std::mem::take(&mut self.chat)
                        .append_message(ChatMessage::assistant(candidate))
                        .append_message(ChatMessage::user(correction));
                }
            }
        }
    }

    // The model's next text, through the tool rounds before it; `None` once
    // the round budget is spent first.
    async fn next_reply(&mut self) -> Result<Option<Reply>> {
        while self.round < self.budgets.max_rounds {
            self.round += 1;
            self.completion.enter(Phase::Model);
            let finishing = self.finishing();
            if finishing == Some(Finishing::Reserved) && self.completion.forced_at().is_none() {
                tracing::debug!(
                    round = self.round,
                    left = self.budgets.max_rounds - self.round,
                    "no candidate has reached the check; the answer is due, the rounds left \
                     kept for the corrections"
                );
                self.completion.forced(self.round);
            }
            let (chat, options) = self.round_request();
            let response = self.exec_chat(chat, &options).await?;

            let cut = self.cut(&response);
            let text = response.first_text().map(ToOwned::to_owned);
            let usage = response.usage.clone();
            let tool_calls = response.into_tool_calls();
            let reply_bytes = text.as_deref().filter(|_| tool_calls.is_empty()).map(str::len);
            self.completion.answered(&usage, reply_bytes, tool_calls.len(), cut);

            if !tool_calls.is_empty() {
                match finishing {
                    Some(finishing) => self.refuse_round(tool_calls, finishing),
                    None if cut => self.refuse_cut(tool_calls),
                    None => self.tool_round(tool_calls).await?,
                }
                continue;
            }

            let Some(text) = text else {
                bail!("genai returned neither content nor tool calls (model `{}`)", self.model);
            };
            self.due = None;
            return Ok(Some(Reply {
                text,
                usage: to_usage(&usage),
            }));
        }
        Ok(None)
    }

    // One round's request to the provider, put again after each wait of
    // `TRANSPORT_RETRY_WAITS` where it failed at the transport — the
    // connection, not the provider's answer — and otherwise failed as the
    // provider failed it.
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

    // What this round puts to the provider: the chat and the options as
    // translated, or, in the closing rounds after a nudge, under JSON mode in
    // place of the schema, the one `response_format` every adapter sends. A
    // finishing round declares the tools and forbids calling them: a provider
    // that caches the prompt by its prefix keeps the cache, one that honours
    // `tool_choice` answers in text, and a call made all the same is refused
    // rather than run.
    fn round_request(&self) -> (ChatRequest, ChatOptions) {
        let chat = self.chat.clone();
        let mut options = self.options.clone();
        if self.due == Some(Due::Nudged) {
            options = options.with_response_format(ChatResponseFormat::JsonMode);
        }
        if self.finishing().is_some() && chat.tools.as_ref().is_some_and(|tools| !tools.is_empty())
        {
            options = options.with_tool_choice(ToolChoice::None);
        }
        (chat, options)
    }

    // Why this round is one the model's text is due in, or `None` for a
    // tool round: closing after a nudge, after one call has returned the
    // same result up to the repeat budget, or at the last round of the
    // budget, which would otherwise be spent on a call and end the
    // completion with no answer asked for; reserved once the rounds before
    // the corrections' share are spent with no candidate offered the check.
    const fn finishing(&self) -> Option<Finishing> {
        if self.due.is_some() || self.round == self.budgets.max_rounds {
            Some(Finishing::Closing)
        } else if self.check && self.candidates == 0 && self.round >= self.budgets.due_at() {
            Some(Finishing::Reserved)
        } else {
            None
        }
    }

    // Whether the provider cut the reply at the token bound: by its own
    // finish reason, or, where it reports none, by a bill that reaches the
    // bound the round was sent under.
    fn cut(&self, response: &ChatResponse) -> bool {
        if let Some(reason) = &response.stop_reason {
            return reason.is_max_tokens();
        }
        match (self.options.max_tokens, response.usage.completion_tokens) {
            (Some(bound), Some(spent)) => u32::try_from(spent).is_ok_and(|spent| spent >= bound),
            _ => false,
        }
    }

    // The fault for the debug log and the nudge a candidate earns, or `None`
    // for one in the shape asked for. The debug fault masks the failing
    // instance value so no reply text crosses into `DEBUG`.
    fn off_shape(&self, candidate: &str) -> Option<(String, String)> {
        if !matches!(self.format, Format::Json | Format::Schema(_)) {
            return None;
        }
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

    // Every result reaches the model cut to the cap, a repeat note at its
    // tail included, since the tail is what the cut keeps.
    async fn tool_round(&mut self, tool_calls: Vec<ToolCall>) -> Result<()> {
        let mut chat = std::mem::take(&mut self.chat).append_message(tool_calls.clone());
        for call in tool_calls {
            self.completion.enter(Phase::of_tool(&call.fn_name));
            let result = match self.tools.call(&call).await? {
                Ok(result) => self.succeeded(&call, result),
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

    // A succeeding call's result as the model reads it, counted against
    // the identical calls that had the same result before it: the count
    // named from `REPEAT_NOTE_FROM`, and the repeat budget spent making the
    // next round a finishing one, since the call has told the model all it
    // will.
    fn succeeded(&mut self, call: &ToolCall, result: String) -> String {
        self.repeats.remove(&identity(call));
        let count = self.echoes.entry(echo(call, &result)).or_insert(0);
        *count += 1;
        let count = *count;
        if count >= self.budgets.max_repeats {
            tracing::debug!(
                round = self.round,
                tool = %call.fn_name,
                count,
                "the same call returned the same result up to the repeat budget; the answer is due"
            );
            self.due = Some(Due::Repeated);
            return format!(
                "{result}\n\nThis exact call has now returned the same result {count} times. \
                 Your final answer is due now: reply with only your final answer in the shape \
                 the prompt asked for, and nothing else."
            );
        }
        if count < REPEAT_NOTE_FROM {
            return result;
        }
        format!(
            "{result}\n\nThis exact call has now returned the same result {count} times, and \
             nothing has changed between them. Act on what it says rather than running it \
             again: after {} identical results your final answer is due.",
            self.budgets.max_repeats
        )
    }

    // A failed call as the model reads it, named for the tool, counted
    // against the identical calls that failed before it: the count named
    // from `REPEAT_NOTE_FROM`, and the repeat budget spent the typed
    // failure, since the same call would fail the same way.
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

    // A tool round in a finishing round: nothing runs, and every call is
    // answered with the JSON answer as what is due, so a provider whose JSON
    // mode does not silence the tools still ends.
    fn refuse_round(&mut self, tool_calls: Vec<ToolCall>, finishing: Finishing) {
        tracing::debug!(
            round = self.round,
            tool_calls = tool_calls.len(),
            ?finishing,
            "tool calls in a closing round refused; the JSON answer is due"
        );
        let refusal = match finishing {
            Finishing::Closing => REFUSED_IN_CLOSING,
            Finishing::Reserved => REFUSED_IN_RESERVE,
        };
        self.refuse(tool_calls, refusal);
    }

    // A tool round of a reply the token bound cut: nothing runs, since the
    // arguments the server handed over are not the ones the model wrote,
    // and every call is answered with the bound and the way within it.
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

    // Every call answered with `refusal` in place of a result, the calls
    // kept in the transcript as the model made them.
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

// A call's identity for the repeat count: its tool and its arguments as
// the model sent them.
fn identity(call: &ToolCall) -> u64 {
    let mut hasher = DefaultHasher::new();
    call.fn_name.hash(&mut hasher);
    call.fn_arguments.to_string().hash(&mut hasher);
    hasher.finish()
}

// A succeeding call's identity for the echo count: the call and what it
// returned.
fn echo(call: &ToolCall, result: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    identity(call).hash(&mut hasher);
    result.hash(&mut hasher);
    hasher.finish()
}
