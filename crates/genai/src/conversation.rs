//! Provider conversation loop.
//!
//! Tool calls and rejected checks each add a provider round. Every round
//! draws on the round budget, so a completion terminates; a rejected
//! candidate draws on the candidate budget beside it, so a model that cannot
//! satisfy the check is ended with the correction rather than asked again
//! until the rounds run out. Each reply of the model runs under the timeout,
//! tool rounds included; a nudge or a correction starts a fresh one. A reply
//! that is not JSON where JSON was asked for, or that is JSON but does not
//! fit the request's schema, is nudged once per candidate: the nudge says
//! what the parser or the schema found, and the rounds after it ask for the
//! provider's JSON mode in place of the schema, the tools still declared so
//! the provider's prompt keeps its prefix, until the model answers in text;
//! a tool call made there is refused, not run. The schema steering returns
//! for the correction after it. The last round of the budget forbids tool
//! calls the same way, the tools still declared and `tool_choice` `none`,
//! so a model that would act once more answers instead where the provider
//! honours it, and a call made there is refused. A tool's result reaches
//! the model cut to a cap, its head and its tail kept, so one result cannot
//! fill the context window. A tool call that fails is counted against the
//! identical calls before it, one tool and the same arguments, the model
//! told the count from the third, and the repeat budget spent on one ends
//! the completion, since a model repeating one refused call is not going to
//! answer. A tool call that succeeds with the result the identical calls
//! before it had is counted the same way, and the repeat budget spent on
//! one makes the next round a finishing one, since a model re-running one
//! call whose result does not change has what it needs to answer; the
//! counts start over at a correction, so the repair may run the call again.
//! Any budget spent, or the timeout passed, is the typed `budget-exhausted`:
//! the same request put again would spend it again.

use std::collections::HashMap;
use std::fmt;
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponseFormat, ToolCall, ToolChoice, ToolResponse,
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

// The identical call from which its result names the count, so the model
// is told it is repeating itself before the repeats end the completion or
// make its answer due.
const REPEAT_NOTE_FROM: u32 = 3;

// The most of one tool's result the model is handed, its head and its
// tail: the host's cap admits a mebibyte, which a context window of a
// quarter-million tokens cannot hold beside the prompt.
const MAX_RESULT_BYTES: usize = 64 * 1024;

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
            let (chat, options) = self.round_request();
            let response =
                self.client.exec_chat(&self.model, chat, Some(&options)).await.with_context(
                    || format!("genai exec_chat failed for model `{}`", self.model),
                )?;

            let text = response.first_text().map(ToOwned::to_owned);
            let usage = response.usage.clone();
            let tool_calls = response.into_tool_calls();
            let reply_bytes = text.as_deref().filter(|_| tool_calls.is_empty()).map(str::len);
            self.completion.answered(&usage, reply_bytes, tool_calls.len());

            if !tool_calls.is_empty() {
                if self.finishing() {
                    self.refuse_round(tool_calls);
                } else {
                    self.tool_round(tool_calls).await?;
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
        if self.finishing() && chat.tools.as_ref().is_some_and(|tools| !tools.is_empty()) {
            options = options.with_tool_choice(ToolChoice::None);
        }
        (chat, options)
    }

    // Whether this round is one the model's text is due in: a round after a
    // nudge or after one call has returned the same result up to the repeat
    // budget, or the last round of the budget, which would otherwise be
    // spent on a call and end the completion with no answer asked for.
    const fn finishing(&self) -> bool {
        self.due.is_some() || self.round == self.budgets.max_rounds
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

    // A tool round in the closing rounds: nothing runs, and every call is
    // answered with the JSON answer as what is due, so a provider whose JSON
    // mode does not silence the tools still ends.
    fn refuse_round(&mut self, tool_calls: Vec<ToolCall>) {
        tracing::debug!(
            round = self.round,
            tool_calls = tool_calls.len(),
            "tool calls in a closing round refused; the JSON answer is due"
        );
        let mut chat = std::mem::take(&mut self.chat).append_message(tool_calls.clone());
        for call in tool_calls {
            self.transcript.turns.push(ToolTurn {
                tool: call.fn_name,
                args: call.fn_arguments,
                result: Value::String(REFUSED_IN_CLOSING.to_owned()),
            });
            chat = chat.append_message(ToolResponse::new(call.call_id, REFUSED_IN_CLOSING));
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
