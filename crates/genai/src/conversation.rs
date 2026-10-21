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
//! for the correction after it. Either budget spent, or the timeout passed,
//! is the typed `budget-exhausted`: the same request put again would spend
//! it again.

use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponseFormat, ToolCall, ToolResponse,
};
use jsonschema::Validator;
use omnia_wasi_model::{Answer, Error, Format, ToolTurn, Transcript, Usage};
use serde_json::Value;
use tokio::time::timeout;

use crate::observe::{Completion, Phase, Timeout, to_usage};
use crate::request::Turn;
use crate::tools::Tools;

// The budgets one completion runs within, from the connect options: the
// provider rounds it may make, the candidates it may offer the guest's
// check, and the time one reply of the model may take.
#[derive(Clone, Copy, Debug)]
pub struct Budgets {
    pub max_rounds: u32,
    pub max_candidates: u32,
    pub timeout: Duration,
}

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
    nudged: bool,
    // The rounds after a nudge, until the model answers in text: JSON mode
    // in place of the schema, a tool call refused.
    closing: bool,
    transcript: Transcript,
    completion: Completion,
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
            nudged: false,
            closing: false,
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
                && let Some(nudge) = self.off_shape(&candidate)
            {
                self.nudged = true;
                self.closing = true;
                tracing::debug!(
                    round = self.round,
                    reply_bytes = candidate.len(),
                    %nudge,
                    "the reply is off the shape asked for; nudging under JSON mode"
                );
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
                if self.closing {
                    self.refuse_round(tool_calls);
                } else {
                    self.tool_round(tool_calls).await?;
                }
                continue;
            }

            let Some(text) = text else {
                bail!("genai returned neither content nor tool calls (model `{}`)", self.model);
            };
            self.closing = false;
            return Ok(Some(Reply {
                text,
                usage: to_usage(&usage),
            }));
        }
        Ok(None)
    }

    // What this round puts to the provider: the chat and the options as
    // translated, or, in the closing rounds after a nudge, under JSON mode in
    // place of the schema, the one `response_format` every adapter sends. The
    // tools stay declared either way: a provider that caches the prompt by
    // its prefix keeps the cache, and a call made under JSON mode is refused
    // rather than run.
    fn round_request(&self) -> (ChatRequest, ChatOptions) {
        let chat = self.chat.clone();
        let mut options = self.options.clone();
        if self.closing {
            options = options.with_response_format(ChatResponseFormat::JsonMode);
        }
        (chat, options)
    }

    // The nudge a candidate earns, or `None` for one in the shape asked for:
    // not JSON where JSON was asked for, with what the parser said of it, or
    // JSON that does not fit the schema, with each fault at its path.
    fn off_shape(&self, candidate: &str) -> Option<String> {
        if !matches!(self.format, Format::Json | Format::Schema(_)) {
            return None;
        }
        let value = match serde_json::from_str::<Value>(candidate) {
            Ok(value) => value,
            Err(fault) => {
                return Some(format!(
                    "Your last reply is not one well-formed JSON value ({fault}), so it is not \
                     the answer. Reply now with only your final answer as a single JSON value \
                     in the shape the prompt asked for, and nothing else."
                ));
            }
        };
        let validator = self.validator.as_ref()?;
        let faults: Vec<String> = validator
            .iter_errors(&value)
            .map(|fault| {
                let path = fault.instance_path().to_string();
                if path.is_empty() { fault.to_string() } else { format!("at `{path}`: {fault}") }
            })
            .collect();
        if faults.is_empty() {
            return None;
        }
        Some(format!(
            "Your last reply is JSON but not in the shape the prompt asked for ({}), so it is \
             not the answer. Reply now with only your final answer as a single JSON value in \
             exactly that shape, and nothing else.",
            faults.join("; ")
        ))
    }

    async fn tool_round(&mut self, tool_calls: Vec<ToolCall>) -> Result<()> {
        let mut chat = std::mem::take(&mut self.chat).append_message(tool_calls.clone());
        for call in tool_calls {
            self.completion.enter(Phase::of_tool(&call.fn_name));
            let result = self.tools.call(&call).await?;
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
