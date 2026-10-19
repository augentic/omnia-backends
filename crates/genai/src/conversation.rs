//! Provider conversation loop.
//!
//! Tool calls and rejected checks each add a provider round. Every round
//! draws on the configured round budget, so a completion terminates; a
//! rejected candidate draws on the configured candidate budget beside it,
//! so a model that cannot satisfy the check is ended with the correction
//! rather than asked again until the rounds run out. Each reply of the model runs under
//! the configured cap, tool rounds included; a nudge or a correction starts
//! a fresh one. Either budget spent, or the cap passed, is the typed
//! `budget-exhausted`: the same request put again would spend it again.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use genai::chat::{ChatMessage, ChatOptions, ChatRequest, ToolCall, ToolResponse};
use omnia_sandbox::Access;
use omnia_wasi_model::{
    Answer, Error, Format, Request, ToolHost, ToolTurn, Transcript, Usage, WasiModelCtx as _,
};
use serde_json::Value;
use tokio::time::timeout;

use crate::Client;
use crate::observe::{Completion, Phase, Timeout, to_usage};
use crate::request::{chat_options, chat_request, prompt_bytes};
use crate::tools::Tools;

pub struct Conversation {
    client: genai::Client,
    model: String,
    chat: ChatRequest,
    options: ChatOptions,
    format: Format,
    check: bool,
    tool_host: Arc<dyn ToolHost>,
    tools: Tools,
    max_rounds: usize,
    max_candidates: u32,
    cap: Duration,
    round: usize,
    candidates: u32,
    nudged: bool,
    transcript: Transcript,
    completion: Completion,
}

// The model's text once a round ends in one, with the round's counts.
struct Reply {
    text: String,
    usage: Option<Usage>,
}

impl Conversation {
    // The request is translated before a session is prepared, and the
    // completion started once nothing is left to refuse: the start event
    // means a completion that will be billed.
    pub async fn prepare(
        client: &Client, request: &Request, model: String, access: Access,
        tool_host: Arc<dyn ToolHost>,
    ) -> Result<Self> {
        let chat = chat_request(request, access)?;
        let options = chat_options(request)?;
        let shell = client.shell(request, access, tool_host.local_path()).await?;
        let completion = Completion::start(prompt_bytes(request), request.tools.len());

        Ok(Self {
            client: client.inner.clone(),
            model,
            chat,
            options,
            format: request.format.clone(),
            check: request.check,
            tools: Tools::new(Arc::clone(&tool_host), shell, client.limits().max_result_bytes),
            tool_host,
            max_rounds: client.max_rounds,
            max_candidates: client.max_candidates,
            cap: client.cap,
            round: 0,
            candidates: 0,
            nudged: false,
            transcript: Transcript::default(),
            completion,
        })
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
            // the model's next reply, under the cap
            let reply = match timeout(self.cap, self.next_reply()).await {
                Ok(reply) => reply?,
                Err(_elapsed) => {
                    return Err(Timeout {
                        cap_secs: self.cap.as_secs(),
                        waiting: self.completion.phase(),
                    }
                    .into_error());
                }
            };
            let Some(Reply { text, usage }) = reply else {
                bail!(Error::BudgetExhausted(format!(
                    "no answer after {} model round-trips",
                    self.max_rounds
                )));
            };

            let candidate = self.format.candidate(&text);
            if !self.check {
                return Ok(self.answer(candidate, usage));
            }

            // a reply that is not JSON is nudged once, told what the parser said of it
            if !self.nudged
                && matches!(self.format, Format::Json | Format::Schema(_))
                && let Err(fault) = serde_json::from_str::<Value>(&candidate)
            {
                self.nudged = true;
                tracing::debug!(
                    round = self.round,
                    reply_bytes = candidate.len(),
                    %fault,
                    "the reply is not JSON; nudging"
                );
                let nudge = format!(
                    "Your last reply is not one well-formed JSON value ({fault}), so it is not \
                     the answer. Reply now with only your final answer as a single JSON value in \
                     the shape the prompt asked for, and nothing else."
                );
                self.chat = std::mem::take(&mut self.chat)
                    .append_message(ChatMessage::assistant(candidate))
                    .append_message(ChatMessage::user(nudge));
                continue;
            }

            // the guest's verdict on the candidate
            self.candidates += 1;
            self.completion.enter(Phase::Check);
            match self.tool_host.check(candidate.clone()).await? {
                Ok(()) => return Ok(self.answer(candidate, usage)),
                // The guest's correction is the model's next turn, verbatim;
                // with no candidate or round left it is the typed failure
                // the guest sees.
                Err(correction)
                    if self.candidates >= self.max_candidates || self.round == self.max_rounds =>
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
        while self.round < self.max_rounds {
            self.round += 1;
            self.completion.enter(Phase::Model);
            let response = self
                .client
                .exec_chat(&self.model, self.chat.clone(), Some(&self.options))
                .await
                .with_context(|| format!("genai exec_chat failed for model `{}`", self.model))?;

            let text = response.first_text().map(ToOwned::to_owned);
            let usage = response.usage.clone();
            let tool_calls = response.into_tool_calls();
            let reply_bytes = text.as_deref().filter(|_| tool_calls.is_empty()).map(str::len);
            self.completion.answered(&usage, reply_bytes, tool_calls.len());

            if !tool_calls.is_empty() {
                self.tool_round(tool_calls).await?;
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

    fn answer(&mut self, answer: String, usage: Option<Usage>) -> Answer {
        Answer {
            answer,
            usage,
            transcript: Some(std::mem::take(&mut self.transcript)),
        }
    }
}
