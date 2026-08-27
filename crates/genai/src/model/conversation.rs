//! An in-process provider conversation: `run` drives chat rounds to a
//! settled answer — executing tool calls between rounds and repairing a
//! rejected answer — inside one shared round budget that bounds cost and
//! guarantees the loop terminates.

use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use genai::chat::{ChatMessage, ChatOptions, ChatRequest, ToolCall, ToolResponse};
use omnia_wasi_model::{
    Answer, Candidate, Format, ToolHost, ToolTurn, Transcript, Usage, WasiModelCtx as _,
};
use serde_json::Value;

use super::observe::{self, Completion, Failure};
use super::options::Turn;
use super::tools;
use crate::Client;

/// Hard cap on model round-trips for one completion: tool-call rounds plus
/// answer-repair attempts share this budget.
const MAX_ROUNDS: usize = 8;

pub struct Conversation {
    client: genai::Client,
    model: String,
    chat: ChatRequest,
    options: ChatOptions,
    format: Format,
    tool_host: Arc<dyn ToolHost>,
    max_result_bytes: usize,
    transcript: Transcript,
    completion: Option<Completion>,
    unchecked: bool,
}

impl Conversation {
    pub fn new(client: &Client, turn: Turn, tool_host: Arc<dyn ToolHost>) -> Self {
        let completion = Completion::start(&turn);

        Self {
            client: client.inner.clone(),
            model: turn.model,
            chat: turn.chat,
            options: turn.options,
            format: turn.format,
            tool_host,
            max_result_bytes: client.limits().max_result_bytes,
            transcript: Transcript::default(),
            completion: Some(completion),
            unchecked: false,
        }
    }

    pub async fn complete(mut self) -> Result<Answer> {
        let result = self.run().await;
        let attempts = self.completion.as_ref().map_or(0, Completion::attempts);
        let outcome = match &result {
            Ok(_) if self.unchecked => "unchecked",
            Ok(_) if attempts > 1 => "repair",
            Ok(_) => "ok",
            Err(error) => observe::outcome_of(error),
        };
        if let Some(completion) = self.completion.take() {
            completion.finish(outcome);
        }
        result
    }

    async fn run(&mut self) -> Result<Answer> {
        for round in 1..=MAX_ROUNDS {
            let response = self
                .client
                .exec_chat(&self.model, self.chat.clone(), Some(&self.options))
                .await
                .with_context(|| format!("genai exec_chat failed for model `{}`", self.model))?;

            // Capture the text turn and usage before consuming the response
            // for tool calls.
            let text = response.first_text().map(ToOwned::to_owned);
            let usage = to_usage(&response.usage);
            let tool_calls = response.into_tool_calls();

            if !tool_calls.is_empty() {
                self.tool_round(tool_calls).await?;
                continue;
            }

            let Some(text) = text else {
                bail!("genai returned neither content nor tool calls (model `{}`)", self.model);
            };

            if let Some(completion) = &mut self.completion {
                completion.new_attempt();
                completion.record(text.len(), self.transcript.turns.len(), usage.as_ref());
            }

            let reason = match self.verdict(&text, usage, round == MAX_ROUNDS)? {
                Verdict::Done(answer) => return Ok(answer),
                Verdict::Repair(reason) => reason,
            };

            tracing::debug!(%reason, "repairing answer");
            self.repair(text, &reason);
        }

        Err(Failure::Exhausted { rounds: MAX_ROUNDS }.into())
    }

    // The assistant turn carries all the tool calls; each tool response
    // follows as its own `tool`-role message.
    async fn tool_round(&mut self, tool_calls: Vec<ToolCall>) -> Result<()> {
        let mut chat = std::mem::take(&mut self.chat).append_message(tool_calls.clone());
        for call in tool_calls {
            let result =
                tools::dispatch_tool(&self.tool_host, &call, self.max_result_bytes).await?;
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

    fn verdict(&mut self, text: &str, usage: Option<Usage>, last_round: bool) -> Result<Verdict> {
        match self.format.parse(text) {
            Ok(Candidate::Valid(value)) => Ok(Verdict::Done(self.answer(value, usage))),
            // Budget spent: hand the value back so the host validation gate
            // remains the single authority and produces the canonical error.
            Ok(Candidate::Invalid { value, .. }) if last_round => {
                self.unchecked = true;
                Ok(Verdict::Done(self.answer(value, usage)))
            }
            Err(reason) if last_round => Err(Failure::Invalid {
                rounds: MAX_ROUNDS,
                reason,
            }
            .into()),
            Ok(Candidate::Invalid { reason, .. }) | Err(reason) => Ok(Verdict::Repair(reason)),
        }
    }

    fn answer(&mut self, value: Value, usage: Option<Usage>) -> Answer {
        Answer {
            value,
            usage,
            transcript: Some(std::mem::take(&mut self.transcript)),
        }
    }

    fn repair(&mut self, answer: String, reason: &str) {
        self.chat = std::mem::take(&mut self.chat)
            .append_message(ChatMessage::assistant(answer))
            .append_message(ChatMessage::user(self.format.repair(reason)));
    }
}

enum Verdict {
    Done(Answer),
    Repair(String),
}

// `None` when the provider surfaced no counts.
fn to_usage(usage: &genai::chat::Usage) -> Option<Usage> {
    if usage.prompt_tokens.is_none() && usage.completion_tokens.is_none() {
        return None;
    }
    Some(Usage {
        input_tokens: usage.prompt_tokens.and_then(|v| u32::try_from(v).ok()).unwrap_or(0),
        output_tokens: usage.completion_tokens.and_then(|v| u32::try_from(v).ok()).unwrap_or(0),
        reasoning_tokens: usage
            .completion_tokens_details
            .as_ref()
            .and_then(|d| d.reasoning_tokens)
            .and_then(|v| u32::try_from(v).ok()),
    })
}
