//! Provider conversation loop.
//!
//! Tool calls and rejected checks each add a provider round. Every round
//! draws on the configured round budget, so a completion terminates; a
//! rejected candidate draws on the candidate budget beside it, so a model
//! that cannot satisfy the check is ended with the correction rather than
//! asked again until the rounds run out.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context as _, Result, bail};
use genai::chat::{ChatMessage, ChatOptions, ChatRequest, ToolCall, ToolResponse};
use omnia_wasi_model::{
    Answer, Error, Format, ToolHost, ToolTurn, Transcript, Usage, WasiModelCtx as _,
};
use serde_json::Value;

use super::observe::{self, Completion, Failure};
use super::options::Turn;
use super::shell::Shell;
use super::tools;
use crate::Client;

// Candidates one completion may offer the guest's check: a rejection of the
// last is the typed `budget-exhausted` carrying the correction, the figure
// the cursor backend's, so a correction counts the same under either.
const MAX_CANDIDATES: u32 = 2;

pub struct Conversation {
    client: genai::Client,
    model: String,
    chat: ChatRequest,
    options: ChatOptions,
    format: Format,
    check: bool,
    tool_host: Arc<dyn ToolHost>,
    shell: Option<Shell>,
    max_rounds: usize,
    max_result_bytes: usize,
    transcript: Transcript,
    completion: Option<Completion>,
}

impl Conversation {
    pub fn new(
        client: &Client, turn: Turn, tool_host: Arc<dyn ToolHost>, shell: Option<Shell>,
    ) -> Self {
        let completion = Completion::from(&turn);

        Self {
            client: client.inner.clone(),
            model: turn.model,
            chat: turn.chat,
            options: turn.options,
            format: turn.format,
            check: turn.check,
            tool_host,
            shell,
            max_rounds: client.max_rounds,
            max_result_bytes: client.limits().max_result_bytes,
            transcript: Transcript::default(),
            completion: Some(completion),
        }
    }

    pub async fn complete(mut self) -> Result<Answer> {
        let result = self.run().await;
        let attempts = self.completion.as_ref().map_or(0, Completion::attempts);
        let outcome = match &result {
            Ok(_) if attempts > 1 => "corrected",
            Ok(_) => "ok",
            Err(error) => observe::outcome_of(error),
        };
        if let Some(completion) = self.completion.take() {
            completion.finish(outcome);
        }
        result
    }

    async fn run(&mut self) -> Result<Answer> {
        let mut candidates = 0;
        for round in 1..=self.max_rounds {
            let asked = Instant::now();
            let response = self
                .client
                .exec_chat(&self.model, self.chat.clone(), Some(&self.options))
                .await
                .with_context(|| format!("genai exec_chat failed for model `{}`", self.model))?;
            if let Some(completion) = &mut self.completion {
                completion.model_time(asked.elapsed());
            }

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

            candidates += 1;
            if let Some(completion) = &mut self.completion {
                completion.attempt();
                completion.record(text.len(), self.transcript.turns.len(), usage.as_ref());
            }

            let candidate = self.format.candidate(&text);
            if !self.check {
                return Ok(self.answer(candidate, usage));
            }

            match self.tool_host.check(candidate.clone()).await? {
                Ok(()) => return Ok(self.answer(candidate, usage)),
                // The guest's correction is the model's next turn, verbatim;
                // with no candidate or round left it is the typed failure
                // the guest sees.
                Err(correction) if candidates >= MAX_CANDIDATES || round == self.max_rounds => {
                    bail!(Error::BudgetExhausted(correction));
                }
                Err(correction) => {
                    tracing::debug!(
                        round,
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

        Err(Failure::Exhausted {
            rounds: self.max_rounds,
        }
        .into())
    }

    async fn tool_round(&mut self, tool_calls: Vec<ToolCall>) -> Result<()> {
        let started = Instant::now();
        let mut chat = std::mem::take(&mut self.chat).append_message(tool_calls.clone());
        for call in tool_calls {
            let result = tools::dispatch_tool(
                &self.tool_host,
                self.shell.as_ref(),
                &call,
                self.max_result_bytes,
            )
            .await?;
            self.transcript.turns.push(ToolTurn {
                tool: call.fn_name,
                args: call.fn_arguments,
                result: Value::String(result.clone()),
            });
            chat = chat.append_message(ToolResponse::new(call.call_id, result));
        }
        self.chat = chat;
        if let Some(completion) = &mut self.completion {
            completion.tool_time(started.elapsed());
        }
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

// `None` when the provider did not surface any counts.
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

// The provider's usage counts onto the boundary's `Usage`. The check loop
// itself is covered by the `model` suite over the fake provider.
#[cfg(test)]
mod tests {
    use genai::chat::{CompletionTokensDetails, Usage as ProviderUsage};

    use super::to_usage;

    #[test]
    fn usage_counts() {
        let counted = ProviderUsage {
            prompt_tokens: Some(3),
            completion_tokens: Some(1),
            completion_tokens_details: Some(CompletionTokensDetails {
                reasoning_tokens: Some(2),
                ..CompletionTokensDetails::default()
            }),
            ..ProviderUsage::default()
        };
        let usage = to_usage(&counted).expect("counts surfaced");
        assert_eq!(
            (usage.input_tokens, usage.output_tokens, usage.reasoning_tokens),
            (3, 1, Some(2))
        );

        // Reasoning is optional; a negative count is clamped, not an error.
        let clamped = ProviderUsage {
            prompt_tokens: Some(-1),
            completion_tokens: Some(4),
            ..ProviderUsage::default()
        };
        let usage = to_usage(&clamped).expect("counts surfaced");
        assert_eq!((usage.input_tokens, usage.output_tokens, usage.reasoning_tokens), (0, 4, None));

        assert!(to_usage(&ProviderUsage::default()).is_none(), "no counts, no usage");
    }
}
