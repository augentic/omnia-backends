//! `wasi-model` implementation backed by the multi-provider genai SDK.
//!
//! Maps the gate-validated [`Request`] onto a genai
//! [`ChatRequest`]/[`ChatOptions`], drives the in-process
//! tool loop — dispatching the host-injected `resolve` tool into the caller's
//! `references` shelf through the lent [`ToolHost`] — self-checks the answer
//! against `response-format`, and returns a host-only [`Answer`] (the
//! parsed value plus a tool transcript for record/replay). The guest only ever
//! sees the validated answer string the `create` binding derives from `value`;
//! the host re-validates as the single authority (§3.1.3), so this self-check
//! is an optimization, not the gate.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use futures::FutureExt as _;
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponseFormat, JsonSpec, ReasoningEffort, Tool,
    ToolCall, ToolResponse,
};
use omnia_wasi_model::{
    Answer, Effort, Format, FutureResult, Reference, Request, Role, Tool as ModelTool, ToolHost,
    ToolTurn, Transcript, Usage, WasiModelCtx,
};
use serde_json::{Value, json};

use crate::Client;

/// Hard cap on model round-trips for one completion: tool-call rounds plus
/// answer-repair attempts share this budget. It bounds cost and guarantees the
/// loop terminates.
const MAX_TURNS: usize = 8;

/// Provider model id used when the request leaves `model` unset. genai routes to
/// the provider by the id's prefix (e.g. `gpt-…`, `claude-…`, `gemini-…`).
const DEFAULT_MODEL: &str = "gpt-5.5";

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        // Clone the swappable vendor handle into the 'static future; the genai
        // client is cheap to clone (an `Arc` inside).
        let client = self.inner.clone();

        async move {
            // The model id is carried on the request; fall back to the backend
            // default when the guest leaves it unset.
            let model = request.model.clone().unwrap_or_else(|| DEFAULT_MODEL.to_owned());
            let format = request.format.clone();
            let mut chat = build_request(&request)?;
            let options = build_options(&request)?;

            let mut transcript = Transcript::default();

            for turn in 1..=MAX_TURNS {
                let response = client
                    .exec_chat(&*model, chat.clone(), Some(&options))
                    .await
                    .with_context(|| format!("genai exec_chat failed for model `{model}`"))?;

                // Capture the text turn and usage before consuming the response for tool calls.
                let text = response.first_text().map(ToOwned::to_owned);
                let usage = to_usage(&response.usage);
                let tool_calls = response.into_tool_calls();

                if !tool_calls.is_empty() {
                    // The assistant turn carries all the tool calls; each tool
                    // response follows as its own `tool`-role message.
                    chat = chat.append_message(tool_calls.clone());
                    for call in tool_calls {
                        let result = dispatch_tool(&request, &tool_host, &call).await?;
                        transcript.turns.push(ToolTurn {
                            tool: call.fn_name,
                            args: call.fn_arguments,
                            result: Value::String(result.clone()),
                        });
                        chat = chat.append_message(ToolResponse::new(call.call_id, result));
                    }
                    continue;
                }

                // No tool calls: this is the model's (attempted) final answer.
                let Some(text) = text else {
                    bail!("genai returned neither content nor tool calls (model `{model}`)");
                };
                let last_turn = turn == MAX_TURNS;

                match format.parse(&text) {
                    Ok(value) => match format.check(&value) {
                        Ok(()) => {
                            return Ok(Answer {
                                value,
                                usage,
                                transcript: Some(transcript),
                            });
                        }
                        // Budget spent: hand the value back so the host validation gate
                        // remains the single authority and produces the canonical error.
                        Err(_) if last_turn => {
                            return Ok(Answer {
                                value,
                                usage,
                                transcript: Some(transcript),
                            });
                        }
                        Err(reason) => {
                            chat = append_repair(chat, text, &reason, &format);
                        }
                    },
                    Err(reason) if last_turn => {
                        bail!(
                            "genai did not return a valid answer for model `{model}` after \
                             {MAX_TURNS} attempts: {reason}"
                        );
                    }
                    Err(reason) => {
                        chat = append_repair(chat, text, &reason, &format);
                    }
                }
            }

            bail!(
                "genai completion exceeded {MAX_TURNS} model round-trips without a final answer \
                 (model `{model}`)"
            )
        }
        .boxed()
    }
}

/// Map the gate-validated [`Request`] onto a genai [`ChatRequest`]; the
/// host-injected `resolve` tool is advertised only when a reference target is granted.
fn build_request(request: &Request) -> Result<ChatRequest> {
    let messages = request
        .messages
        .iter()
        .map(|m| match m.role {
            Role::System => ChatMessage::system(m.content.clone()),
            Role::Assistant => ChatMessage::assistant(m.content.clone()),
            Role::User => ChatMessage::user(m.content.clone()),
        })
        .collect();

    let mut chat = ChatRequest::new(messages);
    if let Some(system) = &request.system {
        chat = chat.with_system(system.clone());
    }

    let mut tools: Vec<Tool> = Vec::new();
    if request.grants.references.is_some() {
        tools.push(resolve_tool());
    }
    match request.tools.first() {
        // Guest-declared function tools are not executable until the model
        // session lands; fail up front rather than advertising a tool whose
        // selection would fail mid-loop.
        Some(ModelTool::Function(function)) => bail!(
            "the genai backend cannot execute the guest-declared function tool `{}`",
            function.name
        ),
        // The genai backend has no MCP client; a spawned-agent backend
        // (omnia-cursor) honors MCP grants. Fail loudly rather than silently
        // dropping the grant.
        Some(ModelTool::Mcp(mcp)) => bail!(
            "the genai backend cannot honor the MCP tool grant for server `{}`; use a \
             spawned-agent backend such as omnia-cursor",
            mcp.name
        ),
        None => {}
    }
    if !tools.is_empty() {
        chat = chat.with_tools(tools);
    }

    Ok(chat)
}

/// The host-injected `resolve` tool advertised to the model (§4). Its single `name`
/// argument mirrors [`Reference`]; the body is opaque to the runtime core and is
/// interpreted only by the caller's `references` shelf.
fn resolve_tool() -> Tool {
    Tool::new("resolve")
        .with_description(
            "Resolve an opaque reference against the caller's references shelf and return its \
             contents. Use this to fetch material the caller exposed by reference.",
        )
        .with_schema(json!({
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "The opaque reference body the shelf interprets."
                }
            },
            "required": ["name"],
            "additionalProperties": false,
        }))
}

/// Translate the boundary's `format` and `generation` controls into genai
/// [`ChatOptions`].
fn build_options(request: &Request) -> Result<ChatOptions> {
    let mut options = ChatOptions::default().with_capture_usage(true);

    options = match &request.format {
        Format::Schema(spec) => {
            let schema: Value =
                serde_json::from_str(&spec.schema).context("format schema is not valid JSON")?;
            options.with_response_format(ChatResponseFormat::JsonSpec(JsonSpec::new(
                spec.name.clone(),
                schema,
            )))
        }
        // `json`: request the provider's JSON mode (the strongest portable
        // structured-output hint).
        Format::Json => options.with_response_format(ChatResponseFormat::JsonMode),
        // `text`: a plain string answer, no structured-output hint.
        Format::Text => options,
    };

    if let Some(generation) = &request.generation {
        if let Some(temperature) = generation.temperature {
            options = options.with_temperature(f64::from(temperature));
        }
        if let Some(top_p) = generation.top_p {
            options = options.with_top_p(f64::from(top_p));
        }
        if let Some(max_tokens) = generation.max_tokens {
            options = options.with_max_tokens(max_tokens);
        }
        if !generation.stop.is_empty() {
            options = options.with_stop_sequences(generation.stop.clone());
        }
        if let Some(seed) = generation.seed {
            options = options.with_seed(seed);
        }
        if let Some(effort) = generation.effort {
            options = options.with_reasoning_effort(reasoning_effort(effort));
        }
    }

    Ok(options)
}

/// Map the boundary's `effort` hint onto genai's [`ReasoningEffort`].
const fn reasoning_effort(effort: Effort) -> ReasoningEffort {
    match effort {
        Effort::Minimal => ReasoningEffort::Minimal,
        Effort::Low => ReasoningEffort::Low,
        Effort::Medium => ReasoningEffort::Medium,
        Effort::High => ReasoningEffort::High,
    }
}

/// Fold a genai response's token counts into the boundary's [`Usage`], reporting
/// `None` when the provider surfaced no counts.
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

/// Execute one model tool call. Only the host-injected `resolve` tool is
/// executable (host-mediated dynamic linking into the caller's `references`
/// shelf); guest-declared function tools are rejected up front in
/// [`build_request`], so any other name here is unadvertised and the backend
/// fails loudly rather than fabricating a result.
async fn dispatch_tool(
    request: &Request, tool_host: &Arc<dyn ToolHost>, call: &ToolCall,
) -> Result<String> {
    match call.fn_name.as_str() {
        "resolve" => {
            // The tool is only advertised with a granted target; re-check.
            if request.grants.references.is_none() {
                bail!("model called `resolve` but `grants.references` is not set");
            }
            let name = call
                .fn_arguments
                .get("name")
                .and_then(Value::as_str)
                .context("`resolve` tool call is missing a string `name` argument")?;
            let bytes = tool_host
                .resolve(Reference {
                    name: name.to_owned(),
                })
                .await
                .with_context(|| format!("resolving reference `{name}`"))?;
            // The shelf returns typed bytes; genai tool responses are strings, so
            // surface them as (lossy) UTF-8 text for the model to read.
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        }
        other => bail!(
            "model called tool `{other}`, which the genai backend cannot execute (only the \
             host-injected `resolve` tool is wired)"
        ),
    }
}

/// Append the rejected answer and a correction instruction so the next round
/// can repair it (bounded by [`MAX_TURNS`]).
fn append_repair(
    request: ChatRequest, answer: String, reason: &str, format: &Format,
) -> ChatRequest {
    request
        .append_message(ChatMessage::assistant(answer))
        .append_message(ChatMessage::user(format.repair(reason)))
}

// Deliberate unit tests: pure request-translation logic (the CI floor);
// `tests/live.rs` proves a real provider run end-to-end.
#[cfg(test)]
mod tests {
    use omnia_wasi_model::{Function, Grants, Mcp, Message};

    use super::*;

    fn request(tools: Vec<ModelTool>) -> Request {
        Request {
            model: None,
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "hello".to_owned(),
            }],
            generation: None,
            format: Format::Text,
            tools,
            grants: Grants {
                references: None,
                workspace: None,
            },
        }
    }

    #[test]
    fn function_tool() {
        let err = build_request(&request(vec![ModelTool::Function(Function {
            name: "lookup".to_owned(),
            description: "look something up".to_owned(),
            parameters: "{\"type\":\"object\"}".to_owned(),
        })]))
        .expect_err("a guest function tool this backend cannot execute must fail up front");
        assert!(err.to_string().contains("`lookup`"), "unexpected error: {err}");
    }

    #[test]
    fn mcp_grant() {
        let err = build_request(&request(vec![ModelTool::Mcp(Mcp {
            name: "docs".to_owned(),
            tools: vec![],
            url: "http://localhost:8080/mcp".to_owned(),
        })]))
        .expect_err("an MCP grant needs a spawned-agent backend");
        assert!(err.to_string().contains("omnia-cursor"), "unexpected error: {err}");
    }
}
