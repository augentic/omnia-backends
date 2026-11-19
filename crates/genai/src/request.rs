//! Translates a validated [`Request`] into the [`PreparedRequest`] a conversation opens with.

use anyhow::{Result, bail};
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponseFormat, JsonSpec, ReasoningEffort, Tool,
};
use jsonschema::Validator;
use omnia_sandbox::Access;
use omnia_wasi_model::{Effort, Error, Format, Function, Request, Role, Tool as ModelTool};
use serde_json::Value;

use crate::tools::{SHELL_TOOL, workspace_tools};

#[derive(Debug)]
pub struct PreparedRequest {
    pub model: String,
    pub chat: ChatRequest,
    pub options: ChatOptions,
    pub format: Format,
    pub validator: Option<Validator>,
    pub check: bool,
    pub prompt_bytes: u64,
}

impl PreparedRequest {
    // A fault in the request is `Error::Invalid`: a reserved tool, a schema
    // that is not JSON or not a JSON Schema, or an effort genai does not know.
    pub fn translate(
        request: &Request, model: &str, max_tokens: u32, access: Access,
    ) -> Result<Self> {
        let (response_format, validator) = translate_format(&request.format)?;
        Ok(Self {
            model: model.to_owned(),
            chat: chat_request(request, access)?,
            options: chat_options(request, max_tokens, response_format)?,
            format: request.format.clone(),
            validator,
            check: request.check,
            prompt_bytes: prompt_bytes(request),
        })
    }

    // The names of every tool the chat advertises, the workspace's included.
    pub fn declared(&self) -> Vec<String> {
        self.chat
            .tools
            .as_ref()
            .map_or_default(|tools| tools.iter().map(|tool| tool.name.to_string()).collect())
    }
}

// The provider's response format and the validator the same schema builds,
// parsed once.
fn translate_format(format: &Format) -> Result<(Option<ChatResponseFormat>, Option<Validator>)> {
    let spec = match format {
        Format::Text => return Ok((None, None)),
        Format::Json => return Ok((Some(ChatResponseFormat::JsonMode), None)),
        Format::Schema(spec) => spec,
    };
    let schema: Value = serde_json::from_str(&spec.schema)
        .map_err(|error| Error::Invalid(format!("format schema is not valid JSON: {error}")))?;
    let validator = jsonschema::validator_for(&schema).map_err(|error| {
        Error::Invalid(format!("format schema `{}` is not a valid JSON Schema: {error}", spec.name))
    })?;
    let json_spec = JsonSpec::new(spec.name.clone(), schema);
    Ok((Some(ChatResponseFormat::JsonSpec(json_spec)), Some(validator)))
}

fn chat_request(request: &Request, access: Access) -> Result<ChatRequest> {
    // map messages and system prompt
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

    // the declared tools first, then the workspace's
    let mut tools: Vec<Tool> = Vec::new();
    for ModelTool::Function(function) in &request.tools {
        if function.name == SHELL_TOOL {
            bail!(Error::Invalid(format!("tool `{SHELL_TOOL}` is reserved by the backend")));
        }
        tools.push(function_tool(function)?);
    }
    tools.extend(workspace_tools(access));
    if !tools.is_empty() {
        chat = chat.with_tools(tools);
    }

    Ok(chat)
}

fn function_tool(function: &Function) -> Result<Tool> {
    let schema: Value = serde_json::from_str(&function.parameters).map_err(|error| {
        Error::Invalid(format!(
            "function tool `{}` parameters is not valid JSON: {error}",
            function.name
        ))
    })?;
    let mut tool = Tool::new(function.name.clone()).with_schema(schema);
    tool.description.clone_from(&function.description);
    Ok(tool)
}

fn chat_options(
    request: &Request, max_tokens: u32, response_format: Option<ChatResponseFormat>,
) -> Result<ChatOptions> {
    let mut options = ChatOptions::default().with_capture_usage(true);
    if let Some(response_format) = response_format {
        options = options.with_response_format(response_format);
    }

    // generation parameters
    let generation = request.generation.as_ref();
    options = options.with_max_tokens(
        generation.and_then(|generation| generation.max_tokens).unwrap_or(max_tokens),
    );

    if let Some(generation) = generation {
        if let Some(temperature) = generation.temperature {
            options = options.with_temperature(f64::from(temperature));
        }
        if let Some(top_p) = generation.top_p {
            options = options.with_top_p(f64::from(top_p));
        }
        if !generation.stop.is_empty() {
            options = options.with_stop_sequences(generation.stop.clone());
        }
        if let Some(seed) = generation.seed {
            options = options.with_seed(seed);
        }
        if let Some(effort) = &generation.effort {
            options = options.with_reasoning_effort(reasoning_effort(effort)?);
        }
    }

    Ok(options)
}

// A level by genai's own name, such as `minimal` or `xhigh`, passes through.
fn reasoning_effort(effort: &Effort) -> Result<ReasoningEffort, Error> {
    let level = effort.as_str();
    ReasoningEffort::from_keyword(level)
        .ok_or_else(|| Error::Invalid(format!("genai has no reasoning effort `{level}`")))
}

fn prompt_bytes(request: &Request) -> u64 {
    let bytes = request.system.as_deref().map_or(0, str::len)
        + request.messages.iter().map(|message| message.content.len()).sum::<usize>();
    u64::try_from(bytes).unwrap_or(u64::MAX)
}
