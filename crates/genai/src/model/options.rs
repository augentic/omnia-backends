//! Provider request translation.
//!
//! [`Turn`] maps a validated request into provider messages, tools, and chat
//! options. A lent workspace adds the host-provided `read` and `list` tools.

use std::path::Path;

use anyhow::{Context as _, Result, bail};
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponseFormat, JsonSpec, ReasoningEffort, Tool,
};
use omnia_wasi_model::{Effort, Format, Function, Request, Role, Tool as ModelTool};
use schemars::schema_for;
use serde_json::Value;

use super::tools::{ListArgs, ReadArgs};

/// Everything one completion derives from the request: the model id, the
/// provider chat request and options, and the format gate.
pub struct Turn {
    pub model: String,
    pub chat: ChatRequest,
    pub options: ChatOptions,
    pub format: Format,
    pub prompt_bytes: u64,
    pub tools: usize,
}

impl Turn {
    /// Translate the request against the lent workspace path.
    pub fn prepare(request: &Request, lent: Option<&Path>, default_model: &str) -> Result<Self> {
        let model = request.model.as_deref().unwrap_or(default_model).to_owned();
        let chat = build_request(request, lent.is_some())?;
        let options = build_options(request)?;
        Ok(Self {
            model,
            chat,
            options,
            format: request.format.clone(),
            prompt_bytes: prompt_bytes(request),
            tools: request.tools.len(),
        })
    }
}

fn prompt_bytes(request: &Request) -> u64 {
    let bytes = request.system.as_deref().map_or(0, str::len)
        + request.messages.iter().map(|message| message.content.len()).sum::<usize>();
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

fn build_request(request: &Request, workspace: bool) -> Result<ChatRequest> {
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
    for tool in &request.tools {
        match tool {
            ModelTool::Function(function) => tools.push(function_tool(function)?),
            ModelTool::Mcp(_) => bail!("genai does not support MCP servers"),
        }
    }
    if workspace {
        tools.extend(workspace_tools());
    }
    if !tools.is_empty() {
        chat = chat.with_tools(tools);
    }

    Ok(chat)
}

// The host gate reserves these names (`read`, `list`, plus the unadvertised
// `write`), so no guest tool can shadow them.
fn workspace_tools() -> [Tool; 2] {
    [
        Tool::new("read")
            .with_description("Read a text file from the workspace for this task.")
            .with_schema(schema_for!(ReadArgs).to_value()),
        Tool::new("list")
            .with_description(
                "List a directory of the workspace for this task. Omit `path` to list \
                 the workspace root.",
            )
            .with_schema(schema_for!(ListArgs).to_value()),
    ]
}

// The host gate already guarantees `parameters` parses as JSON.
fn function_tool(function: &Function) -> Result<Tool> {
    let schema: Value = serde_json::from_str(&function.parameters).with_context(|| {
        format!("function tool `{}` parameters is not valid JSON", function.name)
    })?;
    Ok(Tool::new(function.name.clone())
        .with_description(function.description.clone())
        .with_schema(schema))
}

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
        // JSON mode is the strongest portable structured-output hint.
        Format::Json => options.with_response_format(ChatResponseFormat::JsonMode),
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

const fn reasoning_effort(effort: Effort) -> ReasoningEffort {
    match effort {
        Effort::Minimal => ReasoningEffort::Minimal,
        Effort::Low => ReasoningEffort::Low,
        Effort::Medium => ReasoningEffort::Medium,
        Effort::High => ReasoningEffort::High,
    }
}

// Deliberate unit tests: pure request-translation logic (CI floor);
// `tests/live.rs` proves the mapping against a real provider.
#[cfg(test)]
mod tests {
    use std::path::Path;

    use omnia_wasi_model::{Grants, Mcp, Message};
    use serde_json::json;

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
            grants: Grants { workspace: None },
        }
    }

    fn lookup_tool() -> ModelTool {
        ModelTool::Function(Function {
            name: "lookup".to_owned(),
            description: "look something up".to_owned(),
            parameters: "{\"type\":\"object\"}".to_owned(),
        })
    }

    #[test]
    fn model_selection() {
        let turn = Turn::prepare(&request(vec![]), None, "gpt-5.5").expect("a bare request maps");
        assert_eq!(turn.model, "gpt-5.5", "an unset model falls back to the backend default");

        let mut named = request(vec![]);
        named.model = Some("claude-fable-5".to_owned());
        let turn = Turn::prepare(&named, None, "gpt-5.5").expect("a named model maps");
        assert_eq!(turn.model, "claude-fable-5", "the request's model wins");
    }

    #[test]
    fn function_tool_advertised() {
        let chat = build_request(&request(vec![lookup_tool()]), false)
            .expect("a declared function tool translates");
        let tools = chat.tools.expect("the chat request advertises the tool");
        assert_eq!(tools.len(), 1, "one declared tool, one advertised tool");
        assert_eq!(tools[0].name, "lookup".into());
        assert_eq!(
            tools[0].schema,
            Some(json!({ "type": "object" })),
            "the parameters document rides as the tool schema"
        );
    }

    #[test]
    fn mcp_grant() {
        let err = build_request(
            &request(vec![ModelTool::Mcp(Mcp {
                name: "docs".to_owned(),
                tools: vec![],
                url: "http://localhost:8080/mcp".to_owned(),
            })]),
            false,
        )
        .expect_err("genai rejects MCP grants");
        assert!(err.to_string().contains("does not support MCP"), "unexpected error: {err}");
    }

    #[test]
    fn workspace_tools_advertised() {
        let turn = Turn::prepare(&request(vec![lookup_tool()]), Some(Path::new("/unused")), "auto")
            .expect("declared and injected tools translate");
        let tools = turn.chat.tools.expect("the chat request advertises the tools");
        let names: Vec<_> = tools.iter().map(|tool| tool.name.to_string()).collect();
        assert_eq!(names, ["lookup", "read", "list"], "declared tools first, then read/list");

        let read = &tools[1];
        let schema = read.schema.as_ref().expect("read carries a schema");
        assert_eq!(schema.get("required"), Some(&json!(["path"])), "read requires a path argument");
        let list = &tools[2];
        let schema = list.schema.as_ref().expect("list carries a schema");
        assert_eq!(schema.get("required"), None, "list's path is optional (root listing)");
    }

    #[test]
    fn no_workspace_no_injected_tools() {
        let turn =
            Turn::prepare(&request(vec![]), None, "auto").expect("an empty tool list translates");
        assert!(turn.chat.tools.is_none(), "without a workspace lend nothing is advertised");
    }
}
