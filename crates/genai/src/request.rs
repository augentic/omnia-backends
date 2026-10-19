//! Provider request translation.
//!
//! A validated request becomes the provider's chat request and options: the
//! system text and messages as they are, the declared function tools beside
//! the tools a lend advertises, and the request's `format` as the provider
//! `response_format` — steering only; acceptance is the guest's `check`
//! when the request asks for one.

use anyhow::{Context as _, Result, bail};
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponseFormat, JsonSpec, ReasoningEffort, Tool,
};
use omnia_sandbox::Access;
use omnia_wasi_model::{Effort, Error, Format, Function, Request, Role, Tool as ModelTool};
use serde_json::Value;

use crate::tools::{SHELL_TOOL, workspace_tools};

// The system text and every message, in bytes.
pub fn prompt_bytes(request: &Request) -> u64 {
    let bytes = request.system.as_deref().map_or(0, str::len)
        + request.messages.iter().map(|message| message.content.len()).sum::<usize>();
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

// The messages, the system text, and the tools: the declared function
// tools first, then the ones the lend advertises.
pub fn chat_request(request: &Request, access: Access) -> Result<ChatRequest> {
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
            ModelTool::Function(function) if function.name == SHELL_TOOL => {
                bail!(Error::InvalidRequest(format!(
                    "tool `{SHELL_TOOL}` is reserved by the backend"
                )));
            }
            ModelTool::Function(function) => tools.push(function_tool(function)?),
            ModelTool::Mcp(_) => bail!("genai does not support MCP servers"),
        }
    }
    tools.extend(workspace_tools(access));
    if !tools.is_empty() {
        chat = chat.with_tools(tools);
    }

    Ok(chat)
}

// The host already guarantees `parameters` parses as JSON.
fn function_tool(function: &Function) -> Result<Tool> {
    let schema: Value = serde_json::from_str(&function.parameters).with_context(|| {
        format!("function tool `{}` parameters is not valid JSON", function.name)
    })?;
    Ok(Tool::new(function.name.clone())
        .with_description(function.description.clone())
        .with_schema(schema))
}

pub fn chat_options(request: &Request) -> Result<ChatOptions> {
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

// Request-to-provider translation; end-to-end round-trips run in
// `tests/model.rs`.
#[cfg(test)]
mod tests {
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
            grants: Grants {
                workspace: None,
                worker: None,
            },
            check: false,
        }
    }

    fn lookup_tool() -> ModelTool {
        ModelTool::Function(Function {
            name: "lookup".to_owned(),
            description: "look something up".to_owned(),
            parameters: "{\"type\":\"object\"}".to_owned(),
        })
    }

    fn tool_names(chat: &ChatRequest) -> Vec<String> {
        chat.tools
            .as_ref()
            .expect("the chat request advertises tools")
            .iter()
            .map(|tool| tool.name.to_string())
            .collect()
    }

    #[test]
    fn declared_tool() {
        let chat = chat_request(&request(vec![lookup_tool()]), Access::Private)
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
        let err = chat_request(
            &request(vec![ModelTool::Mcp(Mcp {
                name: "docs".to_owned(),
                tools: vec![],
                url: "http://localhost:8080/mcp".to_owned(),
            })]),
            Access::Private,
        )
        .expect_err("genai rejects MCP grants");
        assert!(err.to_string().contains("does not support MCP"), "unexpected error: {err}");
    }

    #[test]
    fn lent_workspace() {
        let chat = chat_request(&request(vec![lookup_tool()]), Access::Read)
            .expect("declared and injected tools translate");
        assert_eq!(tool_names(&chat), ["lookup", "read", "list"], "declared tools first");

        let tools = chat.tools.expect("the chat request advertises the tools");
        let schema = tools[1].schema.as_ref().expect("read carries a schema");
        assert_eq!(schema.get("required"), Some(&json!(["path"])), "read requires a path argument");
        let schema = tools[2].schema.as_ref().expect("list carries a schema");
        assert_eq!(schema.get("required"), None, "list's path is optional (root listing)");
    }

    #[test]
    fn shell_lend() {
        let chat = chat_request(&request(vec![]), Access::Shell).expect("a shell lend translates");
        assert_eq!(tool_names(&chat), ["read", "list", "shell"], "the shell after read/list");

        let tools = chat.tools.expect("the chat request advertises the tools");
        let schema = tools[2].schema.as_ref().expect("shell carries a schema");
        assert_eq!(schema.get("required"), Some(&json!(["command"])), "shell requires a command");
    }

    #[test]
    fn shell_reserved() {
        let shell = ModelTool::Function(Function {
            name: "shell".to_owned(),
            description: "a guest's own shell".to_owned(),
            parameters: "{}".to_owned(),
        });
        let err = chat_request(&request(vec![shell]), Access::Private)
            .expect_err("a guest tool named `shell` is refused whatever the access");
        let Some(Error::InvalidRequest(detail)) = err.downcast_ref::<Error>() else {
            panic!("the refusal is the request's: {err:#}");
        };
        assert_eq!(detail, "tool `shell` is reserved by the backend");
    }

    #[test]
    fn no_workspace() {
        let chat =
            chat_request(&request(vec![]), Access::Private).expect("an empty tool list translates");
        assert!(chat.tools.is_none(), "without a workspace lend nothing is advertised");
    }
}
