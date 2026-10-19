//! Provider request translation.
//!
//! A validated request becomes a [`Turn`]: the system text and messages as
//! they are, the declared function tools, and the request's `format` as the
//! provider `response_format` — steering only; acceptance is the guest's
//! `check` when the request asks for one. Whatever the request gets wrong is
//! refused here, before any lend is resolved; the tools a lend advertises
//! join the turn once its access is known.

use anyhow::{Context as _, Result, bail};
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponseFormat, JsonSpec, ReasoningEffort, Tool,
};
use omnia_wasi_model::{Effort, Error, Format, Function, Request, Role, Tool as ModelTool};
use serde_json::Value;

use crate::tools::SHELL_TOOL;

// Everything one completion derives from the request: the model to ask, the
// provider's chat request and options, and what the guest asked of the
// answer.
#[derive(Debug)]
pub struct Turn {
    pub model: String,
    pub chat: ChatRequest,
    pub options: ChatOptions,
    pub format: Format,
    pub check: bool,
    // the system text and every message, in bytes
    pub prompt_bytes: u64,
}

impl Turn {
    pub fn translate(request: &Request, default_model: &str) -> Result<Self> {
        Ok(Self {
            model: request.model.as_deref().unwrap_or(default_model).to_owned(),
            chat: chat_request(request)?,
            options: chat_options(request)?,
            format: request.format.clone(),
            check: request.check,
            prompt_bytes: prompt_bytes(request),
        })
    }

    // The tools a lend advertises, after the request's own.
    pub fn advertise(&mut self, tools: Vec<Tool>) {
        if !tools.is_empty() {
            self.chat.tools.get_or_insert_default().extend(tools);
        }
    }
}

fn prompt_bytes(request: &Request) -> u64 {
    let bytes = request.system.as_deref().map_or(0, str::len)
        + request.messages.iter().map(|message| message.content.len()).sum::<usize>();
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

// The messages, the system text, and the declared function tools. A tool
// named `shell` is the backend's, refused whatever the lend; an MCP server
// is nothing genai can reach.
fn chat_request(request: &Request) -> Result<ChatRequest> {
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

fn chat_options(request: &Request) -> Result<ChatOptions> {
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

// Request-to-turn translation; end-to-end round-trips run in
// `tests/model.rs`.
#[cfg(test)]
mod tests {
    use omnia_sandbox::Access;
    use omnia_wasi_model::{Grants, Mcp, Message};
    use serde_json::json;

    use super::*;
    use crate::tools::workspace_tools;

    const DEFAULT_MODEL: &str = "gpt-4o-mini";

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

    // The request translated, the tools a lend of `access` advertises after
    // its own.
    fn turn(tools: Vec<ModelTool>, access: Access) -> Result<Turn> {
        let mut turn = Turn::translate(&request(tools), DEFAULT_MODEL)?;
        turn.advertise(workspace_tools(access));
        Ok(turn)
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
    fn request_model() {
        let turn = turn(vec![], Access::Private).expect("a bare request translates");
        assert_eq!(turn.model, DEFAULT_MODEL, "an unset model is the default");

        let mut request = request(vec![]);
        request.model = Some("claude-fable-5".to_owned());
        let turn = Turn::translate(&request, DEFAULT_MODEL).expect("a named model translates");
        assert_eq!(turn.model, "claude-fable-5", "a set model is the request's");
    }

    #[test]
    fn declared_tool() {
        let turn = turn(vec![lookup_tool()], Access::Private)
            .expect("a declared function tool translates");
        let tools = turn.chat.tools.expect("the chat request advertises the tool");
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
        let err = turn(
            vec![ModelTool::Mcp(Mcp {
                name: "docs".to_owned(),
                tools: vec![],
                url: "http://localhost:8080/mcp".to_owned(),
            })],
            Access::Private,
        )
        .expect_err("genai rejects MCP grants");
        assert!(err.to_string().contains("does not support MCP"), "unexpected error: {err}");
    }

    #[test]
    fn lent_workspace() {
        let turn =
            turn(vec![lookup_tool()], Access::Read).expect("declared and injected tools translate");
        assert_eq!(tool_names(&turn.chat), ["lookup", "read", "list"], "declared tools first");

        let tools = turn.chat.tools.expect("the chat request advertises the tools");
        let schema = tools[1].schema.as_ref().expect("read carries a schema");
        assert_eq!(schema.get("required"), Some(&json!(["path"])), "read requires a path argument");
        let schema = tools[2].schema.as_ref().expect("list carries a schema");
        assert_eq!(schema.get("required"), None, "list's path is optional (root listing)");
    }

    #[test]
    fn shell_lend() {
        let turn = turn(vec![], Access::Shell).expect("a shell lend translates");
        assert_eq!(tool_names(&turn.chat), ["read", "list", "shell"], "the shell after read/list");

        let tools = turn.chat.tools.expect("the chat request advertises the tools");
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
        let err = turn(vec![shell], Access::Private)
            .expect_err("a guest tool named `shell` is refused whatever the access");
        let Some(Error::InvalidRequest(detail)) = err.downcast_ref::<Error>() else {
            panic!("the refusal is the request's: {err:#}");
        };
        assert_eq!(detail, "tool `shell` is reserved by the backend");
    }

    #[test]
    fn no_workspace() {
        let turn = turn(vec![], Access::Private).expect("an empty tool list translates");
        assert!(turn.chat.tools.is_none(), "without a workspace lend nothing is advertised");
    }
}
