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
    // A fault in the request is `Error::InvalidRequest`: a reserved or
    // unsupported tool, or a schema that is not JSON or not a JSON Schema.
    pub fn translate(
        request: &Request, default_model: &str, max_tokens: u32, access: Access,
    ) -> Result<Self> {
        let (response_format, validator) = translate_format(&request.format)?;
        Ok(Self {
            model: request.model.as_deref().unwrap_or(default_model).to_owned(),
            chat: chat_request(request, access)?,
            options: chat_options(request, max_tokens, response_format),
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
            .map(|tools| tools.iter().map(|tool| tool.name.to_string()).collect())
            .unwrap_or_default()
    }
}

fn prompt_bytes(request: &Request) -> u64 {
    let bytes = request.system.as_deref().map_or(0, str::len)
        + request.messages.iter().map(|message| message.content.len()).sum::<usize>();
    u64::try_from(bytes).unwrap_or(u64::MAX)
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
    for tool in &request.tools {
        match tool {
            ModelTool::Function(function) if function.name == SHELL_TOOL => {
                bail!(Error::InvalidRequest(format!(
                    "tool `{SHELL_TOOL}` is reserved by the backend"
                )));
            }
            ModelTool::Function(function) => tools.push(function_tool(function)?),
            ModelTool::Mcp(mcp) => bail!(Error::InvalidRequest(format!(
                "MCP server `{}`: genai does not support MCP servers",
                mcp.name
            ))),
        }
    }
    tools.extend(workspace_tools(access));
    if !tools.is_empty() {
        chat = chat.with_tools(tools);
    }

    Ok(chat)
}

fn function_tool(function: &Function) -> Result<Tool> {
    let schema: Value = serde_json::from_str(&function.parameters).map_err(|error| {
        Error::InvalidRequest(format!(
            "function tool `{}` parameters is not valid JSON: {error}",
            function.name
        ))
    })?;
    Ok(Tool::new(function.name.clone())
        .with_description(function.description.clone())
        .with_schema(schema))
}

// The provider's response format and the validator the same schema builds,
// parsed once.
fn translate_format(format: &Format) -> Result<(Option<ChatResponseFormat>, Option<Validator>)> {
    let spec = match format {
        Format::Text => return Ok((None, None)),
        Format::Json => return Ok((Some(ChatResponseFormat::JsonMode), None)),
        Format::Schema(spec) => spec,
    };
    let schema: Value = serde_json::from_str(&spec.schema).map_err(|error| {
        Error::InvalidRequest(format!("format schema is not valid JSON: {error}"))
    })?;
    let validator = jsonschema::validator_for(&schema).map_err(|error| {
        Error::InvalidRequest(format!(
            "format schema `{}` is not a valid JSON Schema: {error}",
            spec.name
        ))
    })?;
    let json_spec = JsonSpec::new(spec.name.clone(), schema);
    Ok((Some(ChatResponseFormat::JsonSpec(json_spec)), Some(validator)))
}

fn chat_options(
    request: &Request, max_tokens: u32, response_format: Option<ChatResponseFormat>,
) -> ChatOptions {
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
        if let Some(effort) = generation.effort {
            options = options.with_reasoning_effort(reasoning_effort(effort));
        }
    }

    options
}

const fn reasoning_effort(effort: Effort) -> ReasoningEffort {
    match effort {
        Effort::Minimal => ReasoningEffort::Minimal,
        Effort::Low => ReasoningEffort::Low,
        Effort::Medium => ReasoningEffort::Medium,
        Effort::High => ReasoningEffort::High,
    }
}

// Unit tests for request translation.
#[cfg(test)]
mod tests {
    use omnia_wasi_model::{Generation, Grants, Mcp, Message, Schema};
    use serde_json::json;

    use super::*;

    const DEFAULT_MODEL: &str = "gpt-4o-mini";
    const DEFAULT_MAX_TOKENS: u32 = 4096;

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

    fn translate(request: &Request, access: Access) -> Result<PreparedRequest> {
        PreparedRequest::translate(request, DEFAULT_MODEL, DEFAULT_MAX_TOKENS, access)
    }

    fn prepared(tools: Vec<ModelTool>, access: Access) -> Result<PreparedRequest> {
        translate(&request(tools), access)
    }

    fn tool_names(chat: &ChatRequest) -> Vec<String> {
        chat.tools
            .as_ref()
            .expect("the chat request advertises tools")
            .iter()
            .map(|tool| tool.name.to_string())
            .collect()
    }

    fn invalid_request(err: &anyhow::Error) -> &str {
        let Some(Error::InvalidRequest(detail)) = err.downcast_ref::<Error>() else {
            panic!("the refusal is the request's: {err:#}");
        };
        detail
    }

    #[test]
    fn request_model() {
        let prepared = prepared(vec![], Access::Private).expect("a bare request translates");
        assert_eq!(prepared.model, DEFAULT_MODEL, "an unset model is the default");

        let mut request = request(vec![]);
        request.model = Some("claude-fable-5".to_owned());
        let prepared = translate(&request, Access::Private).expect("a named model translates");
        assert_eq!(prepared.model, "claude-fable-5", "a set model is the request's");
    }

    #[test]
    fn request_max_tokens() {
        let prepared = prepared(vec![], Access::Private).expect("a bare request translates");
        assert_eq!(
            prepared.options.max_tokens,
            Some(DEFAULT_MAX_TOKENS),
            "a request naming no bound carries the default"
        );

        let mut bounded = request(vec![]);
        bounded.generation = Some(generation(Some(512)));
        let prepared = translate(&bounded, Access::Private).expect("a bounded request translates");
        assert_eq!(prepared.options.max_tokens, Some(512), "a set bound is the request's");

        let mut unbounded = request(vec![]);
        unbounded.generation = Some(generation(None));
        let prepared = translate(&unbounded, Access::Private)
            .expect("a request with other settings translates");
        assert_eq!(
            prepared.options.max_tokens,
            Some(DEFAULT_MAX_TOKENS),
            "generation settings without a bound carry the default"
        );
    }

    fn generation(max_tokens: Option<u32>) -> Generation {
        Generation {
            temperature: Some(0.5),
            top_p: None,
            max_tokens,
            stop: Vec::new(),
            seed: None,
            effort: None,
        }
    }

    #[test]
    fn declared_tool() {
        let prepared = prepared(vec![lookup_tool()], Access::Private)
            .expect("a declared function tool translates");
        let tools = prepared.chat.tools.expect("the chat request advertises the tool");
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
        let err = prepared(
            vec![ModelTool::Mcp(Mcp {
                name: "docs".to_owned(),
                tools: vec![],
                url: "http://localhost:8080/mcp".to_owned(),
            })],
            Access::Private,
        )
        .expect_err("genai rejects MCP grants");
        let detail = invalid_request(&err);
        assert!(detail.contains("does not support MCP"), "unexpected detail: {detail}");
        assert!(detail.contains("`docs`"), "the detail names the server: {detail}");
    }

    #[test]
    fn lent_workspace() {
        let prepared = prepared(vec![lookup_tool()], Access::Read)
            .expect("declared and injected tools translate");
        assert_eq!(tool_names(&prepared.chat), ["lookup", "read", "list"], "declared tools first");
        assert_eq!(prepared.declared(), ["lookup", "read", "list"]);

        let tools = prepared.chat.tools.expect("the chat request advertises the tools");
        let schema = tools[1].schema.as_ref().expect("read carries a schema");
        assert_eq!(schema.get("required"), Some(&json!(["path"])), "read requires a path argument");
        let schema = tools[2].schema.as_ref().expect("list carries a schema");
        assert_eq!(schema.get("required"), None, "list's path is optional (root listing)");
    }

    #[test]
    fn shell_lend() {
        let prepared = prepared(vec![], Access::Shell).expect("a shell lend translates");
        assert_eq!(
            tool_names(&prepared.chat),
            ["read", "list", "shell"],
            "the shell after read/list"
        );

        let tools = prepared.chat.tools.expect("the chat request advertises the tools");
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
        let err = prepared(vec![shell], Access::Private)
            .expect_err("a guest tool named `shell` is refused whatever the access");
        assert_eq!(invalid_request(&err), "tool `shell` is reserved by the backend");
    }

    #[test]
    fn no_workspace() {
        let prepared = prepared(vec![], Access::Private).expect("an empty tool list translates");
        assert!(prepared.chat.tools.is_none(), "without a workspace lend nothing is advertised");
        assert_eq!(prepared.declared(), Vec::<String>::new());
    }

    #[test]
    fn schema_format() {
        let mut request = request(vec![]);
        request.format = Format::Schema(Schema {
            name: "verdict".to_owned(),
            schema: r#"{"type":"object","properties":{"verdict":{"type":"string"}}}"#.to_owned(),
        });
        let prepared = translate(&request, Access::Private).expect("a schema translates");
        assert!(
            matches!(prepared.options.response_format, Some(ChatResponseFormat::JsonSpec(_))),
            "the schema rides as the provider's response format"
        );
        let validator = prepared.validator.expect("the schema builds a validator");
        assert!(validator.is_valid(&json!({ "verdict": "pass" })));
        assert!(!validator.is_valid(&json!({ "verdict": 1 })));
    }

    #[test]
    fn schema_invalid() {
        let mut request = request(vec![]);
        request.format = Format::Schema(Schema {
            name: "verdict".to_owned(),
            schema: r#"{"type":"nonsense"}"#.to_owned(),
        });
        let err = translate(&request, Access::Private).expect_err("a bad schema is refused");
        let detail = invalid_request(&err);
        assert!(
            detail.starts_with("format schema `verdict` is not a valid JSON Schema"),
            "{detail}"
        );

        request.format = Format::Schema(Schema {
            name: "verdict".to_owned(),
            schema: "{".to_owned(),
        });
        let err = translate(&request, Access::Private).expect_err("non-JSON is refused");
        assert!(invalid_request(&err).starts_with("format schema is not valid JSON"));
    }
}
