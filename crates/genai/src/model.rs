//! `wasi-model` implementation backed by the multi-provider genai SDK.
//!
//! Maps the gate-validated [`Request`] onto a genai
//! [`ChatRequest`]/[`ChatOptions`], advertises the request's declared
//! function tools to the provider — plus the host-injected `read`/`list`
//! workspace tools when the guest lent a workspace — and drives the
//! in-process tool loop: `read`/`list` execute host-side through the lent
//! [`ToolHost`] and never traverse the session, while every other tool call
//! is forwarded through [`ToolHost::call_tool`], where the guest's tool
//! closure answers. Self-checks the answer against `response-format` and
//! returns a host-only [`Answer`] (the parsed value plus a tool transcript
//! for record/replay). The guest only ever sees the validated answer string
//! the `create` binding derives from `value`; the host re-validates as the
//! single authority (§3.1.3), so this self-check is an optimization, not the
//! gate.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use futures::FutureExt as _;
use genai::chat::{
    ChatMessage, ChatOptions, ChatRequest, ChatResponseFormat, JsonSpec, ReasoningEffort, Tool,
    ToolCall, ToolResponse,
};
use omnia_wasi_model::{
    Answer, Effort, Format, Function, FutureResult, Request, Role, Tool as ModelTool, ToolHost,
    ToolTurn, Transcript, Usage, WasiModelCtx,
};
use serde_json::Value;

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
        let max_result_bytes = self.limits().max_result_bytes;

        async move {
            // The model id is carried on the request; fall back to the backend
            // default when the guest leaves it unset.
            let model = request.model.clone().unwrap_or_else(|| DEFAULT_MODEL.to_owned());
            let format = request.format.clone();
            // `local_path` reports a resolved workspace lend; it gates
            // advertising the host-injected `read`/`list` tools.
            let workspace = tool_host.local_path().is_some();
            let mut chat = build_request(&request, workspace)?;
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
                        let result = dispatch_tool(&tool_host, &call, max_result_bytes).await?;
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

/// Map the gate-validated [`Request`] onto a genai [`ChatRequest`],
/// advertising the request's declared function tools to the provider plus —
/// when `workspace` reports a resolved lend — the host-injected `read`/`list`
/// tools.
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
            // The genai backend has no MCP client; a spawned-agent backend
            // (omnia-cursor) honors MCP grants. Fail loudly rather than
            // silently dropping the grant.
            ModelTool::Mcp(mcp) => bail!(
                "the genai backend cannot honor the MCP tool grant for server `{}`; use a \
                 spawned-agent backend such as omnia-cursor",
                mcp.name
            ),
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

/// The host-injected workspace tools advertised alongside the request's
/// declared function tools when the guest lent a workspace. The host gate
/// reserves their names (`read`, `list`, plus the unadvertised `write`), so
/// no guest tool can shadow them.
fn workspace_tools() -> [Tool; 2] {
    [
        Tool::new("read")
            .with_description("Read one UTF-8 text file from the workspace granted for this task.")
            .with_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "`/`-separated file path relative to the workspace root."
                    }
                },
                "required": ["path"],
                "additionalProperties": false
            })),
        Tool::new("list")
            .with_description(
                "List one directory of the workspace granted for this task; omit `path` to list \
                 the workspace root.",
            )
            .with_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "`/`-separated directory path relative to the workspace \
                                        root; omit or leave empty for the root."
                    }
                },
                "additionalProperties": false
            })),
    ]
}

/// Translate a declared function tool into a genai [`Tool`]. The host gate
/// already guarantees `parameters` parses as JSON.
fn function_tool(function: &Function) -> Result<Tool> {
    let schema: Value = serde_json::from_str(&function.parameters).with_context(|| {
        format!("function tool `{}` parameters is not valid JSON", function.name)
    })?;
    Ok(Tool::new(function.name.clone())
        .with_description(function.description.clone())
        .with_schema(schema))
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

/// Route one model tool call: the host-injected `read`/`list` execute
/// host-side through the lent [`ToolHost`] workspace capability and never
/// traverse the session; every other name is forwarded through `call_tool`,
/// where the guest's tool closure answers. For `call_tool` the host enforces
/// the declared-name check, budget, size cap, and timeout; its outer error is
/// a hard failure that ends the completion, while the inner `Err` is the
/// guest tool's own failure text — fed back to the model as repairable
/// content.
async fn dispatch_tool(
    tool_host: &Arc<dyn ToolHost>, call: &ToolCall, max_result_bytes: usize,
) -> Result<String> {
    match call.fn_name.as_str() {
        // Workspace-tool failures (missing file, bounds, no workspace lent)
        // are model-visible repairable text, never hard errors: models probe
        // paths, and a bad one must not end the completion.
        "read" => Ok(workspace_read(tool_host, &call.fn_arguments, max_result_bytes).await),
        "list" => Ok(workspace_list(tool_host, &call.fn_arguments, max_result_bytes).await),
        _ => {
            let outcome = tool_host
                .call_tool(call.fn_name.clone(), call.fn_arguments.to_string())
                .await
                .with_context(|| format!("calling tool `{}`", call.fn_name))?;
            Ok(outcome
                .unwrap_or_else(|failure| format!("tool `{}` failed: {failure}", call.fn_name)))
        }
    }
}

/// Serve a model `read` call from the lent workspace: bytes must decode as
/// UTF-8 and fit the per-result byte cap before they become prompt content.
async fn workspace_read(
    tool_host: &Arc<dyn ToolHost>, arguments: &Value, max_result_bytes: usize,
) -> String {
    let Some(path) = arguments.get("path").and_then(Value::as_str) else {
        return "tool `read` failed: arguments must carry a string `path`".to_owned();
    };
    let bytes = match tool_host.read(path.to_owned()).await {
        Ok(bytes) => bytes,
        Err(error) => return format!("tool `read` failed: {error:#}"),
    };
    String::from_utf8(bytes).map_or_else(
        |_| format!("tool `read` failed: `{path}` is not valid UTF-8 text"),
        |text| bounded_result("read", text, max_result_bytes),
    )
}

/// Serve a model `list` call from the lent workspace as a JSON array of
/// `{"name", "is_directory"}` entries; a missing or empty `path` lists the
/// workspace root.
async fn workspace_list(
    tool_host: &Arc<dyn ToolHost>, arguments: &Value, max_result_bytes: usize,
) -> String {
    let path = arguments.get("path").and_then(Value::as_str).unwrap_or_default().to_owned();
    let entries = match tool_host.list(path).await {
        Ok(entries) => entries,
        Err(error) => return format!("tool `list` failed: {error:#}"),
    };
    match serde_json::to_string(&entries) {
        Ok(json) => bounded_result("list", json, max_result_bytes),
        Err(error) => format!("tool `list` failed: {error}"),
    }
}

/// Apply the session's per-result byte cap to a host-injected tool's output,
/// mirroring the host's enforcement on session tool results.
fn bounded_result(tool: &str, text: String, max_result_bytes: usize) -> String {
    if text.len() > max_result_bytes {
        return format!(
            "tool `{tool}` failed: result of {} bytes exceeds the {max_result_bytes}-byte cap",
            text.len()
        );
    }
    text
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

// Deliberate unit tests: pure request-translation logic plus deterministic,
// service-free tool routing (the CI floor); `tests/live.rs` proves a real
// provider run end-to-end.
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use omnia_wasi_model::{DirEntry, Grants, Mcp, Message};
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
    fn function_tool_advertised() {
        let chat = build_request(&request(vec![lookup_tool()]), false)
            .expect("a declared function tool translates");
        let tools = chat.tools.expect("the chat request advertises the tool");
        assert_eq!(tools.len(), 1, "one declared tool, one advertised tool");
        assert_eq!(tools[0].name, "lookup".into());
        assert_eq!(
            tools[0].schema,
            Some(serde_json::json!({ "type": "object" })),
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
        .expect_err("an MCP grant needs a spawned-agent backend");
        assert!(err.to_string().contains("omnia-cursor"), "unexpected error: {err}");
    }

    #[test]
    fn workspace_tools_advertised() {
        let chat = build_request(&request(vec![lookup_tool()]), true)
            .expect("declared and injected tools translate");
        let tools = chat.tools.expect("the chat request advertises the tools");
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
        let chat = build_request(&request(vec![]), false).expect("an empty tool list translates");
        assert!(chat.tools.is_none(), "without a workspace lend nothing is advertised");
    }

    /// Deterministic stand-in for `BoundToolHost`: an in-memory file map and
    /// root listing, with a `call_tool` echo that proves session routing.
    struct WorkspaceStub {
        files: HashMap<String, Vec<u8>>,
        entries: Vec<DirEntry>,
    }

    impl ToolHost for WorkspaceStub {
        fn call_tool(
            &self, name: String, arguments: String,
        ) -> FutureResult<Result<String, String>> {
            async move { Ok(Ok(format!("session:{name}:{arguments}"))) }.boxed()
        }

        fn read(&self, path: String) -> FutureResult<Vec<u8>> {
            let result = self
                .files
                .get(&path)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("opening `{path}` in workspace"));
            async move { result }.boxed()
        }

        fn list(&self, path: String) -> FutureResult<Vec<DirEntry>> {
            let result = if path.is_empty() {
                Ok(self.entries.clone())
            } else {
                Err(anyhow::anyhow!("listing `{path}` in workspace"))
            };
            async move { result }.boxed()
        }

        fn write(&self, path: String, _bytes: Vec<u8>) -> FutureResult<()> {
            let error = anyhow::anyhow!("write to `{path}` is not exercised");
            async move { Err(error) }.boxed()
        }
    }

    fn workspace_stub() -> Arc<dyn ToolHost> {
        Arc::new(WorkspaceStub {
            files: [
                ("refs.md".to_owned(), b"reference text".to_vec()),
                ("logo.bin".to_owned(), vec![0xFF, 0xFE, 0x00]),
            ]
            .into(),
            entries: vec![
                DirEntry {
                    name: "refs".to_owned(),
                    is_directory: true,
                },
                DirEntry {
                    name: "refs.md".to_owned(),
                    is_directory: false,
                },
            ],
        })
    }

    fn tool_call(name: &str, arguments: Value) -> ToolCall {
        ToolCall {
            call_id: "call-1".to_owned(),
            fn_name: name.to_owned(),
            fn_arguments: arguments,
            thought_signatures: None,
        }
    }

    const CAP: usize = 1024;

    #[tokio::test]
    async fn read_routes_host_side() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("read", json!({"path": "refs.md"})), CAP)
                .await
                .expect("a workspace read is never a hard failure");
        assert_eq!(result, "reference text", "the file body is the tool result");
    }

    #[tokio::test]
    async fn binary_read() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("read", json!({"path": "logo.bin"})), CAP)
                .await
                .expect("a binary read is model-visible, not a hard failure");
        assert!(result.contains("not valid UTF-8"), "unexpected result: {result}");
    }

    #[tokio::test]
    async fn oversize_read() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("read", json!({"path": "refs.md"})), 8)
                .await
                .expect("an oversize read is model-visible, not a hard failure");
        assert!(result.contains("exceeds the 8-byte cap"), "unexpected result: {result}");
    }

    #[tokio::test]
    async fn read_missing_file() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("read", json!({"path": "gone.md"})), CAP)
                .await
                .expect("a missing file is model-visible, not a hard failure");
        assert!(result.starts_with("tool `read` failed:"), "unexpected result: {result}");
        assert!(result.contains("gone.md"), "the failure names the path: {result}");
    }

    #[tokio::test]
    async fn read_missing_path_argument() {
        let result = dispatch_tool(&workspace_stub(), &tool_call("read", json!({})), CAP)
            .await
            .expect("malformed arguments are model-visible, not a hard failure");
        assert!(result.contains("string `path`"), "unexpected result: {result}");
    }

    #[tokio::test]
    async fn list_serialization() {
        let result = dispatch_tool(&workspace_stub(), &tool_call("list", json!({})), CAP)
            .await
            .expect("a root listing is never a hard failure");
        assert_eq!(
            result,
            r#"[{"name":"refs","is_directory":true},{"name":"refs.md","is_directory":false}]"#,
            "entries serialize as a canonical JSON array"
        );
    }

    #[tokio::test]
    async fn unknown_tool_routes_to_session() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("lookup", json!({"name": "alpha"})), CAP)
                .await
                .expect("the session stub answers");
        assert_eq!(
            result, r#"session:lookup:{"name":"alpha"}"#,
            "non-reserved names go through call_tool"
        );
    }
}
