//! The subset of `sdk.v1` messages this backend exchanges with
//! `cursor-sdk-bridge`, in the proto3 JSON mapping (camelCase field names,
//! enums by name, `int64` tolerated as string or number). `sdk.v1` evolves
//! additively, so every deserialized shape ignores unknown fields.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

// --- SdkBridgeControlService ---

/// Empty proto3 JSON object (`{}`), used as Ping / `GetVersion` request bodies
/// and as acknowledgement responses.
#[derive(Serialize, Deserialize, Default)]
#[allow(clippy::empty_structs_with_brackets)] // unit structs serialize as `null`
pub struct Empty {}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GetVersionResponse {
    pub bridge_version: String,
    pub protocol_version: String,
    pub capabilities: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShutdownRequest {
    pub grace_seconds: u32,
}

// --- SdkAgentService ---

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAgentRequest {
    pub options: AgentOptions,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CreateAgentResponse {
    pub agent_id: String,
    pub model: Option<ModelSelection>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteAgentRequest {
    pub agent_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelRunRequest {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendRequest {
    pub agent_id: String,
    pub message: UserMessage,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserMessage {
    pub text: String,
}

// --- Agent options ---

/// Cursor auth and runtime selection for one agent. The API key rides on the
/// request per the bridge protocol ("always set it explicitly"); never derive
/// `Debug` here or log the serialized form.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentOptions {
    pub model: ModelSelection,
    pub api_key: String,
    pub local: LocalAgentOptions,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub mcp_servers: BTreeMap<String, McpServerConfig>,
    /// `None` keeps the bridge's default built-in toolset; `Some` with an
    /// empty `names` list disables every built-in tool (the wrapper exists
    /// precisely to keep that distinction on the wire).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolList>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ModelSelection {
    pub id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalAgentOptions {
    pub cwd: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub custom_tools: BTreeMap<String, CustomToolDefinition>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomToolDefinition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema object describing the tool's input parameters.
    pub input_schema: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolList {
    pub names: Vec<String>,
}

/// The `config` oneof: exactly one case serialized as its own field.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerConfig {
    pub http: HttpMcpServerConfig,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpMcpServerConfig {
    #[serde(rename = "type")]
    pub transport: &'static str,
    pub url: String,
}

impl McpServerConfig {
    /// Streamable-HTTP server at `url`, matching the WIT `tool::mcp` grant.
    pub fn streamable_http(url: &str) -> Self {
        Self {
            http: HttpMcpServerConfig {
                transport: "HTTP_MCP_TRANSPORT_TYPE_HTTP",
                url: url.to_owned(),
            },
        }
    }
}

// --- Run streaming ---

/// One frame of a `Send` stream. A frame with no envelope case and no offset
/// is a keepalive; unknown envelope cases deserialize to the same shape and
/// are skipped the same way.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunStreamMessage {
    pub sdk_message: Option<SdkMessage>,
    pub result: Option<RunStreamResult>,
    pub done: Option<Value>,
    pub offset: Option<String>,
}

impl RunStreamMessage {
    pub const fn is_keepalive(&self) -> bool {
        self.sdk_message.is_none()
            && self.result.is_none()
            && self.done.is_none()
            && self.offset.is_none()
    }
}

/// A typed conversation event; `kind` mirrors the public SDK's message types
/// (`system`, `assistant`, `tool_call`, `status`, ...).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SdkMessage {
    #[serde(rename = "type")]
    pub kind: String,
    pub message: Value,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunStreamResult {
    pub run_id: String,
    pub status: RunStatus,
    pub error_code: Option<String>,
    pub result: Option<RunResult>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunResult {
    pub run_id: String,
    /// Final assistant text for a completed run.
    pub result: String,
    pub usage: Option<TokenUsage>,
}

/// Billed token counts; proto3 JSON writes `int64` as strings, so every field
/// tolerates both encodings.
// Field names mirror the wire message; the shared postfix is the protocol's.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TokenUsage {
    #[serde(deserialize_with = "flexible_i64")]
    pub input_tokens: i64,
    #[serde(deserialize_with = "flexible_i64")]
    pub output_tokens: i64,
    #[serde(deserialize_with = "flexible_i64_opt")]
    pub reasoning_tokens: Option<i64>,
}

/// `RunLifecycleStatus`, tolerating the proto3 JSON name, a bare integer, or
/// values this backend does not know.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RunStatus {
    #[default]
    Unspecified,
    Creating,
    Running,
    Finished,
    Error,
    Cancelled,
    Expired,
    Unknown,
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::Creating => "creating",
            Self::Running => "running",
            Self::Finished => "finished",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
            Self::Unknown => "unknown",
        })
    }
}

impl<'de> Deserialize<'de> for RunStatus {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Ok(match value {
            Value::String(name) => match name.as_str() {
                "RUN_LIFECYCLE_STATUS_UNSPECIFIED" => Self::Unspecified,
                "RUN_LIFECYCLE_STATUS_CREATING" => Self::Creating,
                "RUN_LIFECYCLE_STATUS_RUNNING" => Self::Running,
                "RUN_LIFECYCLE_STATUS_FINISHED" => Self::Finished,
                "RUN_LIFECYCLE_STATUS_ERROR" => Self::Error,
                "RUN_LIFECYCLE_STATUS_CANCELLED" => Self::Cancelled,
                "RUN_LIFECYCLE_STATUS_EXPIRED" => Self::Expired,
                _ => Self::Unknown,
            },
            Value::Number(number) => match number.as_i64() {
                Some(0) => Self::Unspecified,
                Some(1) => Self::Creating,
                Some(2) => Self::Running,
                Some(3) => Self::Finished,
                Some(4) => Self::Error,
                Some(5) => Self::Cancelled,
                Some(6) => Self::Expired,
                _ => Self::Unknown,
            },
            _ => Self::Unknown,
        })
    }
}

fn flexible_i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    Ok(flexible_i64_opt(deserializer)?.unwrap_or_default())
}

fn flexible_i64_opt<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<i64>, D::Error> {
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.and_then(|value| match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }))
}
