//! The subset of `sdk.v1` messages this backend exchanges with
//! `cursor-sdk-bridge`, in the proto3 JSON mapping (camelCase field names,
//! enums by name, `int64` tolerated as string or number). `sdk.v1` evolves
//! additively, so every deserialized shape ignores unknown fields.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

// no `Debug`: carries the API key
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentOptions {
    pub model: ModelSelection,
    pub api_key: String,
    pub local: LocalAgentOptions,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub mcp_servers: BTreeMap<String, McpServerConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolList>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
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
    pub input_schema: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerConfig {
    http: HttpMcpServerConfig,
}

impl McpServerConfig {
    pub fn streamable_http(url: &str) -> Self {
        Self {
            http: HttpMcpServerConfig {
                transport: "HTTP_MCP_TRANSPORT_TYPE_HTTP",
                url: url.to_owned(),
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HttpMcpServerConfig {
    #[serde(rename = "type")]
    transport: &'static str,
    url: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolList {
    pub names: Vec<String>,
}

#[derive(Serialize, Deserialize, Default)]
#[allow(clippy::empty_structs_with_brackets)] // prevent serialization as `null`
pub struct Empty {}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GetVersionResponse {
    pub protocol_version: String,
    pub capabilities: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShutdownRequest {
    pub grace_seconds: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAgentRequest {
    pub options: AgentOptions,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CreateAgentResponse {
    pub agent_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseAgentRequest {
    pub agent_id: String,
}

/// Local delete/get/archive are cwd-scoped; the key is the same pin
/// `CreateAgent` sent so a later call does not depend on the process's env
/// fallback.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentOperationOptions {
    pub cwd: String,
    pub api_key: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteAgentRequest {
    pub agent_id: String,
    pub options: AgentOperationOptions,
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

// --- Connect errors ---

/// Connect's error object: the body of a failed unary call, and the `error`
/// of an `EndStreamResponse`. A code the body does not carry is `unknown`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ConnectStatus {
    pub code: String,
    pub message: String,
    pub details: Option<Value>,
}

impl Default for ConnectStatus {
    fn default() -> Self {
        Self {
            code: "unknown".to_owned(),
            message: String::new(),
            details: None,
        }
    }
}

/// The payload of the frame that closes a server stream.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct EndStreamResponse {
    pub error: Option<ConnectStatus>,
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
    /// Present on the closing frame; its payload carries nothing this
    /// backend reads.
    pub done: Option<IgnoredAny>,
    offset: Option<String>,
}

impl RunStreamMessage {
    pub(super) const fn is_keepalive(&self) -> bool {
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
    #[serde(deserialize_with = "run_status")]
    pub status: RunStatus,
    pub error_code: Option<String>,
    pub result: Option<RunResult>,
}

/// `RunLifecycleStatus`, by its proto3 JSON name; a name this backend does
/// not know is `Unknown`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
pub enum RunStatus {
    /// The stream's result carried no status.
    #[default]
    #[serde(rename = "RUN_LIFECYCLE_STATUS_UNSPECIFIED")]
    Unspecified,
    /// The run is being set up.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_CREATING")]
    Creating,
    /// The run is in progress.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_RUNNING")]
    Running,
    /// The run completed with a result.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_FINISHED")]
    Finished,
    /// The run failed.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_ERROR")]
    Error,
    /// The run was cancelled.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_CANCELLED")]
    Cancelled,
    /// The run outlived its server-side lifetime.
    #[serde(rename = "RUN_LIFECYCLE_STATUS_EXPIRED")]
    Expired,
    /// A status this backend does not know.
    #[serde(other)]
    Unknown,
}

impl RunStatus {
    // in proto declaration order, so the index is the wire number
    const BY_NUMBER: [Self; 7] = [
        Self::Unspecified,
        Self::Creating,
        Self::Running,
        Self::Finished,
        Self::Error,
        Self::Cancelled,
        Self::Expired,
    ];
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

// The proto3 JSON mapping writes an enum by name, but a `cursor-sdk-bridge`
// release may write the number; any other shape is `Unknown`.
fn run_status<'de, D: Deserializer<'de>>(deserializer: D) -> Result<RunStatus, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Wire {
        Name(RunStatus),
        Number(usize),
        Other(IgnoredAny),
    }
    Ok(match Wire::deserialize(deserializer)? {
        Wire::Name(status) => status,
        Wire::Number(number) => {
            RunStatus::BY_NUMBER.get(number).copied().unwrap_or(RunStatus::Unknown)
        }
        Wire::Other(_) => RunStatus::Unknown,
    })
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ConnectStatus, EndStreamResponse, RunStatus, RunStreamResult, TokenUsage};

    #[test]
    fn token_encodings() {
        let usage: TokenUsage = serde_json::from_value(json!({
            "inputTokens": "7",
            "outputTokens": 3,
            "reasoningTokens": "not a number",
        }))
        .expect("int64 as string or number");
        assert_eq!((usage.input_tokens, usage.output_tokens), (7, 3));
        assert_eq!(usage.reasoning_tokens, None, "an unparsable count is absent");
    }

    #[test]
    fn connect_status_defaults() {
        let bare: ConnectStatus = serde_json::from_value(json!({})).expect("all defaulted");
        assert_eq!((bare.code.as_str(), bare.message.as_str()), ("unknown", ""));
        assert!(bare.details.is_none());

        let end: EndStreamResponse =
            serde_json::from_value(json!({ "error": null })).expect("a null error parses");
        assert!(end.error.is_none(), "a null error is a clean end");
        let end: EndStreamResponse =
            serde_json::from_value(json!({ "error": { "message": "boom", "details": [] } }))
                .expect("an error with no code");
        let error = end.error.expect("the error");
        assert_eq!((error.code.as_str(), error.message.as_str()), ("unknown", "boom"));
        assert!(error.details.is_some());
    }

    #[test]
    fn run_status_spellings() {
        let status = |value| {
            serde_json::from_value::<RunStreamResult>(json!({ "status": value }))
                .expect("a result with only a status parses")
                .status
        };
        assert_eq!(status(json!("RUN_LIFECYCLE_STATUS_FINISHED")), RunStatus::Finished);
        assert_eq!(status(json!(3)), RunStatus::Finished);
        assert_eq!(status(json!("RUN_LIFECYCLE_STATUS_PAUSED")), RunStatus::Unknown);
        assert_eq!(status(json!(42)), RunStatus::Unknown);
        assert_eq!(status(json!(-1)), RunStatus::Unknown);
        assert_eq!(status(json!(null)), RunStatus::Unknown);
        assert_eq!(status(json!({ "nested": true })), RunStatus::Unknown);

        let absent: RunStreamResult = serde_json::from_value(json!({})).expect("all defaulted");
        assert_eq!(absent.status, RunStatus::Unspecified);
    }
}
