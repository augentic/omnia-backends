//! The subset of `sdk.v1` messages this backend exchanges with
//! `cursor-sdk-bridge`, in the proto3 JSON mapping (camelCase field names,
//! enums by name, `int64` tolerated as string or number). `sdk.v1` evolves
//! additively, so every deserialized shape ignores unknown fields.

use std::collections::BTreeMap;
use std::fmt;

use omnia_wasi_model::Usage;
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
/// `CreateAgent` sent so a later call does not depend on bridge env fallback.
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
    #[default]
    #[serde(rename = "RUN_LIFECYCLE_STATUS_UNSPECIFIED")]
    Unspecified,
    #[serde(rename = "RUN_LIFECYCLE_STATUS_CREATING")]
    Creating,
    #[serde(rename = "RUN_LIFECYCLE_STATUS_RUNNING")]
    Running,
    #[serde(rename = "RUN_LIFECYCLE_STATUS_FINISHED")]
    Finished,
    #[serde(rename = "RUN_LIFECYCLE_STATUS_ERROR")]
    Error,
    #[serde(rename = "RUN_LIFECYCLE_STATUS_CANCELLED")]
    Cancelled,
    #[serde(rename = "RUN_LIFECYCLE_STATUS_EXPIRED")]
    Expired,
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

// proto3 JSON writes an enum by name, but a bridge may write the number; any
// other shape is `Unknown`
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

impl From<TokenUsage> for Usage {
    fn from(usage: TokenUsage) -> Self {
        Self {
            input_tokens: clamp_u32(usage.input_tokens),
            output_tokens: clamp_u32(usage.output_tokens),
            reasoning_tokens: usage.reasoning_tokens.map(clamp_u32),
        }
    }
}

/// Wire counts are `i64`; negatives become 0, values above `u32::MAX` saturate.
fn clamp_u32(count: i64) -> u32 {
    if count.is_negative() { 0 } else { u32::try_from(count).unwrap_or(u32::MAX) }
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
    use omnia_wasi_model::Usage;
    use serde_json::json;

    use super::{RunStatus, RunStreamResult, TokenUsage};

    fn usage(input: i64, output: i64, reasoning: Option<i64>) -> Usage {
        Usage::from(TokenUsage {
            input_tokens: input,
            output_tokens: output,
            reasoning_tokens: reasoning,
        })
    }

    #[test]
    fn token_counts() {
        assert_eq!(
            usage(-1, -1, Some(-1)),
            Usage {
                input_tokens: 0,
                output_tokens: 0,
                reasoning_tokens: Some(0),
            }
        );
        let saturated = usage(i64::MAX, i64::MAX, Some(i64::MAX));
        assert_eq!(saturated.input_tokens, u32::MAX);
        assert_eq!(saturated.output_tokens, u32::MAX);
        assert_eq!(saturated.reasoning_tokens, Some(u32::MAX));
        assert_eq!(
            usage(7, 3, None),
            Usage {
                input_tokens: 7,
                output_tokens: 3,
                reasoning_tokens: None,
            }
        );
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
