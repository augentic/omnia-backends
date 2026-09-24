//! The protocol `cursor-sdk-bridge` speaks — `sdk.v1` over Connect on a
//! loopback port: the messages this backend exchanges with it, and a client
//! with one typed method per procedure.

mod messages;
mod rpc;

pub use messages::{
    AgentOperationOptions, AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig,
    ModelSelection, RunStatus, RunStreamMessage, RunStreamResult, SdkMessage, TokenUsage, ToolList,
};
pub use rpc::{Rpc, RpcError, RunStream};
