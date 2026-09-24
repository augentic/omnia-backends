//! The `sdk.v1` protocol `cursor-sdk-bridge` serves: the messages this
//! backend exchanges with it, and a Connect client with one typed method
//! per procedure.

mod messages;
mod rpc;

pub use messages::{
    AgentOperationOptions, AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig,
    ModelSelection, RunStatus, RunStreamMessage, RunStreamResult, SdkMessage, TokenUsage, ToolList,
};
pub use rpc::{Rpc, RpcError, RunStream};
