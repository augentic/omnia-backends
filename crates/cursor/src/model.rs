//! `wasi-model` implementation driving one bridge-managed Cursor agent per
//! completion.
//!
//! The gate-validated [`Request`] maps onto `CreateAgent` options: guest
//! function tools become SDK custom tools (executed back through the
//! session via the loopback callback and [`ToolHost::call_tool`]), MCP
//! grants ride inline as `mcp_servers`, and the lent workspace — or a
//! private empty directory when none is lent — becomes the agent's `cwd`.
//! One `Send` stream produces the answer; a failed format gate sends the
//! repair instruction on the same agent, whose session already carries the
//! prompt and the failed answer.

mod agent;
mod observe;
mod options;

use std::sync::Arc;

use agent::Agent;
pub use agent::Deadlines;
use omnia_wasi_model::{Answer, FutureResult, Request, ToolHost, WasiModelCtx};
use options::Turn;

use crate::Client;

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let client = self.clone();
        
        Box::pin(async move {
            let turn = Turn::prepare(&request, tool_host.local_path(), &client.model)?;
            observe::log_completion(&turn);
            Agent::create(&client, turn, tool_host).await?.complete().await
        })
    }
}
