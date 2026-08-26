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
use options::{Workspace, with_mcp_hint};

use crate::Client;
use crate::bridge::AgentOptions;

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let bridge = Arc::clone(&self.bridge);
        let model = self.model.clone();
        let deadlines = self.deadlines;

        Box::pin(async move {
            let workspace = Workspace::new(tool_host.local_path())?;
            let options = AgentOptions::from_request(&request, &workspace, &model)?;
            let mcp_servers = request.mcp_servers();
            let mcp_names: Vec<&str> =
                mcp_servers.iter().map(|server| server.name.as_str()).collect();
            let prompt = with_mcp_hint(&mcp_servers, request.to_string());
            observe::log_completion(&options.model.id, &request.format, prompt.len(), &mcp_names);

            let mut agent =
                Agent::create(&bridge, options, tool_host, deadlines, workspace).await?;
            agent.complete(&prompt, &request.format).await
        })
    }
}
