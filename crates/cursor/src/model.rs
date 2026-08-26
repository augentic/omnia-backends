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
mod deadlines;
mod observe;
mod options;

use std::sync::Arc;

use agent::{Agent, Outcome};
use anyhow::bail;
pub use deadlines::Deadlines;
use omnia_wasi_model::{Answer, FutureResult, Request, ToolHost, WasiModelCtx};
use options::{Workspace, with_mcp_hint};
use tokio::sync::mpsc;

use crate::Client;
use crate::bridge::AgentOptions;

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let bridge = Arc::clone(&self.bridge);
        let deadlines = self.deadlines;
        let default_model = self.model.clone();

        Box::pin(async move {
            let workspace = Workspace::new(tool_host.local_path())?;
            let options = AgentOptions::from_request(&request, &workspace, &default_model)?;
            
            let model_id = options.model.id.clone();
            let format = request.format.clone();
            let mcp_servers = request.mcp_servers();
            let mcp_names: Vec<&str> = mcp_servers.iter().map(|s| s.name.as_str()).collect();
            let prompt = with_mcp_hint(&mcp_servers, request.to_string());
            observe::log_completion(&model_id, &format, prompt.len(), &mcp_names);

            let rpc = bridge.rpc();
            let created = rpc.create_agent(options).await?;
            let agent = Agent::new(rpc.clone(), created.agent_id.clone(), deadlines);

            let (abort_tx, mut abort_rx) = mpsc::unbounded_channel();
            let _attached = bridge.attach(created.agent_id, tool_host, abort_tx);

            let output = agent.send(&prompt, &mut abort_rx).await?;
            observe::log_answer(1, &output);
            let reason = match output.answer(&format) {
                Outcome::Done(answer) => return Ok(answer),
                Outcome::Repair(reason) => reason,
            };

            // The second (and last) attempt sends only the format-repair
            // instruction: the agent's session already carries the prompt
            // and the failed answer.
            tracing::debug!(attempt = 1, %reason, "repairing answer");
            let output = agent.send(&format.repair(&reason), &mut abort_rx).await?;
            observe::log_answer(2, &output);

            match output.answer(&format) {
                Outcome::Done(answer) => Ok(answer),
                Outcome::Repair(reason) => {
                    bail!("no answer after 2 repair attempts: {reason}");
                }
            }
        })
    }
}
