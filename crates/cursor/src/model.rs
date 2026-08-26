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

use std::env;
use std::sync::Arc;

use agent::{Agent, AgentOutput};
use anyhow::{Context as _, bail};
pub use deadlines::Deadlines;
use omnia_wasi_model::{Answer, Candidate, Format, FutureResult, Request, ToolHost, WasiModelCtx};
use options::{Workspace, agent_options, with_mcp_hint};
use tokio::sync::mpsc;

use crate::Client;

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let bridge = Arc::clone(&self.bridge);
        let deadlines = self.deadlines;
        let default_model = self.model.clone();

        Box::pin(async move {
            // key is never recorded
            let api_key = env::var("CURSOR_API_KEY").context("missing CURSOR_API_KEY")?;

            let workspace = Workspace::new(tool_host.local_path())?;
            let format = request.format.clone();
            let options = agent_options(&request, &workspace, &default_model, api_key)?;
            let model_id = options.model.id.clone();

            let mcp_servers = request.mcp_servers();
            let mcp_names: Vec<&str> = mcp_servers.iter().map(|s| s.name.as_str()).collect();
            let prompt = with_mcp_hint(&mcp_servers, request.to_string());
            observe::log_completion(&model_id, &format, prompt.len(), &mcp_names);

            let rpc = bridge.rpc().clone();
            let created = rpc.create_agent(options).await?;
            let agent = Agent::new(rpc, created.agent_id.clone(), deadlines);

            let (abort_tx, mut abort_rx) = mpsc::unbounded_channel();
            let _attached = bridge.attach(created.agent_id, tool_host, abort_tx);

            let output = agent.send(&prompt, &mut abort_rx).await?;
            observe::log_answer(1, &output);
            let reason = match take_answer(&format, output) {
                Outcome::Done(answer) => return Ok(answer),
                Outcome::Repair(reason) => reason,
            };

            // The second (and last) attempt sends only the format-repair
            // instruction: the agent's session already carries the prompt
            // and the failed answer.
            tracing::debug!(attempt = 1, %reason, "repairing answer");
            let output = agent.send(&format.repair(&reason), &mut abort_rx).await?;
            observe::log_answer(2, &output);

            match take_answer(&format, output) {
                Outcome::Done(answer) => Ok(answer),
                Outcome::Repair(reason) => {
                    bail!("no answer after 2 repair attempts: {reason}");
                }
            }
        })
    }
}

/// The format gate's verdict on one answer attempt.
enum Outcome {
    Done(Answer),
    Repair(String),
}

fn take_answer(format: &Format, output: AgentOutput) -> Outcome {
    match format.parse(&output.result) {
        Ok(Candidate::Valid(value)) => Outcome::Done(Answer {
            value,
            usage: output.usage,
            transcript: output.transcript,
        }),
        Ok(Candidate::Invalid { reason, .. }) | Err(reason) => Outcome::Repair(reason),
    }
}
