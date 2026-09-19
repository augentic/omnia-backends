//! `wasi-model` implementation driving one bridge-managed Cursor agent per
//! completion.
//!
//! The host-validated [`Request`] maps onto `CreateAgent` options: guest
//! function tools become SDK custom tools (executed back through the
//! session via the loopback callback and [`ToolHost::call_tool`]), MCP
//! grants ride inline as `mcp_servers`, and the lent workspace — or a
//! private empty directory when none is lent — becomes the agent's `cwd`.
//! One `Send` stream produces the answer; when the request asks for a
//! `check`, the answer is offered to the guest through [`ToolHost::check`]
//! and a rejection sends the guest's correction on the same agent, whose
//! session already carries the prompt and the rejected answer. Each agent
//! runs on a leased bridge from the client's pool, taken before it is
//! created and released once its teardown is done.

mod agent;
mod observe;
mod options;

use std::sync::Arc;

use agent::Agent;
pub use agent::Deadlines;
use omnia_wasi_model::{Answer, FutureResult, Request, ToolHost, WasiModelCtx};
use options::Turn;
use tracing::{Instrument, info_span};

use crate::Client;

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let client = self.clone();

        Box::pin(
            async move {
                // A request that cannot be shaped fails before it queues for
                // a slot; the deadlines start inside `create`, after the wait.
                let turn = Turn::prepare(&request, tool_host.local_path(), &client.model)?;
                let lease = client.pool.lease().await?;
                Agent::create(&client, lease, turn, tool_host).await?.complete().await
            }
            .instrument(info_span!("complete")),
        )
    }
}
