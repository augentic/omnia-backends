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
//!
//! A bridge lost under the opening of a completion — the process exited, or
//! its socket failed below Connect — before any candidate reached the guest
//! is not the prompt's doing: the completion runs once more, on a fresh
//! lease, with the original prompt and fresh deadlines. Exactly one restart
//! per call; the second failure stands.

mod agent;
mod observe;
mod options;

use std::sync::Arc;

pub use agent::Deadlines;
use agent::{Agent, Unanswered};
pub use observe::Failure;
use omnia_wasi_model::{Answer, FutureResult, Request, ToolHost, WasiModelCtx};
use options::Turn;
use tracing::{Instrument, info_span};

use crate::Client;

impl WasiModelCtx for Client {
    /// One completion on a bridge-managed agent of its own, restarted once
    /// on a fresh bridge when the first is lost before any candidate
    /// reaches the guest.
    ///
    /// # Errors
    ///
    /// The error downcasts to one of: [`Failure`] — `Timeout`, `Inactive`,
    /// `Aborted`, or `BridgeExited` (the spawned process died and the
    /// restart, if any, failed too); [`crate::TransportError`] — the socket
    /// to the bridge failed below Connect; or
    /// [`omnia_wasi_model::Error::BudgetExhausted`] — the guest's `check`
    /// rejected every candidate. Anything else is a plain error the bridge
    /// or the provider answered with: a Connect error, an end-stream error,
    /// a run that ended in a failing status, a lease that could not be
    /// taken, or a request that could not be shaped.
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let client = self.clone();

        Box::pin(
            async move {
                let failed = match attempt(&client, &request, &tool_host).await {
                    Ok(answer) => return Ok(answer),
                    Err(failed) if failed.restartable() => failed,
                    Err(failed) => return Err(failed.error),
                };
                let error = format!("{:#}", failed.error);
                tracing::warn!(
                    monotonic_counter.cursor_bridge_restarts = 1_u64,
                    outcome = observe::outcome_of(&failed.error),
                    pid = failed.pid(),
                    %error,
                    "bridge lost before any candidate; completion restarting on a fresh bridge"
                );
                attempt(&client, &request, &tool_host).await.map_err(|failed| failed.error)
            }
            .instrument(info_span!("complete")),
        )
    }
}

/// One run of the completion on one agent: shape the request, lease a
/// bridge, create the agent, drive it to an answer, tear it down. The lease
/// goes back through the agent's teardown before this returns.
async fn attempt(
    client: &Client, request: &Request, tool_host: &Arc<dyn ToolHost>,
) -> Result<Answer, Unanswered> {
    // A request that cannot be shaped fails before it queues for a slot;
    // the deadlines start inside `create`, after the wait. Neither failure
    // is a lost bridge — the probe proved the binary — so both stand.
    let turn = Turn::prepare(request, tool_host.local_path(), &client.model)
        .map_err(Unanswered::settled)?;
    let lease = client.pool.lease().await.map_err(Unanswered::settled)?;
    let agent = Agent::create(client, lease, turn, Arc::clone(tool_host))
        .await
        .map_err(Unanswered::before_candidate)?;
    agent.complete().await
}
