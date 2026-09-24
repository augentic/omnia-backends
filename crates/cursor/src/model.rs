//! `wasi-model` backend over `cursor-sdk-bridge`.
//!
//! Each completion leases a worker, creates an agent from the [`Request`], and
//! streams the answer. Guest tools run through [`ToolHost::call_tool`]; an
//! optional [`ToolHost::check`] can reject and correct on the same session. If
//! the worker is lost before any candidate reaches the guest, the completion
//! retries once on a fresh lease.

mod agent;
mod observe;
mod options;

use std::sync::Arc;

pub use agent::Deadlines;
use agent::{Attempt, Unanswered};
use omnia_wasi_model::{Answer, FutureResult, Request, ToolHost, WasiModelCtx};
use options::Turn;
use tracing::{Instrument, info_span};

use crate::Client;

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let client = self.clone();
        let model = request.model.as_deref().unwrap_or(&self.model);
        let span = info_span!("complete", model, format = %request.format);

        Box::pin(
            async move {
                let failed = match client.attempt(&request, &tool_host).await {
                    Ok(answer) => return Ok(answer),
                    Err(failed) if failed.restartable() => failed,
                    Err(failed) => return Err(failed.into_error()),
                };

                tracing::warn!(
                    pid = failed.pid(),
                    error = format!("{:#}", failed.error()),
                    "worker lost before any candidate; completion restarting on a fresh worker"
                );

                client.attempt(&request, &tool_host).await.map_err(Unanswered::into_error)
            }
            .instrument(span),
        )
    }
}

impl Client {
    async fn attempt(
        &self, request: &Request, tool_host: &Arc<dyn ToolHost>,
    ) -> Result<Answer, Unanswered> {
        let turn = Turn::prepare(request, tool_host.local_path(), &self.model, &self.api_key)
            .await
            .map_err(Unanswered::settled)?;
        let lease = self.pool.lease().await.map_err(Unanswered::settled)?;
        Attempt {
            lease,
            turn,
            tool_host: Arc::clone(tool_host),
            deadlines: self.deadlines,
        }
        .complete()
        .await
    }
}
