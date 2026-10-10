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
use std::sync::atomic::{AtomicU64, Ordering};

pub use agent::Deadlines;
use agent::{Attempt, Unanswered};
use anyhow::anyhow;
use omnia_wasi_model::{Answer, Format, FutureResult, Limits, Request, ToolHost, WasiModelCtx};
use options::{Defaults, Turn};
use tracing::field::Empty;
use tracing::{Instrument, info_span};

use crate::Client;

static COMPLETION_ID: AtomicU64 = AtomicU64::new(0);

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let client = self.clone();
        let model = request.model.as_deref().unwrap_or(&self.model);
        let n = COMPLETION_ID.fetch_add(1, Ordering::Relaxed) + 1;

        // a schema's name is the guest's label for the question it asks
        let span = info_span!("complete", n, model, format = %request.format, label = Empty);
        if let Format::Schema(schema) = &request.format {
            span.record("label", schema.name.as_str());
        }

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

    fn limits(&self) -> Limits {
        Limits {
            max_tool_calls: self.max_tool_calls,
            ..Limits::default()
        }
    }
}

impl Client {
    async fn attempt(
        &self, request: &Request, tool_host: &Arc<dyn ToolHost>,
    ) -> Result<Answer, Unanswered> {
        let api_key = self
            .api_key
            .as_deref()
            .ok_or_else(|| anyhow!("CURSOR_API_KEY must be set"))
            .map_err(Unanswered::settled)?;

        let defaults = Defaults {
            model: &self.model,
            api_key,
            shell_roots: &self.shell_roots,
        };
        let turn = Turn::prepare(request, tool_host.local_path(), defaults)
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
