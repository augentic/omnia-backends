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
mod stream;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use agent::{Agent, Unanswered};
use anyhow::anyhow;
use omnia_sandbox::{Access, Denied, Grant, Session};
use omnia_wasi_model::{
    Answer, Error, Format, FutureResult, Limits, Request, ToolHost, WasiModelCtx,
};
use options::Turn;
use tracing::field::Empty;
use tracing::{Instrument, info_span};

use crate::Client;
use crate::pool::Lease;

static COMPLETION_ID: AtomicU64 = AtomicU64::new(0);

// The two bounds on one run, from the connect options: the inactivity
// window, which stream progress rearms while the run waits on the bridge,
// and the absolute cap, which nothing rearms.
#[derive(Clone, Copy, Debug)]
pub struct Deadlines {
    pub inactivity: Duration,
    pub cap: Duration,
}

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
        let (lease, turn) = self.stage(request, tool_host).await.map_err(Unanswered::settled)?;
        Agent::complete(lease, turn, Arc::clone(tool_host), self.deadlines).await
    }

    // The session and the turn come before the slot, so a bad workspace or
    // grant costs no wait.
    async fn stage(
        &self, request: &Request, tool_host: &Arc<dyn ToolHost>,
    ) -> anyhow::Result<(Lease, Turn)> {
        let api_key =
            self.api_key.as_deref().ok_or_else(|| anyhow!("CURSOR_API_KEY must be set"))?;
        let session = self.prepare(request, tool_host.local_path()).await?;
        let turn = Turn::prepare(request, &session, &self.model, api_key)?;
        let lease = self.pool.acquire(&session).await?;
        Ok((lease, turn))
    }

    // A worker grant needs a shell: a worker reaches beyond its workspace
    // through the commands it runs there, so one over a read-only workspace
    // is the request's fault.
    async fn prepare(
        &self, request: &Request, workspace: Option<&Path>,
    ) -> anyhow::Result<Session> {
        let worker = request.grants.worker.as_ref();
        let grant = worker.map_or_else(Grant::default, |worker| Grant {
            egress: worker.egress.clone(),
            read: worker.read.clone(),
            env: worker.env.clone(),
        });
        let session = self.pool.prepare(workspace, &grant).await.map_err(refusal)?;

        if worker.is_some() && session.access() != Access::Shell {
            let detail = if session.access().is_lent() {
                "worker grant needs a shell: the workspace is not under a shell root"
            } else {
                "worker grant needs a workspace"
            };
            return Err(Error::InvalidRequest(detail.to_owned()).into());
        }

        Ok(session)
    }
}

// A session the sandbox denied for the grant's fault is the request's; a host
// failure stays the backend's.
fn refusal(error: anyhow::Error) -> anyhow::Error {
    match error.downcast::<Denied>() {
        Ok(denied) => Error::InvalidRequest(format!("worker grant: {denied}")).into(),
        Err(error) => error,
    }
}
