//! In-process `wasi-model` backend.
//!
//! Each completion translates a validated [`Request`] into a provider
//! conversation and drives it within the configured round budget. Guest
//! function calls are delegated through [`ToolHost::call_tool`], the `read`
//! and `list` tools operate directly on a lent workspace, and a workspace
//! lent under a shell root adds a `shell` tool whose commands run inside the
//! sandbox session prepared for the completion. When the request asks for a
//! `check`, each final text is offered to the guest through
//! [`ToolHost::check`]; a rejection stays in the conversation with the
//! guest's correction as the next user turn, and the loop goes round. What
//! the request gets wrong is refused before any of that starts.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use omnia_sandbox::{Access, Denied, Grant};
use omnia_wasi_model::{
    Answer, Error, Format, FutureResult, Limits, Request, ToolHost, WasiModelCtx,
};
use tracing::field::Empty;
use tracing::{Instrument, info_span};

use crate::Client;
use crate::conversation::Conversation;
use crate::shell::Shell;

static COMPLETION_ID: AtomicU64 = AtomicU64::new(0);

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let client = self.clone();
        let model = request.model.clone().unwrap_or_else(|| self.model.clone());
        let n = COMPLETION_ID.fetch_add(1, Ordering::Relaxed) + 1;

        // a schema's name is the guest's label for the question it asks
        let span = info_span!("complete", n, model = model.as_str(), format = %request.format, label = Empty);
        if let Format::Schema(schema) = &request.format {
            span.record("label", schema.name.as_str());
        }

        Box::pin(
            async move {
                let access = client.access(&request, tool_host.local_path()).await?;
                Conversation::prepare(&client, &request, model, access, tool_host)
                    .await?
                    .complete()
                    .await
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
    // A worker grant needs a shell: a worker reaches beyond its workspace
    // through the commands it runs there, so one over a read-only workspace,
    // or under a deployment with no shell root, is the request's fault.
    async fn access(&self, request: &Request, workspace: Option<&Path>) -> anyhow::Result<Access> {
        let access = match &self.sandbox {
            Some(sandbox) => sandbox.access(workspace).await?,
            None if workspace.is_some() => Access::Read,
            None => Access::Private,
        };

        if request.grants.worker.is_some() && access != Access::Shell {
            let detail = if !access.is_lent() {
                "worker grant needs a workspace"
            } else if self.sandbox.is_none() {
                "worker grant needs a shell: no shell root is configured"
            } else {
                "worker grant needs a shell: the workspace is not under a shell root"
            };
            return Err(Error::InvalidRequest(detail.to_owned()).into());
        }
        Ok(access)
    }

    // A session is prepared only where the lend opens a shell.
    pub(crate) async fn shell(
        &self, request: &Request, access: Access, workspace: Option<&Path>,
    ) -> anyhow::Result<Option<Shell>> {
        let (Some(sandbox), Access::Shell) = (&self.sandbox, access) else {
            return Ok(None);
        };
        let grant = request.grants.worker.as_ref().map_or_else(Grant::default, |worker| Grant {
            egress: worker.egress.clone(),
            read: worker.read.clone(),
            env: worker.env.clone(),
        });
        let session = sandbox.prepare(workspace, &grant).await.map_err(refusal)?;
        Ok(Some(Shell::new(session, self.shell_timeout, self.limits().max_result_bytes)))
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
