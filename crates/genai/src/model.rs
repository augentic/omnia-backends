//! In-process `wasi-model` backend.
//!
//! Each completion translates a validated [`Request`] into a provider
//! conversation and drives it within the configured budgets. Guest function
//! calls are delegated through [`ToolHost::call_tool`], the `read` and
//! `list` tools operate directly on a lent workspace, and a workspace lent
//! under a shell root adds a `shell` tool whose commands run inside the
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
use crate::request::Turn;
use crate::shell::Shell;
use crate::tools::{Tools, workspace_tools};

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
            async move { client.stage(&request, tool_host).await?.complete().await }
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
    // The request is translated before the lend is resolved, and the lend
    // resolved before a session is prepared, so what the request gets wrong
    // is refused with the least work done.
    async fn stage(
        &self, request: &Request, tool_host: Arc<dyn ToolHost>,
    ) -> anyhow::Result<Conversation> {
        let workspace = tool_host.local_path();
        let mut turn = Turn::translate(request, &self.model)?;
        let access = self.access(workspace).await?;
        let shell = self.shell(request, access, workspace).await?;
        turn.advertise(workspace_tools(access));
        let tools = Tools::new(tool_host, shell, self.limits().max_result_bytes);
        Ok(Conversation::new(self.inner.clone(), turn, tools, self.budgets))
    }

    // What runs in the lend: the sandbox's answer where one is connected,
    // reads alone where none is.
    async fn access(&self, workspace: Option<&Path>) -> anyhow::Result<Access> {
        match &self.sandbox {
            Some(sandbox) => sandbox.access(workspace).await,
            None if workspace.is_some() => Ok(Access::Read),
            None => Ok(Access::Private),
        }
    }

    // A session is prepared only where the lend opens a shell. A worker
    // grant needs one: a worker reaches beyond its workspace through the
    // commands it runs there, so a grant over a read-only workspace, or
    // under a deployment with no shell root, is the request's fault.
    async fn shell(
        &self, request: &Request, access: Access, workspace: Option<&Path>,
    ) -> anyhow::Result<Option<Shell>> {
        let worker = request.grants.worker.as_ref();
        let (Some(sandbox), Access::Shell) = (&self.sandbox, access) else {
            if worker.is_none() {
                return Ok(None);
            }
            let detail = if !access.is_lent() {
                "worker grant needs a workspace"
            } else if self.sandbox.is_none() {
                "worker grant needs a shell: no shell root is configured"
            } else {
                "worker grant needs a shell: the workspace is not under a shell root"
            };
            return Err(Error::InvalidRequest(detail.to_owned()).into());
        };

        let grant = worker.map_or_else(Grant::default, |worker| Grant {
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
