//! In-process `wasi-model` backend.
//!
//! Each validated [`Request`] becomes a provider conversation bounded by the
//! configured budgets. Invalid requests fail before a shell session is
//! prepared and before any provider request is made.
//!
//! Available tools depend on the request and its lend:
//!
//! - Guest functions run through [`ToolHost::call_tool`].
//! - `read` and `list` operate on the lent workspace.
//! - A workspace lent under a shell root adds a sandboxed `shell` tool.
//!
//! When a request includes a check, each answer goes to [`ToolHost::check`].
//! A rejection appends the guest's correction and continues the conversation.

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
use crate::request::PreparedRequest;
use crate::shell::Shell;
use crate::tools::Tools;

static COMPLETION_ID: AtomicU64 = AtomicU64::new(0);

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let client = self.clone();
        let model = request.model.as_deref().unwrap_or(&self.model);
        let n = COMPLETION_ID.fetch_add(1, Ordering::Relaxed) + 1;

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
    async fn stage(
        &self, request: &Request, tool_host: Arc<dyn ToolHost>,
    ) -> anyhow::Result<Conversation> {
        let workspace = tool_host.local_path();
        let access = self.access(workspace).await?;
        let prepared = PreparedRequest::translate(request, &self.model, self.max_tokens, access)?;
        let shell = self.shell(request, access, workspace).await?;
        let tools =
            Tools::new(tool_host, shell, prepared.declared(), self.limits().max_result_bytes);
        Ok(Conversation::new(self.inner.clone(), prepared, tools, self.budgets))
    }

    async fn access(&self, workspace: Option<&Path>) -> anyhow::Result<Access> {
        match &self.sandbox {
            Some(sandbox) => sandbox.access(workspace).await,
            None if workspace.is_some() => Ok(Access::Read),
            None => Ok(Access::Private),
        }
    }

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
        Ok(Some(Shell::new(session, self.shell_timeout)))
    }
}

fn refusal(error: anyhow::Error) -> anyhow::Error {
    match error.downcast::<Denied>() {
        Ok(denied) => Error::InvalidRequest(format!("worker grant: {denied}")).into(),
        Err(error) => error,
    }
}
