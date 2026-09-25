//! In-process `wasi-model` backend.
//!
//! Each completion translates a validated [`Request`] into a provider
//! conversation and drives it within a shared round budget. Guest function
//! calls are delegated through [`ToolHost::call_tool`], while the `read` and
//! `list` tools operate directly on a lent workspace. When the request asks
//! for a `check`, each final text is offered to the guest through
//! [`ToolHost::check`]; a rejection stays in the conversation with the
//! guest's correction as the next user turn, and the loop goes round.

mod conversation;
mod observe;
mod options;
mod tools;

use std::sync::Arc;

use conversation::Conversation;
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
                let turn = Turn::prepare(&request, tool_host.local_path(), &client.model)?;
                Conversation::new(&client, turn, tool_host).complete().await
            }
            .instrument(span),
        )
    }
}
