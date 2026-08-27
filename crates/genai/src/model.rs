//! `wasi-model` implementation driving one in-process provider conversation
//! per completion.
//!
//! The gate-validated [`Request`] maps onto a provider chat request: guest
//! function tools are advertised to the provider (executed in-process
//! through [`ToolHost::call_tool`], where the guest's tool closure answers),
//! and a lent workspace adds the host-injected `read`/`list` tools, served
//! host-side without traversing the session. Chat rounds share one budget;
//! a failed format gate appends the repair instruction to the same
//! conversation, which already carries the prompt and the failed answer.

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

        Box::pin(
            async move {
                let turn = Turn::prepare(&request, tool_host.local_path(), &client.model)?;
                Conversation::new(&client, turn, tool_host).complete().await
            }
            .instrument(info_span!("complete")),
        )
    }
}
