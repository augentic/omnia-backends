//! In-process `wasi-model` backend.
//!
//! Each completion translates a validated [`Request`] into a provider
//! conversation and drives it within a shared round budget. Guest function
//! calls are delegated through [`ToolHost::call_tool`], while the `read` and
//! `list` tools operate directly on a lent workspace. When an answer fails
//! format validation, the backend keeps it in the conversation and asks the
//! provider to repair it.

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
