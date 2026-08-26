//! One bridge-managed agent: `send` drives a turn's run stream to its
//! terminal result, bounded by the deadlines and the callback's abort
//! signal. An abandoned run is cancelled best-effort, and the agent (with
//! its session state) is deleted on drop.

use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, bail};
use omnia_wasi_model::{Transcript, Usage};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use super::deadlines::Deadlines;
use super::observe::{self, EventLog};
use crate::bridge::{Rpc, RunStatus, RunStreamResult, TokenUsage};

/// One completed turn: the final text plus the observed transcript and usage.
#[derive(Debug)]
pub struct AgentOutput {
    pub result: String,
    pub transcript: Option<Transcript>,
    pub usage: Option<Usage>,
}

/// One bridge-managed agent, deleted (and its live run cancelled) on drop.
pub struct Agent {
    rpc: Rpc,
    id: String,
    deadlines: Deadlines,
    live_run: LiveRun,
}

impl Agent {
    pub fn new(rpc: Rpc, id: String, deadlines: Deadlines) -> Self {
        Self {
            rpc,
            id,
            deadlines,
            live_run: LiveRun::default(),
        }
    }

    /// One turn: `Send` the text and consume the run stream to its terminal
    /// result, bounded by the inactivity and absolute deadlines and by the
    /// callback's abort signal.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failures, a breached deadline, an
    /// abort from the tool callback, or a run that ends unfinished.
    pub async fn send(
        &self, text: &str, abort_rx: &mut mpsc::UnboundedReceiver<String>,
    ) -> Result<AgentOutput> {
        observe::log_send(text);
        let mut stream = self.rpc.send(self.id.clone(), text.to_owned()).await?;

        let (activity_tx, activity_rx) = watch::channel(Instant::now());
        let deadline = self.deadlines.watch(activity_rx);
        tokio::pin!(deadline);

        let mut log = EventLog::default();
        let mut outcome: Option<RunStreamResult> = None;

        loop {
            tokio::select! {
                message = stream.next() => {
                    let Some(message) = message? else {
                        break;
                    };
                    activity_tx.send_replace(Instant::now());
                    if let Some(event) = &message.sdk_message {
                        log.observe(event);
                        self.live_run.note(log.run_id());
                    }
                    if let Some(result) = message.result {
                        self.live_run.note(Some(&result.run_id));
                        outcome = Some(result);
                    }
                    if message.done.is_some() {
                        break;
                    }
                }
                error = &mut deadline => {
                    self.cancel_live_run();
                    return Err(error);
                }
                reason = abort_rx.recv() => {
                    self.cancel_live_run();
                    bail!(
                        "completion aborted: {}",
                        reason.unwrap_or_else(|| "session closed".to_owned())
                    );
                }
            }
        }

        // The run reached a terminal state; nothing is left to cancel.
        self.live_run.take();
        let outcome = outcome.context("the run stream ended without a result")?;
        if outcome.status != RunStatus::Finished {
            let detail = outcome
                .error_code
                .filter(|code| !code.is_empty())
                .or_else(|| log.status_message().map(ToOwned::to_owned))
                .unwrap_or_else(|| "<no detail>".to_owned());
            bail!("cursor run {}: {detail}", outcome.status);
        }
        let result = outcome.result.unwrap_or_default();
        Ok(AgentOutput {
            result: result.result,
            transcript: log.finish(),
            usage: result.usage.map(Usage::from),
        })
    }

    /// Best-effort, detached `CancelRun` when a turn is abandoned mid-run
    /// (deadline breach or callback abort) — the completion's own error is
    /// already decided, so the cancel is not awaited.
    fn cancel_live_run(&self) {
        let Some(run_id) = self.live_run.take() else {
            return;
        };
        let rpc = self.rpc.clone();
        let agent_id = self.id.clone();
        tokio::spawn(async move {
            if let Err(error) = rpc.cancel_run(run_id, agent_id).await {
                tracing::debug!(%error, "cancel after abandon failed");
            }
        });
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        self.cancel_live_run();
        let rpc = self.rpc.clone();
        let agent_id = std::mem::take(&mut self.id);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(error) = rpc.delete_agent(agent_id).await {
                    tracing::debug!(%error, "agent delete failed");
                }
            });
        }
    }
}

/// The in-flight run id, noted from the stream for `CancelRun`.
#[derive(Default)]
struct LiveRun(Mutex<Option<String>>);

impl LiveRun {
    /// Keep the first observed run id.
    fn note(&self, run_id: Option<&str>) {
        if let Some(run_id) = run_id {
            self.lock().get_or_insert_with(|| run_id.to_owned());
        }
    }

    fn take(&self) -> Option<String> {
        self.lock().take()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<String>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl From<TokenUsage> for Usage {
    // Wire counts are `i64`; saturate rather than fail on absurd values.
    fn from(usage: TokenUsage) -> Self {
        Self {
            input_tokens: u32::try_from(usage.input_tokens).unwrap_or(u32::MAX),
            output_tokens: u32::try_from(usage.output_tokens).unwrap_or(u32::MAX),
            reasoning_tokens: usage.reasoning_tokens.and_then(|count| u32::try_from(count).ok()),
        }
    }
}
