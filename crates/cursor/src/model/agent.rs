//! One bridge-managed agent: `send` drives a turn's run stream to its
//! terminal result, bounded by an inactivity deadline that stream progress
//! rearms, an absolute wall-clock cap, and the callback's abort signal.
//! An abandoned run is cancelled best-effort, and the agent (with its
//! session state) is deleted on drop.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use omnia_wasi_model::{Answer, Candidate, Format, ToolHost, Transcript, Usage};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};

use super::observe::{self, EventLog};
use super::options::Workspace;
use crate::bridge::{AgentOptions, Bridge, Rpc, RunStatus, RunStreamResult, TokenUsage};
use crate::endpoint::Attached;

/// One bridge-managed agent, deleted (and its live run cancelled) on drop.
pub struct Agent {
    rpc: Rpc,
    id: String,
    deadlines: Deadlines,
    live_run: Option<String>,
    abort_rx: mpsc::UnboundedReceiver<String>,
    _attached: Attached,
    _workspace: Workspace,
}

impl Agent {
    /// Create one agent and attach its callback route.
    ///
    /// # Errors
    ///
    /// Returns an error when the bridge cannot create the agent.
    pub async fn create(
        bridge: &Bridge, options: AgentOptions, tool_host: Arc<dyn ToolHost>, deadlines: Deadlines,
        workspace: Workspace,
    ) -> Result<Self> {
        let rpc = bridge.rpc().clone();
        let created = rpc.create_agent(options).await?;
        let (abort_tx, abort_rx) = mpsc::unbounded_channel();
        let attached = bridge.attach(created.agent_id.clone(), tool_host, abort_tx);
        Ok(Self {
            rpc,
            id: created.agent_id,
            deadlines,
            live_run: None,
            abort_rx,
            _attached: attached,
            _workspace: workspace,
        })
    }

    /// Produce a format-valid answer, with one repair turn when needed.
    ///
    /// # Errors
    ///
    /// Returns an error when either turn fails or the repair is still invalid.
    pub async fn complete(&mut self, prompt: &str, format: &Format) -> Result<Answer> {
        let reason = match self.attempt(prompt, format, 1).await? {
            Outcome::Done(answer) => return Ok(answer),
            Outcome::Repair(reason) => reason,
        };

        tracing::debug!(attempt = 1, %reason, "repairing answer");
        let repair = format.repair(&reason);
        match self.attempt(&repair, format, 2).await? {
            Outcome::Done(answer) => Ok(answer),
            Outcome::Repair(reason) => bail!("no valid answer after 2 attempts: {reason}"),
        }
    }

    async fn attempt(&mut self, text: &str, format: &Format, attempt: u32) -> Result<Outcome> {
        let output = self.send(text).await?;
        observe::log_answer(attempt, &output);
        Ok(output.answer(format))
    }

    /// One turn: `Send` the text and consume the run stream to its terminal
    /// result, bounded by the inactivity and absolute deadlines and by the
    /// callback's abort signal.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failures, a breached deadline, an
    /// abort from the tool callback, or a run that ends unfinished.
    async fn send(&mut self, text: &str) -> Result<Response> {
        observe::log_send(text);
        let mut stream = self.rpc.send(self.id.clone(), text.to_owned()).await?;

        let (activity_tx, activity_rx) = watch::channel(Instant::now());
        let deadlines = self.deadlines;
        let deadline = deadlines.watch(activity_rx);
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
                        self.note_run(log.run_id());
                    }
                    if let Some(result) = message.result {
                        self.note_run(Some(&result.run_id));
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
                reason = self.abort_rx.recv() => {
                    self.cancel_live_run();
                    bail!(
                        "completion aborted: {}",
                        reason.unwrap_or_else(|| "session closed".to_owned())
                    );
                }
            }
        }

        // The run reached a terminal state; nothing is left to cancel.
        self.live_run = None;
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
        
        Ok(Response {
            result: result.result,
            transcript: log.finish(),
            usage: result.usage.map(Usage::from),
        })
    }

    // Keep the first observed run id.
    fn note_run(&mut self, run_id: Option<&str>) {
        if self.live_run.is_none() {
            self.live_run = run_id.map(ToOwned::to_owned);
        }
    }

    // Best-effort, detached `CancelRun` when a turn is abandoned mid-run
    // (deadline breach or callback abort) — the completion's own error is
    // already decided, so the cancel is not awaited.
    fn cancel_live_run(&mut self) {
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

// The format gate's verdict on one answer attempt.
enum Outcome {
    Done(Answer),
    Repair(String),
}

// One completed turn: the final text plus the observed transcript and usage.
#[derive(Debug)]
pub struct Response {
    pub result: String,
    pub transcript: Option<Transcript>,
    pub usage: Option<Usage>,
}

impl Response {
    /// Gate this turn's text against `format`.
    fn answer(self, format: &Format) -> Outcome {
        match format.parse(&self.result) {
            Ok(Candidate::Valid(value)) => Outcome::Done(Answer {
                value,
                usage: self.usage,
                transcript: self.transcript,
            }),
            Ok(Candidate::Invalid { reason, .. }) | Err(reason) => Outcome::Repair(reason),
        }
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

/// Inactivity and absolute bounds on one run, from the connect options.
#[derive(Clone, Copy, Debug)]
pub struct Deadlines {
    /// Kill a run after this long with no stream events.
    pub inactivity: Duration,
    /// Kill a run after this long, streaming or not.
    pub cap: Duration,
}

impl Deadlines {
    /// Resolve when a run breaches its inactivity or absolute bound.
    pub async fn watch(&self, mut activity: watch::Receiver<Instant>) -> anyhow::Error {
        let cap = sleep_until(Instant::now() + self.cap);
        tokio::pin!(cap);
        let mut activity_closed = false;

        loop {
            let last_activity = *activity.borrow_and_update();
            let inactive = sleep_until(last_activity + self.inactivity);
            tokio::pin!(inactive);

            tokio::select! {
                () = &mut cap => {
                    return anyhow::anyhow!(
                        "cursor run timed out after {}s (absolute cap exceeded while still active)",
                        self.cap.as_secs()
                    );
                }
                () = &mut inactive => {
                    let idle = Instant::now().saturating_duration_since(last_activity).as_secs();
                    return anyhow::anyhow!(
                        "cursor run inactive for {idle}s (no stream events; inactivity limit {}s, \
                         absolute cap {}s)",
                        self.inactivity.as_secs(),
                        self.cap.as_secs()
                    );
                }
                changed = activity.changed(), if !activity_closed => {
                    activity_closed = changed.is_err();
                }
            }
        }
    }
}

// Deliberate unit tests: pure deadline logic under a paused clock (CI floor);
// `tests/live.rs` is the acceptance gate proving a real bridge-driven run
// works end-to-end.
#[cfg(test)]
mod tests {
    use tokio::sync::watch;
    use tokio::time::{Duration, Instant, sleep};

    use super::Deadlines;

    const DEADLINES: Deadlines = Deadlines {
        inactivity: Duration::from_mins(2),
        cap: Duration::from_mins(10),
    };

    #[tokio::test(start_paused = true)]
    async fn silent_stream_hits_inactivity_deadline() {
        let (_activity, receiver) = watch::channel(Instant::now());
        let started = Instant::now();
        let error = DEADLINES.watch(receiver).await;
        assert_eq!(started.elapsed(), Duration::from_mins(2));
        assert!(
            error.to_string().contains("inactive for 120s"),
            "the inactivity kill names the idle span: {error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn steady_activity_hits_absolute_cap() {
        let (activity, receiver) = watch::channel(Instant::now());
        let started = Instant::now();
        let toucher = async {
            loop {
                sleep(Duration::from_mins(1)).await;
                activity.send_replace(Instant::now());
            }
        };
        let error = tokio::select! {
            error = DEADLINES.watch(receiver) => error,
            () = toucher => unreachable!("the toucher never finishes"),
        };
        assert_eq!(started.elapsed(), Duration::from_mins(10));
        assert!(
            error.to_string().contains("timed out after 600s"),
            "the cap kill names the absolute bound: {error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn late_activity_rearms_inactivity_deadline() {
        let (activity, receiver) = watch::channel(Instant::now());
        let started = Instant::now();
        let toucher = async {
            sleep(Duration::from_secs(100)).await;
            activity.send_replace(Instant::now());
            std::future::pending::<()>().await;
        };
        let error = tokio::select! {
            error = DEADLINES.watch(receiver) => error,
            () = toucher => unreachable!("the toucher never finishes"),
        };
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(220),
            "one touch at 100s moves the kill to 100s + the 120s window"
        );
        assert!(error.to_string().contains("inactive for 120s"), "unexpected: {error}");
    }
}
