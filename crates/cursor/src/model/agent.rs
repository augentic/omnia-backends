//! A bridge-managed agent: `send` drives a turn's run stream to its
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
use tracing::instrument;

use super::observe::{self, Completion, EventLog};
use super::options::{Turn, Workspace};
use crate::Client;
use crate::bridge::{Rpc, RunStatus, RunStreamResult, TokenUsage};
use crate::endpoint::Attached;

pub struct Agent {
    rpc: Rpc,
    id: String,
    deadlines: Deadlines,
    model: String,
    prompt: String,
    format: Format,
    live_run: Option<String>,
    abort_rx: mpsc::UnboundedReceiver<String>,
    completion: Option<Completion>,
    _attached: Attached,
    _workspace: Workspace,
}

impl Agent {
    #[instrument(
        skip_all,
        fields(
            model = %turn.options.model.id,
            format = observe::format_name(&turn.format),
        )
    )]
    pub async fn create(client: &Client, turn: Turn, tool_host: Arc<dyn ToolHost>) -> Result<Self> {
        let completion = Completion::start(&turn);
        let model = turn.options.model.id.clone();
        let rpc = client.bridge.rpc().clone();
        let created = match rpc.create_agent(turn.options).await {
            Ok(created) => created,
            Err(error) => {
                completion.finish("error");
                return Err(error);
            }
        };
        let (abort_tx, abort_rx) = mpsc::unbounded_channel();
        let attached = client.bridge.attach(created.agent_id.clone(), tool_host, abort_tx);

        Ok(Self {
            rpc,
            id: created.agent_id,
            deadlines: client.deadlines,
            model,
            prompt: turn.prompt,
            format: turn.format,
            live_run: None,
            abort_rx,
            completion: Some(completion),
            _attached: attached,
            _workspace: turn.workspace,
        })
    }

    #[instrument(
        skip_all,
        fields(
            model = %self.model,
            format = observe::format_name(&self.format),
            outcome = tracing::field::Empty,
        )
    )]
    pub async fn complete(mut self) -> Result<Answer> {
        let result = self.run().await;
        let attempts = self.completion.as_ref().map_or(0, Completion::attempts);
        let outcome = match &result {
            Ok(_) if attempts > 1 => "repair",
            Ok(_) => "ok",
            Err(error) => observe::outcome_of(error),
        };
        if let Some(completion) = self.completion.take() {
            completion.finish(outcome);
        }
        result
    }

    async fn run(&mut self) -> Result<Answer> {
        let prompt = std::mem::take(&mut self.prompt);
        let reason = match self.try_complete(&prompt).await? {
            Verdict::Done(answer) => return Ok(answer),
            Verdict::Repair(reason) => reason,
        };

        tracing::debug!(%reason, "repairing answer");
        let repaired = self.format.repair(&reason);

        match self.try_complete(&repaired).await? {
            Verdict::Done(answer) => Ok(answer),
            Verdict::Repair(reason) => bail!("no valid answer after 2 attempts: {reason}"),
        }
    }

    async fn try_complete(&mut self, text: &str) -> Result<Verdict> {
        if let Some(completion) = &mut self.completion {
            completion.new_attempt();
        }

        let response = self.send(text).await?;
        if let Some(completion) = &mut self.completion {
            let tools = response.transcript.as_ref().map_or(0, |t| t.turns.len());
            completion.record(response.result.len(), tools, response.usage.as_ref());
        }

        Ok(response.answer(&self.format))
    }

    async fn send(&mut self, text: &str) -> Result<Response> {
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

        // the run reached a terminal state; nothing is left to cancel.
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

    fn note_run(&mut self, run_id: Option<&str>) {
        if self.live_run.is_none() {
            self.live_run = run_id.map(ToOwned::to_owned);
        }
    }

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

// One completed turn: the final text plus the observed transcript and usage.
#[derive(Debug)]
pub struct Response {
    pub result: String,
    pub transcript: Option<Transcript>,
    pub usage: Option<Usage>,
}

impl Response {
    fn answer(self, format: &Format) -> Verdict {
        match format.parse(&self.result) {
            Ok(Candidate::Valid(value)) => Verdict::Done(Answer {
                value,
                usage: self.usage,
                transcript: self.transcript,
            }),
            Ok(Candidate::Invalid { reason, .. }) | Err(reason) => Verdict::Repair(reason),
        }
    }
}

enum Verdict {
    Done(Answer),
    Repair(String),
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
    pub async fn watch(self, mut activity: watch::Receiver<Instant>) -> anyhow::Error {
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
