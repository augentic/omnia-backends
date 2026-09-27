//! An agent on a leased worker: one completion's `CreateAgent`, the `Send`
//! of its prompt and of each correction the guest's check returns, and the
//! `DeleteAgent` that gives the slot back.
//!
//! The whole of it runs on a task of its own, because the completion future
//! is the guest's to drop at any `.await`: the drop cancels a token the task
//! watches, and the task ends its run — cancelled by id once the stream has
//! named one — and still deletes its agent. No wait on the worker is
//! unbounded — the inactivity window that stream progress rearms while the
//! run waits on the bridge, the absolute cap that bounds it while the model
//! composes, the callback's abort and the worker's own exit each end one —
//! so a worker alive but silent unblocks the guest. A run cut short is
//! cancelled and its bill read back before the completion's one line, so a
//! timeout still reports what it cost.

use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use omnia_wasi_model::{Answer, Error, ToolHost, Transcript, Usage};
use tokio::sync::watch;
use tokio::time::{Instant, interval_at, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Span, instrument};

use super::observe::{Completion, EventLog, Phase};
use super::options::{AgentSpec, Prompt, Turn, Workspace};
use crate::endpoint::Attached;
use crate::failure::Outcome;
use crate::pool::Lease;
use crate::protocol::{AgentOperationOptions, RunStatus, RunStream, RunStreamResult, TokenUsage};
use crate::worker::Worker;
use crate::{Failure, elapsed_ms};

const MAX_ROUNDS: u32 = 2;
const TEARDOWN: Duration = Duration::from_secs(5);
const PROGRESS: Duration = Duration::from_secs(15);

// One completion attempt: its turn on a leased worker, callbacks into the tool host
pub struct Attempt {
    pub lease: Lease,
    pub turn: Turn,
    pub tool_host: Arc<dyn ToolHost>,
    pub deadlines: Deadlines,
}

impl Attempt {
    // `CreateAgent`, prompt to answer, then `DeleteAgent` on its own task
    // Drop ends the run, never the delete; opening failures are `before_candidate`
    pub async fn complete(self) -> Result<Answer, Unanswered> {
        // drop cancels the run
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();

        let task = tokio::spawn(self.run(cancel).instrument(Span::current()));
        task.await.unwrap_or_else(|panicked| {
            let error =
                anyhow::Error::new(panicked).context("the agent's task ended with no answer");
            Err(Unanswered::settled(error))
        })
    }

    // Whole life in order: answer waits on delete, so lease goes last
    async fn run(self, cancel: CancellationToken) -> Result<Answer, Unanswered> {
        let mut agent = Agent::create(self, cancel).await.map_err(Unanswered::before_candidate)?;
        let result = agent.rounds().await;
        agent.settle().await;
        agent.finish(&result);
        agent.delete().await;
        result
    }
}

// One completion's agent on a leased worker, `CreateAgent` to `DeleteAgent`
struct Agent {
    handle: Handle,
    prompt: Prompt,
    tool_host: Arc<dyn ToolHost>,
    deadlines: Deadlines,
    cancel: CancellationToken,
    completion: Completion,
    session: Attached,
}

impl Agent {
    // `CreateAgent`, bounded by inactivity window and worker exit
    async fn create(attempt: Attempt, cancel: CancellationToken) -> Result<Self> {
        let Attempt {
            lease,
            turn,
            tool_host,
            deadlines,
        } = attempt;

        let mut completion = Completion::from(&turn);
        let Turn { agent, prompt } = turn;
        let AgentSpec {
            options,
            operation,
            workspace,
        } = agent;

        let worker = lease.worker();
        let window = deadlines.inactivity;
        let created = worker.fail_on_exit(worker.rpc().create_agent(options));

        let id = match timeout(window, created).await {
            Ok(Ok(created)) if created.agent_id.is_empty() => {
                Err(anyhow!("sdk.v1 RPC `CreateAgent` returned an empty agent id"))
            }
            Ok(Ok(created)) => Ok(created.agent_id),
            Ok(Err(error)) => Err(error),
            Err(_elapsed) => {
                Err(anyhow!("sdk.v1 RPC `CreateAgent` unanswered after {}s", window.as_secs()))
            }
        }
        .inspect_err(|error| completion.finish(Outcome::of(error)))?;

        let session = lease.attach(id.clone(), Arc::clone(&tool_host));

        Ok(Self {
            handle: Handle {
                lease,
                id,
                operation,
                workspace,
                run_id: None,
            },
            prompt,
            tool_host,
            deadlines,
            cancel,
            completion,
            session,
        })
    }

    // Prompt then corrections on the same session until a candidate passes or rounds run out
    async fn rounds(&mut self) -> Result<Answer, Unanswered> {
        let mut prompt = mem::take(&mut self.prompt.text);
        let mut round = 1;
        loop {
            // round 1 fails before any candidate, so the guest has seen nothing
            let response = match self.send(round, &prompt).await {
                Ok(response) => response,
                Err(error) if round == 1 => return Err(Unanswered::before_candidate(error)),
                Err(error) => return Err(Unanswered::settled(error)),
            };

            let candidate = self.prompt.format.candidate(&response.result);
            if !self.prompt.check {
                return Ok(response.answer(candidate));
            }

            match self.tool_host.check(candidate.clone()).await.map_err(Unanswered::settled)? {
                Ok(()) => return Ok(response.answer(candidate)),
                Err(correction) if round < MAX_ROUNDS => {
                    tracing::debug!(%correction, "check rejected the candidate");

                    // the session persists, so the correction becomes the prompt
                    prompt = correction;
                }
                Err(correction) => {
                    return Err(Unanswered::settled(Error::BudgetExhausted(correction).into()));
                }
            }

            round += 1;
        }
    }

    // One `Send` to its answer, booked on the completion either way
    #[instrument(level = "debug", skip(self, text))]
    async fn send(&mut self, round: u32, text: &str) -> Result<Response> {
        self.completion.attempt();

        // bounds run from `Send`, so an unopened stream is an inactivity failure
        let activity = watch::Sender::new(Activity::opening());
        let deadline = self.deadlines.watch(&activity);
        tokio::pin!(deadline);

        let opened = tokio::select! {
            // in this order, so a guest already gone sends nothing
            biased;
            () = self.cancel.cancelled() => Err(abandoned()),
            stream = self.handle.worker().fail_on_exit(self.handle.send(text)) => stream,
            failure = &mut deadline => Err(failure.into()),
        };

        let outcome = match opened {
            Ok(stream) => self.follow(stream, &activity, deadline.as_mut()).await,
            Err(error) => Err(error),
        };

        // the wait since the last frame, in the phase the run ended in
        let Activity { at, phase } = activity.borrow().clone();
        self.completion.spent(&phase, at.elapsed());

        match &outcome {
            Ok(response) => {
                let tools = response.transcript.as_ref().map_or(0, |t| t.turns.len());
                self.completion.record(response.result.len(), tools, response.usage.as_ref());
            }
            // run lost with its process: silence tells a hang killed from a mid-stream crash
            Err(error) => {
                if let Some(Failure::WorkerExited(exit)) = error.downcast_ref::<Failure>() {
                    tracing::debug!(
                        pid = exit.pid,
                        silent_ms = elapsed_ms(activity.borrow().at),
                        "run lost with its process"
                    );
                }
            }
        }
        outcome
    }

    // Follow run to terminal result, noting run id for cancel and reporting every `PROGRESS`
    async fn follow(
        &mut self, mut stream: RunStream, activity: &watch::Sender<Activity>,
        mut deadline: Pin<&mut impl Future<Output = Failure>>,
    ) -> Result<Response> {
        let mut log = EventLog::new(self.handle.operation.cwd.clone());
        let mut outcome: Option<RunStreamResult> = None;
        let mut progress = interval_at(Instant::now() + PROGRESS, PROGRESS);

        loop {
            tokio::select! {
                message = self.handle.worker().fail_on_exit(stream.next()) => {
                    let Some(message) = message? else { break };
                    log.observe_message(&message);
                    self.advance(activity, log.phase());
                    if self.handle.run_id.is_none() {
                        self.handle.run_id = log.run_id().map(ToOwned::to_owned);
                    }
                    if message.result.is_some() {
                        outcome = message.result;
                    }
                    if message.done.is_some() {
                        break;
                    }
                }
                _ = progress.tick() => {
                    let current = activity.borrow();
                    tracing::info!(
                        elapsed_s = self.completion.elapsed().as_secs(),
                        frames = log.frames(),
                        tool_calls = log.tool_calls(),
                        silent_s = current.at.elapsed().as_secs(),
                        waiting = %current.phase,
                        "in progress"
                    );
                }
                failure = &mut deadline => return Err(failure.into()),
                reason = self.session.aborted() => return Err(Failure::Aborted(reason).into()),
                // abandoned run cancels by id, so follow to the opening frame
                () = self.cancel.cancelled(), if self.handle.run_id.is_some() => {
                    return Err(abandoned());
                }
            }
        }

        // terminal state: nothing left to cancel
        self.handle.run_id = None;
        let outcome = outcome.context("the run stream ended without a result")?;
        if outcome.status != RunStatus::Finished {
            let detail = outcome
                .error_code
                .filter(|code| !code.is_empty())
                .or_else(|| log.status_message().map(ToOwned::to_owned));
            return Err(Failure::Run {
                status: outcome.status,
                detail,
            }
            .into());
        }
        let result = outcome.result.unwrap_or_default();

        Ok(Response {
            result: result.result,
            transcript: log.finish(),
            usage: result.usage,
        })
    }

    // A frame arrived: book the wait since the last one on the phase it was
    // spent in, then rearm the window on the phase the stream is now in
    fn advance(&mut self, activity: &watch::Sender<Activity>, phase: Phase) {
        let Activity { at, phase: waited } = activity.borrow().clone();
        self.completion.spent(&waited, at.elapsed());
        activity.send_replace(Activity::now(phase));
    }

    // A run still open was cut short: cancel it and book what the agent was
    // billed, so the `completion` line carries the cost of a timeout too
    async fn settle(&mut self) {
        let Some(run_id) = self.handle.run_id.take() else {
            return;
        };
        let Some(rpc) = self.handle.worker().live_rpc() else {
            return;
        };
        call("CancelRun", rpc.cancel_run(run_id, self.handle.id.clone())).await;
        match timeout(TEARDOWN, rpc.get_usage(self.handle.id.clone())).await {
            Ok(Ok(Some(usage))) => self.completion.settle(&usage),
            Ok(Ok(None)) => tracing::debug!("no usage reported for the cancelled run"),
            Ok(Err(error)) => tracing::debug!(%error, "usage unavailable for the cancelled run"),
            Err(_elapsed) => tracing::debug!("usage unanswered for the cancelled run"),
        }
    }

    // `Completion` line before delete, so `duration_ms` covers the run
    fn finish(&mut self, result: &Result<Answer, Unanswered>) {
        let outcome = match result {
            Ok(_) if self.completion.attempts() > 1 => Outcome::Corrected,
            Ok(_) => Outcome::Ok,
            Err(unanswered) => Outcome::of(unanswered.error()),
        };
        self.completion.finish(outcome);
    }

    // The callback route (`session`) goes with `self`
    async fn delete(self) {
        self.handle.delete().await;
    }
}

// Agent on its worker, with what delete needs: lease, pin/workspace, open run
struct Handle {
    lease: Lease,
    id: String,
    operation: AgentOperationOptions,
    workspace: Workspace,
    run_id: Option<String>,
}

impl Handle {
    const fn worker(&self) -> &Worker {
        self.lease.worker()
    }

    async fn send(&self, text: &str) -> Result<RunStream> {
        self.worker().rpc().send(self.id.clone(), text.to_owned()).await
    }

    // Cancel, close, delete — each bounded, skipped if worker gone — then workspace, lease last
    async fn delete(self) {
        let Self {
            lease,
            id,
            operation,
            workspace,
            run_id,
        } = self;

        if let Some(rpc) = lease.worker().live_rpc() {
            let started = Instant::now();
            if let Some(run_id) = run_id {
                call("CancelRun", rpc.cancel_run(run_id, id.clone())).await;
            }
            call("CloseAgent", rpc.close_agent(id.clone())).await;
            call("DeleteAgent", rpc.delete_agent(id, operation)).await;
            tracing::debug!(teardown_ms = elapsed_ms(started), "agent deleted");
        } else {
            tracing::debug!("worker exited; skipping agent teardown");
        }
        drop(workspace);
        drop(lease);
    }
}

// Inactivity and absolute bounds on one run, from connect options
#[derive(Clone, Copy, Debug)]
pub struct Deadlines {
    // Kill run after this long with no stream events while it waits on the bridge
    pub inactivity: Duration,
    // Kill run after this long, streaming or not
    pub cap: Duration,
}

impl Deadlines {
    // Resolve on inactivity or cap; `activity` rearms inactivity, and a
    // phase the window does not bound leaves the cap alone
    async fn watch(self, activity: &watch::Sender<Activity>) -> Failure {
        let mut activity = activity.subscribe();
        let cap = sleep_until(Instant::now() + self.cap);
        tokio::pin!(cap);

        loop {
            let Activity { at, phase } = activity.borrow_and_update().clone();
            tokio::select! {
                () = &mut cap => {
                    return Failure::Timeout {
                        cap_secs: self.cap.as_secs(),
                    };
                }
                () = sleep_until(at + self.inactivity), if phase.bounded() => {
                    let idle = Instant::now().saturating_duration_since(at).as_secs();
                    return Failure::Inactive {
                        waiting: phase.to_string(),
                        idle_secs: idle,
                        inactivity_secs: self.inactivity.as_secs(),
                        cap_secs: self.cap.as_secs(),
                    };
                }
                // sender borrowed while polled, so channel never closes
                _ = activity.changed() => {}
            }
        }
    }
}

// The stream's last frame: when it arrived and what the run has waited on since
#[derive(Clone, Debug)]
struct Activity {
    at: Instant,
    phase: Phase,
}

impl Activity {
    fn opening() -> Self {
        Self::now(Phase::Opening)
    }

    fn now(phase: Phase) -> Self {
        Self {
            at: Instant::now(),
            phase,
        }
    }
}

// The run's answer as the wire carried it; the usage narrows to the guest's
// `Usage` only once the answer is the guest's
#[derive(Debug)]
struct Response {
    result: String,
    transcript: Option<Transcript>,
    usage: Option<TokenUsage>,
}

impl Response {
    fn answer(self, candidate: String) -> Answer {
        Answer {
            answer: candidate,
            usage: self.usage.map(Usage::from),
            transcript: self.transcript,
        }
    }
}

// No answer: the failure, and whether it struck before any candidate — in
// `CreateAgent` or the opening `Send`
pub struct Unanswered {
    error: anyhow::Error,
    before_candidate: bool,
}

impl Unanswered {
    const fn before_candidate(error: anyhow::Error) -> Self {
        Self {
            error,
            before_candidate: true,
        }
    }

    pub const fn settled(error: anyhow::Error) -> Self {
        Self {
            error,
            before_candidate: false,
        }
    }

    pub const fn error(&self) -> &anyhow::Error {
        &self.error
    }

    pub fn into_error(self) -> anyhow::Error {
        self.error
    }

    // Retryable if worker lost before any candidate: nothing said twice
    pub fn restartable(&self) -> bool {
        self.before_candidate && Outcome::of(&self.error).lost_worker()
    }

    pub fn pid(&self) -> Option<u32> {
        match self.error.downcast_ref::<Failure>() {
            Some(Failure::WorkerExited(exit)) => Some(exit.pid),
            _ => None,
        }
    }
}

// Guest dropped completion: reaches no one, `completion` reads `abort`
fn abandoned() -> anyhow::Error {
    Failure::Aborted("the guest dropped the completion".to_owned()).into()
}

// Best-effort teardown: failures logged, silence bounded
async fn call(method: &'static str, rpc: impl Future<Output = Result<()>>) {
    match timeout(TEARDOWN, rpc).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, method, "agent teardown call failed"),
        Err(_elapsed) => tracing::warn!(method, "agent teardown call unanswered"),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::watch;
    use tokio::time::{self, Instant};

    use super::{Activity, Deadlines, Phase, Unanswered};
    use crate::Failure;
    use crate::protocol::RpcError;
    use crate::worker::Exit;

    const DEADLINES: Deadlines = Deadlines {
        inactivity: Duration::from_secs(1),
        cap: Duration::from_secs(5),
    };
    const FRAME_GAP: Duration = Duration::from_millis(800);
    const EXITED: Exit = Exit { status: None, pid: 7 };

    // Clock paused, so every instant below is exact

    #[tokio::test(start_paused = true)]
    async fn silent() {
        let started = Instant::now();
        let activity = watch::Sender::new(Activity::opening());
        let failure = DEADLINES.watch(&activity).await;
        assert!(
            matches!(
                &failure,
                Failure::Inactive {
                    waiting,
                    idle_secs: 1,
                    inactivity_secs: 1,
                    cap_secs: 5,
                } if waiting == "the opening frame"
            ),
            "{failure}"
        );
        assert_eq!(started.elapsed(), DEADLINES.inactivity);
    }

    #[tokio::test(start_paused = true)]
    async fn rearmed() {
        let started = Instant::now();
        let activity = watch::Sender::new(Activity::opening());
        let deadline = DEADLINES.watch(&activity);
        tokio::pin!(deadline);

        // three tool frames, together well past the window, each inside it
        for _ in 0..3 {
            let still_watching = time::timeout(FRAME_GAP, &mut deadline).await.is_err();
            assert!(still_watching, "the window fired {:?} in", started.elapsed());
            activity.send_replace(Activity::now(Phase::Tool("read".to_owned())));
        }
        let failure = deadline.await;
        assert!(
            matches!(&failure, Failure::Inactive { waiting, idle_secs: 1, .. } if waiting == "tool `read`"),
            "{failure}"
        );
        assert_eq!(started.elapsed(), 3 * FRAME_GAP + DEADLINES.inactivity);
    }

    #[tokio::test(start_paused = true)]
    async fn capped() {
        let started = Instant::now();
        let activity = watch::Sender::new(Activity::opening());
        let deadline = DEADLINES.watch(&activity);
        tokio::pin!(deadline);
        let failure = loop {
            match time::timeout(FRAME_GAP, &mut deadline).await {
                Ok(failure) => break failure,
                Err(_elapsed) => {
                    activity.send_replace(Activity::now(Phase::Tool("read".to_owned())));
                }
            }
        };
        assert!(matches!(failure, Failure::Timeout { cap_secs: 5 }), "{failure}");
        assert_eq!(started.elapsed(), DEADLINES.cap);
    }

    // The model has its last tool result and says nothing until its answer:
    // the window stands down and the cap alone ends the wait.
    #[tokio::test(start_paused = true)]
    async fn composing() {
        let started = Instant::now();
        let activity = watch::Sender::new(Activity::opening());
        let deadline = DEADLINES.watch(&activity);
        tokio::pin!(deadline);
        let still_watching = time::timeout(FRAME_GAP, &mut deadline).await.is_err();
        assert!(still_watching);
        activity.send_replace(Activity::now(Phase::Model));
        let failure = deadline.await;
        assert!(matches!(failure, Failure::Timeout { cap_secs: 5 }), "{failure}");
        assert_eq!(started.elapsed(), DEADLINES.cap);
    }

    // A tool call started mid-compose brings the window back, from that frame.
    #[tokio::test(start_paused = true)]
    async fn tool_rearms_window() {
        let started = Instant::now();
        let activity = watch::Sender::new(Activity::now(Phase::Model));
        let deadline = DEADLINES.watch(&activity);
        tokio::pin!(deadline);
        let still_watching = time::timeout(2 * DEADLINES.inactivity, &mut deadline).await.is_err();
        assert!(still_watching, "the window fired under `Phase::Model`");
        activity.send_replace(Activity::now(Phase::Tool("shell".to_owned())));
        let failure = deadline.await;
        assert!(
            matches!(&failure, Failure::Inactive { waiting, idle_secs: 1, .. } if waiting == "tool `shell`"),
            "{failure}"
        );
        assert_eq!(started.elapsed(), 3 * DEADLINES.inactivity);
    }

    #[test]
    fn restartable() {
        let exited = || Failure::WorkerExited(EXITED).into();
        let reset = || RpcError::truncated("SdkAgentService/Send", 3).into();
        let stalled = || {
            Failure::Inactive {
                waiting: "the opening frame".to_owned(),
                idle_secs: 1,
                inactivity_secs: 1,
                cap_secs: 5,
            }
            .into()
        };

        assert!(Unanswered::before_candidate(exited()).restartable());
        assert!(Unanswered::before_candidate(reset()).restartable());

        // the guest has seen a candidate: nothing is offered twice
        assert!(!Unanswered::settled(exited()).restartable());

        // the worker is up and answering, in its way
        assert!(!Unanswered::before_candidate(stalled()).restartable());

        assert_eq!(Unanswered::before_candidate(exited()).pid(), Some(7));
        assert_eq!(Unanswered::before_candidate(reset()).pid(), None);
    }
}
