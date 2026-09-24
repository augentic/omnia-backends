//! An agent on a leased worker: one completion's `CreateAgent`, the `Send` of
//! its prompt and of each correction the guest's check returns, and the
//! teardown that gives the slot back.
//!
//! No wait on the worker is unbounded — the inactivity window that stream
//! progress rearms, the absolute cap, the callback's abort and the worker's
//! own exit each end one — so a worker alive but silent unblocks the guest.
//! The completion future is the guest's to drop at any `.await`, so
//! `CreateAgent` and the teardown each run on a task of their own with the
//! lease riding on it: the worker closes only once nothing of the agent is
//! left on it.

use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use omnia_wasi_model::{Answer, Error, Format, ToolHost, Transcript, Usage};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until, timeout};

use super::observe::{Completion, EventLog};
use super::options::{Turn, Workspace};
use crate::endpoint::Attached;
use crate::failure::Outcome;
use crate::pool::Lease;
use crate::protocol::{AgentOperationOptions, AgentOptions, RunStatus, RunStream, RunStreamResult};
use crate::{Failure, elapsed_ms};

const MAX_ROUNDS: u32 = 2;
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const RUN_ID_WAIT: Duration = Duration::from_secs(2);

/// One completion's agent on a leased worker, from `CreateAgent` to its
/// teardown.
pub struct Agent {
    lease: Arc<Lease>,
    id: String,
    deadlines: Deadlines,
    prompt: String,
    format: Format,
    check: bool,
    tool_host: Arc<dyn ToolHost>,
    abort: oneshot::Receiver<String>,
    completion: Completion,
    // the turn in flight: its stream while `follow` runs, and the run the
    // stream named until that run ends
    stream: Option<RunStream>,
    run_id: Option<String>,
    // handed to the teardown once, by `complete` or by `Drop`
    created: Option<Created>,
    _attached: Attached,
}

impl Agent {
    /// Create the agent for `turn` on `lease`, with its callbacks routed
    /// into `tool_host`.
    pub async fn create(
        lease: Arc<Lease>, turn: Turn, tool_host: Arc<dyn ToolHost>, deadlines: Deadlines,
    ) -> Result<Self> {
        let Turn {
            options,
            operation,
            workspace,
            prompt,
            format,
            check,
        } = turn;
        let mut completion =
            Completion::start(&options.model.id, &format, &prompt, options.mcp_servers.len());

        let created = Created::create(&lease, options, operation, workspace, deadlines.inactivity)
            .await
            .inspect_err(|error| completion.finish(Outcome::of(error)))?;

        let (abort_tx, abort) = oneshot::channel();
        let attached = lease.attach(created.id.clone(), Arc::clone(&tool_host), abort_tx);

        Ok(Self {
            lease,
            id: created.id.clone(),
            deadlines,
            prompt,
            format,
            check,
            tool_host,
            abort,
            completion,
            stream: None,
            run_id: None,
            created: Some(created),
            _attached: attached,
        })
    }

    /// Drive the prompt to an answer, then tear the agent down. The answer
    /// waits on the teardown, so the lease — and the worker — go only after.
    pub async fn complete(mut self) -> Result<Answer, Unanswered> {
        let prompt = mem::take(&mut self.prompt);
        let result = self.rounds(prompt).await;
        let outcome = match &result {
            Ok(_) if self.completion.attempts() > 1 => Outcome::Corrected,
            Ok(_) => Outcome::Ok,
            Err(unanswered) => Outcome::of(unanswered.error()),
        };
        self.completion.finish(outcome);
        // a completion dropped here leaves the teardown running: `Drop`
        // finds nothing left to hand over
        if let Some(teardown) = self.detach_teardown() {
            let _ = teardown.await;
        }
        result
    }

    // Send the prompt, then each correction the guest's check returns on
    // the same session, until a candidate passes or the rounds run out.
    async fn rounds(&mut self, mut prompt: String) -> Result<Answer, Unanswered> {
        let mut round = 1;
        loop {
            self.completion.new_attempt();
            // a candidate is only offered once a `Send` succeeds, so the
            // opening round's failure is one the guest has seen nothing of
            let response = match self.send(&prompt).await {
                Ok(response) => response,
                Err(error) if round == 1 => return Err(Unanswered::before_candidate(error)),
                Err(error) => return Err(Unanswered::settled(error)),
            };
            let tools = response.transcript.as_ref().map_or(0, |t| t.turns.len());
            self.completion.record(response.result.len(), tools, response.usage.as_ref());

            let candidate = self.format.candidate(&response.result);
            if !self.check {
                return Ok(response.answer(candidate));
            }

            match self.tool_host.check(candidate.clone()).await.map_err(Unanswered::settled)? {
                Ok(()) => return Ok(response.answer(candidate)),
                // the agent keeps its session, so the correction alone is
                // the next prompt
                Err(correction) if round < MAX_ROUNDS => {
                    tracing::debug!(%correction, "check rejected the candidate");
                    prompt = correction;
                    round += 1;
                }
                // out of rounds: the last correction is the typed failure
                // the guest sees
                Err(correction) => {
                    return Err(Unanswered::settled(Error::BudgetExhausted(correction).into()));
                }
            }
        }
    }

    async fn send(&mut self, text: &str) -> Result<Response> {
        // both bounds run from the `Send` call itself, so a worker that takes
        // the request and never opens the stream is an inactivity failure
        let activity = watch::Sender::new(Instant::now());
        let deadline = self.deadlines.watch(&activity);
        tokio::pin!(deadline);

        let worker = self.lease.worker();
        let send = worker.rpc().send(self.id.clone(), text.to_owned());
        let opened = tokio::select! {
            stream = worker.fail_on_exit(send) => stream,
            failure = &mut deadline => Err(failure.into()),
        };
        let outcome = match opened {
            Ok(stream) => {
                // `follow` holds the stream on `self` while it runs, so a
                // drop mid-way — the guest abandoning the completion at that
                // await — hands it to the teardown; a return is the turn over
                let outcome = self.follow(stream, &activity, deadline.as_mut()).await;
                self.stream = None;
                outcome
            }
            Err(error) => Err(error),
        };

        // The run went with its process: how long its stream had been silent
        // tells a hang the kill ended from a crash mid-stream.
        if let Err(error) = &outcome
            && let Some(Failure::WorkerExited(exit)) = error.downcast_ref::<Failure>()
        {
            tracing::debug!(
                pid = exit.pid,
                silent_ms = elapsed_ms(*activity.borrow()),
                "run lost with its process"
            );
        }
        outcome
    }

    // Follow the run to its terminal result, noting the run id as the
    // stream names it so a teardown at any point can cancel the run.
    async fn follow(
        &mut self, stream: RunStream, activity: &watch::Sender<Instant>,
        mut deadline: Pin<&mut impl Future<Output = Failure>>,
    ) -> Result<Response> {
        let Self {
            lease,
            stream: open,
            run_id,
            abort,
            ..
        } = self;
        let stream = open.insert(stream);
        let mut log = EventLog::default();
        let mut outcome: Option<RunStreamResult> = None;

        loop {
            tokio::select! {
                message = lease.worker().fail_on_exit(stream.next()) => {
                    let Some(message) = message? else { break };
                    activity.send_replace(Instant::now());
                    log.observe_message(&message);
                    if run_id.is_none() {
                        *run_id = log.run_id().map(ToOwned::to_owned);
                    }
                    if message.result.is_some() {
                        outcome = message.result;
                    }
                    if message.done.is_some() {
                        break;
                    }
                }
                failure = &mut deadline => return Err(failure.into()),
                // the sender lives in the session `_attached` keeps
                // registered, so it never drops unsent under this
                Ok(reason) = &mut *abort => return Err(Failure::Aborted(reason).into()),
            }
        }

        // the run reached a terminal state; nothing is left to cancel
        *run_id = None;
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
            usage: result.usage.map(Usage::from),
        })
    }

    // Hand the agent to its teardown on a task of its own, with the run to
    // cancel and the stream an abandoned turn left open; once, then `None`.
    fn detach_teardown(&mut self) -> Option<JoinHandle<()>> {
        let created = self.created.take()?;
        detach(created.teardown(self.run_id.take(), self.stream.take()))
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        self.detach_teardown();
    }
}

/// The agent `CreateAgent` made, with what tearing it down needs: the lease,
/// so the worker outlives the teardown, and the workspace, so a private one
/// is still there for the local store to delete from.
struct Created {
    lease: Arc<Lease>,
    id: String,
    operation: AgentOperationOptions,
    workspace: Workspace,
}

impl Created {
    /// `CreateAgent` on a task of its own, which holds the lease and owns
    /// the agent it makes until the caller claims it. A caller that stops
    /// waiting — at `window`, or dropped — leaves an agent the task tears
    /// down itself; a worker silent for one more window gives the slot back
    /// with nothing to tear down.
    async fn create(
        lease: &Arc<Lease>, options: AgentOptions, operation: AgentOperationOptions,
        workspace: Workspace, window: Duration,
    ) -> Result<Self> {
        let (tx, rx) = oneshot::channel();
        tokio::spawn({
            let lease = Arc::clone(lease);
            async move {
                let rpc = lease.worker().rpc().clone();
                let limit = window.saturating_mul(2);
                let Ok(answered) = timeout(limit, rpc.create_agent(options)).await else {
                    tracing::warn!(
                        secs = limit.as_secs(),
                        "CreateAgent still unanswered; its slot reopens with no agent torn down"
                    );
                    return;
                };
                let created = match answered {
                    Ok(created) if created.agent_id.is_empty() => {
                        Err(anyhow!("sdk.v1 RPC `CreateAgent` returned an empty agent id"))
                    }
                    Ok(created) => Ok(Self {
                        lease,
                        id: created.agent_id,
                        operation,
                        workspace,
                    }),
                    Err(error) => Err(error),
                };
                match tx.send(created) {
                    Ok(()) => {}
                    // nobody waiting: the caller timed out or was dropped,
                    // so the agent is this task's to tear down
                    Err(Ok(unclaimed)) => unclaimed.teardown(None, None).await,
                    Err(Err(error)) => tracing::debug!(%error, "abandoned CreateAgent failed"),
                }
            }
        });

        let claimed = async {
            rx.await.unwrap_or_else(|_closed| {
                Err(anyhow!("sdk.v1 RPC `CreateAgent` ended without an outcome"))
            })
        };
        timeout(window, lease.worker().fail_on_exit(claimed)).await.unwrap_or_else(|_elapsed| {
            Err(anyhow!("sdk.v1 RPC `CreateAgent` unanswered after {}s", window.as_secs()))
        })
    }

    /// Cancel the run, close, then delete — each call bounded, all of them
    /// skipped once the worker is gone — then let the workspace go.
    async fn teardown(self, run_id: Option<String>, stream: Option<RunStream>) {
        let run_id = match (run_id, stream) {
            // an abandoned turn's stream may still carry the run id — the
            // init frame flushed and not yet polled — so it is read before
            // anything is dropped
            (None, Some(stream)) => observe_run_id(stream).await,
            (run_id, stream) => {
                drop(stream);
                run_id
            }
        };
        if let Some(rpc) = self.lease.worker().live_rpc() {
            if let Some(run_id) = run_id {
                call("CancelRun", rpc.cancel_run(run_id, self.id.clone())).await;
            }
            call("CloseAgent", rpc.close_agent(self.id.clone())).await;
            call("DeleteAgent", rpc.delete_agent(self.id, self.operation)).await;
        } else {
            tracing::debug!(agent = %self.id, "worker exited; skipping agent teardown");
        }
        drop(self.workspace);
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
    /// Resolve when a run breaches its inactivity or absolute bound; every
    /// value sent on `activity` rearms the inactivity bound.
    async fn watch(self, activity: &watch::Sender<Instant>) -> Failure {
        let mut activity = activity.subscribe();
        let cap = sleep_until(Instant::now() + self.cap);
        tokio::pin!(cap);

        loop {
            let last_activity = *activity.borrow_and_update();
            tokio::select! {
                () = &mut cap => {
                    return Failure::Timeout {
                        cap_secs: self.cap.as_secs(),
                    };
                }
                () = sleep_until(last_activity + self.inactivity) => {
                    let idle = Instant::now().saturating_duration_since(last_activity).as_secs();
                    return Failure::Inactive {
                        idle_secs: idle,
                        inactivity_secs: self.inactivity.as_secs(),
                        cap_secs: self.cap.as_secs(),
                    };
                }
                // the sender is borrowed for as long as this is polled, so
                // the channel never closes under it
                _ = activity.changed() => {}
            }
        }
    }
}

// One completed turn: the final text plus the observed transcript and usage.
#[derive(Debug)]
struct Response {
    result: String,
    transcript: Option<Transcript>,
    usage: Option<Usage>,
}

impl Response {
    fn answer(self, candidate: String) -> Answer {
        Answer {
            answer: candidate,
            usage: self.usage,
            transcript: self.transcript,
        }
    }
}

/// A completion that produced no answer: the failure, and whether it struck
/// before any candidate had been offered to the guest — in `CreateAgent` or
/// the opening `Send`.
pub struct Unanswered {
    error: anyhow::Error,
    before_candidate: bool,
}

impl Unanswered {
    /// A failure in `CreateAgent` or the opening `Send`: the guest has seen
    /// nothing of this agent.
    pub const fn before_candidate(error: anyhow::Error) -> Self {
        Self {
            error,
            before_candidate: true,
        }
    }

    /// A failure to report as it stands.
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

    /// Whether a fresh agent may be given the prompt once more: the worker
    /// or its socket was lost before any candidate reached the guest, so
    /// nothing has been said about the prompt and no candidate would be
    /// offered twice.
    pub fn restartable(&self) -> bool {
        self.before_candidate && Outcome::of(&self.error).lost_worker()
    }

    /// The process that exited under the completion, when that is the failure.
    pub fn pid(&self) -> Option<u32> {
        match self.error.downcast_ref::<Failure>() {
            Some(Failure::WorkerExited(exit)) => Some(exit.pid),
            _ => None,
        }
    }
}

// Spawn `task` so the caller's fate does not cut it short. Without a
// runtime to spawn on, this is `None`.
fn detach(task: impl Future<Output = ()> + Send + 'static) -> Option<JoinHandle<()>> {
    Handle::try_current().ok().map(|handle| handle.spawn(task))
}

// One best-effort teardown call: a failure is logged and a silence is
// bounded.
async fn call(method: &'static str, rpc: impl Future<Output = Result<()>>) {
    match timeout(TEARDOWN_TIMEOUT, rpc).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, method, "agent teardown call failed"),
        Err(_elapsed) => tracing::warn!(method, "agent teardown call unanswered"),
    }
}

/// The first run id on `stream`, or `None` when the stream ends without one
/// or stays silent for [`RUN_ID_WAIT`].
async fn observe_run_id(mut stream: RunStream) -> Option<String> {
    let read = async {
        let mut log = EventLog::default();
        while let Ok(Some(message)) = stream.next().await {
            log.observe_message(&message);
            if log.run_id().is_some() || message.done.is_some() {
                break;
            }
        }
        log.run_id().map(ToOwned::to_owned)
    };
    timeout(RUN_ID_WAIT, read).await.ok().flatten()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::watch;
    use tokio::time::{self, Instant};

    use super::{Deadlines, Unanswered};
    use crate::Failure;
    use crate::protocol::RpcError;
    use crate::worker::Exit;

    const DEADLINES: Deadlines = Deadlines {
        inactivity: Duration::from_secs(1),
        cap: Duration::from_secs(5),
    };
    const FRAME_GAP: Duration = Duration::from_millis(800);
    const EXITED: Exit = Exit { status: None, pid: 7 };

    // The clock is paused, so every instant below is exact.

    #[tokio::test(start_paused = true)]
    async fn silent() {
        let started = Instant::now();
        let activity = watch::Sender::new(started);
        let failure = DEADLINES.watch(&activity).await;
        assert!(
            matches!(
                failure,
                Failure::Inactive {
                    idle_secs: 1,
                    inactivity_secs: 1,
                    cap_secs: 5,
                }
            ),
            "{failure}"
        );
        assert_eq!(started.elapsed(), DEADLINES.inactivity);
    }

    #[tokio::test(start_paused = true)]
    async fn rearmed() {
        let started = Instant::now();
        let activity = watch::Sender::new(started);
        let deadline = DEADLINES.watch(&activity);
        tokio::pin!(deadline);
        // three frames, together well past the window, each inside it
        for _ in 0..3 {
            let still_watching = time::timeout(FRAME_GAP, &mut deadline).await.is_err();
            assert!(still_watching, "the window fired {:?} in", started.elapsed());
            activity.send_replace(Instant::now());
        }
        let failure = deadline.await;
        assert!(matches!(failure, Failure::Inactive { idle_secs: 1, .. }), "{failure}");
        assert_eq!(started.elapsed(), 3 * FRAME_GAP + DEADLINES.inactivity);
    }

    #[tokio::test(start_paused = true)]
    async fn capped() {
        let started = Instant::now();
        let activity = watch::Sender::new(started);
        let deadline = DEADLINES.watch(&activity);
        tokio::pin!(deadline);
        let failure = loop {
            match time::timeout(FRAME_GAP, &mut deadline).await {
                Ok(failure) => break failure,
                Err(_elapsed) => {
                    activity.send_replace(Instant::now());
                }
            }
        };
        assert!(matches!(failure, Failure::Timeout { cap_secs: 5 }), "{failure}");
        assert_eq!(started.elapsed(), DEADLINES.cap);
    }

    #[test]
    fn restartable() {
        let exited = || Failure::WorkerExited(EXITED).into();
        let reset = || RpcError::truncated("SdkAgentService/Send", 3).into();
        let stalled = || {
            Failure::Inactive {
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
