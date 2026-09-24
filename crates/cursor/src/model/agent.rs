//! An agent on a leased worker: one completion's `CreateAgent`, the `Send`
//! of its prompt and of each correction the guest's check returns, and the
//! `DeleteAgent` that gives the slot back.
//!
//! The whole of it runs on a task of its own, because the completion future
//! is the guest's to drop at any `.await`: the drop cancels a token the task
//! watches, and the task ends its run — cancelled by id once the stream has
//! named one — and still deletes its agent. No wait on the worker is
//! unbounded — the inactivity window that stream progress rearms, the
//! absolute cap, the callback's abort and the worker's own exit each end
//! one — so a worker alive but silent unblocks the guest.

use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use omnia_wasi_model::{Answer, Error, ToolHost, Transcript, Usage};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Span};

use super::observe::{Completion, EventLog};
use super::options::{AgentSpec, Prompt, Turn, Workspace};
use crate::endpoint::Attached;
use crate::failure::Outcome;
use crate::pool::Lease;
use crate::protocol::{AgentOperationOptions, RunStatus, RunStream, RunStreamResult};
use crate::worker::Worker;
use crate::{Failure, elapsed_ms};

const MAX_ROUNDS: u32 = 2;
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// One attempt at a completion: the turn to run as an agent on a leased
/// worker, with the agent's callbacks routed into the tool host.
pub struct Attempt {
    pub lease: Arc<Lease>,
    pub turn: Turn,
    pub tool_host: Arc<dyn ToolHost>,
    pub deadlines: Deadlines,
}

impl Attempt {
    /// `CreateAgent`, the prompt to an answer, then `DeleteAgent`, on a task
    /// of its own: dropping this future ends the run, never the delete.
    ///
    /// # Errors
    ///
    /// Returns the failure, marked as before any candidate when it struck
    /// in `CreateAgent` or the opening `Send`.
    pub async fn complete(self) -> Result<Answer, Unanswered> {
        // the guard's cancel is how the task learns nobody is waiting any more
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();

        let task = tokio::spawn(self.run(cancel).instrument(Span::current()));
        task.await.unwrap_or_else(|panicked| {
            let error =
                anyhow::Error::new(panicked).context("the agent's task ended with no answer");
            Err(Unanswered::settled(error))
        })
    }

    // The whole life of one agent, in order. The answer waits on the
    // delete, so the lease — and the worker — go only after.
    async fn run(self, cancel: CancellationToken) -> Result<Answer, Unanswered> {
        let mut agent = Agent::create(self, cancel).await.map_err(Unanswered::before_candidate)?;
        let result = agent.rounds().await;
        agent.finish(&result);
        agent.delete().await;
        result
    }
}

/// One completion's agent on a leased worker, from `CreateAgent` to
/// `DeleteAgent`.
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
    // `CreateAgent`, bounded by one inactivity window and the worker's exit.
    // A failure here is one the guest has seen nothing of.
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

    // Send the prompt, then each correction the guest's check returns on
    // the same session, until a candidate passes or the rounds run out.
    async fn rounds(&mut self) -> Result<Answer, Unanswered> {
        let mut prompt = mem::take(&mut self.prompt.text);
        let mut round = 1;
        loop {
            self.completion.attempt();

            // a candidate is only offered once a `Send` succeeds, so the
            // opening round's failure is one the guest has seen nothing of
            let response = match self.send(&prompt).await {
                Ok(response) => response,
                Err(error) if round == 1 => return Err(Unanswered::before_candidate(error)),
                Err(error) => return Err(Unanswered::settled(error)),
            };
            let tools = response.transcript.as_ref().map_or(0, |t| t.turns.len());
            self.completion.record(response.result.len(), tools, response.usage.as_ref());

            let candidate = self.prompt.format.candidate(&response.result);
            if !self.prompt.check {
                return Ok(response.answer(candidate));
            }

            match self.tool_host.check(candidate.clone()).await.map_err(Unanswered::settled)? {
                Ok(()) => return Ok(response.answer(candidate)),
                Err(correction) if round < MAX_ROUNDS => {
                    tracing::debug!(%correction, "check rejected the candidate");
                    // agents keep their session, so the correction is the prompt
                    prompt = correction;
                }
                Err(correction) => {
                    return Err(Unanswered::settled(Error::BudgetExhausted(correction).into()));
                }
            }

            round += 1;
        }
    }

    async fn send(&mut self, text: &str) -> Result<Response> {
        // both bounds run from the `Send` call itself, so a worker that takes
        // the request and never opens the stream is an inactivity failure
        let activity = watch::Sender::new(Instant::now());
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
    // stream names it so a run ended early can be cancelled.
    async fn follow(
        &mut self, mut stream: RunStream, activity: &watch::Sender<Instant>,
        mut deadline: Pin<&mut impl Future<Output = Failure>>,
    ) -> Result<Response> {
        let mut log = EventLog::default();
        let mut outcome: Option<RunStreamResult> = None;

        loop {
            tokio::select! {
                message = self.handle.worker().fail_on_exit(stream.next()) => {
                    let Some(message) = message? else { break };
                    activity.send_replace(Instant::now());
                    log.observe_message(&message);
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
                failure = &mut deadline => return Err(failure.into()),
                reason = self.session.aborted() => return Err(Failure::Aborted(reason).into()),
                // an abandoned run is cancelled by the id its opening frame
                // names, so the stream is followed as far as that frame
                () = self.cancel.cancelled(), if self.handle.run_id.is_some() => {
                    return Err(abandoned());
                }
            }
        }

        // the run reached a terminal state; nothing is left to cancel
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
            usage: result.usage.map(Usage::from),
        })
    }

    // The one `completion` line, emitted before the delete so its
    // `duration_ms` ends with the run.
    fn finish(&mut self, result: &Result<Answer, Unanswered>) {
        let outcome = match result {
            Ok(_) if self.completion.attempts() > 1 => Outcome::Corrected,
            Ok(_) => Outcome::Ok,
            Err(unanswered) => Outcome::of(unanswered.error()),
        };
        self.completion.finish(outcome);
    }

    // Delete the agent on the worker; its callback route goes after, with
    // the rest of `self`.
    async fn delete(self) {
        self.handle.delete().await;
    }
}

/// One agent on the worker it was created on, with what deleting it needs:
/// the lease that keeps that worker, the pin and workspace the delete names,
/// and the run still open on it.
struct Handle {
    lease: Arc<Lease>,
    id: String,
    operation: AgentOperationOptions,
    workspace: Workspace,
    run_id: Option<String>,
}

impl Handle {
    fn worker(&self) -> &Worker {
        self.lease.worker()
    }

    async fn send(&self, text: &str) -> Result<RunStream> {
        self.worker().rpc().send(self.id.clone(), text.to_owned()).await
    }

    // Cancel the run still open, close, then delete — each call bounded,
    // all of them skipped once the worker is gone — then let the workspace
    // go, and the lease last.
    async fn delete(self) {
        let Self {
            lease,
            id,
            operation,
            workspace,
            run_id,
        } = self;

        if let Some(rpc) = lease.worker().live_rpc() {
            if let Some(run_id) = run_id {
                call("CancelRun", rpc.cancel_run(run_id, id.clone())).await;
            }
            call("CloseAgent", rpc.close_agent(id.clone())).await;
            call("DeleteAgent", rpc.delete_agent(id, operation)).await;
        } else {
            tracing::debug!(agent = %id, "worker exited; skipping agent teardown");
        }
        drop(workspace);
        drop(lease);
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
    // A failure in `CreateAgent` or the opening `Send`: the guest has seen
    // nothing of this agent.
    const fn before_candidate(error: anyhow::Error) -> Self {
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

// The failure a run ends with once the guest has dropped the completion: it
// reaches no one, and the `completion` line reads `abort`.
fn abandoned() -> anyhow::Error {
    Failure::Aborted("the guest dropped the completion".to_owned()).into()
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
