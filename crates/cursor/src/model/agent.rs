//! A bridge-managed agent: `send` drives a turn's run stream to its
//! terminal result, bounded by an inactivity deadline that stream progress
//! rearms, an absolute wall-clock cap, the callback's abort signal, and the
//! bridge's own exit. No call waits on the bridge unbounded, so one that is
//! alive but silent unblocks the guest.
//!
//! The completion future is the guest's to drop at any `.await`, so no RPC
//! that hands an agent over runs on it. `CreateAgent` runs on a task of its
//! own that holds the lease and owns the id it returns: a completion that
//! stops waiting — at the inactivity bound, or dropped — leaves an agent
//! the task closes and deletes itself, and a bridge silent for one more
//! window gives the slot back with nothing to tear down. Teardown — the
//! abandoned run cancelled best-effort, then close and delete against the
//! create-time cwd, each call bounded and all of them skipped once the
//! bridge is gone — runs on a task of its own too, whether the turn ended
//! or the agent was dropped mid-way; `complete` waits for it, and the lease
//! rides on it, so the bridge closes only after.
//!
//! A failed agent reports itself as [`Unanswered`]: the error, and whether
//! it struck before any candidate had been offered to the guest, which is
//! what lets the caller run the prompt again on a fresh agent.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use omnia_wasi_model::{Answer, Error, Format, ToolHost, Transcript, Usage};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};

use super::observe::{self, Completion, EventLog};
use super::options::{Turn, Workspace};
use crate::bridge::{
    AgentOperationOptions, AgentOptions, Bridge, Exit, RunStatus, RunStream, RunStreamResult,
};
use crate::endpoint::Attached;
use crate::pool::Lease;
use crate::{Client, Failure};

// Candidates offered to the guest's check before the round budget ends the
// completion: the opening prompt plus one correction on the same agent.
const MAX_ROUNDS: u32 = 2;
// Teardown is best-effort: a bridge that will not answer it does not keep
// its slot for longer than this per call.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);
// A completion dropped mid-stream may not have polled since the init frame
// was flushed, and `CancelRun` needs the id that frame carries. A bridge
// that never sends one does not hold the slot past this.
const RUN_ID_WAIT: Duration = Duration::from_secs(2);

pub struct Agent {
    lease: Arc<Lease>,
    id: String,
    deadlines: Deadlines,
    prompt: String,
    format: Format,
    check: bool,
    tool_host: Arc<dyn ToolHost>,
    live_run: Option<String>,
    abort_rx: mpsc::UnboundedReceiver<String>,
    completion: Completion,
    _attached: Attached,
    // Owned until the teardown takes it: `None` once the agent is released,
    // by `complete` or by `Drop`, whichever comes first.
    release: Option<Release>,
}

impl Agent {
    pub async fn create(
        client: &Client, lease: Arc<Lease>, turn: Turn, tool_host: Arc<dyn ToolHost>,
    ) -> Result<Self> {
        let Turn {
            options,
            workspace,
            prompt,
            format,
            check,
        } = turn;
        let mut completion =
            Completion::start(&options.model.id, &format, &prompt, options.mcp_servers.len());

        let window = client.deadlines.inactivity;
        let creating = Creating::spawn(Arc::clone(&lease), options, workspace, window);
        let release = match creating.claim(lease.bridge(), window).await {
            Ok(release) => release,
            Err(error) => {
                completion.finish(observe::outcome_of(&error));
                return Err(error);
            }
        };

        let id = release.id.clone();
        let (abort_tx, abort_rx) = mpsc::unbounded_channel();
        let attached = lease.attach(id.clone(), Arc::clone(&tool_host), abort_tx);

        Ok(Self {
            lease,
            id,
            deadlines: client.deadlines,
            prompt,
            format,
            check,
            tool_host,
            live_run: None,
            abort_rx,
            completion,
            _attached: attached,
            release: Some(release),
        })
    }

    pub async fn complete(mut self) -> Result<Answer, Unanswered> {
        let result = self.run().await;
        let outcome = match &result {
            Ok(_) if self.completion.attempts() > 1 => "corrected",
            Ok(_) => "ok",
            Err(unanswered) => observe::outcome_of(unanswered.error()),
        };
        self.completion.finish(outcome);
        // The answer waits on the teardown; a completion dropped here leaves
        // it running, `Drop` having nothing left to release.
        if let Some(teardown) = self.take_release().and_then(|release| detach(release.run())) {
            let _ = teardown.await;
        }
        result
    }

    async fn run(&mut self) -> Result<Answer, Unanswered> {
        let mut prompt = std::mem::take(&mut self.prompt);
        let mut round = 1;
        loop {
            self.completion.new_attempt();
            // A candidate is only offered after a `Send` succeeds, so the
            // opening round's failure is one the guest has seen nothing of.
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
                // The agent keeps its session, so the correction alone is
                // the next prompt.
                Err(correction) if round < MAX_ROUNDS => {
                    tracing::debug!(%correction, "check rejected the candidate");
                    prompt = correction;
                    round += 1;
                }
                // Out of rounds: the last correction is the typed failure
                // the guest sees.
                Err(correction) => {
                    return Err(Unanswered::settled(Error::BudgetExhausted(correction).into()));
                }
            }
        }
    }

    async fn send(&mut self, text: &str) -> Result<Response> {
        // Both bounds run from the `Send` call itself, so a bridge that takes
        // the request and never opens the stream is an inactivity failure.
        let (activity_tx, activity_rx) = watch::channel(Instant::now());
        // Should the process die under this run, its exit report says how
        // long the stream had been silent.
        self.lease.bridge().watch(activity_rx.clone());
        let deadline = self.deadlines.watch(activity_rx);
        tokio::pin!(deadline);
        // Owns its watch, so it does not borrow `self` across the loop.
        let exited = self.lease.bridge().exited();
        tokio::pin!(exited);

        let stream = tokio::select! {
            stream = self.lease.rpc().send(self.id.clone(), text.to_owned()) => {
                match stream {
                    Ok(stream) => stream,
                    Err(error) => return Err(self.lease.bridge().exit_or(error).await),
                }
            }
            error = &mut deadline => return Err(error.into()),
            exit = &mut exited => return Err(Failure::BridgeExited(exit).into()),
        };

        // `drive` disarms the guard when the send finishes. Dropping it
        // still armed — the completion future was dropped at this await —
        // is the guest abandoning a run whose id it may not have polled yet.
        let mut open = OpenSend {
            agent: self,
            stream: Some(stream),
            armed: true,
        };
        let outcome = open.drive(&activity_tx, &mut deadline, &mut exited).await;
        open.armed = false;
        outcome
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
        let Some(rpc) = self.lease.live_rpc().cloned() else {
            return;
        };
        let agent_id = self.id.clone();
        detach(async move {
            teardown("CancelRun", rpc.cancel_run(run_id, agent_id)).await;
        });
    }

    // The teardown, the first time; `None` once it has been handed out.
    fn take_release(&mut self) -> Option<Release> {
        let mut release = self.release.take()?;
        release.run_id = self.live_run.take();
        Some(release)
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        if let Some(release) = self.take_release() {
            detach(release.run());
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

    /// Whether a fresh agent may be given the prompt once more: the bridge
    /// or its socket was lost before any candidate reached the guest, so
    /// nothing has been said about the prompt and no candidate would be
    /// offered twice.
    pub fn restartable(&self) -> bool {
        self.before_candidate && observe::lost_bridge(&self.error)
    }

    /// The process that exited under the completion, when that is the failure.
    pub fn pid(&self) -> Option<u32> {
        match self.error.downcast_ref::<Failure>() {
            Some(Failure::BridgeExited(exit)) => Some(exit.pid),
            _ => None,
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
    async fn watch(self, mut activity: watch::Receiver<Instant>) -> Failure {
        let cap = sleep_until(Instant::now() + self.cap);
        tokio::pin!(cap);
        let mut activity_closed = false;

        loop {
            let last_activity = *activity.borrow_and_update();
            let inactive = sleep_until(last_activity + self.inactivity);
            tokio::pin!(inactive);

            tokio::select! {
                () = &mut cap => {
                    return Failure::Timeout {
                        cap_secs: self.cap.as_secs(),
                    };
                }
                () = &mut inactive => {
                    let idle = Instant::now().saturating_duration_since(last_activity).as_secs();
                    return Failure::Inactive {
                        idle_secs: idle,
                        inactivity_secs: self.inactivity.as_secs(),
                        cap_secs: self.cap.as_secs(),
                    };
                }
                changed = activity.changed(), if !activity_closed => {
                    activity_closed = changed.is_err();
                }
            }
        }
    }
}

/// `CreateAgent` on a task of its own, which holds the lease and owns the
/// agent it returns: a completion that stops waiting — at the inactivity
/// bound, or dropped — leaves an agent the task closes and deletes itself.
struct Creating(oneshot::Receiver<Result<Release>>);

impl Creating {
    // The lease is the slot, so the task's wait is bounded too: the window
    // the completion waits, then one more for a late id — or a bridge that
    // stays alive and silent would hold the slot for good.
    fn spawn(
        lease: Arc<Lease>, options: AgentOptions, workspace: Workspace, window: Duration,
    ) -> Self {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            // what teardown needs of the options, before the RPC consumes them
            let cwd = options.local.cwd.first().cloned().unwrap_or_default();
            let api_key = options.api_key.clone();
            let rpc = lease.rpc().clone();
            let limit = window.saturating_mul(2);
            let outcome = match timeout(limit, rpc.create_agent(options)).await {
                Ok(Ok(created)) if created.agent_id.is_empty() => {
                    Err(anyhow!("bridge RPC `CreateAgent` returned an empty agent id"))
                }
                Ok(Ok(created)) => Ok(Release {
                    lease,
                    id: created.agent_id,
                    cwd,
                    api_key,
                    run_id: None,
                    workspace,
                }),
                Ok(Err(error)) => Err(error),
                Err(_elapsed) => {
                    tracing::warn!(
                        secs = limit.as_secs(),
                        "CreateAgent still unanswered; its slot reopens with no agent torn down"
                    );
                    return;
                }
            };
            match tx.send(outcome) {
                Ok(()) => {}
                // Nobody waiting: the completion timed out or was dropped, so
                // the agent is this task's to tear down.
                Err(Ok(unclaimed)) => unclaimed.run().await,
                Err(Err(error)) => tracing::debug!(%error, "abandoned CreateAgent failed"),
            }
        });
        Self(rx)
    }

    /// The agent, within `window`; past it the agent is the task's.
    async fn claim(self, bridge: &Bridge, window: Duration) -> Result<Release> {
        let exited = bridge.exited();
        tokio::pin!(exited);
        tokio::select! {
            outcome = self.0 => match outcome {
                Ok(Ok(release)) => Ok(release),
                Ok(Err(error)) => Err(bridge.exit_or(error).await),
                Err(_closed) => Err(anyhow!("bridge RPC `CreateAgent` ended without an outcome")),
            },
            exit = &mut exited => Err(Failure::BridgeExited(exit).into()),
            () = sleep(window) => Err(anyhow!(
                "bridge RPC `CreateAgent` unanswered after {}s",
                window.as_secs()
            )),
        }
    }
}

/// The open `Send` stream. `drive` disarms it when the turn finishes;
/// dropping it still armed means the guest abandoned the completion at
/// this await, so the run id — possibly flushed and not yet polled — is
/// read before the agent is torn down.
struct OpenSend<'a> {
    agent: &'a mut Agent,
    stream: Option<RunStream>,
    armed: bool,
}

impl OpenSend<'_> {
    async fn drive<D, X>(
        &mut self, activity_tx: &watch::Sender<Instant>, deadline: &mut D, exited: &mut X,
    ) -> Result<Response>
    where
        D: Future<Output = Failure> + Unpin,
        X: Future<Output = Exit> + Unpin,
    {
        let OpenSend { agent, stream, .. } = self;
        // `None` only once `Drop` has handed the stream to the teardown
        let stream = stream.as_mut().context("the send stream is no longer this turn's")?;
        let mut log = EventLog::default();
        let mut outcome: Option<RunStreamResult> = None;

        loop {
            tokio::select! {
                message = stream.next() => {
                    let message = match message {
                        Ok(Some(message)) => message,
                        Ok(None) => break,
                        Err(error) => return Err(agent.lease.bridge().exit_or(error).await),
                    };
                    activity_tx.send_replace(Instant::now());
                    log.observe_message(&message);
                    agent.note_run(log.run_id());
                    if message.result.is_some() {
                        outcome = message.result;
                    }
                    if message.done.is_some() {
                        break;
                    }
                }
                error = &mut *deadline => {
                    agent.cancel_live_run();
                    return Err(error.into());
                }
                reason = agent.abort_rx.recv() => {
                    agent.cancel_live_run();
                    return Err(Failure::Aborted(
                        reason.unwrap_or_else(|| "session closed".to_owned()),
                    )
                    .into());
                }
                exit = &mut *exited => {
                    // the run exited with its process; nothing is left to cancel
                    agent.live_run = None;
                    return Err(Failure::BridgeExited(exit).into());
                }
            }
        }

        // the run reached a terminal state; nothing is left to cancel.
        agent.live_run = None;
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
}

impl Drop for OpenSend<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let stream = self.stream.take();
        let Some(mut release) = self.agent.take_release() else {
            return;
        };
        detach(async move {
            // Already noted: the stream has nothing more to add. Not yet:
            // the init frame may be buffered or still in flight, and
            // dropping the stream first would lose the id `CancelRun` needs.
            let stream = if release.run_id.is_none() {
                stream
            } else {
                drop(stream);
                None
            };
            if let Some(stream) = stream {
                release.run_id = observe_run_id(stream).await;
            }
            release.run().await;
        });
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

/// A created agent and its teardown: close then delete, holding the
/// create-time cwd until both RPCs finish so a private workspace is still
/// visible to the local store. Holds the lease so the bridge outlives the
/// teardown.
struct Release {
    lease: Arc<Lease>,
    id: String,
    cwd: String,
    api_key: String,
    run_id: Option<String>,
    workspace: Workspace,
}

impl Release {
    async fn run(self) {
        if let Some(rpc) = self.lease.live_rpc() {
            if let Some(run_id) = self.run_id {
                teardown("CancelRun", rpc.cancel_run(run_id, self.id.clone())).await;
            }
            teardown("CloseAgent", rpc.close_agent(self.id.clone())).await;
            let options = AgentOperationOptions {
                cwd: self.cwd,
                api_key: self.api_key,
            };
            teardown("DeleteAgent", rpc.delete_agent(self.id, options)).await;
        } else {
            tracing::debug!(agent = %self.id, "bridge exited; skipping agent teardown");
        }
        drop(self.workspace);
    }
}

// On a task of its own, so the caller's fate does not cut it short; `None`
// without a runtime.
fn detach(task: impl Future<Output = ()> + Send + 'static) -> Option<JoinHandle<()>> {
    Handle::try_current().ok().map(|handle| handle.spawn(task))
}

// One best-effort teardown call: a failure is logged, a silence is bounded.
async fn teardown(method: &'static str, rpc: impl Future<Output = Result<()>>) {
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
