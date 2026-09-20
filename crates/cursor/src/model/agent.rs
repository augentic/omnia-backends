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
//! the task closes and deletes itself (an attached bridge would otherwise
//! keep it), and a bridge silent for one more window gives the slot back
//! with nothing to tear down. Teardown — the abandoned run cancelled
//! best-effort, then close and delete against the create-time cwd, each
//! call bounded and all of them skipped once the bridge is gone — runs on a
//! task of its own too, whether the turn ended or the agent was dropped
//! mid-way; `complete` waits for it, and the lease rides on it, so the
//! bridge closes only after.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use omnia_wasi_model::{Answer, Error, Format, ToolHost, Transcript, Usage};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};

use super::observe::{self, Completion, EventLog, Failure};
use super::options::{Turn, Workspace};
use crate::Client;
use crate::bridge::{AgentOptions, Bridge, EXIT_OBSERVE, RunStatus, RunStreamResult};
use crate::endpoint::Attached;
use crate::pool::Lease;

// Candidates offered to the guest's check before the round budget ends the
// completion: the opening prompt plus one correction on the same agent.
const MAX_ROUNDS: usize = 2;
// Teardown is best-effort: a bridge that will not answer it does not keep
// its slot for longer than this per call.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Agent {
    lease: Arc<Lease>,
    id: String,
    cwd: String,
    deadlines: Deadlines,
    prompt: String,
    format: Format,
    check: bool,
    tool_host: Arc<dyn ToolHost>,
    live_run: Option<String>,
    abort_rx: mpsc::UnboundedReceiver<String>,
    completion: Option<Completion>,
    _attached: Attached,
    workspace: Option<Workspace>,
}

impl Agent {
    pub async fn create(
        client: &Client, lease: Arc<Lease>, turn: Turn, tool_host: Arc<dyn ToolHost>,
    ) -> Result<Self> {
        let completion = Completion::start(&turn);
        let Turn {
            options,
            workspace,
            prompt,
            format,
            check,
        } = turn;
        let cwd = options.local.cwd.first().cloned().unwrap_or_default();

        let window = client.deadlines.inactivity;
        let creating = Creating::spawn(Arc::clone(&lease), options, workspace, cwd.clone(), window);
        let Created { id, workspace } = match creating.claim(lease.bridge(), window).await {
            Ok(created) => created,
            Err(error) => {
                completion.finish(observe::outcome_of(&error));
                return Err(error);
            }
        };

        let (abort_tx, abort_rx) = mpsc::unbounded_channel();
        let attached = lease.attach(id.clone(), Arc::clone(&tool_host), abort_tx);

        Ok(Self {
            lease,
            id,
            cwd,
            deadlines: client.deadlines,
            prompt,
            format,
            check,
            tool_host,
            live_run: None,
            abort_rx,
            completion: Some(completion),
            _attached: attached,
            workspace: Some(workspace),
        })
    }

    pub async fn complete(mut self) -> Result<Answer> {
        let result = self.run().await;
        let attempts = self.completion.as_ref().map_or(0, Completion::attempts);
        let outcome = match &result {
            Ok(_) if attempts > 1 => "corrected",
            Ok(_) => "ok",
            Err(error) => observe::outcome_of(error),
        };
        if let Some(completion) = self.completion.take() {
            completion.finish(outcome);
        }
        // The answer waits on the teardown; a completion dropped here leaves
        // it running, `Drop` having nothing left to release.
        if let Some(teardown) = self.take_release().and_then(Release::spawn) {
            let _ = teardown.await;
        }
        result
    }

    async fn run(&mut self) -> Result<Answer> {
        let mut prompt = std::mem::take(&mut self.prompt);
        for round in 1..=MAX_ROUNDS {
            if let Some(completion) = &mut self.completion {
                completion.new_attempt();
            }
            let response = self.send(&prompt).await?;
            if let Some(completion) = &mut self.completion {
                let tools = response.transcript.as_ref().map_or(0, |t| t.turns.len());
                completion.record(response.result.len(), tools, response.usage.as_ref());
            }

            let candidate = self.format.candidate(&response.result);
            if !self.check {
                return Ok(response.answer(candidate));
            }

            match self.tool_host.check(candidate.clone()).await? {
                Ok(()) => return Ok(response.answer(candidate)),
                // The agent keeps its session, so the correction alone is
                // the next prompt; on the last round it is the typed
                // failure the guest sees.
                Err(correction) if round == MAX_ROUNDS => {
                    bail!(Error::BudgetExhausted(correction));
                }
                Err(correction) => {
                    tracing::debug!(%correction, "check rejected the candidate");
                    prompt = correction;
                }
            }
        }
        unreachable!("every round returns or bails")
    }

    async fn send(&mut self, text: &str) -> Result<Response> {
        // Both bounds run from the `Send` call itself, so a bridge that takes
        // the request and never opens the stream is an inactivity failure.
        let (activity_tx, activity_rx) = watch::channel(Instant::now());
        let deadline = self.deadlines.watch(activity_rx);
        tokio::pin!(deadline);
        // Owns its watch, so it does not borrow `self` across the loop.
        let died = self.lease.bridge().died();
        tokio::pin!(died);

        let mut stream = tokio::select! {
            stream = self.lease.bridge().rpc().send(self.id.clone(), text.to_owned()) => {
                match stream {
                    Ok(stream) => stream,
                    Err(error) => return Err(exit_or(self.lease.bridge(), error).await),
                }
            }
            error = &mut deadline => return Err(error.into()),
            exit = &mut died => return Err(Failure::BridgeExited(exit).into()),
        };

        let mut log = EventLog::default();
        let mut outcome: Option<RunStreamResult> = None;

        loop {
            tokio::select! {
                message = stream.next() => {
                    let message = match message {
                        Ok(Some(message)) => message,
                        Ok(None) => break,
                        Err(error) => return Err(exit_or(self.lease.bridge(), error).await),
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
                    return Err(error.into());
                }
                reason = self.abort_rx.recv() => {
                    self.cancel_live_run();
                    return Err(Failure::Aborted(
                        reason.unwrap_or_else(|| "session closed".to_owned()),
                    )
                    .into());
                }
                exit = &mut died => {
                    // the run died with its process; nothing is left to cancel
                    self.live_run = None;
                    return Err(Failure::BridgeExited(exit).into());
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
        let bridge = self.lease.bridge();
        if bridge.is_dead() {
            return;
        }

        let rpc = bridge.rpc().clone();
        let agent_id = self.id.clone();
        if let Ok(handle) = Handle::try_current() {
            handle.spawn(async move {
                teardown("CancelRun", rpc.cancel_run(run_id, agent_id)).await;
            });
        }
    }

    fn take_release(&mut self) -> Option<Release> {
        let id = std::mem::take(&mut self.id);
        if id.is_empty() {
            return None;
        }
        Some(Release {
            lease: Arc::clone(&self.lease),
            id,
            cwd: std::mem::take(&mut self.cwd),
            run_id: self.live_run.take(),
            workspace: self.workspace.take(),
        })
    }
}

/// `CreateAgent` on a task of its own, which holds the lease and owns the
/// id it returns: a completion that stops waiting — at the inactivity
/// bound, or dropped — leaves an agent the task closes and deletes itself.
struct Creating(oneshot::Receiver<Result<Created>>);

/// A created agent and the workspace its cwd points into.
struct Created {
    id: String,
    workspace: Workspace,
}

impl Creating {
    // The lease is the slot, so the task's wait is bounded too: the window
    // the completion waits, then one more for a late id — or a bridge that
    // stays alive and silent would hold the slot for good.
    fn spawn(
        lease: Arc<Lease>, options: AgentOptions, workspace: Workspace, cwd: String,
        window: Duration,
    ) -> Self {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let rpc = lease.bridge().rpc().clone();
            let limit = window.saturating_mul(2);
            let outcome = match timeout(limit, rpc.create_agent(options)).await {
                Ok(Ok(created)) if created.agent_id.is_empty() => {
                    Err(anyhow!("bridge RPC `CreateAgent` returned an empty agent id"))
                }
                Ok(Ok(created)) => Ok(Created {
                    id: created.agent_id,
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
                Err(Ok(unclaimed)) => {
                    Release {
                        lease,
                        id: unclaimed.id,
                        cwd,
                        run_id: None,
                        workspace: Some(unclaimed.workspace),
                    }
                    .run()
                    .await;
                }
                Err(Err(error)) => tracing::debug!(%error, "abandoned CreateAgent failed"),
            }
        });
        Self(rx)
    }

    /// The id, within `window`; past it the agent is the task's.
    async fn claim(self, bridge: &Bridge, window: Duration) -> Result<Created> {
        let died = bridge.died();
        tokio::pin!(died);
        tokio::select! {
            outcome = self.0 => match outcome {
                Ok(Ok(created)) => Ok(created),
                Ok(Err(error)) => Err(exit_or(bridge, error).await),
                Err(_closed) => Err(anyhow!("bridge RPC `CreateAgent` ended without an outcome")),
            },
            exit = &mut died => Err(Failure::BridgeExited(exit).into()),
            () = sleep(window) => Err(anyhow!(
                "bridge RPC `CreateAgent` unanswered after {}s",
                window.as_secs()
            )),
        }
    }
}

// A socket fails before the watcher publishes the exit: `watch_child`
// spends up to `EXIT_GRACE` draining stderr first, so the observe budget
// is that window plus one of its own. An attached bridge is never seen to
// die, so its error stands.
async fn exit_or(bridge: &Bridge, error: anyhow::Error) -> anyhow::Error {
    if !bridge.is_owned() {
        return error;
    }
    match timeout(EXIT_OBSERVE, bridge.died()).await {
        Ok(exit) => Failure::BridgeExited(exit).into(),
        Err(_elapsed) => error,
    }
}

// One best-effort teardown call: a failure is logged, a silence is bounded.
async fn teardown(method: &'static str, rpc: impl Future<Output = Result<()>>) {
    match timeout(TEARDOWN_TIMEOUT, rpc).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, method, "agent teardown call failed"),
        Err(_elapsed) => tracing::warn!(method, "agent teardown call unanswered"),
    }
}

/// Close then delete, holding the create-time cwd until both RPCs finish so
/// a private workspace is still visible to the local store. Holds the lease
/// so the bridge outlives the teardown.
struct Release {
    lease: Arc<Lease>,
    id: String,
    cwd: String,
    run_id: Option<String>,
    workspace: Option<Workspace>,
}

impl Release {
    /// Run on a task of its own, so the caller's fate does not cut the
    /// teardown short; `None` without a runtime.
    fn spawn(self) -> Option<JoinHandle<()>> {
        Handle::try_current().ok().map(|handle| handle.spawn(self.run()))
    }

    async fn run(self) {
        let bridge = self.lease.bridge();
        if bridge.is_dead() {
            tracing::debug!(agent = %self.id, "bridge exited; skipping agent teardown");
        } else {
            let rpc = bridge.rpc();
            if let Some(run_id) = self.run_id {
                teardown("CancelRun", rpc.cancel_run(run_id, self.id.clone())).await;
            }
            teardown("CloseAgent", rpc.close_agent(self.id.clone())).await;
            let api_key = env::var("CURSOR_API_KEY").unwrap_or_default();
            teardown("DeleteAgent", rpc.delete_agent(self.id, self.cwd, api_key)).await;
        }
        drop(self.workspace);
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        if let Some(release) = self.take_release() {
            release.spawn();
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
    pub async fn watch(self, mut activity: watch::Receiver<Instant>) -> Failure {
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

// The deadline arithmetic, and the check loop over a scripted `sdk.v1`
// bridge (CI floor): accept, correct-then-accept, exhaust. `tests/live.rs`
// proves the same loop against a real bridge.
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use http_body_util::{BodyExt as _, Full};
    use hyper::body::{Bytes, Incoming};
    use hyper::header::CONTENT_TYPE;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use omnia::Backend as _;
    use omnia_wasi_model::{
        DirEntry, Error, Format, FutureResult, Grants, Message, Request, Role, ToolHost,
        WasiModelCtx as _,
    };
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, watch};
    use tokio::time::{Duration, Instant, sleep};

    use super::{Deadlines, MAX_ROUNDS};
    use crate::bridge::{Bridge, EXIT_GRACE, Exit};
    use crate::model::observe;
    use crate::model::options::with_dummy_key;
    use crate::{Client, ConnectOptions};

    const DEADLINES: Deadlines = Deadlines {
        inactivity: Duration::from_mins(2),
        cap: Duration::from_mins(10),
    };

    /// Every `Send` text a scripted bridge received, in order.
    type Sends = Arc<Mutex<Vec<String>>>;
    /// Every `DeleteAgent` body, so cleanup can assert the create-time cwd.
    type Deletes = Arc<Mutex<Vec<Value>>>;
    /// Every `CloseAgent` id, so a late create can assert it was reaped.
    type Closes = Arc<Mutex<Vec<String>>>;

    /// Holds every request for one `SdkAgentService` method until opened,
    /// and reports that one is waiting.
    struct Gate {
        method: &'static str,
        arrived: Notify,
        release: Notify,
    }

    impl Gate {
        fn on(method: &'static str) -> Arc<Self> {
            Arc::new(Self {
                method,
                arrived: Notify::new(),
                release: Notify::new(),
            })
        }

        async fn hold(&self, path: &str) {
            if path == format!("/sdk.v1.SdkAgentService/{}", self.method) {
                self.arrived.notify_one();
                self.release.notified().await;
            }
        }

        async fn arrived(&self) {
            self.arrived.notified().await;
        }

        fn open(&self) {
            self.release.notify_one();
        }
    }

    /// A client attached to a loopback `sdk.v1` bridge whose agent answers
    /// `Send` number `n` with `replies[n]` (the last reply repeats) and
    /// records each text sent.
    async fn scripted(replies: &[&str]) -> (Client, Sends, Deletes) {
        let (client, sends, deletes, _closes) = scripted_on(replies, None, DEADLINES, 4).await;
        (client, sends, deletes)
    }

    async fn scripted_on(
        replies: &[&str], gate: Option<Arc<Gate>>, deadlines: Deadlines, max_agents: usize,
    ) -> (Client, Sends, Deletes, Closes) {
        with_dummy_key();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind loopback");
        let addr = listener.local_addr().expect("local address");
        let replies: Arc<Vec<String>> = Arc::new(replies.iter().map(|r| (*r).to_owned()).collect());
        let sends = Sends::default();
        let deletes = Deletes::default();
        let closes = Closes::default();
        tokio::spawn(serve(
            listener,
            replies,
            Arc::clone(&sends),
            Arc::clone(&deletes),
            Arc::clone(&closes),
            gate,
        ));

        let client = Client::connect_with(ConnectOptions {
            model: "auto".to_owned(),
            timeout_secs: deadlines.cap.as_secs(),
            inactivity_secs: deadlines.inactivity.as_secs(),
            max_agents,
            bridge_bin: "cursor-sdk-bridge".to_owned(),
            bridge_url: Some(format!("http://{addr}")),
            bridge_token: Some("test-token".to_owned()),
        })
        .await
        .expect("the scripted bridge answers the handshake");
        (client, sends, deletes, closes)
    }

    async fn serve(
        listener: TcpListener, replies: Arc<Vec<String>>, sends: Sends, deletes: Deletes,
        closes: Closes, gate: Option<Arc<Gate>>,
    ) {
        while let Ok((stream, _)) = listener.accept().await {
            let replies = Arc::clone(&replies);
            let sends = Arc::clone(&sends);
            let deletes = Arc::clone(&deletes);
            let closes = Arc::clone(&closes);
            let gate = gate.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: hyper::Request<Incoming>| {
                    let replies = Arc::clone(&replies);
                    let sends = Arc::clone(&sends);
                    let deletes = Arc::clone(&deletes);
                    let closes = Arc::clone(&closes);
                    let gate = gate.clone();
                    async move {
                        let path = request.uri().path().to_owned();
                        let body = request.into_body().collect().await?.to_bytes();
                        if let Some(gate) = &gate {
                            gate.hold(&path).await;
                        }
                        Ok::<_, hyper::Error>(procedure(
                            &path, &body, &replies, &sends, &deletes, &closes,
                        ))
                    }
                });
                let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
            });
        }
    }

    /// One `sdk.v1` procedure: the handshake and lifecycle calls answer
    /// minimally; `Send` records the text and streams the scripted result.
    fn procedure(
        path: &str, body: &[u8], replies: &[String], sends: &Sends, deletes: &Deletes,
        closes: &Closes,
    ) -> hyper::Response<Full<Bytes>> {
        let (content_type, body) = match path {
            "/sdk.v1.SdkBridgeControlService/GetVersion" => (
                "application/json",
                json!({ "protocolVersion": "sdk.v1" }).to_string().into_bytes(),
            ),
            "/sdk.v1.SdkAgentService/CreateAgent" => {
                ("application/json", json!({ "agentId": "agent-1" }).to_string().into_bytes())
            }
            "/sdk.v1.SdkAgentService/CloseAgent" => {
                let request: Value =
                    serde_json::from_slice(body).expect("a JSON CloseAgentRequest");
                closes
                    .lock()
                    .expect("closes lock")
                    .push(request["agentId"].as_str().unwrap_or_default().to_owned());
                ("application/json", b"{}".to_vec())
            }
            "/sdk.v1.SdkAgentService/Send" => {
                // The request rides as one Connect envelope: a 5-byte prefix,
                // then the JSON `SendRequest`.
                let request: Value =
                    serde_json::from_slice(&body[5..]).expect("an enveloped SendRequest");
                let text = request["message"]["text"].as_str().unwrap_or_default().to_owned();
                let round = {
                    let mut seen = sends.lock().expect("sends lock");
                    seen.push(text);
                    seen.len() - 1
                };
                let result = replies.get(round).or_else(|| replies.last());
                ("application/connect+json", run_stream(round, result.map_or("", String::as_str)))
            }
            "/sdk.v1.SdkAgentService/DeleteAgent" => {
                let request: Value =
                    serde_json::from_slice(body).expect("a JSON DeleteAgentRequest");
                deletes.lock().expect("deletes lock").push(request);
                ("application/json", b"{}".to_vec())
            }
            // Ping, Shutdown, CancelRun
            _ => ("application/json", b"{}".to_vec()),
        };
        hyper::Response::builder()
            .header(CONTENT_TYPE, content_type)
            .body(Full::new(Bytes::from(body)))
            .expect("a well-formed response")
    }

    /// A finished run's stream: the result-and-done frame, then the end
    /// frame.
    fn run_stream(round: usize, result: &str) -> Vec<u8> {
        let run_id = format!("run-{round}");
        let message = json!({
            "result": {
                "runId": run_id,
                "status": "RUN_LIFECYCLE_STATUS_FINISHED",
                "result": {
                    "runId": run_id,
                    "result": result,
                    "usage": { "inputTokens": "3", "outputTokens": "1" },
                },
            },
            "done": {},
        });
        let mut body = envelope(0, message.to_string().as_bytes());
        body.extend(envelope(0x02, b"{}"));
        body
    }

    fn envelope(flags: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![flags];
        frame.extend_from_slice(&u32::try_from(payload.len()).expect("frame fits").to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// The guest's stand-in: rejects the first `rejections` candidates with a
    /// correction naming them, accepts the rest, and records every candidate.
    #[derive(Debug)]
    struct Check {
        rejections: usize,
        seen: AtomicUsize,
        candidates: Mutex<Vec<String>>,
    }

    impl Check {
        fn rejecting(rejections: usize) -> Arc<Self> {
            Arc::new(Self {
                rejections,
                seen: AtomicUsize::new(0),
                candidates: Mutex::new(Vec::new()),
            })
        }

        fn host(self: &Arc<Self>) -> Arc<dyn ToolHost> {
            let host: Arc<Self> = Arc::clone(self);
            host
        }

        fn candidates(&self) -> Vec<String> {
            self.candidates.lock().expect("candidates lock").clone()
        }
    }

    impl ToolHost for Check {
        fn call_tool(
            &self, name: String, _arguments: String,
        ) -> FutureResult<Result<String, String>> {
            Box::pin(
                async move { Err(anyhow::anyhow!("no function tools are declared: `{name}`")) },
            )
        }

        fn read(&self, _path: String) -> FutureResult<Vec<u8>> {
            Box::pin(async { Err(anyhow::anyhow!("cursor never routes `read` through the host")) })
        }

        fn list(&self, _path: String) -> FutureResult<Vec<DirEntry>> {
            Box::pin(async { Err(anyhow::anyhow!("cursor never routes `list` through the host")) })
        }

        fn write(&self, _path: String, _bytes: Vec<u8>) -> FutureResult<()> {
            Box::pin(async { Err(anyhow::anyhow!("cursor never routes `write` through the host")) })
        }

        fn check(&self, candidate: String) -> FutureResult<Result<(), String>> {
            let seen = self.seen.fetch_add(1, Ordering::SeqCst);
            self.candidates.lock().expect("candidates lock").push(candidate.clone());
            let verdict = if seen < self.rejections {
                Err(format!(
                    "## Previous answer (rejected)\n\n{candidate}\n\n## Findings\n\nnot it"
                ))
            } else {
                Ok(())
            };
            Box::pin(async move { Ok(verdict) })
        }
    }

    fn assert_scoped_delete(deletes: &Deletes) {
        let (cwd, key, body) = {
            let deletes = deletes.lock().expect("deletes lock");
            assert_eq!(deletes.len(), 1, "complete awaits one DeleteAgent: {deletes:?}");
            let cwd = deletes[0]["options"]["cwd"].as_str().unwrap_or_default().to_owned();
            let key = deletes[0]["options"]["apiKey"].as_str().unwrap_or_default().to_owned();
            (cwd, key, deletes[0].clone())
        };
        assert!(!cwd.is_empty(), "delete repeats the create-time cwd: {body}");
        assert!(!key.is_empty(), "delete repeats the create-time key: {body}");
    }

    fn request(check: bool) -> Request {
        Request {
            model: None,
            system: Some("answer with one word".to_owned()),
            messages: vec![Message {
                role: Role::User,
                content: "hi".to_owned(),
            }],
            generation: None,
            format: Format::Text,
            tools: vec![],
            grants: Grants { workspace: None },
            check,
        }
    }

    #[tokio::test]
    async fn unchecked() {
        let (client, sends, deletes) = scripted(&["alpha"]).await;
        let check = Check::rejecting(usize::MAX);
        let answer = client.complete(request(false), check.host()).await.expect("completes");
        assert_eq!(answer.answer, "alpha");
        assert_eq!(answer.usage.map(|u| (u.input_tokens, u.output_tokens)), Some((3, 1)));
        assert!(check.candidates().is_empty(), "no check was asked for");
        let sends = sends.lock().expect("sends lock").clone();
        assert_eq!(sends.len(), 1);
        assert!(sends[0].contains("hi"), "the opening prompt carries the request: {}", sends[0]);
        assert_scoped_delete(&deletes);
    }

    #[tokio::test]
    async fn check_accepts() {
        let (client, sends, deletes) = scripted(&["alpha"]).await;
        let check = Check::rejecting(0);
        let answer = client.complete(request(true), check.host()).await.expect("completes");
        assert_eq!(answer.answer, "alpha");
        assert_eq!(check.candidates(), ["alpha"]);
        assert_eq!(sends.lock().expect("sends lock").len(), 1);
        assert_scoped_delete(&deletes);
    }

    #[tokio::test]
    async fn check_corrects() {
        let (client, sends, deletes) = scripted(&["alpha", "beta"]).await;
        let check = Check::rejecting(1);
        let answer = client.complete(request(true), check.host()).await.expect("completes");
        assert_eq!(answer.answer, "beta", "the accepted candidate is the answer");
        assert_eq!(check.candidates(), ["alpha", "beta"]);

        // The agent keeps its session, so the second send is the correction
        // alone, verbatim.
        let sends = sends.lock().expect("sends lock").clone();
        assert_eq!(sends.len(), 2);
        assert_eq!(sends[1], "## Previous answer (rejected)\n\nalpha\n\n## Findings\n\nnot it");
        assert_scoped_delete(&deletes);
    }

    #[tokio::test]
    async fn check_exhausts() {
        let (client, sends, deletes) = scripted(&["alpha"]).await;
        let check = Check::rejecting(usize::MAX);
        let error = client
            .complete(request(true), check.host())
            .await
            .expect_err("every candidate is rejected");
        let Some(Error::BudgetExhausted(correction)) = error.downcast_ref::<Error>() else {
            panic!("expected the typed budget-exhausted: {error:?}");
        };
        assert!(correction.contains("## Findings\n\nnot it"), "the last correction: {correction}");
        assert_eq!(check.candidates().len(), MAX_ROUNDS, "every round offered a candidate");
        assert_eq!(sends.lock().expect("sends lock").len(), MAX_ROUNDS);
        assert_scoped_delete(&deletes);
    }

    /// Wait until the scripted bridge has seen `n` `DeleteAgent` calls.
    async fn await_deletes(deletes: &Deletes, closes: &Closes, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while deletes.lock().expect("deletes lock").len() < n {
            assert!(
                Instant::now() < deadline,
                "agent was not closed and deleted: closes={:?} deletes={:?}",
                closes.lock().expect("closes lock"),
                deletes.lock().expect("deletes lock"),
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn create_timeout_reaps_late_agent() {
        let gate = Gate::on("CreateAgent");
        let deadlines = Deadlines {
            inactivity: Duration::from_secs(1),
            cap: Duration::from_secs(10),
        };
        let (client, _sends, deletes, closes) =
            scripted_on(&["unused"], Some(Arc::clone(&gate)), deadlines, 4).await;
        let check = Check::rejecting(usize::MAX);
        let error =
            client.complete(request(false), check.host()).await.expect_err("CreateAgent timed out");
        assert!(
            error.to_string().contains("unanswered after 1s"),
            "the inactivity kill names the unanswered RPC: {error}"
        );
        assert!(
            deletes.lock().expect("deletes lock").is_empty(),
            "reap must not run before the late CreateAgent answers"
        );
        assert!(closes.lock().expect("closes lock").is_empty());

        gate.open();
        await_deletes(&deletes, &closes, 1).await;
        assert_eq!(
            closes.lock().expect("closes lock").as_slice(),
            ["agent-1"],
            "CloseAgent uses the late id"
        );
        assert_scoped_delete(&deletes);
    }

    #[tokio::test]
    async fn cancelled_create_reaps_late_agent() {
        // One slot: the reap must also give the lease back afterwards.
        let gate = Gate::on("CreateAgent");
        let (client, _sends, deletes, closes) =
            scripted_on(&["alpha"], Some(Arc::clone(&gate)), DEADLINES, 1).await;
        let check = Check::rejecting(usize::MAX);

        // The guest drops the completion while CreateAgent is in flight.
        let completion = tokio::spawn(client.complete(request(false), check.host()));
        gate.arrived().await;
        completion.abort();
        completion.await.expect_err("the completion was cancelled");
        assert!(closes.lock().expect("closes lock").is_empty(), "no id has arrived yet");
        assert!(deletes.lock().expect("deletes lock").is_empty());

        gate.open();
        await_deletes(&deletes, &closes, 1).await;
        assert_eq!(
            closes.lock().expect("closes lock").as_slice(),
            ["agent-1"],
            "the id nobody claimed is closed"
        );
        assert_scoped_delete(&deletes);

        // The slot reopened with the reap; a fresh completion runs on it.
        let next = tokio::spawn(client.complete(request(false), check.host()));
        tokio::time::timeout(Duration::from_secs(2), gate.arrived())
            .await
            .expect("the slot reopens once the reap is done");
        gate.open();
        let answer = next.await.expect("joins").expect("completes");
        assert_eq!(answer.answer, "alpha");
        assert_eq!(deletes.lock().expect("deletes lock").len(), 2);
    }

    #[tokio::test]
    async fn cancelled_teardown_still_deletes() {
        let gate = Gate::on("CloseAgent");
        let (client, sends, deletes, closes) =
            scripted_on(&["alpha"], Some(Arc::clone(&gate)), DEADLINES, 4).await;
        let check = Check::rejecting(usize::MAX);

        // The guest drops the completion after the run, mid-teardown.
        let completion = tokio::spawn(client.complete(request(false), check.host()));
        gate.arrived().await;
        assert_eq!(sends.lock().expect("sends lock").len(), 1, "the run finished first");
        completion.abort();
        completion.await.expect_err("the completion was cancelled");
        assert!(deletes.lock().expect("deletes lock").is_empty(), "CloseAgent is still held");

        gate.open();
        await_deletes(&deletes, &closes, 1).await;
        assert_eq!(closes.lock().expect("closes lock").as_slice(), ["agent-1"]);
        assert_scoped_delete(&deletes);
    }

    #[tokio::test]
    async fn create_never_answered_frees_slot() {
        // Never opened: the bridge stays alive and never answers.
        let gate = Gate::on("CreateAgent");
        let deadlines = Deadlines {
            inactivity: Duration::from_secs(1),
            cap: Duration::from_secs(10),
        };
        let (client, _sends, deletes, closes) =
            scripted_on(&["unused"], Some(gate), deadlines, 1).await;
        let check = Check::rejecting(usize::MAX);
        let started = Instant::now();

        let error =
            client.complete(request(false), check.host()).await.expect_err("CreateAgent timed out");
        assert!(error.to_string().contains("unanswered after 1s"), "{error}");

        // The reap keeps the only slot for one more window, then gives it up:
        // the second completion must reach its own CreateAgent rather than
        // queue on the slot forever.
        let error = tokio::time::timeout(
            Duration::from_secs(8),
            client.complete(request(false), check.host()),
        )
        .await
        .expect("the slot reopens once the reap gives up on CreateAgent")
        .expect_err("the second CreateAgent timed out too");
        assert!(error.to_string().contains("unanswered after 1s"), "{error}");
        assert!(
            started.elapsed() >= 3 * deadlines.inactivity,
            "the reap waits a full window before the slot reopens: {:?}",
            started.elapsed()
        );
        assert!(closes.lock().expect("closes lock").is_empty(), "no id arrived to close");
        assert!(deletes.lock().expect("deletes lock").is_empty(), "no id arrived to delete");
    }

    #[tokio::test(start_paused = true)]
    async fn hit_deadline() {
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
    async fn hit_timeout() {
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
    async fn reset_deadline() {
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

    #[tokio::test(start_paused = true)]
    async fn exit_or_after_full_drain() {
        let (bridge, tx) = Bridge::pending();
        let wait = super::exit_or(&bridge, anyhow::anyhow!("connection reset"));
        tokio::pin!(wait);

        // The watcher spends this window draining stderr before it publishes.
        tokio::select! {
            biased;
            _ = &mut wait => panic!("one EXIT_GRACE is still the drain, not the observe budget"),
            () = sleep(EXIT_GRACE) => {}
        }

        tx.send(Some(Exit::default())).expect("receiver lives");
        let error = wait.await;
        assert_eq!(observe::outcome_of(&error), "bridge_exit", "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn exit_or_unanswered() {
        let (bridge, _tx) = Bridge::pending();
        let error = super::exit_or(&bridge, anyhow::anyhow!("connection reset")).await;
        assert_eq!(error.to_string(), "connection reset");
        assert_eq!(observe::outcome_of(&error), "error");
    }
}
