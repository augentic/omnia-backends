//! `wasi-model` implementation driving one bridge-managed Cursor agent per
//! completion.
//!
//! The gate-validated [`Request`] maps onto `CreateAgent` options: guest
//! function tools become SDK custom tools (executed back through the
//! session via the loopback callback and [`ToolHost::call_tool`]), MCP
//! grants ride inline as `mcp_servers`, and the lent workspace — or a
//! private empty directory when none is lent — becomes the agent's `cwd`.
//! One `Send` stream produces the answer; a failed format gate sends the
//! repair instruction on the same agent, whose session already carries the
//! prompt and the failed answer.

mod events;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{Context as _, Result, bail};
use events::{EventLog, PROMPT_PREVIEW_CHARS, is_noisy_tool, truncate};
use omnia_wasi_model::{
    Answer, Format, FutureResult, Mcp, Request, Tool, ToolHost, Transcript, Usage, WasiModelCtx,
};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};

use crate::bridge::transport::{Transport, end_stream_error};
use crate::bridge::types::{
    AgentOptions, CancelRunRequest, CreateAgentRequest, CreateAgentResponse, CustomToolDefinition,
    DeleteAgentRequest, LocalAgentOptions, McpServerConfig, ModelSelection, RunStatus,
    RunStreamMessage, RunStreamResult, SendRequest, TokenUsage, ToolList, UserMessage,
};
use crate::{Client, Deadlines};

/// The model id sent when neither the request nor `CURSOR_MODEL` names one:
/// Cursor's own server-side selection.
const AUTO_MODEL: &str = "auto";

#[derive(Debug)]
struct AgentOutput {
    result: String,
    transcript: Option<Transcript>,
    usage: Option<Usage>,
}

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let shared = Arc::clone(&self.shared);
        let deadlines = self.deadlines;
        let default_model = self.model.clone();

        Box::pin(async move {
            // Read per completion rather than held on `Client`, so the key is
            // never stored, logged, or recorded into fixtures.
            let api_key = std::env::var("CURSOR_API_KEY")
                .context("CURSOR_API_KEY must be set for the cursor backend")?;

            let workspace = match tool_host.local_path() {
                Some(path) => Workspace::Lent(prepare_workspace(path)?),
                // No lent tree: a references-only completion (function tools
                // and MCP grants) runs in a private empty cwd with every
                // built-in tool disabled.
                None => Workspace::Private(
                    tempfile::Builder::new()
                        .prefix("omnia-cursor-cwd-")
                        .tempdir()
                        .context("creating a private working directory")?,
                ),
            };

            let format = request.format.clone();
            let options = agent_options(&request, &workspace, default_model.as_deref(), api_key)?;
            let model_id = options.model.id.clone();

            let mcp_servers = request.mcp_servers();
            let mcp_names: Vec<&str> = mcp_servers.iter().map(|s| s.name.as_str()).collect();
            let prompt = with_mcp_hint(&mcp_servers, request.to_string());
            log_completion(&model_id, &format, prompt.len(), &mcp_names);

            let transport = shared.bridge.transport().clone();
            let created: CreateAgentResponse = transport
                .unary("SdkAgentService/CreateAgent", &CreateAgentRequest { options })
                .await?;
            let agent = Agent {
                transport,
                id: created.agent_id.clone(),
                deadlines,
                live_run: Mutex::new(None),
            };

            let (abort_tx, mut abort_rx) = mpsc::unbounded_channel();
            let _registration = shared.registry.register(created.agent_id, tool_host, abort_tx);

            let output = agent.send(&prompt, &mut abort_rx).await?;
            output.log(1);
            let reason = match take_answer(&format, output) {
                Outcome::Done(answer) => return Ok(answer),
                Outcome::Repair(reason) => reason,
            };

            // The second (and last) attempt sends only the format-repair
            // instruction: the agent's session already carries the prompt
            // and the failed answer.
            tracing::debug!(attempt = 1, %reason, "repairing answer");
            let output = agent.send(&format.repair(&reason), &mut abort_rx).await?;
            output.log(2);

            match take_answer(&format, output) {
                Outcome::Done(answer) => Ok(answer),
                Outcome::Repair(reason) => {
                    bail!("no answer after 2 repair attempts: {reason}");
                }
            }
        })
    }
}

/// One bridge-managed agent, deleted (and its live run cancelled) on drop.
struct Agent {
    transport: Transport,
    id: String,
    deadlines: Deadlines,
    live_run: Mutex<Option<String>>,
}

impl Agent {
    /// One turn: `Send` the text and consume the run stream to its terminal
    /// result, bounded by the inactivity and absolute deadlines and by the
    /// callback's abort signal.
    async fn send(
        &self, text: &str, abort_rx: &mut mpsc::UnboundedReceiver<String>,
    ) -> Result<AgentOutput> {
        let request = SendRequest {
            agent_id: self.id.clone(),
            message: UserMessage {
                text: text.to_owned(),
            },
        };
        tracing::debug!(
            prompt_len = text.len(),
            preview = %truncate(text, PROMPT_PREVIEW_CHARS),
            "send"
        );
        let mut stream = self.transport.server_stream("SdkAgentService/Send", &request).await?;

        let (activity_tx, activity_rx) = watch::channel(Instant::now());
        let deadline = self.deadlines.watch(activity_rx);
        tokio::pin!(deadline);

        let mut log = EventLog::default();
        let mut outcome: Option<RunStreamResult> = None;

        loop {
            tokio::select! {
                frame = stream.next() => {
                    let Some(frame) = frame? else {
                        break;
                    };
                    if frame.is_end_stream() {
                        end_stream_error("SdkAgentService/Send", &frame.payload)?;
                        break;
                    }
                    let message: RunStreamMessage = match serde_json::from_slice(&frame.payload) {
                        Ok(message) => message,
                        Err(error) => {
                            tracing::debug!(%error, "skipping unparsable stream frame");
                            continue;
                        }
                    };
                    // Keepalives (and unknown envelope cases) do not rearm
                    // the inactivity deadline: only real progress counts.
                    if message.is_keepalive() {
                        continue;
                    }
                    activity_tx.send_replace(Instant::now());
                    if let Some(event) = &message.sdk_message {
                        log.observe(event);
                        self.note_live_run(log.run_id());
                    }
                    if let Some(result) = message.result {
                        self.note_live_run(Some(&result.run_id));
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
        self.lock_live_run().take();
        let outcome = outcome.context("the run stream ended without a result")?;
        if outcome.status != RunStatus::Finished {
            let detail = outcome
                .error_code
                .filter(|code| !code.is_empty())
                .or_else(|| log.status_message().map(ToOwned::to_owned))
                .unwrap_or_else(|| "<no detail>".to_owned());
            bail!("cursor run {}: {detail}", outcome.status.describe());
        }
        let result = outcome.result.unwrap_or_default();
        Ok(AgentOutput {
            result: result.result,
            transcript: log.finish(),
            usage: result.usage.map(Usage::from),
        })
    }

    fn note_live_run(&self, run_id: Option<&str>) {
        if let Some(run_id) = run_id {
            self.lock_live_run().get_or_insert_with(|| run_id.to_owned());
        }
    }

    /// Best-effort, detached `CancelRun` when a turn is abandoned mid-run
    /// (deadline breach or callback abort) — the completion's own error is
    /// already decided, so the cancel is not awaited.
    fn cancel_live_run(&self) {
        let Some(run_id) = self.lock_live_run().take() else {
            return;
        };
        let transport = self.transport.clone();
        let agent_id = self.id.clone();
        tokio::spawn(async move {
            let cancel = CancelRunRequest {
                run_id,
                agent_id: Some(agent_id),
            };
            if let Err(error) =
                transport.unary::<_, Value>("SdkAgentService/CancelRun", &cancel).await
            {
                tracing::debug!(%error, "cancel after abandon failed");
            }
        });
    }

    fn lock_live_run(&self) -> std::sync::MutexGuard<'_, Option<String>> {
        self.live_run.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        self.cancel_live_run();
        let transport = self.transport.clone();
        let agent_id = std::mem::take(&mut self.id);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let delete = DeleteAgentRequest { agent_id };
                if let Err(error) =
                    transport.unary::<_, Value>("SdkAgentService/DeleteAgent", &delete).await
                {
                    tracing::debug!(%error, "agent delete failed");
                }
            });
        }
    }
}

/// The agent's working directory: the lent tree, or a private empty one for
/// references-only completions.
enum Workspace {
    Lent(PathBuf),
    Private(tempfile::TempDir),
}

impl Workspace {
    fn path(&self) -> &Path {
        match self {
            Self::Lent(path) => path,
            Self::Private(dir) => dir.path(),
        }
    }

    const fn lent(&self) -> bool {
        matches!(self, Self::Lent(_))
    }
}

/// Map the gate-validated request onto `CreateAgent` options.
fn agent_options(
    request: &Request, workspace: &Workspace, default_model: Option<&str>, api_key: String,
) -> Result<AgentOptions> {
    let mut custom_tools = BTreeMap::new();
    let mut mcp_servers = BTreeMap::new();
    for tool in &request.tools {
        match tool {
            Tool::Function(function) => {
                let input_schema: Value =
                    serde_json::from_str(&function.parameters).with_context(|| {
                        format!("function tool `{}` parameters is not valid JSON", function.name)
                    })?;
                custom_tools.insert(
                    function.name.clone(),
                    CustomToolDefinition {
                        description: (!function.description.is_empty())
                            .then(|| function.description.clone()),
                        input_schema,
                    },
                );
            }
            // The grant's `tools` allowlist stays advisory (the prompt hint
            // names it); enforcing it needs a filtering proxy — future work.
            Tool::Mcp(mcp) => {
                mcp_servers.insert(mcp.name.clone(), McpServerConfig::streamable_http(&mcp.url));
            }
        }
    }

    // Guest-supplied request.model wins; else CURSOR_MODEL; else `auto`.
    let model = request.model.as_deref().or(default_model).unwrap_or(AUTO_MODEL).to_owned();
    Ok(AgentOptions {
        model: ModelSelection { id: model },
        api_key,
        local: LocalAgentOptions {
            cwd: vec![workspace.path().display().to_string()],
            // A lent tree's own project settings apply (matching the old
            // spawned CLI's discovery); nothing is read from the host user.
            setting_sources: if workspace.lent() {
                vec!["SETTING_SOURCE_PROJECT".to_owned()]
            } else {
                Vec::new()
            },
            custom_tools,
        },
        mcp_servers,
        // With a lent tree the agent keeps the default built-in toolset; a
        // references-only run explicitly disables every built-in tool.
        tools: if workspace.lent() { None } else { Some(ToolList { names: Vec::new() }) },
    })
}

enum Outcome {
    Done(Answer),
    Repair(String),
}

fn take_answer(format: &Format, output: AgentOutput) -> Outcome {
    match format.parse(&output.result) {
        Ok(value) => Outcome::Done(Answer {
            value,
            usage: output.usage,
            transcript: output.transcript,
        }),
        Err(reason) => Outcome::Repair(reason),
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

fn prepare_workspace(path: &Path) -> Result<PathBuf> {
    fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
    path.canonicalize().with_context(|| format!("canonicalizing {}", path.display()))
}

/// Prepend a natural-language hint naming the granted MCP servers and any
/// tool allowlist, so the agent prefers them over assumptions.
fn with_mcp_hint(servers: &[&Mcp], prompt: String) -> String {
    if servers.is_empty() {
        return prompt;
    }
    let lines: Vec<String> = servers
        .iter()
        .map(|server| {
            if server.tools.is_empty() {
                format!("- `{}`", server.name)
            } else {
                format!("- `{}` (use only: {})", server.name, server.tools.join(", "))
            }
        })
        .collect();
    format!(
        "The following read-only MCP servers are available. Consult their tools and resources for \
         authoritative reference material before answering, and prefer that material over \
         assumptions:\n{}\n\n{prompt}",
        lines.join("\n")
    )
}

/// One-line INFO for the completion start.
fn log_completion(model: &str, format: &Format, prompt_len: usize, mcp_servers: &[&str]) {
    let format_name = match format {
        Format::Text => "text",
        Format::Json => "json",
        Format::Schema(spec) => {
            tracing::trace!(
                schema_name = %spec.name,
                schema = %truncate(&spec.schema, PROMPT_PREVIEW_CHARS),
                "completion schema"
            );
            "schema"
        }
    };
    tracing::info!(model, format = format_name, prompt_len, ?mcp_servers, "completion");
}

impl AgentOutput {
    fn log(&self, attempt: u32) {
        let turns = self.transcript.as_ref().map_or(&[][..], |t| t.turns.as_slice());
        let noisy = turns.iter().filter(|turn| is_noisy_tool(&turn.tool)).count();
        tracing::debug!(
            attempt,
            result_len = self.result.len(),
            interesting_tools = turns.len() - noisy,
            noisy_tools = noisy,
            "answer"
        );
    }
}

impl Deadlines {
    /// Resolve when a run breaches its inactivity or absolute bound.
    async fn watch(&self, mut activity: watch::Receiver<Instant>) -> anyhow::Error {
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

// Deliberate unit tests: pure request-mapping and deadline logic (CI floor);
// `tests/live.rs` is the acceptance gate proving a real bridge-driven run
// works end-to-end.
#[cfg(test)]
mod tests {
    use omnia_wasi_model::{Format, Function, Grants, Mcp, Message, Request, Role, Tool};
    use serde_json::{Value, json};

    use super::{Workspace, agent_options, with_mcp_hint};

    fn request(tools: Vec<Tool>) -> Request {
        Request {
            model: None,
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "hello".to_owned(),
            }],
            generation: None,
            format: Format::Text,
            tools,
            grants: Grants { workspace: None },
        }
    }

    fn lookup_tool() -> Tool {
        Tool::Function(Function {
            name: "lookup".to_owned(),
            description: "look something up".to_owned(),
            parameters: r#"{"type":"object"}"#.to_owned(),
        })
    }

    fn options_json(request: &Request, workspace: &Workspace) -> Value {
        let options = agent_options(request, workspace, None, "key".to_owned())
            .expect("a gate-validated request maps");
        serde_json::to_value(options).expect("options serialize")
    }

    fn lent() -> Workspace {
        Workspace::Lent(std::env::temp_dir())
    }

    fn private() -> Workspace {
        Workspace::Private(tempfile::tempdir().expect("temp cwd"))
    }

    #[test]
    fn function_tool_becomes_a_custom_tool() {
        let options = options_json(&request(vec![lookup_tool()]), &lent());
        let tool = &options["local"]["customTools"]["lookup"];
        assert_eq!(tool["description"], "look something up");
        assert_eq!(tool["inputSchema"], json!({ "type": "object" }));
    }

    #[test]
    fn invalid_parameters_fail_loudly() {
        let mut request = request(vec![]);
        request.tools.push(Tool::Function(Function {
            name: "broken".to_owned(),
            description: String::new(),
            parameters: "not json".to_owned(),
        }));
        let Err(error) = agent_options(&request, &lent(), None, "key".to_owned()) else {
            panic!("unparseable parameters cannot be advertised");
        };
        assert!(error.to_string().contains("`broken`"), "unexpected error: {error}");
    }

    #[test]
    fn mcp_grant_rides_inline() {
        let options = options_json(
            &request(vec![Tool::Mcp(Mcp {
                name: "docs".to_owned(),
                tools: vec![],
                url: "http://localhost:8080/mcp".to_owned(),
            })]),
            &lent(),
        );
        let server = &options["mcpServers"]["docs"]["http"];
        assert_eq!(server["type"], "HTTP_MCP_TRANSPORT_TYPE_HTTP");
        assert_eq!(server["url"], "http://localhost:8080/mcp");
    }

    #[test]
    fn lent_workspace_keeps_the_default_toolset() {
        let options = options_json(&request(vec![]), &lent());
        assert_eq!(options.get("tools"), None, "absent tools means the default built-in set");
        assert_eq!(options["local"]["settingSources"], json!(["SETTING_SOURCE_PROJECT"]));
    }

    #[test]
    fn no_workspace_disables_builtin_tools() {
        let workspace = private();
        let options = options_json(&request(vec![lookup_tool()]), &workspace);
        assert_eq!(
            options["tools"],
            json!({ "names": [] }),
            "an explicit empty list disables every built-in tool"
        );
        assert_eq!(options["local"].get("settingSources"), None);
        assert_eq!(
            options["local"]["cwd"],
            json!([workspace.path().display().to_string()]),
            "the private cwd is the agent's working directory"
        );
    }

    #[test]
    fn model_fallback_chain() {
        let mut with_model = request(vec![]);
        with_model.model = Some("composer-2".to_owned());
        let options = agent_options(&with_model, &lent(), Some("default-model"), "key".to_owned())
            .expect("maps");
        assert_eq!(options.model.id, "composer-2", "the request's model wins");

        let options =
            agent_options(&request(vec![]), &lent(), Some("default-model"), "key".to_owned())
                .expect("maps");
        assert_eq!(options.model.id, "default-model", "else the configured default");

        let options =
            agent_options(&request(vec![]), &lent(), None, "key".to_owned()).expect("maps");
        assert_eq!(options.model.id, "auto", "else Cursor's server-side selection");
    }

    #[test]
    fn mcp_hint_names_servers_and_allowlists() {
        let docs = Mcp {
            name: "docs".to_owned(),
            tools: vec!["read_doc".to_owned()],
            url: "http://localhost/mcp".to_owned(),
        };
        let hinted = with_mcp_hint(&[&docs], "the prompt".to_owned());
        assert!(hinted.contains("`docs` (use only: read_doc)"), "hint: {hinted}");
        assert!(hinted.ends_with("the prompt"), "the original prompt closes the text");
        assert_eq!(with_mcp_hint(&[], "bare".to_owned()), "bare", "no grant, no hint");
    }

    mod timeouts {
        use tokio::sync::watch;
        use tokio::time::{Duration, Instant, sleep};

        use crate::Deadlines;

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
            assert!(
                error.to_string().contains("absolute cap"),
                "the cap kill is distinguishable from inactivity: {error}"
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

        #[tokio::test(start_paused = true)]
        async fn activity_before_watch_is_not_lost() {
            let (activity, receiver) = watch::channel(Instant::now());
            sleep(Duration::from_secs(100)).await;
            activity.send_replace(Instant::now());

            let started = Instant::now();
            let error = DEADLINES.watch(receiver).await;
            assert_eq!(started.elapsed(), Duration::from_mins(2));
            assert!(error.to_string().contains("inactive for 120s"), "unexpected: {error}");
        }
    }
}
