use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use omnia_wasi_model::{
    Answer, Format, FutureResult, Mcp, Request, ToolHost, ToolTurn, Transcript, WasiModelCtx,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, BufReader};
use tokio::process::Command;
use tokio::time::Instant;
use tracing::instrument;

use crate::{Client, mcp};

const CURSOR_BIN: &str = "cursor-agent";
const PROMPT_PREVIEW_CHARS: usize = 500;
const TEXT_PREVIEW_CHARS: usize = 300;
/// Coalesced thinking blocks stay readable; flush when a turn grows past this.
const THINKING_PREVIEW_CHARS: usize = 2_000;

/// Verify `cursor-agent` is on `PATH` and responds to `--version`.
pub async fn check_cursor() -> Result<()> {
    let status = Command::new(CURSOR_BIN)
        .arg("--version")
        .status()
        .await
        .context("cursor-agent not found")?;
    ensure!(status.success(), "`{CURSOR_BIN} --version` failed ({status})");
    Ok(())
}

struct SpawnOptions<'a> {
    model: Option<&'a str>,
    workspace: &'a Path,
    timeout: Duration,
    inactivity: Duration,
    approve_mcps: bool,
}

#[derive(Debug)]
struct AgentOutput {
    result: String,
    transcript: Option<Transcript>,
    /// The spawn's `session_id` from the stream, for `--resume` repairs.
    session_id: Option<String>,
}

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let workspace = tool_host.local_path().map(Path::to_path_buf);
        let timeout = self.timeout;
        let inactivity = self.inactivity;
        let default_model = self.model.clone();

        Box::pin(async move {
            let format = &request.format;
            let mut prompt = request.to_string();

            let Some(workspace) = workspace else {
                bail!("no local tree on this node");
            };
            fs::create_dir_all(&workspace)
                .with_context(|| format!("creating {}", workspace.display()))?;
            let workspace = workspace
                .canonicalize()
                .with_context(|| format!("canonicalizing {}", workspace.display()))?;

            // Per-prompt MCP grants carry their own endpoint URL.
            // No grant means no MCP wiring (MCP is opt-in per completion).
            let mcp_servers = request.mcp_servers();
            let mcp_names: Vec<&str> = mcp_servers.iter().map(|s| s.name.as_str()).collect();
            let mcp_guard = if mcp_servers.is_empty() {
                None
            } else {
                prompt = format!("{}\n\n{prompt}", mcp_hint(&mcp_servers));
                let map: BTreeMap<String, String> =
                    mcp_servers.iter().map(|s| (s.name.clone(), s.url.clone())).collect();
                Some(mcp::McpGuard::install(&workspace, &map)?)
            };

            // Guest-supplied request.model wins; else CURSOR_MODEL; else
            // cursor-agent chooses.
            let spawn = SpawnOptions {
                model: request.model.as_deref().or(default_model.as_deref()),
                workspace: &workspace,
                timeout,
                inactivity,
                approve_mcps: mcp_guard.is_some(),
            };

            log_completion(
                spawn.model,
                format,
                prompt.len(),
                &mcp_names,
                request.grants.references.is_some(),
            );

            let AgentOutput {
                result,
                transcript,
                session_id,
            } = spawn_agent(&prompt, &spawn, None).await?;
            log_attempt(1, &result, transcript.as_ref());
            let resume;
            match take_answer(format, result, transcript, false) {
                Outcome::Done(answer) => return Ok(answer),
                Outcome::Repair { result, reason } => {
                    tracing::debug!(
                        attempt = 1,
                        %reason,
                        resumes = session_id.is_some(),
                        "repairing cursor-agent answer"
                    );
                    (prompt, resume) = repair_plan(&prompt, &result, &reason, format, session_id);
                }
            }

            let AgentOutput {
                result, transcript, ..
            } = spawn_agent(&prompt, &spawn, resume.as_deref()).await?;
            log_attempt(2, &result, transcript.as_ref());
            match take_answer(format, result, transcript, true) {
                Outcome::Done(answer) => Ok(answer),
                Outcome::Repair { reason, .. } => {
                    bail!("cursor-agent did not return an answer after 2 attempts: {reason}");
                }
            }
        })
    }
}

/// The second attempt's prompt and the session to resume, if any.
///
/// With a session id from the first spawn, the repair resumes that session and
/// sends only the format-repair instruction (the reason is embedded; the
/// session already carries the failed answer). Without one, it falls back to a
/// cold spawn whose prompt keeps the original as a byte-identical prefix — so
/// provider-side prompt caching stays warm — with the failed answer and the
/// repair instruction appended.
fn repair_plan(
    prompt: &str, answer: &str, reason: &str, format: &Format, session_id: Option<String>,
) -> (String, Option<String>) {
    session_id.map_or_else(
        || (append_repair(prompt, answer, reason, format), None),
        |id| (format.repair(reason), Some(id)),
    )
}

enum Outcome {
    Done(Answer),
    Repair { result: String, reason: String },
}

fn take_answer(
    format: &Format, result: String, transcript: Option<Transcript>, last: bool,
) -> Outcome {
    match format.parse(&result) {
        Ok(value) => match format.check(&value) {
            Err(reason) if !last => Outcome::Repair { result, reason },
            // Wrong shape is better than no answer on the last attempt.
            _ => Outcome::Done(Answer {
                value,
                usage: None,
                transcript,
            }),
        },
        Err(reason) => Outcome::Repair { result, reason },
    }
}

fn log_attempt(attempt: u32, result: &str, transcript: Option<&Transcript>) {
    let (interesting_tools, noisy_tools) = tool_counts(transcript);
    tracing::debug!(
        attempt,
        result_len = result.len(),
        interesting_tools,
        noisy_tools,
        "cursor-agent answer"
    );
}

fn tool_counts(transcript: Option<&Transcript>) -> (usize, usize) {
    let Some(transcript) = transcript else {
        return (0, 0);
    };
    let mut interesting = 0;
    let mut noisy = 0;
    for turn in &transcript.turns {
        if is_noisy_tool(&turn.tool) {
            noisy += 1;
        } else {
            interesting += 1;
        }
    }
    (interesting, noisy)
}

/// Spilled prompt file: CLI arg points at a path that lives as long as this value.
struct Prompt {
    arg: String,
    path: PathBuf,
    _guard: PromptFile,
}

// Removes a spill-to-disk prompt file when the spawn finishes.
struct PromptFile {
    path: PathBuf,
}

impl Drop for PromptFile {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path) {
            tracing::warn!(path = %self.path.display(), %error, "failed to remove prompt file");
        }
    }
}

static PROMPT_COUNTER: AtomicU64 = AtomicU64::new(0);

impl Prompt {
    fn spill(prompt: &str, workspace: &Path) -> Result<Self> {
        let cursor_dir = workspace.join(".cursor");
        fs::create_dir_all(&cursor_dir)
            .with_context(|| format!("creating {}", cursor_dir.display()))?;

        // The name carries the pid: concurrent host processes may lend the same workspace.
        let id = PROMPT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = cursor_dir.join(format!("omnia-prompt-{}-{id}.txt", std::process::id()));
        fs::write(&path, prompt)
            .with_context(|| format!("writing prompt file {}", path.display()))?;

        let arg = format!(
            "Follow every instruction in the file at `{}`. When you are done, reply exactly as \
             that file instructs.",
            path.display()
        );

        Ok(Self {
            arg,
            path: path.clone(),
            _guard: PromptFile { path },
        })
    }
}

// A natural-language hint naming the granted MCP servers and any tool allowlist,
// prepended so the spawned agent prefers them over assumptions.
fn mcp_hint(servers: &[&Mcp]) -> String {
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
         assumptions:\n{}",
        lines.join("\n")
    )
}

/// The `cursor-agent` invocation for one spawn; `resume` re-enters the named
/// session instead of starting a fresh one.
fn agent_command(options: &SpawnOptions<'_>, resume: Option<&str>, prompt_arg: &str) -> Command {
    let mut cmd = Command::new(CURSOR_BIN);
    cmd.kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .args(["--print", "--force", "--trust", "--output-format", "stream-json", "--workspace"])
        .arg(options.workspace);
    if options.approve_mcps {
        cmd.arg("--approve-mcps");
    }
    if let Some(model) = options.model {
        cmd.arg("--model").arg(model);
    }
    if let Some(session_id) = resume {
        // `--resume` takes an optional value; the attached form keeps the
        // session id from being read as the prompt.
        cmd.arg(format!("--resume={session_id}"));
    }
    cmd.arg(prompt_arg);
    cmd
}

#[instrument(skip(prompt, options, resume), fields(model = options.model))]
async fn spawn_agent(
    prompt: &str, options: &SpawnOptions<'_>, resume: Option<&str>,
) -> Result<AgentOutput> {
    let spilled = Prompt::spill(prompt, options.workspace)?;
    tracing::debug!(
        prompt_path = %spilled.path.display(),
        prompt_len = prompt.len(),
        resume,
        preview = %truncate(prompt, PROMPT_PREVIEW_CHARS),
        "cursor-agent prompt"
    );

    let mut child = agent_command(options, resume, &spilled.arg)
        .spawn()
        .with_context(|| format!("spawning `{CURSOR_BIN}`"))?;
    let stdout = child.stdout.take().context("child stdout is piped")?;
    let stderr = child.stderr.take().context("child stderr is piped")?;

    // Parse stdout as it streams so memory stays bounded on chatty runs, and
    // drain stderr concurrently so the child can never block on a full pipe.
    let activity = Activity::now();
    let drive = async {
        let (parsed, stderr) = tokio::join!(parse_stream(stdout, &activity), drain(stderr));
        let status = child.wait().await.with_context(|| format!("waiting on `{CURSOR_BIN}`"))?;
        anyhow::Ok((parsed, stderr, status))
    };

    // On timeout `drive` is dropped, and `kill_on_drop` reaps the orphaned agent.
    let deadlines = Deadlines {
        inactivity: options.inactivity,
        cap: options.timeout,
    };
    let (parsed, stderr, status) = tokio::select! {
        driven = drive => driven?,
        error = watchdog(&activity, &deadlines) => return Err(error),
    };

    if !status.success() {
        bail!("cursor-agent exited with {status}: {}", String::from_utf8_lossy(&stderr).trim());
    }

    parsed
}

/// Last-seen stream progress; every stdout line from the agent counts.
struct Activity(std::sync::Mutex<Instant>);

impl Activity {
    fn now() -> Self {
        Self(std::sync::Mutex::new(Instant::now()))
    }

    fn touch(&self) {
        *self.0.lock().expect("activity lock is never poisoned") = Instant::now();
    }

    fn last(&self) -> Instant {
        *self.0.lock().expect("activity lock is never poisoned")
    }
}

/// The two spawn bounds: a short inactivity window over stream events and a
/// generous absolute wall-clock cap.
struct Deadlines {
    inactivity: Duration,
    cap: Duration,
}

/// Resolves when a spawn breaches either bound; the error names which one, so
/// "stalled agent" and "agent that outlived the cap" stay distinguishable.
async fn watchdog(activity: &Activity, deadlines: &Deadlines) -> anyhow::Error {
    let start = Instant::now();
    loop {
        let now = Instant::now();
        let idle = now.saturating_duration_since(activity.last());
        if idle >= deadlines.inactivity {
            return anyhow!(
                "cursor-agent inactive for {}s (no stream events; inactivity limit {}s, absolute \
                 cap {}s)",
                idle.as_secs(),
                deadlines.inactivity.as_secs(),
                deadlines.cap.as_secs()
            );
        }
        let elapsed = now.saturating_duration_since(start);
        if elapsed >= deadlines.cap {
            return anyhow!(
                "cursor-agent timed out after {}s (absolute cap exceeded while still active)",
                deadlines.cap.as_secs()
            );
        }
        let next_check =
            deadlines.inactivity.saturating_sub(idle).min(deadlines.cap.saturating_sub(elapsed));
        tokio::time::sleep(next_check).await;
    }
}

async fn parse_stream(stdout: impl AsyncRead + Unpin, activity: &Activity) -> Result<AgentOutput> {
    let mut lines = BufReader::new(stdout).lines();
    let mut parser = OutputParser::default();
    while let Some(line) = lines.next_line().await? {
        activity.touch();
        parser.line(&line)?;
    }
    parser.finish()
}

async fn drain(mut stream: impl AsyncRead + Unpin) -> Vec<u8> {
    let mut buffer = Vec::new();
    let _ = stream.read_to_end(&mut buffer).await;
    buffer
}

fn append_repair(prompt: &str, answer: &str, reason: &str, format: &Format) -> String {
    format!("{prompt}\n\nYour previous answer was:\n{answer}\n\n{}", format.repair(reason))
}

/// One-line INFO for the completion start.
fn log_completion(
    model: Option<&str>, format: &Format, prompt_len: usize, mcp_servers: &[&str],
    has_references: bool,
) {
    match format {
        Format::Text => {
            tracing::info!(
                model,
                format = "text",
                prompt_len,
                ?mcp_servers,
                has_references,
                "cursor completion"
            );
        }
        Format::Json => {
            tracing::info!(
                model,
                format = "json",
                prompt_len,
                ?mcp_servers,
                has_references,
                "cursor completion"
            );
        }
        Format::Schema(spec) => {
            tracing::info!(
                model,
                format = "schema",
                schema_name = %spec.name,
                prompt_len,
                ?mcp_servers,
                has_references,
                "cursor completion"
            );
            tracing::trace!(
                schema_name = %spec.name,
                schema = %truncate(&spec.schema, PROMPT_PREVIEW_CHARS),
                "cursor completion schema"
            );
        }
    }
}

/// Compact JSON when parseable; otherwise collapse whitespace so a log field stays one line.
fn single_line(text: &str) -> String {
    serde_json::from_str::<Value>(text.trim()).map_or_else(
        |_| text.split_whitespace().collect::<Vec<_>>().join(" "),
        |value| value.to_string(),
    )
}

fn truncate(text: &str, max: usize) -> String {
    let collapsed = single_line(text);
    let mut chars = collapsed.chars();
    let head: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() { format!("{head}…") } else { head }
}

fn is_noisy_tool(name: &str) -> bool {
    matches!(
        name,
        "read"
            | "write"
            | "shell"
            | "grep"
            | "glob"
            | "edit"
            | "delete"
            | "listDir"
            | "searchReplace"
            | "ls"
            | "SemSearch"
            | "ReadLints"
            | "AwaitShell"
            | "TodoWrite"
    )
}

fn args_summary(args: &Value) -> String {
    if let Some(path) = args.get("path").and_then(Value::as_str) {
        return truncate(path, TEXT_PREVIEW_CHARS);
    }
    if let Some(url) = args.get("url").and_then(Value::as_str) {
        return truncate(url, TEXT_PREVIEW_CHARS);
    }
    if let Some(query) = args.get("query").and_then(Value::as_str) {
        return truncate(query, TEXT_PREVIEW_CHARS);
    }
    truncate(&args.to_string(), TEXT_PREVIEW_CHARS)
}

/// The subset of `cursor-agent` stream events the backend consumes. `result`
/// and `tool_call` drive the answer; `system` (the `init` event) and `result`
/// carry the `session_id` used to resume the session on a repair attempt;
/// `assistant` and `thinking` are parsed for DEBUG visibility. Thinking
/// `delta` chunks are coalesced into one log line per turn (`completed`, a
/// size backstop, or the next non-thinking event). Everything else parses to
/// `Other` without building a JSON tree.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Event {
    System {
        session_id: Option<String>,
    },
    Result {
        is_error: Option<bool>,
        result: Option<String>,
        session_id: Option<String>,
    },
    ToolCall {
        subtype: String,
        call_id: Option<String>,
        tool_call: Option<Value>,
    },
    Assistant {
        message: Option<AssistantMessage>,
    },
    Thinking {
        subtype: Option<String>,
        text: Option<String>,
    },
    #[serde(other)]
    Other,
}

/// The `message` body of a stream-json `assistant` event.
#[derive(Deserialize)]
struct AssistantMessage {
    #[serde(default)]
    content: Vec<ContentPart>,
}

/// One `message.content[]` entry; only `text` parts carry prose.
#[derive(Deserialize)]
struct ContentPart {
    #[serde(default)]
    text: Option<String>,
}

impl AssistantMessage {
    fn text(&self) -> String {
        self.content.iter().filter_map(|part| part.text.as_deref()).collect()
    }
}

/// Coalesces stream-json thinking deltas into turn-sized blocks for DEBUG logs.
#[derive(Default)]
struct ThinkingBuf(String);

impl ThinkingBuf {
    /// Apply one thinking event; return text ready to log, if any.
    fn event(&mut self, subtype: Option<&str>, text: &str) -> Option<String> {
        match subtype {
            Some("completed") => self.take(),
            Some("delta") | None => {
                if text.is_empty() {
                    return None;
                }
                self.0.push_str(text);
                if self.0.chars().count() >= THINKING_PREVIEW_CHARS {
                    return self.take();
                }
                None
            }
            // Full-payload subtypes (e.g. `extended`): one shot.
            _ => {
                if !text.is_empty() {
                    self.0.push_str(text);
                }
                self.take()
            }
        }
    }

    fn take(&mut self) -> Option<String> {
        if self.0.is_empty() { None } else { Some(std::mem::take(&mut self.0)) }
    }
}

fn log_thinking(text: &str) {
    tracing::debug!("thinking: {}", truncate(text, THINKING_PREVIEW_CHARS));
}

#[derive(Default)]
struct OutputParser {
    result: Option<String>,
    session_id: Option<String>,
    pending_tools: HashMap<String, (String, Value)>,
    turns: Vec<ToolTurn>,
    thinking: ThinkingBuf,
}

impl OutputParser {
    fn line(&mut self, line: &str) -> Result<()> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(());
        }

        // One garbled line must not cost an otherwise-successful answer.
        let event = match serde_json::from_str::<Event>(line) {
            Ok(event) => event,
            Err(error) => {
                tracing::debug!(
                    %error,
                    line = %truncate(line, TEXT_PREVIEW_CHARS),
                    "skipping unparsable cursor-agent event"
                );
                return Ok(());
            }
        };

        match event {
            Event::System { session_id } => {
                self.session(session_id);
            }
            Event::Result {
                is_error,
                result,
                session_id,
            } => {
                self.flush_thinking();
                self.session(session_id);
                if is_error == Some(true) {
                    bail!(
                        "cursor-agent reported an error: {}",
                        result.as_deref().unwrap_or("<no detail>")
                    );
                }
                if result.is_some() {
                    self.result = result;
                }
            }
            Event::ToolCall {
                subtype,
                call_id,
                tool_call,
            } => {
                self.flush_thinking();
                self.tool_call(&subtype, call_id, tool_call);
            }
            Event::Assistant { message } => {
                self.flush_thinking();
                let text = message.as_ref().map(AssistantMessage::text).unwrap_or_default();
                if !text.is_empty() {
                    tracing::debug!(
                        text = %truncate(&text, TEXT_PREVIEW_CHARS),
                        "cursor-agent assistant text"
                    );
                }
            }
            Event::Thinking { subtype, text } => {
                if let Some(text) =
                    self.thinking.event(subtype.as_deref(), text.as_deref().unwrap_or_default())
                {
                    log_thinking(&text);
                }
            }
            Event::Other => {
                tracing::trace!(line = %truncate(line, TEXT_PREVIEW_CHARS), "cursor-agent other event");
            }
        }
        Ok(())
    }

    fn flush_thinking(&mut self) {
        if let Some(text) = self.thinking.take() {
            log_thinking(&text);
        }
    }

    /// Keep the first `session_id` seen (the `init` event's; the terminal
    /// `result` event repeats it as a fallback).
    fn session(&mut self, session_id: Option<String>) {
        if self.session_id.is_none() {
            self.session_id = session_id;
        }
    }

    fn tool_call(&mut self, subtype: &str, call_id: Option<String>, tool_call: Option<Value>) {
        match subtype {
            "started" => {
                if let (Some(call_id), Some((tool, args))) =
                    (call_id, tool_call.as_ref().and_then(tool_call_identity))
                {
                    if is_noisy_tool(&tool) {
                        tracing::trace!(subtype, %call_id, %tool, "cursor-agent tool call");
                    }
                    self.pending_tools.insert(call_id, (tool, args));
                }
            }
            "completed" => {
                if let (Some(call_id), Some(tool_call)) = (call_id, tool_call) {
                    let (tool, args) = self.pending_tools.remove(&call_id).unwrap_or_else(|| {
                        tool_call_identity(&tool_call)
                            .unwrap_or_else(|| ("unknown".to_owned(), Value::Null))
                    });

                    if is_noisy_tool(&tool) {
                        tracing::trace!(subtype, %call_id, %tool, "cursor-agent tool call");
                    } else {
                        tracing::debug!(
                            %tool,
                            args = %args_summary(&args),
                            "cursor-agent tool"
                        );
                    }

                    let result = tool_call
                        .as_object()
                        .and_then(|map| map.values().find_map(|v| v.get("result").cloned()))
                        .unwrap_or_default();

                    self.turns.push(ToolTurn { tool, args, result });
                }
            }
            _ => {}
        }
    }

    fn finish(mut self) -> Result<AgentOutput> {
        self.flush_thinking();
        let Some(result) = self.result else {
            bail!("cursor-agent did not emit a terminal result event");
        };
        let transcript =
            if self.turns.is_empty() { None } else { Some(Transcript { turns: self.turns }) };
        Ok(AgentOutput {
            result,
            transcript,
            session_id: self.session_id,
        })
    }
}

// Extract the tool name and arguments from a `*ToolCall` envelope.
fn tool_call_identity(tool_call: &Value) -> Option<(String, Value)> {
    let object = tool_call.as_object()?;
    for (key, value) in object {
        if let Some(tool) = key.strip_suffix("ToolCall") {
            let args = value.get("args").cloned().unwrap_or_else(|| value.clone());
            return Some((tool.to_owned(), args));
        }
    }
    None
}

// Deliberate unit tests: pure stream-parse and prompt-build logic (CI floor).
// The edge variants (thinking deltas, session-id fallback, garbled lines)
// cannot be induced deterministically from a real agent; `tests/live.rs` is
// the acceptance gate proving a real cursor-agent stream parses end-to-end.
#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use omnia_wasi_model::{
        DirEntry, Format, FutureResult, Grants, Message, Reference, Request, Role, Schema,
        ToolHost, WasiModelCtx as _,
    };
    use serde_json::json;

    use super::{
        Activity, AgentOutput, Deadlines, OutputParser, Prompt, SpawnOptions, ThinkingBuf,
        agent_command, repair_plan, single_line, truncate, watchdog,
    };
    use crate::Client;

    #[test]
    fn single_line_compacts_json() {
        let pretty = "{\n  \"verdict\": \"pass\"\n}";
        assert_eq!(single_line(pretty), r#"{"verdict":"pass"}"#);
    }

    #[test]
    fn single_line_collapses_non_json() {
        assert_eq!(single_line("hello\n  world"), "hello world");
    }

    #[test]
    fn truncate_appends_ellipsis() {
        assert_eq!(truncate("abcdef", 3), "abc…");
        assert_eq!(truncate("ab", 3), "ab");
    }

    #[test]
    fn thinking_buf_coalesces_deltas() {
        let mut buf = ThinkingBuf::default();
        assert!(buf.event(Some("delta"), "line 22, the canc").is_none());
        assert!(buf.event(Some("delta"), "ellation constraint").is_none());
        assert_eq!(
            buf.event(Some("completed"), "").as_deref(),
            Some("line 22, the cancellation constraint")
        );
        assert!(buf.take().is_none(), "completed clears the buffer");
    }

    #[test]
    fn thinking_buf_extended_is_one_shot() {
        let mut buf = ThinkingBuf::default();
        assert_eq!(
            buf.event(Some("extended"), "weighing the verdict").as_deref(),
            Some("weighing the verdict")
        );
    }

    #[test]
    fn thinking_buf_flushes_before_lost_tail() {
        let mut buf = ThinkingBuf::default();
        assert!(buf.event(Some("delta"), "partial thought").is_none());
        assert_eq!(buf.take().as_deref(), Some("partial thought"));
    }

    fn parse_output(stdout: &[u8]) -> anyhow::Result<AgentOutput> {
        let text = std::str::from_utf8(stdout).expect("test payloads are UTF-8");
        let mut parser = OutputParser::default();
        for line in text.lines() {
            parser.line(line)?;
        }
        parser.finish()
    }

    fn schema_request() -> Request {
        Request {
            model: None,
            system: Some("a terse judge".to_owned()),
            messages: vec![Message {
                role: Role::User,
                content: "decide pass or fail".to_owned(),
            }],
            generation: None,
            format: Format::Schema(Schema {
                name: "verdict".to_owned(),
                schema: json!({
                    "type": "object",
                    "properties": { "verdict": { "type": "string" } },
                    "required": ["verdict"],
                })
                .to_string(),
            }),
            tools: vec![],
            grants: Grants {
                references: None,
                workspace: None,
            },
        }
    }

    #[tokio::test]
    async fn no_local_tree() {
        let err = client()
            .complete(schema_request(), Arc::new(StubToolHost { path: None }))
            .await
            .expect_err("a backend with no local tree must fail");
        assert!(err.to_string().contains("no local tree on this node"), "unexpected error: {err}");
    }

    #[test]
    fn parse_result_error() {
        let stdout = br#"{"type":"result","is_error":true,"result":"boom"}"#;
        let err = parse_output(stdout).expect_err("an agent error must surface");
        assert!(err.to_string().contains("cursor-agent reported an error"), "unexpected: {err}");
    }

    #[test]
    fn parse_stream_json() {
        let stdout = br#"{"type":"thinking","subtype":"extended","text":"weighing the verdict"}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"I'll read the README"}]}}
{"type":"tool_call","subtype":"started","call_id":"c1","tool_call":{"readToolCall":{"args":{"path":"README.md"}}}}
{"type":"tool_call","subtype":"completed","call_id":"c1","tool_call":{"readToolCall":{"args":{"path":"README.md"},"result":{"success":{"content":"hi"}}}}}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Deciding now"}]}}
{"type":"result","subtype":"success","is_error":false,"result":"{\"verdict\":\"pass\"}"}"#;
        let AgentOutput {
            result, transcript, ..
        } = parse_output(stdout).expect("parse stream");
        assert_eq!(result, r#"{"verdict":"pass"}"#);
        let transcript = transcript.expect("tool transcript");
        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(transcript.turns[0].tool, "read");
        assert_eq!(transcript.turns[0].args, json!({ "path": "README.md" }));
    }

    #[test]
    fn parse_session_id_from_init() {
        let stdout =
            br#"{"type":"system","subtype":"init","cwd":"/ws","session_id":"s-init","model":"m"}
{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s-later"}"#;
        let output = parse_output(stdout).expect("parse stream");
        assert_eq!(output.session_id.as_deref(), Some("s-init"), "the init event's id wins");
    }

    #[test]
    fn parse_session_id_from_result_fallback() {
        let stdout =
            br#"{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s-result"}"#;
        let output = parse_output(stdout).expect("parse stream");
        assert_eq!(output.session_id.as_deref(), Some("s-result"));
    }

    #[test]
    fn parse_without_session_id() {
        let stdout = br#"{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#;
        let output = parse_output(stdout).expect("parse stream");
        assert!(output.session_id.is_none());
    }

    #[test]
    fn parse_thinking_deltas_then_result() {
        let stdout = br#"{"type":"thinking","subtype":"delta","text":"line 22, the canc"}
{"type":"thinking","subtype":"delta","text":"ellation constraint"}
{"type":"thinking","subtype":"completed","text":""}
{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#;
        let AgentOutput {
            result, transcript, ..
        } = parse_output(stdout).expect("parse deltas");
        assert_eq!(result, "ok");
        assert!(transcript.is_none());
    }

    #[test]
    fn assistant_prefix_reaches_result() {
        let stdout = br#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working on it"}]}}
{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#;
        let AgentOutput {
            result, transcript, ..
        } = parse_output(stdout).expect("parse stream");
        assert_eq!(result, "ok");
        assert!(transcript.is_none(), "no tool turns means no transcript");
    }

    #[test]
    fn skip_garbled_line() {
        let stdout =
            b"warning: not an event\n{\"type\":\"result\",\"is_error\":false,\"result\":\"ok\"}";
        let AgentOutput { result, .. } = parse_output(stdout).expect("garbled line is skipped");
        assert_eq!(result, "ok");
    }

    #[test]
    fn spills_prompt() {
        let workspace =
            std::env::temp_dir().join(format!("omnia-cursor-prompt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&workspace).expect("temp workspace");

        let prompt = Prompt::spill("hello", workspace.as_path()).expect("spill prompt");
        assert!(prompt.arg.contains("omnia-prompt-"), "arg references prompt file: {}", prompt.arg);
        assert!(prompt.path.exists(), "the prompt file is on disk while the guard lives");
        let path = prompt.path.clone();
        drop(prompt);
        assert!(!path.exists(), "the prompt file is removed on drop");
        let _ = fs::remove_dir_all(&workspace);
    }

    /// Shared stub: unit tests use `path: None`; the shape matches live support.
    #[derive(Debug)]
    pub struct StubToolHost {
        pub path: Option<PathBuf>,
    }

    impl ToolHost for StubToolHost {
        fn resolve(&self, _reference: Reference) -> FutureResult<Vec<u8>> {
            Box::pin(async { Err(anyhow::anyhow!("cursor ignores the tool host")) })
        }

        fn read(&self, _path: String) -> FutureResult<Vec<u8>> {
            Box::pin(async { Err(anyhow::anyhow!("cursor ignores the tool host")) })
        }

        fn list(&self, _path: String) -> FutureResult<Vec<DirEntry>> {
            Box::pin(async { Err(anyhow::anyhow!("cursor ignores the tool host")) })
        }

        fn write(&self, _path: String, _bytes: Vec<u8>) -> FutureResult<()> {
            Box::pin(async { Err(anyhow::anyhow!("cursor ignores the tool host")) })
        }

        fn local_path(&self) -> Option<&std::path::Path> {
            self.path.as_deref()
        }
    }

    /// Build a [`Client`] directly, bypassing `connect_with` (and its `PATH` check).
    pub fn client() -> Client {
        Client {
            timeout: Duration::from_secs(1),
            inactivity: Duration::from_secs(1),
            model: None,
        }
    }

    mod repair {
        use super::*;

        #[test]
        fn resumes_with_findings_only() {
            let (prompt, resume) = repair_plan(
                "the original prompt",
                "not json",
                "answer is not valid JSON",
                &Format::Json,
                Some("s-1".to_owned()),
            );
            assert_eq!(resume.as_deref(), Some("s-1"));
            assert!(
                !prompt.contains("the original prompt"),
                "a resumed repair must not re-send the original prompt: {prompt}"
            );
            assert!(
                !prompt.contains("not json"),
                "the session already carries the failed answer: {prompt}"
            );
            assert!(prompt.contains("answer is not valid JSON"), "findings ride along: {prompt}");
        }

        #[test]
        fn cold_fallback_keeps_prompt_prefix() {
            let (prompt, resume) = repair_plan(
                "the original prompt",
                "not json",
                "answer is not valid JSON",
                &Format::Json,
                None,
            );
            assert!(resume.is_none());
            assert!(
                prompt.starts_with("the original prompt"),
                "the fallback keeps a byte-identical prompt prefix for provider caching: {prompt}"
            );
            assert!(prompt.contains("not json"), "the failed answer is appended: {prompt}");
            assert!(prompt.contains("answer is not valid JSON"), "findings ride along: {prompt}");
        }
    }

    mod spawn_args {
        use super::*;

        fn options(workspace: &std::path::Path) -> SpawnOptions<'_> {
            SpawnOptions {
                model: None,
                workspace,
                timeout: Duration::from_mins(10),
                inactivity: Duration::from_mins(2),
                approve_mcps: false,
            }
        }

        fn args(cmd: &tokio::process::Command) -> Vec<String> {
            cmd.as_std().get_args().map(|a| a.to_string_lossy().into_owned()).collect()
        }

        #[test]
        fn resume_uses_attached_form() {
            let workspace = std::env::temp_dir();
            let cmd = agent_command(&options(&workspace), Some("s-1"), "the prompt");
            let args = args(&cmd);
            assert!(args.contains(&"--resume=s-1".to_owned()), "args: {args:?}");
            assert_eq!(args.last().map(String::as_str), Some("the prompt"));
        }

        #[test]
        fn fresh_spawn_has_no_resume() {
            let workspace = std::env::temp_dir();
            let cmd = agent_command(&options(&workspace), None, "the prompt");
            let args = args(&cmd);
            assert!(!args.iter().any(|a| a.starts_with("--resume")), "args: {args:?}");
        }
    }

    mod timeouts {
        use tokio::time::{Duration, sleep};

        use super::{Activity, Deadlines, watchdog};

        const DEADLINES: Deadlines = Deadlines {
            inactivity: Duration::from_mins(2),
            cap: Duration::from_mins(10),
        };

        #[tokio::test(start_paused = true)]
        async fn silent_stream_dies_at_inactivity_window() {
            let activity = Activity::now();
            let started = tokio::time::Instant::now();
            let error = watchdog(&activity, &DEADLINES).await;
            assert_eq!(started.elapsed(), Duration::from_mins(2));
            assert!(
                error.to_string().contains("inactive for 120s"),
                "the inactivity kill names the idle span: {error}"
            );
        }

        #[tokio::test(start_paused = true)]
        async fn steady_activity_survives_to_absolute_cap() {
            let activity = Activity::now();
            let started = tokio::time::Instant::now();
            let toucher = async {
                loop {
                    sleep(Duration::from_mins(1)).await;
                    activity.touch();
                }
            };
            let error = tokio::select! {
                error = watchdog(&activity, &DEADLINES) => error,
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
        async fn late_activity_defers_the_inactivity_kill() {
            let activity = Activity::now();
            let started = tokio::time::Instant::now();
            let toucher = async {
                sleep(Duration::from_secs(100)).await;
                activity.touch();
                std::future::pending::<()>().await;
            };
            let error = tokio::select! {
                error = watchdog(&activity, &DEADLINES) => error,
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
}
