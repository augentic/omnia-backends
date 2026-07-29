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
use tracing::instrument;

use crate::{Client, mcp};

const CURSOR_BIN: &str = "cursor-agent";
const PROMPT_PREVIEW_CHARS: usize = 500;
const TEXT_PREVIEW_CHARS: usize = 300;

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
    approve_mcps: bool,
}

#[derive(Debug)]
struct AgentOutput {
    result: String,
    transcript: Option<Transcript>,
}

impl WasiModelCtx for Client {
    fn complete(&self, request: Request, tool_host: Arc<dyn ToolHost>) -> FutureResult<Answer> {
        let workspace = tool_host.local_path().map(Path::to_path_buf);
        let timeout = self.timeout;
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
                approve_mcps: mcp_guard.is_some(),
            };

            log_completion(
                spawn.model,
                format,
                prompt.len(),
                &mcp_names,
                request.grants.references.is_some(),
            );

            let AgentOutput { result, transcript } = spawn_agent(&prompt, &spawn).await?;
            log_attempt(1, &result, transcript.as_ref());
            match take_answer(format, result, transcript, false) {
                Outcome::Done(answer) => return Ok(answer),
                Outcome::Repair { result, reason } => {
                    tracing::debug!(attempt = 1, %reason, "repairing cursor-agent answer");
                    prompt = append_repair(&prompt, &result, &reason, format);
                }
            }

            let AgentOutput { result, transcript } = spawn_agent(&prompt, &spawn).await?;
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

#[instrument(skip(prompt, options), fields(model = options.model))]
async fn spawn_agent(prompt: &str, options: &SpawnOptions<'_>) -> Result<AgentOutput> {
    let spilled = Prompt::spill(prompt, options.workspace)?;
    tracing::debug!(
        prompt_path = %spilled.path.display(),
        prompt_len = prompt.len(),
        preview = %truncate(prompt, PROMPT_PREVIEW_CHARS),
        "cursor-agent prompt"
    );

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
    cmd.arg(&spilled.arg);

    let mut child = cmd.spawn().with_context(|| format!("spawning `{CURSOR_BIN}`"))?;
    let stdout = child.stdout.take().context("child stdout is piped")?;
    let stderr = child.stderr.take().context("child stderr is piped")?;

    // Parse stdout as it streams so memory stays bounded on chatty runs, and
    // drain stderr concurrently so the child can never block on a full pipe.
    let drive = async {
        let (parsed, stderr) = tokio::join!(parse_stream(stdout), drain(stderr));
        let status = child.wait().await.with_context(|| format!("waiting on `{CURSOR_BIN}`"))?;
        anyhow::Ok((parsed, stderr, status))
    };

    // On timeout `drive` is dropped, and `kill_on_drop` reaps the orphaned agent.
    let (parsed, stderr, status) =
        tokio::time::timeout(options.timeout, drive).await.map_err(|_elapsed| {
            anyhow!("cursor-agent timed out after {}s", options.timeout.as_secs())
        })??;

    if !status.success() {
        bail!("cursor-agent exited with {status}: {}", String::from_utf8_lossy(&stderr).trim());
    }

    parsed
}

async fn parse_stream(stdout: impl AsyncRead + Unpin) -> Result<AgentOutput> {
    let mut lines = BufReader::new(stdout).lines();
    let mut parser = OutputParser::default();
    while let Some(line) = lines.next_line().await? {
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
/// and `tool_call` drive the answer; `assistant` and `thinking` are parsed
/// for DEBUG visibility. Everything else parses to `Other` without building a
/// JSON tree.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Event {
    Result {
        is_error: Option<bool>,
        result: Option<String>,
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

#[derive(Default)]
struct OutputParser {
    result: Option<String>,
    pending_tools: HashMap<String, (String, Value)>,
    turns: Vec<ToolTurn>,
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
            Event::Result { is_error, result } => {
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
                self.tool_call(&subtype, call_id, tool_call);
            }
            Event::Assistant { message } => {
                let text = message.as_ref().map(AssistantMessage::text).unwrap_or_default();
                tracing::debug!(
                    text_len = text.len(),
                    text = %truncate(&text, TEXT_PREVIEW_CHARS),
                    "cursor-agent assistant text"
                );
            }
            Event::Thinking { subtype, text } => {
                let text = text.as_deref().unwrap_or_default();
                tracing::debug!(
                    ?subtype,
                    text_len = text.len(),
                    text = %truncate(text, TEXT_PREVIEW_CHARS),
                    "cursor-agent thinking"
                );
            }
            Event::Other => {
                tracing::trace!(line = %truncate(line, TEXT_PREVIEW_CHARS), "cursor-agent other event");
            }
        }
        Ok(())
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

    fn finish(self) -> Result<AgentOutput> {
        let Some(result) = self.result else {
            bail!("cursor-agent did not emit a terminal result event");
        };
        let transcript =
            if self.turns.is_empty() { None } else { Some(Transcript { turns: self.turns }) };
        Ok(AgentOutput { result, transcript })
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use omnia_wasi_model::{
        DirEntry, Format, FutureResult, Grants, Message, Reference, Request, Role, Schema,
        ToolHost, VerifyReport, WasiModelCtx as _,
    };
    use serde_json::json;

    use super::{AgentOutput, OutputParser, Prompt, single_line, truncate};
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
                verify: vec![],
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
        let AgentOutput { result, transcript } = parse_output(stdout).expect("parse stream");
        assert_eq!(result, r#"{"verdict":"pass"}"#);
        let transcript = transcript.expect("tool transcript");
        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(transcript.turns[0].tool, "read");
        assert_eq!(transcript.turns[0].args, json!({ "path": "README.md" }));
    }

    #[test]
    fn assistant_prefix_reaches_result() {
        let stdout = br#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working on it"}]}}
{"type":"result","subtype":"success","is_error":false,"result":"ok"}"#;
        let AgentOutput { result, transcript } = parse_output(stdout).expect("parse stream");
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

        fn verify(&self, _check: String) -> FutureResult<VerifyReport> {
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
            model: None,
        }
    }
}
