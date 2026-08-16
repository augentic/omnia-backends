use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use omnia_wasi_model::{
    Answer, Format, FutureResult, Mcp, Request, ToolHost, Transcript, WasiModelCtx,
};

use crate::{Client, mcp};

mod agent;
mod stream;

use agent::SpawnOptions;
use stream::{OutputParser, truncate};
#[cfg(test)]
use stream::{ThinkingBuf, single_line};

const PROMPT_PREVIEW_CHARS: usize = 500;

pub async fn check_cursor() -> Result<()> {
    agent::check_cursor().await
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
        let deadlines = self.deadlines;
        let default_model = self.model.clone();

        Box::pin(async move {
            let format = &request.format;
            let Some(workspace) = workspace else {
                bail!("no local tree on this node");
            };
            let workspace = prepare_workspace(&workspace)?;

            let mcp_servers = request.mcp_servers();
            let mcp_names: Vec<&str> = mcp_servers.iter().map(|s| s.name.as_str()).collect();
            let (prompt, mcp_guard) =
                attach_mcp(&workspace, &mcp_servers, request.to_string()).await?;

            // Guest-supplied request.model wins; else CURSOR_MODEL; else
            // cursor-agent chooses.
            let spawn = SpawnOptions {
                model: request.model.as_deref().or(default_model.as_deref()),
                workspace: &workspace,
                deadlines,
                approve_mcps: mcp_guard.is_some(),
            };

            log_completion(
                spawn.model,
                format,
                prompt.len(),
                &mcp_names,
                request.grants.references.is_some(),
            );

            let output = spawn.run_agent(&prompt, None).await?;
            output.log(1);
            let AgentOutput {
                result,
                transcript,
                session_id,
            } = output;
            match take_answer(format, result, transcript) {
                Outcome::Done(answer) => Ok(answer),
                Outcome::Repair { result, reason } => {
                    tracing::debug!(
                        attempt = 1,
                        %reason,
                        resumes = session_id.is_some(),
                        "repairing cursor-agent answer"
                    );

                    let (prompt, resume) = session_id.map_or_else(
                        || (append_repair(&prompt, &result, &reason, format), None),
                        |id| (format.repair(&reason), Some(id)),
                    );

                    let output = spawn.run_agent(&prompt, resume.as_deref()).await?;
                    output.log(2);
                    let AgentOutput {
                        result, transcript, ..
                    } = output;
                    match take_answer(format, result, transcript) {
                        Outcome::Done(answer) => Ok(answer),
                        Outcome::Repair { reason, .. } => {
                            bail!("no answer after 2 repair attempts: {reason}");
                        }
                    }
                }
            }
        })
    }
}

enum Outcome {
    Done(Answer),
    Repair { result: String, reason: String },
}

fn take_answer(format: &Format, result: String, transcript: Option<Transcript>) -> Outcome {
    match format.parse(&result) {
        Ok(value) => match format.check(&value) {
            Ok(()) => Outcome::Done(Answer {
                value,
                usage: None,
                transcript,
            }),
            Err(reason) => Outcome::Repair { result, reason },
        },
        Err(reason) => Outcome::Repair { result, reason },
    }
}

fn prepare_workspace(path: &Path) -> Result<PathBuf> {
    fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
    path.canonicalize().with_context(|| format!("canonicalizing {}", path.display()))
}

async fn attach_mcp(
    workspace: &Path, servers: &[&Mcp], prompt: String,
) -> Result<(String, Option<mcp::McpGuard>)> {
    if servers.is_empty() {
        return Ok((prompt, None));
    }
    let prompt = format!("{}\n\n{prompt}", mcp_hint(servers));
    let guard = mcp::McpGuard::install(
        workspace,
        servers.iter().map(|server| (server.name.as_str(), server.url.as_str())),
    )
    .await?;
    Ok((prompt, Some(guard)))
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

fn append_repair(prompt: &str, answer: &str, reason: &str, format: &Format) -> String {
    format!("{prompt}\n\nYour previous answer was:\n{answer}\n\n{}", format.repair(reason))
}

/// One-line INFO for the completion start.
fn log_completion(
    model: Option<&str>, format: &Format, prompt_len: usize, mcp_servers: &[&str],
    has_references: bool,
) {
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
    tracing::info!(
        model,
        format = format_name,
        prompt_len,
        ?mcp_servers,
        has_references,
        "cursor completion"
    );
}

// Deliberate unit tests: pure stream-parse and prompt-build logic (CI floor).
// The edge variants (thinking deltas, session-id fallback, garbled lines)
// cannot be induced deterministically from a real agent; `tests/live.rs` is
// the acceptance gate proving a real cursor-agent stream parses end-to-end.
#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use omnia_wasi_model::{
        DirEntry, Format, FutureResult, Grants, Message, Reference, Request, Role, Schema,
        ToolHost, WasiModelCtx as _,
    };
    use serde_json::json;
    use tempfile::tempdir;

    use super::agent::Prompt;
    use super::{
        AgentOutput, OutputParser, SpawnOptions, ThinkingBuf, append_repair, single_line, truncate,
    };
    use crate::{Client, Deadlines};

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
    fn nullable_event_fields_remain_compatible() {
        let stdout = br#"{"type":"assistant","message":null}
{"type":"thinking","subtype":"delta","text":null}
{"type":"result","is_error":null,"result":"ok"}"#;
        let output = parse_output(stdout).expect("nullable protocol fields are accepted");
        assert_eq!(output.result, "ok");
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
        let workspace = tempdir().expect("temp workspace");

        let prompt = Prompt::spill("hello", workspace.path()).expect("spill prompt");
        assert!(prompt.arg.contains("omnia-prompt-"), "arg references prompt file: {}", prompt.arg);
        assert!(prompt.file.path().exists(), "the prompt file is on disk while the guard lives");
        let path = prompt.file.path().to_path_buf();
        drop(prompt);
        assert!(!path.exists(), "the prompt file is removed on drop");
    }

    /// Shared stub: unit tests use `path: None`; the shape matches live support.
    #[derive(Debug)]
    struct StubToolHost {
        path: Option<PathBuf>,
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
    fn client() -> Client {
        Client {
            deadlines: Deadlines {
                inactivity: Duration::from_secs(1),
                cap: Duration::from_secs(1),
            },
            model: None,
        }
    }

    mod repair {
        use super::*;

        #[test]
        fn resumes_with_findings_only() {
            let format = Format::Json;
            let (prompt, resume) = Some("s-1".to_owned()).map_or_else(
                || {
                    (
                        append_repair(
                            "the original prompt",
                            "not json",
                            "answer is not valid JSON",
                            &format,
                        ),
                        None,
                    )
                },
                |id| (format.repair("answer is not valid JSON"), Some(id)),
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
            let format = Format::Json;
            let (prompt, resume) = None::<String>.map_or_else(
                || {
                    (
                        append_repair(
                            "the original prompt",
                            "not json",
                            "answer is not valid JSON",
                            &format,
                        ),
                        None,
                    )
                },
                |id| (format.repair("answer is not valid JSON"), Some(id)),
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
                deadlines: Deadlines {
                    inactivity: Duration::from_mins(2),
                    cap: Duration::from_mins(10),
                },
                approve_mcps: false,
            }
        }

        fn args(cmd: &tokio::process::Command) -> Vec<String> {
            cmd.as_std().get_args().map(|a| a.to_string_lossy().into_owned()).collect()
        }

        fn explicit_env(cmd: &tokio::process::Command, name: &str) -> Option<String> {
            cmd.as_std()
                .get_envs()
                .find(|(key, _)| *key == name)
                .and_then(|(_, value)| value)
                .map(|value| value.to_string_lossy().into_owned())
        }

        #[test]
        fn resume_id_uses_attached_flag() {
            let workspace = std::env::temp_dir();
            let cmd = options(&workspace).command("the prompt", Some("s-1"));
            let args = args(&cmd);
            assert!(args.contains(&"--resume=s-1".to_owned()), "args: {args:?}");
            assert_eq!(args.last().map(String::as_str), Some("the prompt"));
        }

        #[test]
        fn fresh_spawn_has_no_resume_flag() {
            let workspace = std::env::temp_dir();
            let cmd = options(&workspace).command("the prompt", None);
            let args = args(&cmd);
            assert!(!args.iter().any(|a| a.starts_with("--resume")), "args: {args:?}");
        }

        #[test]
        fn credential_store_follows_api_key_environment() {
            let workspace = std::env::temp_dir();
            let cmd = options(&workspace).command("the prompt", None);
            let expected = std::env::var_os("CURSOR_API_KEY").map(|_| "memory".to_owned());
            assert_eq!(explicit_env(&cmd, "AGENT_CLI_CREDENTIAL_STORE"), expected);
        }

        #[test]
        fn host_git_identity_is_removed() {
            let workspace = std::env::temp_dir();
            let cmd = options(&workspace).command("the prompt", None);
            let removed: Vec<_> = cmd
                .as_std()
                .get_envs()
                .filter(|(_, value)| value.is_none())
                .map(|(key, _)| key.to_string_lossy().into_owned())
                .collect();
            for var in crate::mcp::GIT_IDENTITY {
                assert!(
                    removed.iter().any(|key| key == var),
                    "{var} must be cleared so the agent cannot inherit the host checkout: {removed:?}"
                );
            }
        }
    }

    mod timeouts {
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
