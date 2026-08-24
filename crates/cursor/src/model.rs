mod agent;
mod stream;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent::SpawnOptions;
pub use agent::check_cursor;
use anyhow::{Context, Result, bail};
use omnia_wasi_model::{
    Answer, Format, FutureResult, Mcp, Request, Tool, ToolHost, Transcript, WasiModelCtx,
};
use stream::{PROMPT_PREVIEW_CHARS, truncate};

use crate::{Client, mcp};

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
            // Interim honesty until the model session lands: fail loudly on
            // request surfaces this backend cannot execute instead of silently
            // dropping them. MCP grants are unaffected — they forward below.
            if request.grants.references.is_some() {
                bail!(
                    "the cursor backend cannot honor `grants.references`; use an in-process \
                     tool-loop backend such as omnia-genai"
                );
            }
            for tool in &request.tools {
                if let Tool::Function(function) = tool {
                    bail!(
                        "the cursor backend cannot execute the guest-declared function tool `{}`",
                        function.name
                    );
                }
            }

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

            log_completion(spawn.model, format, prompt.len(), &mcp_names);

            let output = spawn.run_agent(&prompt, None).await?;
            output.log(1);
            let (result, reason) = match take_answer(format, output.result, output.transcript) {
                Outcome::Done(answer) => return Ok(answer),
                Outcome::Repair { result, reason } => (result, reason),
            };

            tracing::debug!(
                attempt = 1,
                %reason,
                resumes = output.session_id.is_some(),
                "repairing answer"
            );

            let repair = Repair::attempt(&prompt, &result, &reason, format, output.session_id);
            let output = spawn.run_agent(&repair.prompt, repair.resume.as_deref()).await?;
            output.log(2);

            match take_answer(format, output.result, output.transcript) {
                Outcome::Done(answer) => Ok(answer),
                Outcome::Repair { reason, .. } => {
                    bail!("no answer after 2 repair attempts: {reason}");
                }
            }
        })
    }
}

/// The second attempt: its prompt and the session to resume, if any.
struct Repair {
    prompt: String,
    resume: Option<String>,
}

impl Repair {
    // With a session id from the first spawn, the repair resumes that session
    // and sends only the format-repair instruction (the reason is embedded;
    // the session already carries the failed answer). Without one, it falls
    // back to a cold spawn whose prompt keeps the original as a byte-identical
    // prefix — so provider-side prompt caching stays warm — with the failed
    // answer and the repair instruction appended.
    fn attempt(
        prompt: &str, answer: &str, reason: &str, format: &Format, session_id: Option<String>,
    ) -> Self {
        session_id.map_or_else(
            || Self {
                prompt: append_repair(prompt, answer, reason, format),
                resume: None,
            },
            |id| Self {
                prompt: format.repair(reason),
                resume: Some(id),
            },
        )
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
fn log_completion(model: Option<&str>, format: &Format, prompt_len: usize, mcp_servers: &[&str]) {
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

// Deliberate unit tests: pure prompt-build and repair-plan logic (CI floor);
// `tests/live.rs` is the acceptance gate proving a real cursor-agent run
// works end-to-end. Stream-parse and spawn tests live with their modules.
#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use omnia_wasi_model::{
        DirEntry, Format, Function, FutureResult, Grants, Mcp, Message, Reference, Request, Role,
        Schema, Tool, ToolHost, WasiModelCtx as _,
    };
    use serde_json::json;

    use super::Repair;
    use crate::{Client, Deadlines};

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

    #[tokio::test]
    async fn references_grant() {
        let mut request = schema_request();
        request.grants.references = Some("shelf".to_owned());
        let err = client()
            .complete(request, Arc::new(StubToolHost { path: None }))
            .await
            .expect_err("a references grant this backend cannot honor must fail loudly");
        assert!(err.to_string().contains("grants.references"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn function_tool() {
        let mut request = schema_request();
        request.tools.push(Tool::Function(Function {
            name: "lookup".to_owned(),
            description: "look something up".to_owned(),
            parameters: "{\"type\":\"object\"}".to_owned(),
        }));
        let err = client()
            .complete(request, Arc::new(StubToolHost { path: None }))
            .await
            .expect_err("a function tool this backend cannot execute must fail loudly");
        assert!(err.to_string().contains("`lookup`"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn mcp_grant_only() {
        let mut request = schema_request();
        request.tools.push(Tool::Mcp(Mcp {
            name: "docs".to_owned(),
            tools: vec![],
            url: "http://localhost:8080/mcp".to_owned(),
        }));
        let err = client()
            .complete(request, Arc::new(StubToolHost { path: None }))
            .await
            .expect_err("the stub host has no local tree");
        // MCP grants pass the honesty gate; the request fails later, on node state.
        assert!(err.to_string().contains("no local tree on this node"), "unexpected error: {err}");
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
            let Repair { prompt, resume } = Repair::attempt(
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
            let Repair { prompt, resume } = Repair::attempt(
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
}
