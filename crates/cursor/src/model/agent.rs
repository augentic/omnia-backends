use std::io::Write as _;
use std::path::Path;
use std::process::Stdio;

use anyhow::{Context as _, Result, bail, ensure};
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, BufReader};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};
use tracing::instrument;

use super::{AgentOutput, OutputParser, PROMPT_PREVIEW_CHARS, truncate};
use crate::{Deadlines, mcp};

const CURSOR_BIN: &str = "cursor-agent";

pub(super) async fn check_cursor() -> Result<()> {
    let status = Command::new(CURSOR_BIN)
        .arg("--version")
        .status()
        .await
        .context("cursor-agent not found")?;
    ensure!(status.success(), "`{CURSOR_BIN} --version` failed ({status})");
    Ok(())
}

pub(super) struct SpawnOptions<'a> {
    pub(super) model: Option<&'a str>,
    pub(super) workspace: &'a Path,
    pub(super) deadlines: Deadlines,
    pub(super) approve_mcps: bool,
}

/// Spilled prompt file: CLI arg points at a path that lives as long as this value.
pub(super) struct Prompt {
    pub(super) arg: String,
    pub(super) file: NamedTempFile,
}

impl Prompt {
    pub(super) fn spill(prompt: &str, workspace: &Path) -> Result<Self> {
        let cursor_dir = workspace.join(".cursor");
        std::fs::create_dir_all(&cursor_dir)
            .with_context(|| format!("creating {}", cursor_dir.display()))?;

        let mut file = tempfile::Builder::new()
            .prefix("omnia-prompt-")
            .suffix(".txt")
            .tempfile_in(&cursor_dir)
            .with_context(|| format!("creating prompt file in {}", cursor_dir.display()))?;
        file.as_file_mut()
            .write_all(prompt.as_bytes())
            .with_context(|| format!("writing prompt file {}", file.path().display()))?;

        let arg = format!(
            "Follow every instruction in the file at `{}`. When you are done, reply exactly as \
             that file instructs.",
            file.path().display()
        );

        Ok(Self { arg, file })
    }
}

impl SpawnOptions<'_> {
    #[instrument(skip(self, prompt, resume), fields(model = self.model))]
    pub(super) async fn run_agent(
        &self, prompt: &str, resume: Option<&str>,
    ) -> Result<AgentOutput> {
        let spilled = Prompt::spill(prompt, self.workspace)?;
        tracing::debug!(
            prompt_path = %spilled.file.path().display(),
            prompt_len = prompt.len(),
            resume,
            preview = %truncate(prompt, PROMPT_PREVIEW_CHARS),
            "cursor-agent prompt"
        );

        let mut child = self
            .command(&spilled.arg, resume)
            .spawn()
            .with_context(|| format!("spawning `{CURSOR_BIN}`"))?;
        let stdout = child.stdout.take().context("child stdout is piped")?;
        let stderr = child.stderr.take().context("child stderr is piped")?;

        // Parse both pipes concurrently so the child cannot block on a full buffer.
        let (activity_tx, activity_rx) = watch::channel(Instant::now());
        let stdout_task = tokio::spawn(parse_stream(stdout, activity_tx));
        let stderr_task = tokio::spawn(drain(stderr));

        let (status, timeout_error) = tokio::select! {
            status = child.wait() => {
                let status = status.with_context(|| format!("waiting on `{CURSOR_BIN}`"))?;
                (Some(status), None)
            }
            error = self.deadlines.watch(activity_rx) => {
                child.start_kill().with_context(|| format!("killing timed-out `{CURSOR_BIN}`"))?;
                child
                    .wait()
                    .await
                    .with_context(|| format!("reaping timed-out `{CURSOR_BIN}`"))?;
                (None, Some(error))
            }
        };

        let parsed = stdout_task.await.context("joining cursor-agent stdout task")?;
        let stderr = stderr_task.await.context("joining cursor-agent stderr task")?;
        if let Some(error) = timeout_error {
            return Err(error);
        }
        let parsed = parsed?;
        let status = status.context("cursor-agent status is available after a normal exit")?;
        if !status.success() {
            bail!("cursor-agent exited with {status}: {}", String::from_utf8_lossy(&stderr).trim());
        }

        Ok(parsed)
    }

    /// The `cursor-agent` invocation for one spawn; `resume` re-enters the named
    /// session instead of starting a fresh one.
    pub(super) fn command(&self, prompt_arg: &str, resume: Option<&str>) -> Command {
        let mut cmd = Command::new(CURSOR_BIN);
        cmd.kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .args([
                "--print",
                "--force",
                "--trust",
                "--output-format",
                "stream-json",
                "--workspace",
            ])
            .arg(self.workspace);

        // An environment key needs no persisted store and must not touch one.
        if std::env::var_os("CURSOR_API_KEY").is_some() {
            cmd.env("AGENT_CLI_CREDENTIAL_STORE", "memory");
        }
        // Prevent the agent from discovering the host checkout and missing the MCP config.
        for var in mcp::GIT_IDENTITY {
            cmd.env_remove(var);
        }
        if self.approve_mcps {
            cmd.arg("--approve-mcps");
        }
        if let Some(model) = self.model {
            cmd.arg("--model").arg(model);
        }
        if let Some(session_id) = resume {
            cmd.arg(format!("--resume={session_id}"));
        }

        cmd.arg(prompt_arg);
        cmd
    }
}

impl Deadlines {
    /// Resolve when a spawn breaches its inactivity or absolute bound.
    pub(super) async fn watch(&self, mut activity: watch::Receiver<Instant>) -> anyhow::Error {
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
                        "cursor-agent timed out after {}s (absolute cap exceeded while still active)",
                        self.cap.as_secs()
                    );
                }
                () = &mut inactive => {
                    let idle = Instant::now().saturating_duration_since(last_activity).as_secs();
                    return anyhow::anyhow!(
                        "cursor-agent inactive for {idle}s (no stream events; inactivity limit {}s, \
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

async fn parse_stream(
    stdout: impl AsyncRead + Unpin, activity: watch::Sender<Instant>,
) -> Result<AgentOutput> {
    let mut lines = BufReader::new(stdout).lines();
    let mut parser = OutputParser::default();
    while let Some(line) = lines.next_line().await? {
        activity.send_replace(Instant::now());
        parser.line(&line)?;
    }
    parser.finish()
}

async fn drain(mut stream: impl AsyncRead + Unpin) -> Vec<u8> {
    let mut buffer = Vec::new();
    let _ = stream.read_to_end(&mut buffer).await;
    buffer
}
