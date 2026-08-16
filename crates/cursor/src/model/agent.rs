use std::io::Write as _;
use std::path::Path;
use std::process::Stdio;

use anyhow::{Context as _, Result, ensure};
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, BufReader};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};
use tracing::instrument;

use super::AgentOutput;
use super::stream::{OutputParser, PROMPT_PREVIEW_CHARS, truncate};
use crate::{Deadlines, mcp};

const CURSOR_BIN: &str = "cursor-agent";

/// Retry budget for empty spawns — clean exits whose stream carried no
/// terminal result event, so the agent did no work and a retry is cheap.
/// Crashes and timeout kills are errors and are never retried here.
const EMPTY_SPAWN_RETRIES: u32 = 2;

/// Verify `cursor-agent` is on `PATH` and responds to `--version`.
///
/// # Errors
///
/// Returns an error if the binary is missing or the probe exits unsuccessfully.
pub async fn check_cursor() -> Result<()> {
    let status = Command::new(CURSOR_BIN)
        .arg("--version")
        .status()
        .await
        .context("cursor-agent not found")?;
    ensure!(status.success(), "`{CURSOR_BIN} --version` failed ({status})");
    Ok(())
}

pub struct SpawnOptions<'a> {
    pub model: Option<&'a str>,
    pub workspace: &'a Path,
    pub deadlines: Deadlines,
    pub approve_mcps: bool,
}

/// Spilled prompt file: CLI arg points at a path that lives as long as this value.
struct Prompt {
    arg: String,
    file: NamedTempFile,
}

impl Prompt {
    fn spill(prompt: &str, workspace: &Path) -> Result<Self> {
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
    /// One agent run: spawn, re-spawning empty spawns (a clean exit with no
    /// terminal result event — a session the service dropped before any work
    /// happened) until the retry budget runs out.
    pub async fn run_agent(
        &self, prompt: &str, resume: Option<&str>,
    ) -> Result<AgentOutput> {
        for retry in 1..=EMPTY_SPAWN_RETRIES {
            if let Some(output) = self.spawn_once(prompt, resume).await? {
                return Ok(output);
            }
            tracing::warn!(retry, of = EMPTY_SPAWN_RETRIES, "no result event; retrying the spawn");
        }
        self.spawn_once(prompt, resume).await?.with_context(|| {
            format!(
                "cursor-agent did not emit a terminal result event in {} spawns",
                EMPTY_SPAWN_RETRIES + 1
            )
        })
    }

    /// `None` is an empty spawn: a clean exit whose stream had no terminal
    /// result event.
    #[instrument(skip(self, prompt, resume), fields(model = self.model))]
    async fn spawn_once(&self, prompt: &str, resume: Option<&str>) -> Result<Option<AgentOutput>> {
        let spilled = Prompt::spill(prompt, self.workspace)?;
        tracing::debug!(
            prompt_path = %spilled.file.path().display(),
            prompt_len = prompt.len(),
            resume,
            preview = %truncate(prompt, PROMPT_PREVIEW_CHARS),
            "prompt"
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
        let status = status.context("cursor-agent status is available after a normal exit")?;
        if !status.success() {
            let exit = format!(
                "cursor-agent exited with {status}: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
            // A crash can leave the stream without a terminal result event; the
            // exit status and stderr are the diagnostic, with any parse failure
            // (e.g. the agent's own error event) kept as the cause.
            return Err(match parsed {
                Err(error) => error.context(exit),
                Ok(_) => anyhow::anyhow!(exit),
            });
        }

        parsed
    }

    /// The `cursor-agent` invocation for one spawn; `resume` re-enters the named
    /// session instead of starting a fresh one.
    fn command(&self, prompt_arg: &str, resume: Option<&str>) -> Command {
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
) -> Result<Option<AgentOutput>> {
    let mut lines = BufReader::new(stdout).lines();
    let mut parser = OutputParser::default();
    while let Some(line) = lines.next_line().await? {
        activity.send_replace(Instant::now());
        parser.line(&line)?;
    }
    Ok(parser.finish())
}

async fn drain(mut stream: impl AsyncRead + Unpin) -> Vec<u8> {
    let mut buffer = Vec::new();
    let _ = stream.read_to_end(&mut buffer).await;
    buffer
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::Prompt;

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

    mod spawn_args {
        use std::time::Duration;

        use super::super::SpawnOptions;
        use crate::Deadlines;

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
