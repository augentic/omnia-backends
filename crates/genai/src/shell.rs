//! Sandboxed `shell` tool execution and output formatting.

use std::fmt::Write as _;
use std::os::unix::process::ExitStatusExt as _;
use std::path::Path;
use std::process::{ExitStatus, Output};
use std::time::Duration;

use anyhow::{Context as _, Result};
use omnia_sandbox::{Session, TimedOut};

// Host environment variables inherited by the shell beyond `PATH`.
pub const INHERIT: &[&str] = &["USER", "LOGNAME", "SSL_CERT_FILE", "SSL_CERT_DIR"];

// Provider API keys blocked from worker grants.
pub const REFUSED: &[&str] = &[
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
    "GROQ_API_KEY",
    "XAI_API_KEY",
    "DEEPSEEK_API_KEY",
    "COHERE_API_KEY",
    "MISTRAL_API_KEY",
    "OLLAMA_API_KEY",
];

pub struct Shell {
    session: Session,
    timeout: Duration,
}

// What the model reads of a command, and the exit the log line carries.
pub struct Run {
    pub result: String,
    pub exit: String,
}

impl Shell {
    pub const fn new(session: Session, timeout: Duration) -> Self {
        Self { session, timeout }
    }

    pub fn cwd(&self) -> &Path {
        self.session.cwd()
    }

    pub async fn run(&self, command: &str) -> Result<Run> {
        let mut process =
            self.session.command("sh").context("resolving `sh` for the shell tool")?;
        process.args(["-c", command]).env("WORKSPACE", self.cwd());
        match process.output(self.timeout).await {
            Ok(output) => Ok(Run {
                result: render(&output),
                exit: exit_of(output.status),
            }),
            Err(error) => match error.downcast::<TimedOut>() {
                Ok(timed_out) => Ok(Run {
                    result: format!(
                        "the command did not exit within {}s and was killed with everything it \
                         started",
                        timed_out.bound.as_secs()
                    ),
                    exit: "timed out".to_owned(),
                }),
                Err(error) => Err(error.context(format!(
                    "running `{}` in the shell",
                    command.lines().next().unwrap_or_default()
                ))),
            },
        }
    }
}

fn render(output: &Output) -> String {
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    if !output.stderr.is_empty() {
        end_line(&mut text);
        text.push_str("--- stderr ---\n");
        text.push_str(&String::from_utf8_lossy(&output.stderr));
    }
    if !output.status.success() {
        end_line(&mut text);
        text.push_str("--- exit ---\n");
        match (output.status.code(), output.status.signal()) {
            (Some(code), _) => {
                let _ = write!(text, "exit {code}");
            }
            (None, Some(signal)) => {
                let _ = write!(text, "killed by signal {signal}");
            }
            (None, None) => text.push_str("exit unknown"),
        }
    } else if text.is_empty() {
        text.push_str("exit 0");
    }
    text
}

fn end_line(text: &mut String) {
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
}

fn exit_of(status: ExitStatus) -> String {
    match (status.code(), status.signal()) {
        (Some(code), _) => code.to_string(),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => "unknown".to_owned(),
    }
}
