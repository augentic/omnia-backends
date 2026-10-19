//! The `shell` tool's command: `sh -c` inside the completion's sandbox
//! session, what it wrote rendered for the model.

use std::fmt::Write as _;
use std::os::unix::process::ExitStatusExt as _;
use std::path::Path;
use std::process::{ExitStatus, Output};
use std::time::Duration;

use anyhow::{Context as _, Result};
use omnia_sandbox::{Session, TimedOut};

// What a shell inherits of the host's environment beyond `PATH`: who runs
// it, and where the system's certificates are.
pub const INHERIT: &[&str] = &["USER", "LOGNAME", "SSL_CERT_FILE", "SSL_CERT_DIR"];

// The provider keys the SDK reads from the process environment, which no
// grant may set for a shell.
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
    max_result_bytes: usize,
}

// One command as it came out: the result the model reads, and the exit as
// the log carries it (`0`, `3`, `signal 9`, `timed out`).
pub struct Run {
    pub result: String,
    pub exit: String,
}

impl Shell {
    pub const fn new(session: Session, timeout: Duration, max_result_bytes: usize) -> Self {
        Self {
            session,
            timeout,
            max_result_bytes,
        }
    }

    // The directory the commands run in, canonical.
    pub fn cwd(&self) -> &Path {
        self.session.cwd()
    }

    // The result is the command's stdout, its stderr under a marker where
    // any, its exit under one where not clean, cut to the cap. A command
    // past the timeout is killed with everything it started and told so as
    // a result the model can act on; one the host cannot spawn or wait on
    // is the completion's failure.
    pub async fn run(&self, command: &str) -> Result<Run> {
        let mut process =
            self.session.command("sh").context("resolving `sh` for the shell tool")?;
        process.args(["-c", command]).env("WORKSPACE", self.cwd());
        match process.output(self.timeout).await {
            Ok(output) => Ok(Run {
                result: cut(render(&output), self.max_result_bytes),
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

// A result past the cap keeps its head and its tail with the omitted count
// between them, since a log's start names the step and its end holds the
// verdict. The cuts fall on character boundaries.
fn cut(text: String, cap: usize) -> String {
    if text.len() <= cap {
        return text;
    }
    let mut head = cap / 2;
    while !text.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = text.len() - (cap - head);
    while !text.is_char_boundary(tail) {
        tail += 1;
    }
    format!("{}\n[… {} bytes omitted …]\n{}", &text[..head], tail - head, &text[tail..])
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

// `0`, `3`, or `signal 9`, as the log carries it.
fn exit_of(status: ExitStatus) -> String {
    match (status.code(), status.signal()) {
        (Some(code), _) => code.to_string(),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => "unknown".to_owned(),
    }
}

// The cut of a result alone; a command run in a session, its rendering
// included, is the `model` suite's.
#[cfg(test)]
mod tests {
    use super::cut;

    #[test]
    fn cut_within_cap() {
        assert_eq!(cut("short".to_owned(), 5), "short");
        assert_eq!(cut(String::new(), 0), "");
    }

    #[test]
    fn cut_head_and_tail() {
        let text = "0123456789".to_owned();
        assert_eq!(cut(text, 4), "01\n[… 6 bytes omitted …]\n89");
    }

    #[test]
    fn cut_on_char_boundaries() {
        // each `é` is two bytes, so the tail's byte cut at 5 would split one
        let text = "éééé".to_owned();
        let cut = cut(text, 5);
        assert_eq!(cut, "é\n[… 4 bytes omitted …]\né");
    }
}
