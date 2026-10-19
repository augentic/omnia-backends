//! The `shell` tool: one command under `sh -c` inside the completion's
//! sandbox session, what it wrote back to the model as text.

use std::fmt::Write as _;
use std::os::unix::process::ExitStatusExt as _;
use std::process::{ExitStatus, Output};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use omnia_sandbox::{Session, TimedOut};

use super::options::SHELL_TOOL;

// The width a command is cut to in a log line.
const SUBJECT_WIDTH: usize = 80;

pub struct Shell {
    session: Session,
    bound: Duration,
    cap: usize,
}

impl Shell {
    pub const fn new(session: Session, bound: Duration, cap: usize) -> Self {
        Self { session, bound, cap }
    }

    // What the model reads back: the command's stdout, its stderr under a
    // marker where any, its exit under one where not clean, cut to the cap.
    // A command past the bound is killed with everything it started and
    // told so as a result the model can act on; one the host cannot spawn
    // or wait on is the completion's failure.
    pub async fn run(&self, command: &str) -> Result<String> {
        let cwd = self.session.cwd();
        let subject = subject(command, &cwd.to_string_lossy());
        tracing::debug!(tool = %SHELL_TOOL, subject = subject.as_str(), "tool call");
        let started = Instant::now();

        let mut process =
            self.session.command("sh").context("resolving `sh` for the shell tool")?;
        process.args(["-c", command]).env("WORKSPACE", cwd);
        let (text, exit) = match process.output(self.bound).await {
            Ok(output) => (cut(render(&output), self.cap), exit_of(output.status)),
            Err(error) => match error.downcast::<TimedOut>() {
                Ok(timed_out) => (
                    format!(
                        "the command did not exit within {}s and was killed with everything it \
                         started",
                        timed_out.bound.as_secs()
                    ),
                    "timed out".to_owned(),
                ),
                Err(error) => {
                    return Err(error.context(format!("running `{subject}` in the shell")));
                }
            },
        };

        tracing::debug!(
            tool = %SHELL_TOOL,
            subject = subject.as_str(),
            result_bytes = text.len(),
            exit = %exit,
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "tool call completed"
        );
        Ok(text)
    }
}

// The first line of the command, every `<cwd>/` in it spelled `./`, cut to
// a readable width: the one argument worth a log line.
pub fn subject(command: &str, cwd: &str) -> String {
    let line = command.lines().next().unwrap_or_default();
    let root = cwd.trim_end_matches('/');
    let line =
        if root.is_empty() { line.to_owned() } else { line.replace(&format!("{root}/"), "./") };
    match line.char_indices().nth(SUBJECT_WIDTH) {
        Some((cut, _)) => format!("{}…", &line[..cut]),
        None => line,
    }
}

// A result past the cap keeps its head and its tail with the omitted count
// between them, since a log's start names the step and its end holds the
// verdict. The cuts fall on character boundaries.
pub fn cut(text: String, cap: usize) -> String {
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

// The pure readings of a command and its result; a command run in a session
// is the `model` suite's.
#[cfg(test)]
mod tests {
    use super::{cut, subject};

    #[test]
    fn subject_first_line() {
        assert_eq!(subject("npm test\nnpm run lint", "/work"), "npm test");
        assert_eq!(subject("", "/work"), "");
    }

    #[test]
    fn subject_relative_to_cwd() {
        assert_eq!(
            subject("cat /work/src/a.ts /work/src/b.ts", "/work/"),
            "cat ./src/a.ts ./src/b.ts"
        );
        assert_eq!(subject("cat /work/src/a.ts", ""), "cat /work/src/a.ts", "no cwd, no rewrite");
    }

    #[test]
    fn subject_width() {
        let long = "x".repeat(100);
        let cut = subject(&long, "/work");
        assert_eq!(cut.chars().count(), 81, "eighty characters and the ellipsis");
        assert!(cut.ends_with('…'));
        assert_eq!(subject(&"y".repeat(80), "/work"), "y".repeat(80), "at the width, whole");
    }

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
