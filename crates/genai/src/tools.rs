//! The model's tool calls, dispatched, and the guest's check of a candidate.
//!
//! `read` and `list` go to the lent workspace, `shell` to the completion's
//! session, and every other declared call through the guest, as the check
//! does. A workspace or shell failure, and a call to a tool the request
//! never declared, go back to the model as repairable tool output; a host
//! failure ends the completion.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use genai::chat::{Tool, ToolCall};
use omnia_sandbox::Access;
use omnia_wasi_model::ToolHost;
use schemars::{JsonSchema, schema_for};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::Instant;

use crate::elapsed_ms;
use crate::shell::{Run, Shell};

// The one tool name this backend reserves beyond the host's: a guest tool
// of the name would be shadowed where a shell is advertised.
pub const SHELL_TOOL: &str = "shell";

// A result the model repairs: `Err` is the text it is given in its next
// turn, a failed call's reason or the check's correction. A host failure is
// the outer `Err` that ends the completion, never this.
pub type Repairable<T> = Result<T, String>;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    path: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListArgs {
    path: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ShellArgs {
    command: String,
}

// The tools a lend advertises beside the request's own: `read` and `list`
// over any lent workspace, `shell` with them under a shell root. The host
// reserves `read`, `list`, `write` and `check`, so no guest tool shadows
// them; `shell` is this backend's, refused in the request's translation.
pub fn workspace_tools(access: Access) -> Vec<Tool> {
    let mut tools = Vec::new();
    if access.is_lent() {
        tools.push(
            Tool::new("read")
                .with_description("Read a text file from the workspace for this task.")
                .with_schema(schema_for!(ReadArgs).to_value()),
        );
        tools.push(
            Tool::new("list")
                .with_description(
                    "List a directory of the workspace for this task. Omit `path` to list \
                     the workspace root.",
                )
                .with_schema(schema_for!(ListArgs).to_value()),
        );
    }
    if access == Access::Shell {
        tools.push(
            Tool::new(SHELL_TOOL)
                .with_description(
                    "Run a shell command in the workspace for this task, as `sh -c`, with the \
                     workspace as the working directory and as `$WORKSPACE`. The result is what \
                     it wrote, its stderr after `--- stderr ---` when any, and its exit after \
                     `--- exit ---` when it failed. A command that runs too long is killed.",
                )
                .with_schema(schema_for!(ShellArgs).to_value()),
        );
    }
    tools
}

// Where one completion reaches the guest: its tool calls, to the guest's
// host or to the shell where the lend opened one, and the check of a
// candidate.
pub struct Tools {
    host: Arc<dyn ToolHost>,
    shell: Option<Shell>,
    // every tool the turn declares, the request's own and the lend's; a
    // call to any other name never reaches the host, which would end the
    // completion on it
    declared: Vec<String>,
    max_result_bytes: usize,
}

impl Tools {
    pub const fn new(
        host: Arc<dyn ToolHost>, shell: Option<Shell>, declared: Vec<String>,
        max_result_bytes: usize,
    ) -> Self {
        Self {
            host,
            shell,
            declared,
            max_result_bytes,
        }
    }

    // One call's outcome, logged as the cursor backend's stream is: the
    // call with its subject before, the result's size, a command's exit,
    // and the time taken after. The inner `Err` is a failure the model
    // repairs, its reason bare; the caller spells it as the tool's result.
    pub async fn call(&self, call: &ToolCall) -> Result<Repairable<String>> {
        let subject = subject(&call.fn_arguments, self.shell.as_ref().map(Shell::cwd));
        tracing::debug!(tool = %call.fn_name, subject = subject.as_deref(), "tool call");
        // the arguments whole, under a target of their own so a filter can
        // open them without the rest of the trace
        tracing::trace!(
            target: "omnia_genai::tools",
            tool = %call.fn_name,
            args = %call.fn_arguments,
            "tool call args"
        );
        let started = Instant::now();

        let (outcome, exit) = match call.fn_name.as_str() {
            SHELL_TOOL => match self.shell(&call.fn_arguments).await? {
                Ok(Run { result, exit }) => (Ok(result), Some(exit)),
                Err(reason) => (Err(reason), None),
            },
            "read" => (self.read(&call.fn_arguments).await, None),
            "list" => (self.list(&call.fn_arguments).await, None),
            name if !self.declared.iter().any(|declared| declared == name) => {
                (Err(self.undeclared(name)), None)
            }
            _ => (self.guest(call).await?, None),
        };

        tracing::debug!(
            tool = %call.fn_name,
            subject = subject.as_deref(),
            result_bytes = outcome.as_ref().map_or_else(String::len, String::len),
            failed = outcome.is_err(),
            exit = exit.as_deref(),
            duration_ms = elapsed_ms(started),
            "tool call completed"
        );
        Ok(outcome)
    }

    // The slip the host would end the completion on, answered instead with
    // the names the model may call.
    fn undeclared(&self, name: &str) -> String {
        if self.declared.is_empty() {
            return format!("`{name}` is not declared; this request declares no tools");
        }
        let names: Vec<String> =
            self.declared.iter().map(|declared| format!("`{declared}`")).collect();
        format!("`{name}` is not declared; the tools are {}", names.join(", "))
    }

    pub async fn check(&self, candidate: String) -> Result<Repairable<()>> {
        self.host.check(candidate).await
    }

    async fn read(&self, arguments: &Value) -> Repairable<String> {
        let ReadArgs { path } = args(arguments)?;
        let bytes = self.host.read(path.clone()).await.map_err(|error| format!("{error:#}"))?;
        let text = String::from_utf8(bytes)
            .map_err(|error| format!("`{path}` is not UTF-8: {}", error.utf8_error()))?;
        self.within_cap(text)
    }

    async fn list(&self, arguments: &Value) -> Repairable<String> {
        let ListArgs { path } = args(arguments)?;
        let entries =
            self.host.list(path.unwrap_or_default()).await.map_err(|error| format!("{error:#}"))?;
        let json = serde_json::to_string(&entries).map_err(|error| error.to_string())?;
        self.within_cap(json)
    }

    // A call where no shell was advertised is the model's slip, answered as
    // any bad call is; only a command that ran has an exit.
    async fn shell(&self, arguments: &Value) -> Result<Repairable<Run>> {
        let Some(shell) = &self.shell else {
            return Ok(Err("no shell on this workspace".to_owned()));
        };
        let ShellArgs { command } = match args(arguments) {
            Ok(arguments) => arguments,
            Err(reason) => return Ok(Err(reason)),
        };
        Ok(Ok(shell.run(&command).await?))
    }

    async fn guest(&self, call: &ToolCall) -> Result<Repairable<String>> {
        self.host
            .call_tool(call.fn_name.clone(), call.fn_arguments.to_string())
            .await
            .with_context(|| format!("calling tool `{}`", call.fn_name))
    }

    // A result past the cap is refused whole, the model told its size.
    fn within_cap(&self, text: String) -> Repairable<String> {
        if text.len() > self.max_result_bytes {
            return Err(format!(
                "result of {} bytes exceeds the {}-byte cap",
                text.len(),
                self.max_result_bytes
            ));
        }
        Ok(text)
    }
}

// The call's arguments as the tool's own; a mismatch is the model's to repair.
fn args<'de, T: Deserialize<'de>>(arguments: &'de Value) -> Repairable<T> {
    T::deserialize(arguments).map_err(|error| format!("invalid arguments: {error}"))
}

// The one argument worth a log line: a path, a pattern, a query, a URL, or
// the first line of a command, every `<cwd>/` in it spelled `./`, cut to a
// readable width.
fn subject(arguments: &Value, cwd: Option<&Path>) -> Option<String> {
    const KEYS: &[&str] = &["path", "command", "pattern", "query", "url"];
    const WIDTH: usize = 80;
    let text = KEYS.iter().find_map(|key| arguments.get(key).and_then(Value::as_str))?;
    let line = text.lines().next().unwrap_or_default();
    let line = match cwd.map(|cwd| cwd.to_string_lossy()) {
        Some(root) if !root.trim_end_matches('/').is_empty() => {
            line.replace(&format!("{}/", root.trim_end_matches('/')), "./")
        }
        _ => line.to_owned(),
    };
    Some(match line.char_indices().nth(WIDTH) {
        Some((cut, _)) => format!("{}…", &line[..cut]),
        None => line,
    })
}

// Dispatch over a scripted host and the reading of a call's subject; a
// call that reaches a workspace or a shell is the `model` suite's.
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use omnia_wasi_model::{DirEntry, FutureResult};
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn text_read() {
        let result = dispatch("read", json!({"path": "refs.md"}), CAP)
            .await
            .expect("a workspace read is never a hard failure")
            .expect("the file is held");
        assert_eq!(result, "reference text", "the file body is the tool result");
    }

    #[tokio::test]
    async fn binary_read() {
        let reason = dispatch("read", json!({"path": "logo.bin"}), CAP)
            .await
            .expect("a binary read is model-visible, not a hard failure")
            .expect_err("bytes that are not text are the model's to repair");
        assert!(reason.contains("is not UTF-8"), "unexpected reason: {reason}");
    }

    #[tokio::test]
    async fn oversize_read() {
        let reason = dispatch("read", json!({"path": "refs.md"}), 8)
            .await
            .expect("an oversize read is model-visible, not a hard failure")
            .expect_err("a result past the cap is the model's to repair");
        assert!(reason.contains("exceeds the 8-byte cap"), "unexpected reason: {reason}");
    }

    #[tokio::test]
    async fn read_missing_file() {
        let reason = dispatch("read", json!({"path": "gone.md"}), CAP)
            .await
            .expect("a missing file is model-visible, not a hard failure")
            .expect_err("a missing file is the model's to repair");
        assert!(reason.contains("gone.md"), "the failure names the path: {reason}");
    }

    #[tokio::test]
    async fn read_missing_path() {
        let reason = dispatch("read", json!({}), CAP)
            .await
            .expect("malformed arguments are model-visible, not a hard failure")
            .expect_err("malformed arguments are the model's to repair");
        assert!(reason.contains("missing field `path`"), "unexpected reason: {reason}");
    }

    #[tokio::test]
    async fn list_serialization() {
        let result = dispatch("list", json!({}), CAP)
            .await
            .expect("a root listing is never a hard failure")
            .expect("the root lists");
        assert_eq!(
            result,
            r#"[{"name":"refs","is_directory":true},{"name":"refs.md","is_directory":false}]"#,
            "entries serialize as a canonical JSON array"
        );
    }

    #[tokio::test]
    async fn declared_tool() {
        let result = dispatch("lookup", json!({"name": "alpha"}), CAP)
            .await
            .expect("the session stub answers")
            .expect("the stub's answer is the result");
        assert_eq!(
            result, r#"session:lookup:{"name":"alpha"}"#,
            "a declared name goes through call_tool"
        );
    }

    #[tokio::test]
    async fn undeclared_tool() {
        let reason = dispatch("write", json!({"content": "x"}), CAP)
            .await
            .expect("an undeclared name never reaches the host")
            .expect_err("the slip is the model's to repair");
        assert_eq!(reason, "`write` is not declared; the tools are `lookup`, `read`");

        let reason = Tools::new(workspace_stub(), None, Vec::new(), CAP)
            .call(&tool_call("write", json!({})))
            .await
            .expect("an undeclared name never reaches the host")
            .expect_err("the slip is the model's to repair");
        assert_eq!(reason, "`write` is not declared; this request declares no tools");
    }

    #[tokio::test]
    async fn shell_without_session() {
        let reason = dispatch("shell", json!({"command": "pwd"}), CAP)
            .await
            .expect("a shell call with no shell is model-visible, not a hard failure")
            .expect_err("a shell call with no shell is the model's to repair");
        assert_eq!(reason, "no shell on this workspace");
    }

    #[test]
    fn subject_first_line() {
        let cwd = Some(Path::new("/work"));
        assert_eq!(
            subject(&json!({ "command": "npm test\nnpm run lint" }), cwd).as_deref(),
            Some("npm test")
        );
        assert_eq!(subject(&json!({ "command": "" }), cwd).as_deref(), Some(""));
        assert_eq!(subject(&json!({ "path": "src/a.ts" }), cwd).as_deref(), Some("src/a.ts"));
        assert_eq!(
            subject(&json!({ "query": "fn main", "limit": 3 }), cwd).as_deref(),
            Some("fn main"),
            "a guest tool's query is its subject"
        );
        assert_eq!(subject(&json!({}), cwd), None, "a root listing names nothing");
    }

    #[test]
    fn subject_relative_to_cwd() {
        assert_eq!(
            subject(
                &json!({ "command": "cat /work/src/a.ts /work/src/b.ts" }),
                Some(Path::new("/work/"))
            )
            .as_deref(),
            Some("cat ./src/a.ts ./src/b.ts")
        );
        assert_eq!(
            subject(&json!({ "command": "cat /work/src/a.ts" }), None).as_deref(),
            Some("cat /work/src/a.ts"),
            "no cwd, no rewrite"
        );
    }

    #[test]
    fn subject_width() {
        let cwd = Some(Path::new("/work"));
        let long = "x".repeat(100);
        let cut = subject(&json!({ "command": long }), cwd).expect("a command");
        assert_eq!(cut.chars().count(), 81, "eighty characters and the ellipsis");
        assert!(cut.ends_with('…'));
        assert_eq!(
            subject(&json!({ "command": "y".repeat(80) }), cwd).as_deref(),
            Some("y".repeat(80).as_str()),
            "at the width, whole"
        );
    }

    // A call over the stub with `lookup` and `read` declared.
    async fn dispatch(name: &str, arguments: Value, cap: usize) -> Result<Repairable<String>> {
        let declared = vec!["lookup".to_owned(), "read".to_owned()];
        Tools::new(workspace_stub(), None, declared, cap).call(&tool_call(name, arguments)).await
    }

    #[derive(Debug)]
    struct WorkspaceStub {
        files: HashMap<String, Vec<u8>>,
        entries: Vec<DirEntry>,
    }

    impl ToolHost for WorkspaceStub {
        fn call_tool(
            &self, name: String, arguments: String,
        ) -> FutureResult<Result<String, String>> {
            Box::pin(async move { Ok(Ok(format!("session:{name}:{arguments}"))) })
        }

        fn read(&self, path: String) -> FutureResult<Vec<u8>> {
            let result = self
                .files
                .get(&path)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("opening `{path}` in workspace"));
            Box::pin(async move { result })
        }

        fn list(&self, path: String) -> FutureResult<Vec<DirEntry>> {
            let result = if path.is_empty() {
                Ok(self.entries.clone())
            } else {
                Err(anyhow::anyhow!("listing `{path}` in workspace"))
            };
            Box::pin(async move { result })
        }

        fn write(&self, path: String, _bytes: Vec<u8>) -> FutureResult<()> {
            let error = anyhow::anyhow!("write to `{path}` is not exercised");
            Box::pin(async move { Err(error) })
        }

        fn check(&self, _candidate: String) -> FutureResult<Result<(), String>> {
            Box::pin(async { Err(anyhow::anyhow!("check is not exercised")) })
        }
    }

    fn workspace_stub() -> Arc<dyn ToolHost> {
        Arc::new(WorkspaceStub {
            files: [
                ("refs.md".to_owned(), b"reference text".to_vec()),
                ("logo.bin".to_owned(), vec![0xFF, 0xFE, 0x00]),
            ]
            .into(),
            entries: vec![
                DirEntry {
                    name: "refs".to_owned(),
                    is_directory: true,
                },
                DirEntry {
                    name: "refs.md".to_owned(),
                    is_directory: false,
                },
            ],
        })
    }

    fn tool_call(name: &str, arguments: Value) -> ToolCall {
        ToolCall {
            call_id: "call-1".to_owned(),
            fn_name: name.to_owned(),
            fn_arguments: arguments,
            thought_signatures: None,
        }
    }

    const CAP: usize = 1024;
}
