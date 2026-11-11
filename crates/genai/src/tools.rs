//! Tool-call dispatch across workspace tools, the sandboxed shell, and guest functions.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use genai::chat::{Tool, ToolCall};
use omnia_sandbox::Access;
use omnia_wasi_model::{ToolHost, ToolOutcome};
use schemars::{JsonSchema, schema_for};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::Instant;

use crate::elapsed_ms;
use crate::shell::{Run, Shell};

pub const SHELL_TOOL: &str = "shell";

// `Err` is a failure the model reads and repairs, never a host failure.
type Repairable<T> = Result<T, String>;

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

pub struct Tools {
    host: Arc<dyn ToolHost>,
    shell: Option<Shell>,
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

    // A shell call is answered before the declared names are checked, so one
    // made where no shell was advertised is told so rather than told the names.
    pub async fn call(&self, call: &ToolCall) -> Result<ToolOutcome> {
        let subject = subject(&call.fn_arguments, self.shell.as_ref().map(Shell::cwd));
        tracing::debug!(tool = %call.fn_name, subject = subject.as_deref(), "tool call");
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
            name if !self.declared.iter().any(|declared| declared == name) => {
                (Err(self.undeclared(name)), None)
            }
            "read" => (self.read(&call.fn_arguments).await, None),
            "list" => (self.list(&call.fn_arguments).await, None),
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

    async fn read(&self, arguments: &Value) -> ToolOutcome {
        let ReadArgs { path } = args(arguments)?;
        let bytes = self.host.read(path.clone()).await.map_err(|error| format!("{error:#}"))?;
        let text = String::from_utf8(bytes)
            .map_err(|error| format!("`{path}` is not UTF-8: {}", error.utf8_error()))?;
        self.within_cap(text)
    }

    async fn list(&self, arguments: &Value) -> ToolOutcome {
        let ListArgs { path } = args(arguments)?;
        let entries =
            self.host.list(path.unwrap_or_default()).await.map_err(|error| format!("{error:#}"))?;
        let json = serde_json::to_string(&entries).map_err(|error| error.to_string())?;
        self.within_cap(json)
    }

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

    async fn guest(&self, call: &ToolCall) -> Result<ToolOutcome> {
        self.host
            .call_tool(call.fn_name.clone(), call.fn_arguments.to_string())
            .await
            .with_context(|| format!("calling tool `{}`", call.fn_name))
    }

    fn within_cap(&self, text: String) -> ToolOutcome {
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

fn args<'de, T: Deserialize<'de>>(arguments: &'de Value) -> Repairable<T> {
    T::deserialize(arguments).map_err(|error| format!("invalid arguments: {error}"))
}

// Extract a short log summary from common argument keys, relativizing `<cwd>/` to `./`.
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
