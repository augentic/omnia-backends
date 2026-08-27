//! In-process tool routing — this backend's counterpart of the cursor
//! crate's callback endpoint. The host-injected `read`/`list` execute
//! host-side through the lent [`ToolHost`] workspace capability and never
//! traverse the session; every other name is forwarded through
//! [`ToolHost::call_tool`], where the guest's tool closure answers.
//! Workspace failures (missing file, bounds, no workspace lent) are
//! model-visible repairable text, never hard errors.

use std::sync::Arc;

use anyhow::{Context as _, Result};
use genai::chat::ToolCall;
use omnia_wasi_model::ToolHost;
use serde_json::Value;

/// Route one model tool call. For `call_tool` the host enforces the
/// declared-name check, budget, size cap, and timeout; its outer error is a
/// hard failure that ends the completion, while the inner `Err` is the
/// guest tool's own failure text — fed back to the model as repairable
/// content.
///
/// # Errors
///
/// Returns an error only on a hard `call_tool` failure (undeclared tool,
/// exhausted budget, closed session, oversize result, timeout).
pub async fn dispatch_tool(
    tool_host: &Arc<dyn ToolHost>, call: &ToolCall, max_result_bytes: usize,
) -> Result<String> {
    tracing::info!(monotonic_counter.genai_tool_calls = 1_u64, "tool call");
    tracing::debug!(tool = %call.fn_name, "tool call");

    match call.fn_name.as_str() {
        "read" => Ok(workspace_read(tool_host, &call.fn_arguments, max_result_bytes).await),
        "list" => Ok(workspace_list(tool_host, &call.fn_arguments, max_result_bytes).await),
        _ => {
            let outcome = tool_host
                .call_tool(call.fn_name.clone(), call.fn_arguments.to_string())
                .await
                .with_context(|| format!("calling tool `{}`", call.fn_name))?;
            Ok(outcome
                .unwrap_or_else(|failure| format!("tool `{}` failed: {failure}", call.fn_name)))
        }
    }
}

/// Serve a model `read` call from the lent workspace: bytes must decode as
/// UTF-8 and fit the per-result byte cap before they become prompt content.
async fn workspace_read(
    tool_host: &Arc<dyn ToolHost>, arguments: &Value, max_result_bytes: usize,
) -> String {
    let Some(path) = arguments.get("path").and_then(Value::as_str) else {
        return "tool `read` failed: arguments must carry a string `path`".to_owned();
    };
    let bytes = match tool_host.read(path.to_owned()).await {
        Ok(bytes) => bytes,
        Err(error) => return format!("tool `read` failed: {error:#}"),
    };
    String::from_utf8(bytes).map_or_else(
        |_| format!("tool `read` failed: `{path}` is not valid UTF-8 text"),
        |text| bounded_result("read", text, max_result_bytes),
    )
}

/// Serve a model `list` call from the lent workspace as a JSON array of
/// `{"name", "is_directory"}` entries; a missing or empty `path` lists the
/// workspace root.
async fn workspace_list(
    tool_host: &Arc<dyn ToolHost>, arguments: &Value, max_result_bytes: usize,
) -> String {
    let path = arguments.get("path").and_then(Value::as_str).unwrap_or_default().to_owned();
    let entries = match tool_host.list(path).await {
        Ok(entries) => entries,
        Err(error) => return format!("tool `list` failed: {error:#}"),
    };
    match serde_json::to_string(&entries) {
        Ok(json) => bounded_result("list", json, max_result_bytes),
        Err(error) => format!("tool `list` failed: {error}"),
    }
}

/// Apply the session's per-result byte cap to a host-injected tool's output,
/// mirroring the host's enforcement on session tool results.
fn bounded_result(tool: &str, text: String, max_result_bytes: usize) -> String {
    if text.len() > max_result_bytes {
        return format!(
            "tool `{tool}` failed: result of {} bytes exceeds the {max_result_bytes}-byte cap",
            text.len()
        );
    }
    text
}

// Deliberate unit tests: deterministic, service-free tool routing (CI
// floor); `tests/live.rs` proves a real provider drives the loop end-to-end.
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use omnia_wasi_model::{DirEntry, FutureResult};
    use serde_json::json;

    use super::*;

    /// Deterministic stand-in for `BoundToolHost`: an in-memory file map and
    /// root listing, with a `call_tool` echo that proves session routing.
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

    #[tokio::test]
    async fn read_routes_host_side() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("read", json!({"path": "refs.md"})), CAP)
                .await
                .expect("a workspace read is never a hard failure");
        assert_eq!(result, "reference text", "the file body is the tool result");
    }

    #[tokio::test]
    async fn binary_read() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("read", json!({"path": "logo.bin"})), CAP)
                .await
                .expect("a binary read is model-visible, not a hard failure");
        assert!(result.contains("not valid UTF-8"), "unexpected result: {result}");
    }

    #[tokio::test]
    async fn oversize_read() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("read", json!({"path": "refs.md"})), 8)
                .await
                .expect("an oversize read is model-visible, not a hard failure");
        assert!(result.contains("exceeds the 8-byte cap"), "unexpected result: {result}");
    }

    #[tokio::test]
    async fn read_missing_file() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("read", json!({"path": "gone.md"})), CAP)
                .await
                .expect("a missing file is model-visible, not a hard failure");
        assert!(result.starts_with("tool `read` failed:"), "unexpected result: {result}");
        assert!(result.contains("gone.md"), "the failure names the path: {result}");
    }

    #[tokio::test]
    async fn read_missing_path_argument() {
        let result = dispatch_tool(&workspace_stub(), &tool_call("read", json!({})), CAP)
            .await
            .expect("malformed arguments are model-visible, not a hard failure");
        assert!(result.contains("string `path`"), "unexpected result: {result}");
    }

    #[tokio::test]
    async fn list_serialization() {
        let result = dispatch_tool(&workspace_stub(), &tool_call("list", json!({})), CAP)
            .await
            .expect("a root listing is never a hard failure");
        assert_eq!(
            result,
            r#"[{"name":"refs","is_directory":true},{"name":"refs.md","is_directory":false}]"#,
            "entries serialize as a canonical JSON array"
        );
    }

    #[tokio::test]
    async fn unknown_tool_routes_to_session() {
        let result =
            dispatch_tool(&workspace_stub(), &tool_call("lookup", json!({"name": "alpha"})), CAP)
                .await
                .expect("the session stub answers");
        assert_eq!(
            result, r#"session:lookup:{"name":"alpha"}"#,
            "non-reserved names go through call_tool"
        );
    }
}
