//! [`ToolHost`] stubs for the live tests: an optional node-local workspace
//! lend, plus a `call_tool` responder proving the session round-trip.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use omnia_wasi_model::{DirEntry, FutureResult, ToolHost};
use serde_json::json;

/// A unique token returned by the stub session's `lookup` tool.
pub const TOOL_SENTINEL: &str = "OMNIA-TOOL-SENTINEL-7d21c3aa";

/// Tool host that resolves the lent workspace to an optional path and answers
/// `lookup` calls with [`TOOL_SENTINEL`], standing in for the guest's tool
/// closure behind the session.
#[derive(Debug)]
pub struct StubToolHost {
    path: Option<PathBuf>,
}

impl ToolHost for StubToolHost {
    fn call_tool(&self, name: String, arguments: String) -> FutureResult<Result<String, String>> {
        Box::pin(async move {
            if name == "lookup" {
                Ok(Ok(json!({ "secret": TOOL_SENTINEL }).to_string()))
            } else {
                Ok(Err(format!("unknown tool `{name}` (arguments: {arguments})")))
            }
        })
    }

    fn read(&self, _path: String) -> FutureResult<Vec<u8>> {
        Box::pin(async { Err(anyhow::anyhow!("cursor never routes `read` through the host")) })
    }

    fn list(&self, _path: String) -> FutureResult<Vec<DirEntry>> {
        Box::pin(async { Err(anyhow::anyhow!("cursor never routes `list` through the host")) })
    }

    fn write(&self, _path: String, _bytes: Vec<u8>) -> FutureResult<()> {
        Box::pin(async { Err(anyhow::anyhow!("cursor never routes `write` through the host")) })
    }

    fn local_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

/// A tool host that lends `path` as the completion's node-local workspace.
pub fn local_path_tool_host(path: PathBuf) -> Arc<dyn ToolHost> {
    Arc::new(StubToolHost { path: Some(path) })
}

/// A tool host with no local tree: the references-only completion shape.
pub fn no_workspace_tool_host() -> Arc<dyn ToolHost> {
    Arc::new(StubToolHost { path: None })
}
