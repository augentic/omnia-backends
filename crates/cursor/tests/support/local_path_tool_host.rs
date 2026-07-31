//! A [`ToolHost`] stub that optionally lends a node-local workspace path.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use omnia_wasi_model::{DirEntry, FutureResult, Reference, ToolHost};

/// Tool host that resolves the lent workspace to an optional path; cursor ignores
/// every other capability.
#[derive(Debug)]
pub struct StubToolHost {
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

    fn local_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

/// A tool host that lends `path` as the completion's node-local workspace.
pub fn local_path_tool_host(path: PathBuf) -> Arc<dyn ToolHost> {
    Arc::new(StubToolHost { path: Some(path) })
}
