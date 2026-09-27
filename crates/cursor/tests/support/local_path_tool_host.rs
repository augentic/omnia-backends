//! [`ToolHost`] stubs for the live tests: an optional node-local workspace
//! lend, a `call_tool` responder proving the session round-trip, and a
//! `check` standing in for the guest's judgement.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use omnia_wasi_model::{DirEntry, FutureResult, ToolHost};
use serde_json::json;

/// A unique token returned by the stub session's `lookup` tool.
pub const TOOL_SENTINEL: &str = "OMNIA-TOOL-SENTINEL-7d21c3aa";

/// The word the stub `check` demands; the model only learns it from the
/// correction turn.
pub const CHECK_WORD: &str = "quokka";

// Lends an optional path, answers `lookup` with `TOOL_SENTINEL`, and plays
// the guest's `check`: the first `rejections` candidates are corrected toward
// `CHECK_WORD`, the rest accepted; every candidate offered is recorded.
#[derive(Debug)]
pub struct StubToolHost {
    path: Option<PathBuf>,
    rejections: usize,
    seen: AtomicUsize,
    candidates: Arc<Mutex<Vec<String>>>,
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

    fn check(&self, candidate: String) -> FutureResult<Result<(), String>> {
        let seen = self.seen.fetch_add(1, Ordering::SeqCst);
        self.candidates.lock().expect("candidates lock").push(candidate.clone());
        let verdict = if seen < self.rejections {
            Err(format!(
                "## Previous answer (rejected)\n\n{candidate}\n\n## Findings\n\nThe `word` \
                 property must be exactly \"{CHECK_WORD}\".\n\nProduce a corrected, complete \
                 answer that resolves every finding."
            ))
        } else {
            Ok(())
        };
        Box::pin(async move { Ok(verdict) })
    }

    fn local_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

fn stub(path: Option<PathBuf>, rejections: usize) -> (Arc<dyn ToolHost>, Arc<Mutex<Vec<String>>>) {
    let candidates = Arc::new(Mutex::new(Vec::new()));
    let host = StubToolHost {
        path,
        rejections,
        seen: AtomicUsize::new(0),
        candidates: Arc::clone(&candidates),
    };
    (Arc::new(host), candidates)
}

/// A tool host that lends `path` as the completion's node-local workspace.
pub fn local_path_tool_host(path: PathBuf) -> Arc<dyn ToolHost> {
    stub(Some(path), 0).0
}

/// A tool host with no local tree: the references-only completion shape.
pub fn no_tool_host() -> Arc<dyn ToolHost> {
    stub(None, 0).0
}

/// A workspace-less tool host whose `check` rejects the first `rejections`
/// candidates, plus the record of every candidate offered.
pub fn checking_tool_host(rejections: usize) -> (Arc<dyn ToolHost>, Arc<Mutex<Vec<String>>>) {
    stub(None, rejections)
}
