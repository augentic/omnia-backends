//! [`ToolHost`] stubs for the live tests: an optional node-local workspace
//! lend, a `call_tool` responder proving the session round-trip, and a
//! `check` standing in for the guest's judgement.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use omnia::FutureResult;
use omnia_wasi_model::{DirEntry, Lend, ToolHost};
use serde_json::json;

/// A unique token returned by the stub session's `lookup` tool.
pub const TOOL_SENTINEL: &str = "OMNIA-TOOL-SENTINEL-7d21c3aa";

/// The word the stub `check` demands; the model only learns it from the
/// correction turn.
pub const CHECK_WORD: &str = "quokka";

// Lends an optional directory, answers `lookup` with `TOOL_SENTINEL`, and
// plays the guest's `check`: the first `rejections` candidates are corrected
// toward `CHECK_WORD`, the rest accepted; every candidate offered is recorded.
#[derive(Debug)]
pub struct StubToolHost {
    lend: Option<Lend>,
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

    fn lend(&self) -> Option<&Lend> {
        self.lend.as_ref()
    }
}

fn stub(lend: Option<Lend>, rejections: usize) -> (Arc<dyn ToolHost>, Arc<Mutex<Vec<String>>>) {
    let candidates = Arc::new(Mutex::new(Vec::new()));
    let host = StubToolHost {
        lend,
        rejections,
        seen: AtomicUsize::new(0),
        candidates: Arc::clone(&candidates),
    };
    (Arc::new(host), candidates)
}

/// A tool host that lends the directory at `path`, writable, as the
/// completion's node-local workspace.
pub fn lending_tool_host(path: PathBuf) -> anyhow::Result<Arc<dyn ToolHost>> {
    let dir = Dir::open_ambient_dir(&path, ambient_authority())?;
    Ok(stub(Some(Lend::new(dir, path, true)), 0).0)
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
