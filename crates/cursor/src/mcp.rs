//! Manage `<workspace>/.cursor/mcp.json` around a `cursor-agent` spawn.
//!
//! `cursor-agent` has no `--mcp-config` flag; it discovers MCP servers only from
//! `.cursor/mcp.json` in its workspace (or `~/.cursor/mcp.json`). To point a
//! spawned agent at the omnia-hosted MCP servers a prompt granted, the backend
//! snapshots the workspace file, merges the granted server entries in before
//! the spawn, and restores the snapshot when the guard drops. Completions are
//! sequential per workspace; a process-wide lock only keeps two guards from
//! interleaving a read-merge-write.

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result};
use serde_json::{Map, Value, json};

// Serializes install/restore so concurrent guards cannot interleave mid-write.
static FILE_LOCK: Mutex<()> = Mutex::new(());

/// Restores `<workspace>/.cursor/mcp.json` to its pre-install snapshot on drop.
pub struct McpGuard {
    path: PathBuf,
    original: Option<Vec<u8>>,
}

impl McpGuard {
    /// Merge each `name -> url` server into `<workspace>/.cursor/mcp.json`,
    /// snapshotting the prior file content for restore on drop.
    pub fn install(workspace: &Path, servers: &BTreeMap<String, String>) -> Result<Self> {
        let path = workspace.join(".cursor").join("mcp.json");
        let _lock = FILE_LOCK.lock().unwrap_or_else(PoisonError::into_inner);

        let original = match fs::read(&path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", path.display()));
            }
        };

        let merged = merge(original.as_deref(), servers)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        fs::write(&path, merged).with_context(|| format!("writing {}", path.display()))?;

        Ok(Self { path, original })
    }
}

impl Drop for McpGuard {
    fn drop(&mut self) {
        let _lock = FILE_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let restore = match self.original.take() {
            Some(bytes) => fs::write(&self.path, bytes),
            None => match fs::remove_file(&self.path) {
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                other => other,
            },
        };
        if let Err(error) = restore {
            tracing::warn!(path = %self.path.display(), %error, "failed to restore mcp.json");
        }
    }
}

// Merge the omnia servers into `original`, preserving any user-defined servers.
fn merge(original: Option<&[u8]>, servers: &BTreeMap<String, String>) -> Result<Vec<u8>> {
    let mut root = match original {
        Some(bytes) => serde_json::from_slice::<Value>(bytes)
            .context("existing .cursor/mcp.json is not valid JSON")?,
        None => json!({}),
    };

    let object = root.as_object_mut().context("existing .cursor/mcp.json is not a JSON object")?;
    let entries = object.entry("mcpServers").or_insert_with(|| Value::Object(Map::new()));
    let entries =
        entries.as_object_mut().context("`mcpServers` in .cursor/mcp.json is not an object")?;
    for (name, url) in servers {
        entries.insert(name.clone(), json!({ "url": url }));
    }

    let mut bytes = serde_json::to_vec_pretty(&root).context("serializing .cursor/mcp.json")?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde_json::{Value, json};

    use super::McpGuard;

    /// A fresh, empty temp directory unique to this process and `label`.
    fn temp_workspace(label: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("omnia-cursor-mcp-{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("creating temp workspace");
        dir
    }

    fn servers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(name, url)| ((*name).to_owned(), (*url).to_owned())).collect()
    }

    fn read_servers(path: &Path) -> Value {
        let bytes = fs::read(path).expect("reading mcp.json");
        let value: Value = serde_json::from_slice(&bytes).expect("mcp.json is JSON");
        value["mcpServers"].clone()
    }

    #[test]
    fn create_remove_absent() {
        let workspace = temp_workspace("absent");
        let path = workspace.join(".cursor/mcp.json");
        let guard =
            McpGuard::install(&workspace, &servers(&[("docs", "http://127.0.0.1:8080/mcp/docs")]))
                .unwrap();
        assert_eq!(read_servers(&path)["docs"]["url"], "http://127.0.0.1:8080/mcp/docs");
        drop(guard);
        assert!(!path.exists(), "a file we created is removed on drop");
    }

    #[test]
    fn merge_restore_existing() {
        let workspace = temp_workspace("existing");
        let cursor_dir = workspace.join(".cursor");
        fs::create_dir_all(&cursor_dir).unwrap();
        let path = cursor_dir.join("mcp.json");
        let original = json!({ "mcpServers": { "other": { "url": "http://example" } } });
        fs::write(&path, serde_json::to_vec_pretty(&original).unwrap()).unwrap();

        let guard =
            McpGuard::install(&workspace, &servers(&[("docs", "http://127.0.0.1:9/x")])).unwrap();
        let entries = read_servers(&path);
        assert_eq!(entries["docs"]["url"], "http://127.0.0.1:9/x");
        assert_eq!(entries["other"]["url"], "http://example", "existing servers survive");
        drop(guard);

        let restored: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(restored, original, "the original file is restored verbatim");
    }
}
