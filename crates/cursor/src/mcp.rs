//! Manage `<workspace>/.cursor/mcp.json` around a `cursor-agent` spawn.
//!
//! `cursor-agent` has no `--mcp-config` flag; it discovers MCP servers from
//! `.cursor/mcp.json` at the git toplevel of `--workspace` (or `~/.cursor/mcp.json`).
//! A lent tree inside another checkout is not itself a root, so this module
//! `git init`s the workspace when `.git` is missing — then snapshots, merges,
//! and restores the grant file as before. Host `GIT_*` identity vars are
//! stripped from both `git init` and the `cursor-agent` spawn so discovery
//! cannot skip the workspace. A per-workspace lock serializes MCP-enabled
//! completions until their original configuration has been restored.

use std::collections::HashMap;
use std::fs;
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, LazyLock, Mutex, PoisonError, Weak};

use anyhow::{Context as _, Result, ensure};
use serde_json::{Map, Value, json};
use tokio::process::Command;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

type WorkspaceLock = AsyncMutex<()>;

// A guard holds the workspace lock until its MCP configuration is restored.
static WORKSPACE_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<WorkspaceLock>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Host git-identity vars that would make `git` and `cursor-agent` ignore
/// `--workspace` and operate on the parent checkout instead.
pub const GIT_IDENTITY: &[&str] = &["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

/// `git init` `workspace` when it is not already a git root.
///
/// # Errors
///
/// Returns an error if `git init` cannot be spawned or exits unsuccessfully.
async fn ensure_git(workspace: &Path) -> Result<()> {
    if workspace.join(".git").exists() {
        return Ok(());
    }

    // Inherit no git identity from the host process: GIT_DIR would init the
    // parent checkout instead of the lent tree.
    let mut command = Command::new("git");
    command.arg("init").current_dir(workspace).stdin(Stdio::null());
    for var in GIT_IDENTITY {
        command.env_remove(var);
    }
    let output = command.output().await.context("running git init in cursor workspace")?;
    ensure!(
        output.status.success(),
        "git init of {} failed ({}): {}",
        workspace.display(),
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

/// Restores `<workspace>/.cursor/mcp.json` to its pre-install snapshot on drop.
pub struct McpGuard {
    path: PathBuf,
    original: Option<Vec<u8>>,
    _workspace_lock: OwnedMutexGuard<()>,
}

impl McpGuard {
    /// Merge each `name -> url` server into `<workspace>/.cursor/mcp.json`,
    /// snapshotting the prior file content for restore on drop.
    pub async fn install<'a>(
        workspace: &Path, servers: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self> {
        // The per-workspace lock is keyed by canonical path; canonicalize here
        // (even though completions already do) so direct callers cannot alias
        // one workspace under two keys.
        let workspace = workspace
            .canonicalize()
            .with_context(|| format!("canonicalizing {}", workspace.display()))?;
        let workspace_lock = lock_workspace(&workspace).await;
        ensure_git(&workspace).await?;

        let path = workspace.join(".cursor").join("mcp.json");
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
        atomic_write(&path, &merged)?;

        Ok(Self {
            path,
            original,
            _workspace_lock: workspace_lock,
        })
    }
}

impl Drop for McpGuard {
    fn drop(&mut self) {
        let restore = match self.original.take() {
            Some(bytes) => atomic_write(&self.path, &bytes),
            None => match fs::remove_file(&self.path) {
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                other => other.with_context(|| format!("removing {}", self.path.display())),
            },
        };
        if let Err(error) = restore {
            tracing::warn!(path = %self.path.display(), %error, "failed to restore mcp.json");
        }
    }
}

async fn lock_workspace(workspace: &Path) -> OwnedMutexGuard<()> {
    let lock = {
        let mut locks = WORKSPACE_LOCKS.lock().unwrap_or_else(PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() > 0);
        locks.get(workspace).and_then(Weak::upgrade).unwrap_or_else(|| {
            let lock = Arc::new(WorkspaceLock::new(()));
            locks.insert(workspace.to_path_buf(), Arc::downgrade(&lock));
            lock
        })
    };
    lock.lock_owned().await
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("mcp.json has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;

    let mut file = tempfile::Builder::new()
        .prefix(".mcp-")
        .tempfile_in(parent)
        .with_context(|| format!("creating temporary MCP config in {}", parent.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("writing temporary config for {}", path.display()))?;
    file.as_file_mut()
        .sync_all()
        .with_context(|| format!("syncing temporary config for {}", path.display()))?;
    if let Ok(metadata) = fs::metadata(path) {
        file.as_file()
            .set_permissions(metadata.permissions())
            .with_context(|| format!("preserving permissions for {}", path.display()))?;
    }
    file.persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("replacing {}", path.display()))?;
    Ok(())
}

// Merge the omnia servers into `original`, preserving any user-defined servers.
fn merge<'a>(
    original: Option<&[u8]>, servers: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<Vec<u8>> {
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
        entries.insert(name.to_owned(), json!({ "url": url }));
    }

    let mut bytes = serde_json::to_vec_pretty(&root).context("serializing .cursor/mcp.json")?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::Duration;

    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::{McpGuard, ensure_git};

    fn temp_workspace() -> TempDir {
        tempfile::tempdir().expect("creating temp workspace")
    }

    fn read_servers(path: &Path) -> Value {
        let bytes = fs::read(path).expect("reading mcp.json");
        let value: Value = serde_json::from_slice(&bytes).expect("mcp.json is JSON");
        value["mcpServers"].clone()
    }

    #[tokio::test]
    async fn absent_config_is_removed_after_completion() {
        let workspace = temp_workspace();
        let path = workspace.path().join(".cursor/mcp.json");
        let guard =
            McpGuard::install(workspace.path(), [("docs", "http://127.0.0.1:8080/mcp/docs")])
                .await
                .unwrap();
        assert_eq!(read_servers(&path)["docs"]["url"], "http://127.0.0.1:8080/mcp/docs");
        drop(guard);
        assert!(!path.exists(), "a file we created is removed on drop");
    }

    #[tokio::test]
    async fn existing_config_is_merged_then_restored() {
        let workspace = temp_workspace();
        let cursor_dir = workspace.path().join(".cursor");
        fs::create_dir_all(&cursor_dir).unwrap();
        let path = cursor_dir.join("mcp.json");
        let original = json!({ "mcpServers": { "other": { "url": "http://example" } } });
        fs::write(&path, serde_json::to_vec_pretty(&original).unwrap()).unwrap();

        let guard =
            McpGuard::install(workspace.path(), [("docs", "http://127.0.0.1:9/x")]).await.unwrap();
        let entries = read_servers(&path);
        assert_eq!(entries["docs"]["url"], "http://127.0.0.1:9/x");
        assert_eq!(entries["other"]["url"], "http://example", "existing servers survive");
        drop(guard);

        let restored: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(restored, original, "the original file is restored verbatim");
    }

    #[tokio::test]
    async fn overlapping_guards_are_serialized_per_workspace() {
        let workspace = temp_workspace();
        let path = workspace.path().join(".cursor/mcp.json");
        let first = McpGuard::install(workspace.path(), [("first", "http://127.0.0.1/first")])
            .await
            .unwrap();

        let workspace_path = workspace.path().to_path_buf();
        let (acquired_tx, mut acquired_rx) = oneshot::channel();
        let second = tokio::spawn(async move {
            let guard = McpGuard::install(&workspace_path, [("second", "http://127.0.0.1/second")])
                .await
                .unwrap();
            acquired_tx.send(()).unwrap();
            guard
        });

        assert!(
            timeout(Duration::from_millis(50), &mut acquired_rx).await.is_err(),
            "the second guard must wait for the first completion"
        );
        assert!(read_servers(&path).get("first").is_some());

        drop(first);
        timeout(Duration::from_secs(5), &mut acquired_rx)
            .await
            .expect("the second guard acquires after the first drops")
            .expect("acquisition sender remains live");
        let second = second.await.expect("joining second guard");
        let entries = read_servers(&path);
        assert!(entries.get("first").is_none(), "the first grant was restored before the second");
        assert!(entries.get("second").is_some());

        drop(second);
        assert!(!path.exists(), "the original absent config is restored");
    }

    #[tokio::test]
    async fn missing_git_root_is_initialized() {
        let workspace = temp_workspace();
        assert!(!workspace.path().join(".git").exists());
        ensure_git(workspace.path()).await.unwrap();
        assert!(workspace.path().join(".git").exists(), "git init creates a root");
    }

    #[tokio::test]
    async fn existing_git_root_is_preserved() {
        let workspace = temp_workspace();
        let git = workspace.path().join(".git");
        fs::create_dir_all(&git).unwrap();
        let marker = git.join("HEAD");
        fs::write(&marker, "ref: refs/heads/kept\n").unwrap();
        ensure_git(workspace.path()).await.unwrap();
        assert_eq!(fs::read_to_string(&marker).unwrap(), "ref: refs/heads/kept\n");
    }

    #[tokio::test]
    async fn git_worktree_file_is_preserved() {
        let workspace = temp_workspace();
        let git = workspace.path().join(".git");
        fs::write(&git, "gitdir: /tmp/worktree\n").unwrap();
        ensure_git(workspace.path()).await.unwrap();
        assert_eq!(fs::read_to_string(&git).unwrap(), "gitdir: /tmp/worktree\n");
        assert!(git.is_file(), "a worktree .git file is not replaced by a directory");
    }
}
