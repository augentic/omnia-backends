#![doc = include_str!("../README.md")]

mod command;
mod vcs;

use std::collections::BTreeMap;
use std::fmt;
use std::path::{self, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use anyhow::{Context as _, Result, ensure};
use omnia::Backend;
use omnia_wasi_vcs::Place;
use tracing::instrument;

// First git with `config --show-scope`, which host-policy pins read.
const MIN_VERSION: (u32, u32) = (2, 26);

/// Git backend for `omnia:vcs`: one `git` process per operation.
#[derive(Clone)]
pub struct Client {
    binary: String,
    // Working-copy path to the recorded gitdir `add`. Guests can rewrite `.git` files.
    worktrees: Arc<Mutex<BTreeMap<PathBuf, PathBuf>>>,
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "Git::connect")]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let client = Self {
            binary: options.binary,
            worktrees: Arc::default(),
        };

        // check git version
        let output = client.git(None, ["--version"], &[]).await?;
        ensure!(output.status.success(), "`git --version` failed: {}", output.stderr.trim());

        let reported = output.text();
        let (major, minor, patch) = version(&reported)
            .with_context(|| format!("unrecognised `git --version`: {}", reported.trim()))?;
        ensure!(
            (major, minor) >= MIN_VERSION,
            "git {major}.{minor}.{patch} is older than {}.{}, the first with `config --show-scope`",
            MIN_VERSION.0,
            MIN_VERSION.1
        );

        Ok(client)
    }
}

impl Client {
    // Record the gitdir a working copy at `at` was laid with.
    fn register(&self, at: &Place, gitdir: PathBuf) {
        if let Some(worktree) = worktree_key(at) {
            self.worktrees().insert(worktree, gitdir);
        }
    }

    // Drop a working copy's gitdir once the copy is gone, or once the place
    // is laid afresh as a repository of its own.
    fn forget(&self, at: &Place) {
        if let Some(worktree) = worktree_key(at) {
            self.worktrees().remove(&worktree);
        }
    }

    // The gitdir a working copy at `at` is addressed through, none for a
    // place this client did not lay.
    fn gitdir(&self, at: &Place) -> Option<PathBuf> {
        worktree_key(at).and_then(|worktree| self.worktrees().get(&worktree).cloned())
    }

    fn worktrees(&self) -> MutexGuard<'_, BTreeMap<PathBuf, PathBuf>> {
        self.worktrees.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").field("binary", &self.binary).finish_non_exhaustive()
    }
}

// The place's path as `add` hands it to git, which is how its gitdir is
// keyed; the runtime spells a location the same way every time it resolves.
fn worktree_key(at: &Place) -> Option<PathBuf> {
    path::absolute(at.path()).ok()
}

// `git version 2.50.1 (Apple Git-155)` and `git version 2.47.0.windows.1`
// both read as their first three numbers; a missing patch is zero.
fn version(stdout: &str) -> Option<(u32, u32, u32)> {
    let rest = stdout.trim().strip_prefix("git version ")?;
    let token = rest.split_whitespace().next()?;
    let mut parts = token.split('.').map(leading_number);
    let major = parts.next().flatten()?;
    let minor = parts.next().flatten()?;
    let patch = parts.next().flatten().unwrap_or(0);
    Some((major, minor, patch))
}

fn leading_number(part: &str) -> Option<u32> {
    let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the git backend.
    #[derive(Clone, Debug, FromEnv)]
    pub struct ConnectOptions {
        /// The git to run: a name on `PATH` or a path.
        #[env(from = "GIT_BINARY", default = "git")]
        pub binary: String,
    }
}
pub use config::ConnectOptions;

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            binary: "git".to_owned(),
        }
    }
}

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}

// Reading `git --version` is pure over text; `tests/connect.rs` drives the
// startup refusal over a scripted git.
#[cfg(test)]
mod tests {
    use super::version;

    #[test]
    fn versions() {
        assert_eq!(version("git version 2.50.1 (Apple Git-155)\n"), Some((2, 50, 1)));
        assert_eq!(version("git version 2.47.0.windows.1"), Some((2, 47, 0)));
        assert_eq!(version("git version 2.4.0"), Some((2, 4, 0)));
        assert_eq!(version("git version 2.5"), Some((2, 5, 0)));
        assert_eq!(version("git version 2.39.5-rc1"), Some((2, 39, 5)));
        assert_eq!(version("not git"), None);
        assert_eq!(version("git version"), None);
        assert_eq!(version("git version x.y"), None);
    }
}
