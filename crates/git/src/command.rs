//! One `git` process: spawned at a directory with the operator's repository
//! selection scrubbed from its environment, its output captured whole.
//!
//! The process leads a group of its own and is killed when its future is
//! dropped, so a helper git forks — a credential helper, `ssh`, a merge
//! driver — never outlives the operation that started it.

use std::ffi::OsStr;
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use anyhow::{Context as _, Result};
use process_wrap::tokio::{CommandWrap, KillOnDrop, ProcessGroup};
use tokio::process::Command;

use crate::Client;

// The repository selection a shell exports; left in place, it would point
// every operation at the operator's own repository rather than the location
// the guest named.
const SCRUBBED: [&str; 4] = ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

// What one git process said, both streams read lossily: a path git prints
// is a WIT string on its way back to the guest.
#[derive(Debug)]
pub struct Output {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

impl Client {
    // Run git at `at` — none for a command that reads no repository — and
    // wait for it; a failure here is the spawn's, never git's own exit.
    pub(crate) async fn git<I, S>(&self, at: Option<&Path>, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(&self.binary);
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        if let Some(at) = at {
            command.arg("-C").arg(at);
        }
        command.args(args);
        for var in SCRUBBED {
            command.env_remove(var);
        }

        let mut command = CommandWrap::from(command);
        command.wrap(KillOnDrop);
        command.wrap(ProcessGroup::leader());
        let child = command
            .spawn()
            .with_context(|| format!("issue spawning `{}`", self.binary.display()))?;
        let output = Box::into_pin(child.wait_with_output())
            .await
            .with_context(|| format!("issue waiting for `{}`", self.binary.display()))?;

        Ok(Output {
            status: output.status,
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}
