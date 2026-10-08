//! One `git` process: spawned in the place the runtime opened, with the
//! operator's repository selection scrubbed from its environment, its
//! output captured whole.
//!
//! The child enters the place's open directory handle before it executes,
//! so git works where the runtime resolved the guest's location and never
//! walks a path a guest could redirect meanwhile. The process leads a group
//! of its own and is killed when its future is dropped, so a helper git
//! forks — a credential helper, `ssh`, a merge driver — never outlives the
//! operation that started it.

mod pins;

use std::borrow::Cow;
use std::ffi::OsStr;
use std::io;
use std::os::fd::{AsRawFd as _, RawFd};
use std::process::{ExitStatus, Stdio};

use anyhow::{Context as _, Result};
use omnia_wasi_vcs::Place;
use process_wrap::tokio::{CommandWrap, KillOnDrop, ProcessGroup};
use tokio::process::Command;

pub use self::pins::Pins;
use crate::Client;

// The repository selection a shell exports; left in place, it would point
// every operation at the operator's own repository rather than the location
// the guest named.
const SCRUBBED: [&str; 4] = ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"];

// What an interactive git reads from a terminal, an editor, or a prompt —
// none answerable from a guest, so each is closed before git can block on
// it or run a program to fill it — and the protocols any process may speak:
// a lazy fetch from a promisor remote the repository names would otherwise
// reach an `ext::` command or a remote helper under whatever
// `protocol.<name>.allow` the repository set. A transport operation names
// the list for its own kind in `env`, which applies after.
const FIXED: [(&str, &str); 4] = [
    ("GIT_TERMINAL_PROMPT", "0"),
    ("GIT_EDITOR", ":"),
    ("GIT_MERGE_AUTOEDIT", "no"),
    ("GIT_ALLOW_PROTOCOL", "http:https:ssh"),
];

// What one git process said: stdout as the bytes git wrote, since a blob
// read for a merge must round-trip whole, and stderr read lossily, since it
// is only ever quoted back to the guest.
#[derive(Debug)]
pub struct Output {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: String,
}

impl Output {
    // stdout as text, for a hash, a path, or a listing on its way to the guest.
    pub fn text(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.stdout)
    }
}

impl Client {
    // Run git in `at` — none for a command that reads no repository — with
    // `env` set on top of the fixed interactive lockout, and wait for it; a
    // failure here is the spawn's, never git's own exit.
    pub(crate) async fn git<I, S>(
        &self, at: Option<&Place>, args: I, env: &[(&str, &str)],
    ) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(&self.binary);
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        if let Some(place) = at {
            enter(&mut command, place.dir().as_raw_fd());
        }
        command.args(args);
        for var in SCRUBBED {
            command.env_remove(var);
        }
        for (key, value) in FIXED.iter().chain(env) {
            command.env(key, value);
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
            stdout: output.stdout,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

// The child's working directory is the open handle itself, entered between
// `fork` and `exec`: the descriptor is inherited across the fork, the place
// that owns it is held until `spawn` returns, and `fchdir` is
// async-signal-safe, so the hook runs nothing a forked child may not.
#[expect(
    unsafe_code,
    reason = "`pre_exec` runs in the forked child; `fchdir` is async-signal-safe"
)]
fn enter(command: &mut Command, dir: RawFd) {
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(dir) == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
        });
    }
}
