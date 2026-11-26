//! The backend's own repositories for a network remote: a mirror per URL,
//! the one repository a dial fetches into, and a staging per push, the one
//! it pushes from.
//!
//! A guest's repository never meets the network. A clone or a fetch fills
//! the mirror outside the boundary, then reads it into the place from inside
//! it, where the mirror is read-only. A push fills a staging from the place
//! through an upload-pack inside the boundary, which serves a fetch outside
//! it over a pipe, then pushes the staging. A process outside the boundary
//! reads only what the backend owns.

use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, ensure};
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;
use tokio::process::Command;

use crate::Client;
use crate::command::{self, Listing, Output, Pins, Run};

// Every branch and tag the remote has, and nothing it dropped.
const MIRRORED: [&str; 2] = ["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"];

// The branch a staging's fill lands on, the source of its push.
const STAGED: &str = "refs/heads/staged";

pub struct Mirror {
    dir: PathBuf,
    pins: Pins,
    // Held while the operation runs: one writer at a time, and no fetch
    // moves what a staging borrows.
    _lock: File,
}

impl Mirror {
    // The mirror of `url`, laid empty where there is none yet, and held for
    // this operation alone. Its directory is the URL's digest, so a token in
    // a URL names no directory.
    pub async fn open(client: &Client, url: &OsStr) -> Result<Self> {
        let name: String = Sha256::digest(url.as_bytes())
            .iter()
            .flat_map(|byte| [byte >> 4, byte & 0xf])
            .filter_map(|nibble| char::from_digit(nibble.into(), 16))
            .collect();
        let mirrors = client.host.mirrors();
        let path = mirrors.join(format!("{name}.lock"));
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        let lock = tokio::task::spawn_blocking(move || exclusive(lock))
            .await
            .context("locking a mirror")??;

        let dir = mirrors.join(format!("{name}.git"));
        if !dir.join("HEAD").exists() {
            lay(client, &dir).await?;
        }
        let listing = Listing::read_owned(client, &dir).await?;
        Ok(Self {
            pins: Pins::owned(&listing),
            dir,
            _lock: lock,
        })
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    // Fill the mirror from `url`, outside the boundary: every branch and
    // tag, pruned to the remote. Where `head`, the mirror's `HEAD` then
    // follows the remote's, for a clone to check out.
    pub async fn fetch(
        &self, client: &Client, url: &OsStr, head: bool, hold_pins: &[OsString],
    ) -> Result<Output> {
        let (protocol, pack) = client.transport_args(false, false);
        let env = env(&self.pins, protocol);
        let mut args =
            owned_args(&self.pins, hold_pins, ["fetch", "--quiet", "--prune", &pack, "--"]);
        args.push(url.to_owned());
        args.extend(MIRRORED.map(OsString::from));
        let output = client.run(Run::Owned(&self.dir), args, &env).await?;
        if !output.status.success() || !head {
            return Ok(output);
        }

        // the branch the remote's HEAD names, where it names one
        let mut args = owned_args(&self.pins, hold_pins, ["ls-remote", "--symref", &pack, "--"]);
        args.extend([url.to_owned(), OsString::from("HEAD")]);
        let listed = client.run(Run::Owned(&self.dir), args, &env).await?;
        if let Some(branch) = listed.status.success().then(|| head_branch(&listed.text())).flatten()
        {
            let args = ["symbolic-ref", "HEAD", &branch];
            let set = client.run(Run::Owned(&self.dir), args, &[]).await?;
            ensure!(set.status.success(), "setting a mirror's HEAD: {}", set.stderr.trim());
        }
        Ok(output)
    }
}

// Block until no other operation holds the mirror.
#[expect(unsafe_code, reason = "`flock` on a descriptor the file holds open")]
fn exclusive(lock: File) -> Result<File> {
    // SAFETY: the descriptor is the file's own, open for the call.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } == 0 {
        Ok(lock)
    } else {
        Err(io::Error::last_os_error()).context("locking a mirror")
    }
}

async fn lay(client: &Client, dir: &Path) -> Result<()> {
    let args = [OsStr::new("init"), OsStr::new("--quiet"), OsStr::new("--bare"), dir.as_os_str()];
    let output = client.run(Run::Backend, args, &[]).await?;
    ensure!(output.status.success(), "laying {}: {}", dir.display(), output.stderr.trim());
    Ok(())
}

// The branch `ls-remote --symref` printed for HEAD, none for a detached or
// missing one.
fn head_branch(listed: &str) -> Option<String> {
    let line = listed.lines().next()?;
    let branch = line.strip_prefix("ref: ")?.strip_suffix("\tHEAD")?;
    branch.starts_with("refs/heads/").then(|| branch.to_owned())
}

// A bare repository a push is laid in, beside every scratch, so no bounded
// process sees it, borrowing the mirror's objects, so a fill carries only
// what the mirror lacks. Removed when it is dropped.
pub struct Staging {
    dir: TempDir,
    pins: Pins,
}

impl Staging {
    pub async fn lay(client: &Client, mirror: &Mirror) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("staging-")
            .tempdir_in(client.host.root())
            .context("creating a push's staging")?;
        lay(client, dir.path()).await?;
        let alternates = dir.path().join("objects/info/alternates");
        let line = [mirror.path().join("objects").as_os_str().as_bytes(), b"\n"].concat();
        fs::write(&alternates, line)
            .with_context(|| format!("writing {}", alternates.display()))?;
        let listing = Listing::read_owned(client, dir.path()).await?;
        Ok(Self {
            pins: Pins::owned(&listing),
            dir,
        })
    }

    // The fetch of `commit` into the staging over `fds`, the two ends of the
    // pipes an upload-pack serves it on, outside the boundary.
    pub fn fill(&self, client: &Client, commit: &str, fds: [OwnedFd; 2]) -> Result<Command> {
        let refspec = format!("+{commit}:{STAGED}");
        let args =
            owned_args(&self.pins, &[], ["fetch", "--quiet", "--no-tags", "fd::3,4", &refspec]);
        let env = env(&self.pins, "fd");
        let mut fill = client.command(Run::Owned(self.dir.path()), args, &env)?;
        command::hand(&mut fill, fds);
        Ok(fill)
    }

    // Push what the fill staged to `url` as `full`, outside the boundary.
    pub async fn push(
        &self, client: &Client, url: &OsStr, full: &str, hold_pins: &[OsString],
    ) -> Result<Output> {
        let (protocol, pack) = client.transport_args(false, true);
        let mut args = owned_args(&self.pins, hold_pins, ["push", "--quiet", &pack, "--"]);
        args.extend([url.to_owned(), OsString::from(format!("{STAGED}:{full}"))]);
        client.run(Run::Owned(self.dir.path()), args, &env(&self.pins, protocol)).await
    }
}

fn owned_args<const N: usize>(
    pins: &Pins, hold_pins: &[OsString], args: [&str; N],
) -> Vec<OsString> {
    pins.args()
        .map(OsStr::to_owned)
        .chain(hold_pins.iter().cloned())
        .chain(args.map(OsString::from))
        .collect()
}

fn env<'a>(pins: &'a Pins, protocol: &'a str) -> Vec<(&'a str, &'a OsStr)> {
    pins.env().chain([("GIT_ALLOW_PROTOCOL", OsStr::new(protocol))]).collect()
}
