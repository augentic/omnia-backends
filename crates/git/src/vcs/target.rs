//! What a transport operation dials, resolved once before anything dials.
//!
//! A local address is read as git reads it, placed beneath the guest's
//! mounts, and held to the exact gitdir it names, so git's own search beside
//! or inside it has nothing to find. Any other address is the network's, as
//! the host's configuration expands it. Either is refused where the host's
//! rewrites would carry git past what was resolved.

use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::path::{self, Path, PathBuf};

use anyhow::{Context as _, Result};
use omnia_wasi_vcs::{Error, Place};

use crate::Client;
use crate::command::Listing;

pub enum Target {
    // the exact gitdir of a repository beneath the guest's mounts
    Local(Place),
    // the address as the host's configuration expands it
    Network(OsString),
}

impl Target {
    // What `address` dials from `at`, held to the guest's mounts for writing
    // where `write`. `missing` is what a refusal names where the address
    // holds no repository.
    pub async fn resolve(
        client: &Client, at: &Place, address: &OsStr, missing: &str, write: bool,
    ) -> Result<Self> {
        if !is_local(&address.to_string_lossy()) {
            return Ok(Self::Network(address.to_owned()));
        }
        let not_found = || Error::NotFound(missing.to_owned());
        let Some(path) = local_path(address.as_bytes()) else {
            return Err(not_found().into());
        };
        let located = at.locate(&path, write).map_err(|error| match error {
            Error::NotFound(_) => not_found(),
            error => error,
        })?;

        // the repository's exact gitdir, and the common directory it shares,
        // each beneath the mounts too
        let Some(gitdir) = client.exact_gitdir(&located).await? else {
            return Err(not_found().into());
        };
        let gitdir = if gitdir == Path::new(".") {
            located
        } else {
            located.locate(&located.path().join(gitdir), write)?
        };
        if let Some(common) = common_dir(&gitdir)? {
            gitdir.locate(&common, write)?;
        }

        // receive-pack, which has no `--strict`, tries the gitdir's own `.git` first
        if write && gitdir.dir().symlink_metadata(".git").is_ok() {
            let message = format!("{}: the repository holds a .git of its own", path.display());
            return Err(Error::Denied(message).into());
        }
        Ok(Self::Local(gitdir))
    }

    // The address git is handed: a local repository's gitdir by its absolute
    // path, never the spelling it was resolved from.
    pub fn dial(&self) -> Result<OsString> {
        match self {
            Self::Local(gitdir) => Ok(path::absolute(gitdir.path())
                .with_context(|| format!("resolving {}", gitdir.path().display()))?
                .into_os_string()),
            Self::Network(address) => Ok(address.clone()),
        }
    }

    // Refused where the host's configuration rewrites the address git is
    // handed, which would dial past what was resolved.
    pub fn hold_rewrites(&self, listing: &Listing, push: bool) -> Result<()> {
        hold_rewrites(&self.dial()?, listing, push)
    }
}

// `address`, refused where the host's rewrites would change it.
pub fn hold_rewrites(address: &OsStr, listing: &Listing, push: bool) -> Result<()> {
    let Some(rule) = listing.rewrite(address.as_bytes(), push) else {
        return Ok(());
    };
    let shown = address.to_string_lossy();
    Err(Error::Denied(format!("{shown}: the host's {rule} rewrites the address git dials")).into())
}

// git's own rule: a `file://` URL, or an address with no scheme and no
// scp-like `host:path`, is a local path; everything else crosses a network.
pub fn is_local(address: &str) -> bool {
    if let Some(scheme) = address.split_once("://") {
        return scheme.0.eq_ignore_ascii_case("file");
    }
    match (address.find(':'), address.find('/')) {
        (Some(colon), Some(slash)) => colon > slash,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

// The path git reads a local address as: a `file://` URL percent-decoded
// whole, whatever host it names dropped, and a plain path as is. None for a
// URL that names no path.
fn local_path(address: &[u8]) -> Option<PathBuf> {
    if !address.windows(3).any(|window| window == b"://") {
        return Some(PathBuf::from(OsStr::from_bytes(address)));
    }
    let decoded = percent_decoded(address);
    let host = decoded.windows(3).position(|window| window == b"://")? + 3;
    let path = host + decoded[host..].iter().position(|&byte| byte == b'/')?;
    Some(PathBuf::from(OsString::from_vec(decoded[path..].to_vec())))
}

// Every `%` and two hex digits as the byte they spell; any other `%` stays.
fn percent_decoded(raw: &[u8]) -> Vec<u8> {
    let mut decoded = Vec::with_capacity(raw.len());
    let mut rest = raw;
    while let Some((&byte, tail)) = rest.split_first() {
        let escaped = match tail {
            [high, low, ..] if byte == b'%' => hex(*high).zip(hex(*low)),
            _ => None,
        };
        if let Some((high, low)) = escaped {
            decoded.push(high << 4 | low);
            rest = &tail[2..];
        } else {
            decoded.push(byte);
            rest = tail;
        }
    }
    decoded
}

fn hex(digit: u8) -> Option<u8> {
    char::from(digit).to_digit(16).and_then(|value| u8::try_from(value).ok())
}

// The common directory a gitdir's `commondir` names, joined to the gitdir
// where relative; none where the gitdir is its own. git trims only the line
// ending from the file.
fn common_dir(gitdir: &Place) -> Result<Option<PathBuf>> {
    let shown = gitdir.path().display();
    let named = match gitdir.dir().read("commondir") {
        Ok(named) => named,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            let message = format!("{shown}: its commondir leads out of its mount");
            return Err(Error::Denied(message).into());
        }
        Err(error) => return Err(error).with_context(|| format!("reading {shown}/commondir")),
    };
    let end = named.iter().rposition(|&byte| !matches!(byte, b'\n' | b'\r')).map_or(0, |at| at + 1);
    Ok(Some(gitdir.path().join(OsStr::from_bytes(&named[..end]))))
}

impl Client {
    // The gitdir the directory at `at` holds exactly, as
    // `rev-parse --resolve-git-dir` reads one: its `.git`, a directory or the
    // file naming one, else the directory itself. None where neither is. A
    // `.git` file or link is followed here first, since a bounded git cannot
    // see the gitdir it leads to outside the guest's mounts, which is the
    // backend's to refuse.
    async fn exact_gitdir(&self, at: &Place) -> Result<Option<PathBuf>> {
        if let Some(named) = led_to(at)? {
            at.locate(&named, false)?;
        }
        let candidates =
            if at.dir().symlink_metadata(".git").is_ok() { &[".git", "."][..] } else { &["."] };
        for candidate in candidates {
            let output =
                self.git(Some(at), ["rev-parse", "--resolve-git-dir", candidate], &[]).await?;
            if output.status.success() {
                let printed = output.stdout.strip_suffix(b"\n").unwrap_or(&output.stdout);
                return Ok(Some(PathBuf::from(OsStr::from_bytes(printed))));
            }
        }
        Ok(None)
    }
}

// Where the `.git` at `at` leads as git reads it: a link to its target, and
// a file to the gitdir its `gitdir: ` line names, relative to `at`. None for
// no `.git`, a directory, or a file git would not read as naming one.
fn led_to(at: &Place) -> Result<Option<PathBuf>> {
    let gitfile = at.path().join(".git");
    let Ok(metadata) = at.dir().symlink_metadata(".git") else {
        return Ok(None);
    };
    if metadata.is_symlink() {
        return Ok(Some(gitfile));
    }
    if !metadata.is_file() {
        return Ok(None);
    }
    let read = at.dir().read(".git").with_context(|| format!("reading {}", gitfile.display()))?;
    let Some(named) = read.strip_prefix(b"gitdir: ") else {
        return Ok(None);
    };
    let end = named.iter().rposition(|&byte| !matches!(byte, b'\n' | b'\r')).map_or(0, |at| at + 1);
    Ok(Some(at.path().join(OsStr::from_bytes(&named[..end]))))
}
