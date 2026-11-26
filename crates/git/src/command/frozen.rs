//! A frozen copy of one repository's configuration, which every process of
//! an operation reads as the repository's own.
//!
//! The copy is a directory the backend owns, handed to git as
//! `GIT_COMMON_DIR`. It holds a `config` written from the operation's
//! listing and a link to every other entry of the repository's common
//! directory, so objects, refs, and reflogs land in the repository while
//! what git reads of its configuration is exactly what the pins were built
//! from. The listing already expanded the repository's includes and its
//! worktree configuration, so the copy carries their entries and neither
//! switch, which would send git back to the files a guest can rewrite.

use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;

use anyhow::{Context as _, Result};
use tempfile::TempDir;

use super::Listing;

// The common files git writes through a lock beside them. git takes the lock
// beside a link's target, so a write to a file the copy links lands in the
// repository. The copy's own `config` is no link, and neither is a
// `packed-refs` or `shallow` the repository lacks. A write to one of those
// would land in the copy and be lost, so a lock held in its place makes the
// write fail instead.
const GUARDED: [&str; 3] = ["config", "packed-refs", "shallow"];

#[derive(Debug)]
pub struct Frozen {
    dir: TempDir,
}

impl Frozen {
    // Lay the copy for the repository whose common directory is `common`.
    pub fn lay(common: &Path, listing: &Listing) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("omnia-git-")
            .tempdir()
            .context("creating a frozen configuration")?;
        let root = dir.path();

        // the configuration as listed
        fs::write(root.join("config"), config(listing))
            .context("writing a frozen configuration")?;

        // HACK: a directory where git takes a guarded file's lock fails its exclusive create
        for name in GUARDED {
            fs::create_dir(root.join(format!("{name}.lock")))
                .context("guarding a frozen configuration")?;
        }

        // the repository's reflogs, made now where it has kept none yet, so
        // the link below leads them into the repository
        let _ = fs::create_dir(common.join("logs"));

        // every other entry, linked
        let entries =
            fs::read_dir(common).with_context(|| format!("reading {}", common.display()))?;
        for entry in entries {
            let name = entry.with_context(|| format!("reading {}", common.display()))?.file_name();
            let link = root.join(&name);
            if link.symlink_metadata().is_ok() {
                continue;
            }
            symlink(common.join(&name), &link)
                .with_context(|| format!("linking {}", link.display()))?;
        }

        Ok(Self { dir })
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }
}

// The repository's entries as git reads them back: a section header for
// each, so order and repeats stand as listed, and every value quoted.
fn config(listing: &Listing) -> Vec<u8> {
    let mut file = Vec::new();
    for (key, value) in listing.repo_entries() {
        let (section, rest) = key.split_once('.').unwrap_or((key, ""));
        if matches!(section, "include" | "includeif") || key == "extensions.worktreeconfig" {
            continue;
        }
        file.push(b'[');
        file.extend_from_slice(section.as_bytes());
        let name = match rest.rsplit_once('.') {
            Some((subsection, name)) => {
                file.push(b' ');
                quote(&mut file, subsection.as_bytes());
                name
            }
            None => rest,
        };
        file.extend_from_slice(b"]\n\t");
        file.extend_from_slice(name.as_bytes());
        if let Some(value) = value {
            file.extend_from_slice(b" = ");
            quote(&mut file, value);
        }
        file.push(b'\n');
    }
    file
}

// A quoted string as git's parser reads one back, a subsection or a value
// alike: a backslash, a quote, or a newline escaped, everything else as is.
fn quote(file: &mut Vec<u8>, raw: &[u8]) {
    file.push(b'"');
    for &byte in raw {
        match byte {
            b'\\' => file.extend_from_slice(b"\\\\"),
            b'"' => file.extend_from_slice(b"\\\""),
            b'\n' => file.extend_from_slice(b"\\n"),
            byte => file.push(byte),
        }
    }
    file.push(b'"');
}
