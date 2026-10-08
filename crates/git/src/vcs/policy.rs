//! Resolving a merge by the guest's policy, from the commit trees rather than
//! git's own attribute machinery — which a repository pre-empts with a
//! `.gitattributes` or an `info/attributes` of its own.
//!
//! The merge runs held before its commit, so the index is the backend's to
//! settle. Every path the two sides both touch is checked against the rules:
//! a path a rule matches is resolved from the three commits — `ours` the
//! working copy's side, `theirs` the merged-in side, `union` both, each line
//! once — whatever git's attributes did to it, byte for byte, and a path
//! still in conflict that no rule matched abandons the whole merge. Nothing
//! of the resolution touches the working tree but the resolved path itself.

use std::ffi::OsStr;
use std::io::Write as _;

use anyhow::{Context as _, Result, bail};
use omnia_wasi_vcs::{Rule, Strategy};
use tempfile::NamedTempFile;

use super::Repo;

// Where the two sides of a path are read from.
const OURS: &str = "HEAD";
const THEIRS: &str = "MERGE_HEAD";

// Whether the merge can be sealed, or the paths no rule resolved.
pub enum Resolution {
    Complete,
    Unresolved(Vec<String>),
}

pub async fn resolve(repo: &Repo<'_>, rules: &[Rule]) -> Result<Resolution> {
    let base = repo.merge_base().await?;
    for path in repo.touched().await? {
        if let Some(strategy) = rule_for(&path, rules) {
            apply(repo, &path, strategy, base.as_deref()).await?;
        }
    }
    // what a rule resolved is staged; a conflict still unmerged is a path no
    // rule matched
    let unresolved = repo.conflicts().await?;
    if unresolved.is_empty() {
        Ok(Resolution::Complete)
    } else {
        Ok(Resolution::Unresolved(unresolved))
    }
}

fn rule_for(path: &str, rules: &[Rule]) -> Option<Strategy> {
    rules.iter().find(|rule| matches(&rule.paths, path)).map(|rule| rule.strategy)
}

async fn apply(repo: &Repo<'_>, path: &str, strategy: Strategy, base: Option<&str>) -> Result<()> {
    match strategy {
        Strategy::Ours => keep(repo, path, OURS).await,
        Strategy::Theirs => keep(repo, path, THEIRS).await,
        Strategy::Union => union(repo, path, base).await,
    }
}

// The kept side laid whole over the path, or the path removed where that side
// deleted it.
async fn keep(repo: &Repo<'_>, path: &str, side: &str) -> Result<()> {
    match repo.show(side, path).await? {
        Some(content) => {
            repo.write(path, &content)?;
            repo.run(["add", "--", path], &repo.shown(), path).await?;
        }
        None => {
            repo.run(["rm", "--quiet", "--ignore-unmatch", "--", path], &repo.shown(), path)
                .await?;
        }
    }
    Ok(())
}

// Both sides' lines, each once: a 3-way union through `merge-file` over the
// three commits' versions, laid in the host's temporary directory so nothing
// of them touches the working tree however the merge ends. A side git cannot
// merge as lines — a binary — fails the merge rather than seal a guess.
async fn union(repo: &Repo<'_>, path: &str, base: Option<&str>) -> Result<()> {
    let ours = side(repo, Some(OURS), path).await?;
    let basis = side(repo, base, path).await?;
    let theirs = side(repo, Some(THEIRS), path).await?;
    let args = [
        OsStr::new("merge-file"),
        OsStr::new("-p"),
        OsStr::new("--union"),
        ours.path().as_os_str(),
        basis.path().as_os_str(),
        theirs.path().as_os_str(),
    ];
    let merged = repo.git(args, &[]).await?;
    if !merged.status.success() {
        bail!("{}: cannot union {path}: {}", repo.shown(), merged.stderr.trim());
    }
    repo.write(path, &merged.stdout)?;
    repo.run(["add", "--", path], &repo.shown(), path).await?;
    Ok(())
}

// One commit's version of the path in a temporary file, empty where the
// commit lacks it or the sides share no ancestor.
async fn side(repo: &Repo<'_>, revision: Option<&str>, path: &str) -> Result<NamedTempFile> {
    let content = match revision {
        Some(revision) => repo.show(revision, path).await?.unwrap_or_default(),
        None => Vec::new(),
    };
    let mut file = NamedTempFile::new().context("creating a merge temporary")?;
    file.write_all(&content).context("writing a merge temporary")?;
    Ok(file)
}

// A glob over root-relative paths, in gitignore's reading: a pattern with no
// slash matches the path's final component at any depth, one with a slash is
// anchored to the root; `?` and `*` stay within a component and `**` crosses.
fn matches(pattern: &str, path: &str) -> bool {
    let pattern = pattern.strip_prefix('/').unwrap_or(pattern);
    if pattern.contains('/') {
        glob(pattern.as_bytes(), path.as_bytes())
    } else {
        let name = path.rsplit('/').next().unwrap_or(path);
        glob(pattern.as_bytes(), name.as_bytes())
    }
}

fn glob(pattern: &[u8], text: &[u8]) -> bool {
    if let Some(rest) = pattern.strip_prefix(b"**") {
        let rest = rest.strip_prefix(b"/").unwrap_or(rest);
        return (0..=text.len()).any(|cut| glob(rest, &text[cut..]));
    }
    match (pattern.first(), text.first()) {
        (None, None) => true,
        (Some(b'*'), _) => {
            glob(&pattern[1..], text)
                || matches!(text.first(), Some(&byte) if byte != b'/') && glob(pattern, &text[1..])
        }
        (Some(b'?'), Some(&byte)) => byte != b'/' && glob(&pattern[1..], &text[1..]),
        (Some(&want), Some(&byte)) if want == byte => glob(&pattern[1..], &text[1..]),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::matches;

    #[test]
    fn globs() {
        assert!(matches("*.lock", "Cargo.lock"));
        assert!(matches("*.lock", "deep/nested/Cargo.lock"), "a bare name matches at any depth");
        assert!(matches("a.txt", "a.txt"));
        assert!(matches("list.txt", "list.txt"));
        assert!(matches("src/index.ts", "src/index.ts"), "a slashed pattern is anchored");
        assert!(!matches("src/index.ts", "lib/src/index.ts"), "and not matched elsewhere");
        assert!(matches("src/*.ts", "src/main.ts"));
        assert!(!matches("src/*.ts", "src/sub/main.ts"), "a star stays within a component");
        assert!(matches("src/**/main.ts", "src/a/b/main.ts"), "a double star crosses");
        assert!(matches("/README.md", "README.md"), "a leading slash anchors");
    }
}
