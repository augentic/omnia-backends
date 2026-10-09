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
// anchored to the root; `?` and `*` stay within a component, `**/` crosses
// whole components, and any other `**` crosses anything.
fn matches(pattern: &str, path: &str) -> bool {
    if pattern.contains('/') {
        let anchored = pattern.strip_prefix('/').unwrap_or(pattern);
        glob(anchored.as_bytes(), path.as_bytes())
    } else {
        let name = path.rsplit('/').next().unwrap_or(path);
        glob(pattern.as_bytes(), name.as_bytes())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token {
    Byte(u8),
    One,
    Star,
    Dirs,
    All,
}

impl Token {
    const fn wild(self) -> bool {
        matches!(self, Self::Star | Self::Dirs | Self::All)
    }

    const fn needs_byte(self) -> bool {
        matches!(self, Self::Byte(_) | Self::One)
    }
}

// The pattern as tokens. Adjacent wildcards fold — two of a kind into one,
// any beside `**` into `**` — so a run of them is at most `**/*`, and the
// token count is bounded by the bytes the path must supply.
fn tokens(pattern: &[u8]) -> Vec<Token> {
    let mut tokens: Vec<Token> = Vec::new();
    let mut rest = pattern;
    while let Some((&byte, after)) = rest.split_first() {
        let (token, after) = match byte {
            b'*' if after.first() == Some(&b'*') => after[1..]
                .strip_prefix(b"/")
                .map_or_else(|| (Token::All, &after[1..]), |after| (Token::Dirs, after)),
            b'*' => (Token::Star, after),
            b'?' => (Token::One, after),
            byte => (Token::Byte(byte), after),
        };
        rest = after;
        match tokens.last_mut() {
            Some(last) if last.wild() && token.wild() => {
                if *last == Token::All || token == Token::All {
                    *last = Token::All;
                } else if *last != token {
                    tokens.push(token);
                }
            }
            _ => tokens.push(token),
        }
    }
    tokens
}

// Whether `pattern` matches `text` whole, by a table of which prefixes of
// each match: one row per token over every cut of the text, so a pattern of
// many stars costs its length times the text's and never backtracks.
fn glob(pattern: &[u8], text: &[u8]) -> bool {
    let tokens = tokens(pattern);
    if tokens.iter().filter(|token| token.needs_byte()).count() > text.len() {
        return false;
    }

    // row[j] holds once the tokens so far match text[..j]
    let mut row = vec![false; text.len() + 1];
    let mut next = vec![false; text.len() + 1];
    row[0] = true;
    for token in tokens {
        next.fill(false);
        match token {
            Token::Byte(want) => {
                for (j, &byte) in text.iter().enumerate() {
                    next[j + 1] = row[j] && byte == want;
                }
            }
            Token::One => {
                for (j, &byte) in text.iter().enumerate() {
                    next[j + 1] = row[j] && byte != b'/';
                }
            }
            Token::Star => {
                next[0] = row[0];
                for (j, &byte) in text.iter().enumerate() {
                    next[j + 1] = row[j + 1] || (next[j] && byte != b'/');
                }
            }
            Token::All => {
                next[0] = row[0];
                for j in 0..text.len() {
                    next[j + 1] = row[j + 1] || next[j];
                }
            }
            Token::Dirs => {
                // a component ending at j is swallowed once any earlier cut matched
                let mut open = row[0];
                next[0] = row[0];
                for (j, &byte) in text.iter().enumerate() {
                    next[j + 1] = row[j + 1] || (open && byte == b'/');
                    open |= row[j + 1];
                }
            }
        }
        std::mem::swap(&mut row, &mut next);
    }
    row[text.len()]
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
        assert!(matches("src/**/main.ts", "src/main.ts"), "or crosses nothing");
        assert!(!matches("**/main.ts", "a/bmain.ts"), "and crosses whole components");
        assert!(matches("src/**", "src/a/b.ts"), "a trailing double star takes all beneath");
        assert!(matches("a?c", "abc"));
        assert!(!matches("a?c", "a/c"), "a question mark stays within a component");
        assert!(matches("/README.md", "README.md"), "a leading slash anchors");
        assert!(!matches("/README.md", "docs/README.md"), "to the root alone");
    }

    // A pattern of many stars against a path of the bytes between them is the
    // backtracking matcher's worst case; the table costs the product of their
    // lengths, and a pattern longer than the path can supply is refused in the
    // pattern's length.
    #[test]
    fn glob_stars() {
        let pattern = format!("{}b", "*a".repeat(64));
        assert!(!matches(&pattern, &"a".repeat(256)));
        assert!(matches(&pattern, &format!("{}b", "a".repeat(256))));
        assert!(!matches(&"?".repeat(1 << 20), "a/b/c"));
        assert!(!matches(&"**/".repeat(1 << 18), "a/b/c"), "folded to one crossing");
        assert!(matches(&format!("{}c", "**/".repeat(1 << 18)), "a/b/c"));
    }
}
