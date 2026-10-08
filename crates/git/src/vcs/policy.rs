//! A merge policy as git reads it: attribute lines in a file beside the
//! tree, named on the merge command line through `core.attributesFile`.
//!
//! Nothing is written into the repository — a tracked `.gitattributes` would
//! show up in `pending`, and `info/attributes` is the operator's — so a
//! policy lives exactly as long as its merge. `union` is git's own driver;
//! `ours` and `theirs` are declared beside the file as drivers that keep one
//! side whole.

use std::io::Write as _;

use anyhow::{Context as _, Result};
use omnia_wasi_vcs::{Rule, Strategy};
use tempfile::NamedTempFile;

const OURS: &str = "omnia-ours";
const THEIRS: &str = "omnia-theirs";

// The policy's file, kept open until the merge has run.
#[derive(Debug)]
pub struct Policy {
    attributes: Option<NamedTempFile>,
}

impl Policy {
    pub fn write(rules: &[Rule]) -> Result<Self> {
        if rules.is_empty() {
            return Ok(Self { attributes: None });
        }
        let mut file = NamedTempFile::with_prefix("omnia-git-policy-")
            .context("creating the merge policy file")?;
        for rule in rules {
            writeln!(file, "{} merge={}", pattern(&rule.paths), driver(rule.strategy))
                .context("writing the merge policy")?;
        }
        file.flush().context("writing the merge policy")?;
        Ok(Self {
            attributes: Some(file),
        })
    }

    // The `-c` settings that put the policy on one merge's command line.
    pub fn args(&self) -> Vec<String> {
        let Some(file) = &self.attributes else {
            return Vec::new();
        };
        vec![
            "-c".to_owned(),
            format!("core.attributesFile={}", file.path().display()),
            "-c".to_owned(),
            format!("merge.{OURS}.driver=true"),
            "-c".to_owned(),
            format!("merge.{THEIRS}.driver=cp %B %A"),
        ]
    }
}

const fn driver(strategy: Strategy) -> &'static str {
    match strategy {
        Strategy::Union => "union",
        Strategy::Ours => OURS,
        Strategy::Theirs => THEIRS,
    }
}

// gitattributes reads a quoted pattern where the glob holds whitespace
fn pattern(paths: &str) -> String {
    if paths.chars().any(char::is_whitespace) {
        format!("\"{}\"", paths.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        paths.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use omnia_wasi_vcs::{Rule, Strategy};

    use super::Policy;

    fn rule(paths: &str, strategy: Strategy) -> Rule {
        Rule {
            paths: paths.to_owned(),
            strategy,
        }
    }

    #[test]
    fn empty_policy() {
        let policy = Policy::write(&[]).unwrap();
        assert_eq!(policy.args(), Vec::<String>::new());
    }

    #[test]
    fn attribute_lines() {
        let policy = Policy::write(&[
            rule("*.lock", Strategy::Ours),
            rule("src/index.ts", Strategy::Union),
            rule("docs/release notes.md", Strategy::Theirs),
        ])
        .unwrap();
        let args = policy.args();
        assert_eq!(args.len(), 6);
        let file = args[1].strip_prefix("core.attributesFile=").unwrap();
        assert_eq!(
            fs::read_to_string(file).unwrap(),
            "*.lock merge=omnia-ours\n\
             src/index.ts merge=union\n\
             \"docs/release notes.md\" merge=omnia-theirs\n"
        );
        assert_eq!(args[3], "merge.omnia-ours.driver=true");
        assert_eq!(args[5], "merge.omnia-theirs.driver=cp %B %A");
    }
}
