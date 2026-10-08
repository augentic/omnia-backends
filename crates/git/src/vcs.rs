//! `WasiVcsCtx` over git: one process per operation, run in the place the
//! runtime opened, and what git says read into the typed error.

mod policy;
pub mod refusal;

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path;

use anyhow::{Context as _, Result};
use futures::FutureExt as _;
use omnia_wasi_vcs::{
    Change, ChangeKind, CloneOptions, Error, FutureResult, Merged, Place, Rule, WasiVcsCtx,
};

use self::policy::Policy;
use self::refusal::Class;
use crate::Client;
use crate::command::Output;

impl WasiVcsCtx for Client {
    fn resolve(&self, repo: Place, revision: String) -> FutureResult<String> {
        tracing::trace!("resolving {revision} in {}", repo.path().display());
        let client = self.clone();
        async move { client.rev_parse(&repo, &revision).await }.boxed()
    }

    fn head(&self, at: Place) -> FutureResult<String> {
        tracing::trace!("reading the head of {}", at.path().display());
        let client = self.clone();
        async move { client.rev_parse(&at, "HEAD").await }.boxed()
    }

    fn commit(&self, at: Place, message: String) -> FutureResult<Option<String>> {
        tracing::trace!("committing {}", at.path().display());
        let client = self.clone();
        async move {
            if client.status(&at).await?.is_empty() {
                return Ok(None);
            }
            client.run(&at, ["add", "-A"], &shown(&at), "HEAD").await?;
            client.run(&at, ["commit", "--quiet", "-m", &message], &shown(&at), "HEAD").await?;
            Ok(Some(client.rev_parse(&at, "HEAD").await?))
        }
        .boxed()
    }

    fn merge(
        &self, at: Place, revision: String, message: String, policy: Vec<Rule>,
    ) -> FutureResult<Merged> {
        tracing::trace!("merging {revision} into {}", at.path().display());
        let client = self.clone();
        async move {
            let policy = Policy::write(&policy)?;
            let mut args = policy.args();
            args.extend(["merge", "--no-ff", "-m", &message, &revision].map(str::to_owned));
            let output = client.git(Some(&at), &args).await?;
            if output.status.success() {
                return Ok(Merged {
                    commit: Some(client.rev_parse(&at, "HEAD").await?),
                    conflicts: Vec::new(),
                });
            }

            // a merge that started and stopped on conflicts is put back; one
            // that never started is the refusal git gave
            let conflicts = client.conflicts(&at).await?;
            if conflicts.is_empty() {
                return Err(refuse(&output, &shown(&at), &revision).into());
            }
            client.run(&at, ["merge", "--abort"], &shown(&at), &revision).await?;
            Ok(Merged {
                commit: None,
                conflicts,
            })
        }
        .boxed()
    }

    fn init(&self, at: Place) -> FutureResult<()> {
        tracing::trace!("initialising {}", at.path().display());
        let client = self.clone();
        async move {
            // git reinitialises an existing repository and exits clean
            if at.dir().exists(".git") {
                return Err(Error::Exists(shown(&at)).into());
            }
            client.run(&at, ["init", "--quiet"], &shown(&at), "HEAD").await?;
            Ok(())
        }
        .boxed()
    }

    fn add(&self, repo: Place, at: Place, revision: String) -> FutureResult<()> {
        tracing::trace!(
            "adding a working copy of {} at {}",
            repo.path().display(),
            at.path().display()
        );
        let client = self.clone();
        async move {
            // the one path handed to git: a working copy is laid at a path
            // git records in the repository, which no handle can stand for;
            // the runtime laid it empty and holds it open meanwhile
            let destination = path::absolute(at.path())
                .with_context(|| format!("resolving {}", at.path().display()))?;
            let args = [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("--detach"),
                destination.as_os_str(),
                OsStr::new(&revision),
            ];
            client.run(&repo, args, &shown(&at), &revision).await?;
            Ok(())
        }
        .boxed()
    }

    fn remove(&self, at: Place) -> FutureResult<()> {
        tracing::trace!("removing the working copy at {}", at.path().display());
        let client = self.clone();
        async move {
            client
                .run(&at, ["worktree", "remove", "--force", "."], &shown(&at), &shown(&at))
                .await?;
            Ok(())
        }
        .boxed()
    }

    fn pending(&self, at: Place) -> FutureResult<Vec<Change>> {
        tracing::trace!("reading what is pending at {}", at.path().display());
        let client = self.clone();
        async move { client.status(&at).await }.boxed()
    }

    fn clone_repo(&self, url: String, at: Place, options: CloneOptions) -> FutureResult<()> {
        tracing::trace!("cloning {url} to {}", at.path().display());
        let client = self.clone();
        async move {
            let depth = options.depth.map(|depth| depth.to_string());
            let mut args = vec!["clone", "--quiet"];
            // depth cuts history, never the labels: git's implied --single-branch would
            if let Some(depth) = &depth {
                args.extend(["--depth", depth, "--no-single-branch"]);
            }
            args.extend([url.as_str(), "."]);
            let output = client.git(Some(&at), args).await?;
            if !output.status.success() {
                return Err(refuse(&output, &shown(&at), &url).into());
            }
            Ok(())
        }
        .boxed()
    }

    fn fetch(&self, repo: Place, remote: String) -> FutureResult<()> {
        tracing::trace!("fetching {remote} into {}", repo.path().display());
        let client = self.clone();
        async move {
            client.run(&repo, ["fetch", "--quiet", &remote], &shown(&repo), &remote).await?;
            Ok(())
        }
        .boxed()
    }

    fn label(&self, repo: Place, name: String, revision: String) -> FutureResult<()> {
        tracing::trace!("labelling {revision} as {name} in {}", repo.path().display());
        let client = self.clone();
        async move {
            client.run(&repo, ["branch", "-f", &name, &revision], &shown(&repo), &revision).await?;
            Ok(())
        }
        .boxed()
    }

    fn push(&self, repo: Place, remote: String, label: String) -> FutureResult<()> {
        tracing::trace!("pushing {label} to {remote} from {}", repo.path().display());
        let client = self.clone();
        async move {
            let output = client.git(Some(&repo), ["push", "--quiet", &remote, &label]).await?;
            if !output.status.success() {
                // the label is what a refspec names; anything else missing is the remote
                let missing = if output.stderr.contains("src refspec") { &label } else { &remote };
                return Err(refuse(&output, &shown(&repo), missing).into());
            }
            Ok(())
        }
        .boxed()
    }
}

impl Client {
    // Run git at `at` and hold it to success, else the typed refusal naming
    // `exists` for a location taken and `missing` for what the repository
    // lacks.
    async fn run<I, S>(&self, at: &Place, args: I, exists: &str, missing: &str) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.git(Some(at), args).await?;
        if output.status.success() {
            Ok(output)
        } else {
            Err(refuse(&output, exists, missing).into())
        }
    }

    // `--quiet` leaves an unknown revision unsaid, so a failure here is
    // `not-found` unless git names a missing repository.
    async fn rev_parse(&self, at: &Place, revision: &str) -> Result<String> {
        let spec = format!("{revision}^{{commit}}");
        let output = self.git(Some(at), ["rev-parse", "--verify", "--quiet", &spec]).await?;
        if output.status.success() {
            return Ok(output.stdout.trim().to_owned());
        }
        Err(match refusal::classify(&output.stderr) {
            Class::NotARepository => Error::NotARepository,
            _ => Error::NotFound(revision.to_owned()),
        }
        .into())
    }

    async fn status(&self, at: &Place) -> Result<Vec<Change>> {
        let args = ["status", "--porcelain", "-z", "--no-renames", "--untracked-files=all"];
        let output = self.run(at, args, &shown(at), "HEAD").await?;
        Ok(pending(&output.stdout))
    }

    async fn conflicts(&self, at: &Place) -> Result<Vec<String>> {
        let args = ["diff", "--name-only", "--diff-filter=U", "-z"];
        let output = self.run(at, args, &shown(at), "HEAD").await?;
        Ok(output.stdout.split('\0').filter(|path| !path.is_empty()).map(str::to_owned).collect())
    }
}

fn refuse(output: &Output, exists: &str, missing: &str) -> Error {
    let detail = output.stderr.trim().to_owned();
    match refusal::classify(&output.stderr) {
        Class::NotARepository => Error::NotARepository,
        Class::Exists => Error::Exists(exists.to_owned()),
        Class::NotFound => Error::NotFound(missing.to_owned()),
        Class::Access => Error::Access(detail),
        Class::Other => Error::Other(detail),
    }
}

fn shown(place: &Place) -> String {
    place.path().display().to_string()
}

// One change per path from `status --porcelain -z`, the index and tree
// columns read together. A path listed twice — deleted from the index and
// back on disk untracked — differs from its head, so it is modified.
fn pending(porcelain: &str) -> Vec<Change> {
    let mut changes: BTreeMap<String, ChangeKind> = BTreeMap::new();
    for entry in porcelain.split('\0').filter(|entry| entry.len() > 3) {
        let (code, path) = entry.split_at(3);
        let mut columns = code.chars();
        let (index, tree) = (columns.next().unwrap_or(' '), columns.next().unwrap_or(' '));
        let Some(kind) = kind(index, tree) else {
            continue;
        };
        let kind = match changes.get(path) {
            Some(previous) if *previous != kind => ChangeKind::Modified,
            _ => kind,
        };
        changes.insert(path.to_owned(), kind);
    }
    changes.into_iter().map(|(path, kind)| Change { path, kind }).collect()
}

// Added then deleted before any commit is nothing the head lacks or holds
// differently.
const fn kind(index: char, tree: char) -> Option<ChangeKind> {
    match (index, tree) {
        ('A', 'D') => None,
        ('?', '?') | ('A', _) => Some(ChangeKind::Added),
        (_, 'D') | ('D', _) => Some(ChangeKind::Deleted),
        _ => Some(ChangeKind::Modified),
    }
}

#[cfg(test)]
mod tests {
    use omnia_wasi_vcs::ChangeKind;

    use super::pending;

    fn kinds(porcelain: &str) -> Vec<(String, ChangeKind)> {
        pending(porcelain).into_iter().map(|change| (change.path, change.kind)).collect()
    }

    #[test]
    fn porcelain_entries() {
        let porcelain =
            "?? new.rs\0 M src/lib.rs\0M  staged.rs\0 D gone.rs\0D  rm.rs\0A  added.rs\0";
        assert_eq!(
            kinds(porcelain),
            [
                ("added.rs".to_owned(), ChangeKind::Added),
                ("gone.rs".to_owned(), ChangeKind::Deleted),
                ("new.rs".to_owned(), ChangeKind::Added),
                ("rm.rs".to_owned(), ChangeKind::Deleted),
                ("src/lib.rs".to_owned(), ChangeKind::Modified),
                ("staged.rs".to_owned(), ChangeKind::Modified),
            ]
        );
    }

    #[test]
    fn porcelain_twice_listed() {
        assert_eq!(kinds("D  a.txt\0?? a.txt\0"), [("a.txt".to_owned(), ChangeKind::Modified)]);
        assert_eq!(kinds("AD a.txt\0"), []);
        assert_eq!(kinds(""), []);
    }
}
