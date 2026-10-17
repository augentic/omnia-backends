//! `WasiVcsCtx` over git: one process per operation, run in the place the
//! runtime opened, under the host-policy pins that keep the repository's own
//! configuration from choosing a program git runs, and what git says read
//! into the typed error.

mod policy;
mod refusal;

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{self, PathBuf};

use anyhow::{Context as _, Result};
use futures::FutureExt as _;
use omnia_wasi_vcs::{
    Change, ChangeKind, CloneOptions, Entry, Error, FutureResult, Merged, Place, Rule, WasiVcsCtx,
};

use self::policy::Resolution;
use self::refusal::Class;
use crate::Client;
use crate::command::{Output, Pins};

impl WasiVcsCtx for Client {
    fn resolve(&self, repo: Place, revision: String) -> FutureResult<String> {
        tracing::trace!("resolving {revision} in {}", repo.path().display());
        let client = self.clone();
        async move { client.rev_parse(&repo, &revision).await }.boxed()
    }

    fn descends(&self, repo: Place, ancestor: String, descendant: String) -> FutureResult<bool> {
        tracing::trace!("asking whether {descendant} descends from {ancestor}");
        let client = self.clone();
        async move {
            let repo = Repo::open(&client, &repo).await?;
            let ancestor = repo.rev_parse(&ancestor).await?;
            let descendant = repo.rev_parse(&descendant).await?;
            repo.is_ancestor(&ancestor, &descendant).await
        }
        .boxed()
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
            let repo = Repo::open(&client, &at).await?;
            if repo.status().await?.is_empty() {
                return Ok(None);
            }
            repo.run(["add", "-A"], "HEAD").await?;
            repo.run(["commit", "--quiet", "-m", &message], "HEAD").await?;
            Ok(Some(repo.rev_parse("HEAD").await?))
        }
        .boxed()
    }

    fn merge(
        &self, at: Place, revision: String, message: String, policy: Vec<Rule>,
    ) -> FutureResult<Merged> {
        tracing::trace!("merging {revision} into {}", at.path().display());
        let client = self.clone();
        async move {
            validate_arg(&revision)?;
            let repo = Repo::open(&client, &at).await?;
            let head = repo.rev_parse(&revision).await?;
            if !repo.begin_merge(&head, &revision).await? {
                return Ok(Merged {
                    commit: Some(repo.rev_parse("HEAD").await?),
                    conflicts: Vec::new(),
                });
            }
            repo.seal_merge(&policy, &message).await
        }
        .boxed()
    }

    fn log(&self, repo: Place, revision: String, base: String) -> FutureResult<Vec<Entry>> {
        tracing::trace!("reading {base}..{revision} in {}", repo.path().display());
        let client = self.clone();
        async move {
            let repo = Repo::open(&client, &repo).await?;
            let range =
                format!("{}..{}", repo.rev_parse(&base).await?, repo.rev_parse(&revision).await?);
            let args = [
                "log",
                "--first-parent",
                "-z",
                "--no-show-signature",
                "--format=%H%x1f%B",
                "--end-of-options",
                &range,
            ];
            let output = repo.run(args, &revision).await?;
            Ok(entries(&output.text()))
        }
        .boxed()
    }

    fn init(&self, at: Place) -> FutureResult<()> {
        tracing::trace!("initialising {}", at.path().display());
        let client = self.clone();
        async move {
            // a repository already there is `exists`: git would reinitialise it clean
            if at.dir().exists(".git") {
                return Err(Error::Exists(display_path(&at)).into());
            }
            client.create(&at, ["init", "--quiet"], &[], "HEAD").await
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
            validate_arg(&revision)?;
            let repo = Repo::open(&client, &repo).await?;
            repo.add_worktree(&at, &revision).await?;
            let gitdir = client.read_gitdir(&at, &revision).await?;
            client.register(&at, gitdir);
            Ok(())
        }
        .boxed()
    }

    fn remove(&self, at: Place) -> FutureResult<()> {
        tracing::trace!("removing the working copy at {}", at.path().display());
        let client = self.clone();
        async move {
            Repo::open(&client, &at).await?.remove_worktree().await?;
            client.forget(&at);
            Ok(())
        }
        .boxed()
    }

    fn pending(&self, at: Place) -> FutureResult<Vec<Change>> {
        tracing::trace!("reading what is pending at {}", at.path().display());
        let client = self.clone();
        async move { Repo::open(&client, &at).await?.status().await }.boxed()
    }

    fn clone_repo(&self, url: String, at: Place, options: CloneOptions) -> FutureResult<()> {
        tracing::trace!("cloning {url} to {}", at.path().display());
        let client = self.clone();
        async move {
            validate_arg(&url)?;
            let address = client.expand_url(&at, &url).await;
            let (protocol, pack) = client.transport_args(&address, false);
            let depth = options.depth.map(|depth| depth.to_string());
            let mut args = vec!["clone", "--quiet", pack.as_str()];
            // depth cuts history, never the labels: git's implied --single-branch would
            if let Some(depth) = depth.as_deref() {
                args.extend(["--depth", depth, "--no-single-branch"]);
            }
            args.extend(["--", url.as_str(), "."]);
            client.create(&at, args, &[("GIT_ALLOW_PROTOCOL", protocol)], &url).await
        }
        .boxed()
    }

    fn fetch(&self, repo: Place, remote: String) -> FutureResult<()> {
        tracing::trace!("fetching {remote} into {}", repo.path().display());
        let client = self.clone();
        async move {
            validate_arg(&remote)?;
            let repo = Repo::open(&client, &repo).await?;
            repo.check_transport_config()?;
            let address = repo.expand(&remote, false).await;
            let (protocol, pack) = client.transport_args(&address, false);
            let args = ["fetch", "--quiet", &pack, "--", &remote];
            let output = repo.exec(args, &[("GIT_ALLOW_PROTOCOL", protocol)]).await?;
            if !output.status.success() {
                return Err(repo.refuse(&output, &remote).into());
            }
            Ok(())
        }
        .boxed()
    }

    fn label(&self, repo: Place, name: String, revision: String) -> FutureResult<()> {
        tracing::trace!("labelling {revision} as {name} in {}", repo.path().display());
        let client = self.clone();
        async move {
            validate_arg(&name)?;
            validate_arg(&revision)?;
            let repo = Repo::open(&client, &repo).await?;
            let head = repo.rev_parse(&revision).await?;
            repo.detach_from(&name).await?;
            repo.run(["branch", "-f", "--", &name, &head], &revision).await?;
            Ok(())
        }
        .boxed()
    }

    fn labelled(&self, repo: Place, name: String) -> FutureResult<String> {
        tracing::trace!("reading the label {name} in {}", repo.path().display());
        let client = self.clone();
        async move {
            validate_arg(&name)?;
            let full = format!("refs/heads/{name}");
            Ok(client.read_ref(&repo, &full).await?.ok_or(Error::NotFound(name))?)
        }
        .boxed()
    }

    fn fetched(&self, repo: Place, remote: String, name: String) -> FutureResult<String> {
        tracing::trace!("reading {remote}'s label {name} in {}", repo.path().display());
        let client = self.clone();
        async move {
            validate_arg(&remote)?;
            validate_arg(&name)?;
            let full = format!("refs/remotes/{remote}/{name}");
            if let Some(commit) = client.read_ref(&repo, &full).await? {
                return Ok(commit);
            }
            let missing = if client.has_remote(&repo, &remote).await? { name } else { remote };
            Err(Error::NotFound(missing).into())
        }
        .boxed()
    }

    fn push(&self, repo: Place, remote: String, label: String) -> FutureResult<()> {
        tracing::trace!("pushing {label} to {remote} from {}", repo.path().display());
        let client = self.clone();
        async move {
            validate_arg(&remote)?;
            validate_arg(&label)?;
            let repo = Repo::open(&client, &repo).await?;
            repo.check_transport_config()?;
            let address = repo.expand(&remote, true).await;
            let (protocol, pack) = client.transport_args(&address, true);
            let refspec = format!("refs/heads/{label}:refs/heads/{label}");
            let args = ["push", "--quiet", &pack, "--", &remote, &refspec];
            let output = repo.exec(args, &[("GIT_ALLOW_PROTOCOL", protocol)]).await?;
            if !output.status.success() {
                return Err(repo.refuse_push(&output, &remote, &label).into());
            }
            Ok(())
        }
        .boxed()
    }
}

impl Client {
    // `--quiet` leaves an unknown revision unsaid, so a failure here is
    // `not-found` unless git names a missing repository. `rev-parse` runs no
    // program the repository chooses, so it needs no pins.
    async fn rev_parse(&self, at: &Place, revision: &str) -> Result<String> {
        validate_arg(revision)?;
        let spec = format!("{revision}^{{commit}}");
        let args = ["rev-parse", "--verify", "--quiet", "--end-of-options", &spec];
        let output = self.git(Some(at), args, &[]).await?;
        if output.status.success() {
            return Ok(output.text().trim().to_owned());
        }
        Err(match refusal::classify(&output.stderr) {
            Class::NotARepository => Error::NotARepository,
            _ => Error::NotFound(revision.to_owned()),
        }
        .into())
    }

    // The commit the ref at `full` holds, read by its exact path, which
    // `--verify` takes as a name and never as revision syntax: a tag or a
    // remote's branch of the same spelling, which a bare name resolves to
    // first, is never it, and nor is the commit a `~`, `^`, or `@{}` in the
    // name would walk to; a branch holds a commit, so there is nothing to
    // peel. None for a ref the repository lacks. Reading a ref runs no
    // program, so it needs no pins.
    async fn read_ref(&self, at: &Place, full: &str) -> Result<Option<String>> {
        let args = ["show-ref", "--verify", "--hash", "--", full];
        let output = self.git(Some(at), args, &[]).await?;
        if output.status.success() {
            return Ok(Some(output.text().trim().to_owned()));
        }
        if refusal::classify(&output.stderr) == Class::NotARepository {
            return Err(Error::NotARepository.into());
        }
        Ok(None)
    }

    async fn has_remote(&self, at: &Place, remote: &str) -> Result<bool> {
        let output = self.git(Some(at), ["remote", "get-url", "--", remote], &[]).await?;
        Ok(output.status.success())
    }

    // The URL an address reaches as the host's configuration rewrites it
    // (`url.<base>.insteadOf`), for the transport rule; the address as given
    // where git rewrites nothing.
    async fn expand_url(&self, at: &Place, address: &str) -> String {
        match self.git(Some(at), ["ls-remote", "--get-url", "--", address], &[]).await {
            Ok(output) if output.status.success() => output.text().trim().to_owned(),
            _ => address.to_owned(),
        }
    }

    // The gitdir git laid the working copy at `at` with, read while the copy
    // holds nothing but what `worktree add` wrote; every operation at the
    // copy is addressed through it from here on.
    async fn read_gitdir(&self, at: &Place, revision: &str) -> Result<PathBuf> {
        let output = self.git(Some(at), ["rev-parse", "--absolute-git-dir"], &[]).await?;
        if !output.status.success() {
            return Err(refuse(&output, at, revision, None).into());
        }
        Ok(PathBuf::from(output.text().trim()))
    }

    // Run git to make a repository at `at`, held to success, else the typed
    // refusal with `missing` naming what the repository lacks. The place
    // holds no repository yet, so the forced pins alone apply and the work
    // tree is git's to lay; a working copy this client laid there before is
    // forgotten first, so the place is addressed as itself rather than
    // through a gitdir no longer its own.
    async fn create<I, S>(
        &self, at: &Place, args: I, env: &[(&str, &str)], missing: &str,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.forget(at);
        let pins = Pins::forced();
        let mut full: Vec<OsString> = pins.args().map(OsString::from).collect();
        full.extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        let output = self.git(Some(at), full, env).await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(refuse(&output, at, missing, None).into())
        }
    }
}

// One repository under one operation's host-policy pins: every command git
// runs for it reads the pins first, so the repository's own configuration
// cannot choose a program.
struct Repo<'a> {
    client: &'a Client,
    at: &'a Place,
    pins: Pins,
}

impl<'a> Repo<'a> {
    async fn open(client: &'a Client, at: &'a Place) -> Result<Self> {
        let pins = Pins::read(client, at).await?;
        Ok(Self { client, at, pins })
    }

    // Run git for the repository and read what it said, whatever its exit:
    // the work tree is the place itself, since a `core.worktree` the
    // repository set would point git at any directory on the host, and the
    // pins lead the arguments.
    async fn exec<I, S>(&self, args: I, env: &[(&str, &str)]) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut full = vec![OsString::from("--work-tree=.")];
        full.extend(self.pins.args().map(OsString::from));
        full.extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        self.client.git(Some(self.at), full, env).await
    }

    // Run git for the repository and hold it to success, else the typed
    // refusal with `missing` naming what the repository lacks.
    async fn run<I, S>(&self, args: I, missing: &str) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.exec(args, &[]).await?;
        if output.status.success() { Ok(output) } else { Err(self.refuse(&output, missing).into()) }
    }

    fn refuse(&self, output: &Output, missing: &str) -> Error {
        refuse(output, self.at, missing, None)
    }

    // The typed refusal for a failed push: `diverged` over `label` where the
    // remote's branch has moved past it; `not-found` naming the label where
    // no refspec names it, the one thing a push can lack, else the remote.
    fn refuse_push(&self, output: &Output, remote: &str, label: &str) -> Error {
        let missing = if output.stderr.contains("src refspec") { label } else { remote };
        refuse(output, self.at, missing, Some(label))
    }

    async fn rev_parse(&self, revision: &str) -> Result<String> {
        self.client.rev_parse(self.at, revision).await
    }

    // How a transport runs — the URL it reaches, the proxy it crosses, the
    // TLS it trusts — is the host's configuration to set: a setting the
    // repository made is config no pin can put back, so a transport operation
    // refuses the repository whole rather than carry the operator's
    // credentials anywhere or over anything.
    fn check_transport_config(&self) -> Result<()> {
        if let Some(key) = self.pins.blocked_transport_key() {
            let shown = self.display_path();
            let message = format!(
                "{shown}: the repository sets {key}, and how a transport runs is the host's to set"
            );
            return Err(Error::Other(message).into());
        }
        Ok(())
    }

    // The URL a remote name or address reaches as the host's configuration
    // expands it, for the transport rule: a remote's own URL, else the
    // address as `url.<base>.insteadOf` rewrites it. A repository with
    // transport settings of its own was refused before this, so the
    // expansion is the host's alone.
    async fn expand(&self, remote: &str, push: bool) -> String {
        let mut args = vec!["remote", "get-url"];
        if push {
            args.push("--push");
        }
        args.extend(["--", remote]);
        if let Ok(output) = self.exec(args, &[]).await
            && output.status.success()
        {
            return output.text().trim().to_owned();
        }
        self.client.expand_url(self.at, remote).await
    }

    async fn status(&self) -> Result<Vec<Change>> {
        let args = ["status", "--porcelain", "-z", "--no-renames", "--untracked-files=all"];
        let output = self.run(args, "HEAD").await?;
        Ok(pending(&output.text()))
    }

    // `--is-ancestor` answers by exit status alone, 1 writing nothing, so
    // the status is read rather than the stderr.
    async fn is_ancestor(&self, ancestor: &str, descendant: &str) -> Result<bool> {
        let args = ["merge-base", "--is-ancestor", ancestor, descendant];
        let output = self.exec(args, &[]).await?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(self.refuse(&output, descendant).into()),
        }
    }

    // Start merging `head`, held before its commit so the index is the
    // backend's to settle by policy rather than git's attributes. False
    // where there was nothing to merge: an ancestor, which git reports clean
    // without starting one. A merge that never started is the refusal git
    // gave, naming `revision`.
    async fn begin_merge(&self, head: &str, revision: &str) -> Result<bool> {
        let args = ["merge", "--no-ff", "--no-commit", "--", head];
        let output = self.exec(args, &[]).await?;
        if self.merging().await? {
            return Ok(true);
        }
        if output.status.success() {
            return Ok(false);
        }
        Err(self.refuse(&output, revision).into())
    }

    // Settle the merge in progress by `policy` and commit it as `message`,
    // or unwind it: the paths no rule resolved come back as data and a step
    // that fails as its error, the working copy on its head either way, so
    // nothing of a failed merge stays for a later commit to finish.
    async fn seal_merge(&self, policy: &[Rule], message: &str) -> Result<Merged> {
        let merged = self.commit_merge(policy, message).await;
        if !matches!(merged, Ok(Merged { commit: Some(_), .. })) {
            self.abort().await;
        }
        merged
    }

    // The merge in progress resolved by `policy` and committed as `message`,
    // or the paths no rule resolved and no commit. The message goes on the
    // command line, never read from `MERGE_MSG`: that carries the
    // `# Conflicts:` hint git appends to a merge it left in conflict, and
    // stripping it would strip the message's own `#` lines too, to nothing
    // where it has no others.
    async fn commit_merge(&self, policy: &[Rule], message: &str) -> Result<Merged> {
        if let Resolution::Unresolved(conflicts) = self.resolve(policy).await? {
            return Ok(Merged {
                commit: None,
                conflicts,
            });
        }
        self.run(["commit", "--quiet", "-m", message], "HEAD").await?;
        Ok(Merged {
            commit: Some(self.rev_parse("HEAD").await?),
            conflicts: Vec::new(),
        })
    }

    // Whether a merge is in progress: a clean `--no-commit` merge and a
    // conflicted one both leave `MERGE_HEAD`, an ancestor leaves none.
    async fn merging(&self) -> Result<bool> {
        let args = ["rev-parse", "--verify", "--quiet", "--end-of-options", "MERGE_HEAD"];
        Ok(self.exec(args, &[]).await?.status.success())
    }

    // Best-effort unwind of a merge git left in progress; a repository with
    // none to abort is already on its head.
    async fn abort(&self) {
        let _ = self.exec(["merge", "--abort"], &[]).await;
    }

    // Detach HEAD when the repository is on the branch `name`, so `branch -f`
    // can move it: a label is a branch, which git refuses to force while it
    // is checked out, and the working copy sits on its sealed commit either
    // way. An unborn branch cannot be detached and is refused.
    async fn detach_from(&self, name: &str) -> Result<()> {
        let head = self.exec(["symbolic-ref", "--quiet", "HEAD"], &[]).await?;
        if head.text().trim() != format!("refs/heads/{name}") {
            return Ok(());
        }
        if self.exec(["checkout", "--quiet", "--detach"], &[]).await?.status.success() {
            return Ok(());
        }
        Err(Error::Other(format!("cannot label the branch {name} the repository is on")).into())
    }

    // Lay a detached working copy of `revision` at `at`, whose absolute path
    // is the one path handed to git: a working copy is laid at a path git
    // records in the repository, which no handle can stand for; the runtime
    // laid it empty and holds it open meanwhile.
    async fn add_worktree(&self, at: &Place, revision: &str) -> Result<()> {
        let destination = path::absolute(at.path())
            .with_context(|| format!("resolving {}", at.path().display()))?;
        let args = [
            OsStr::new("worktree"),
            OsStr::new("add"),
            OsStr::new("--detach"),
            OsStr::new("--"),
            destination.as_os_str(),
            OsStr::new(revision),
        ];
        let output = self.exec(args, &[]).await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(refuse(&output, at, revision, None).into())
        }
    }

    // Remove the working copy, pending changes with it. A copy another
    // process laid is addressed through its `.git` file: a rewritten one
    // leads `worktree remove` to a repository the copy is no working copy of,
    // which refuses rather than removes, and the copy stays for the operator.
    async fn remove_worktree(&self) -> Result<()> {
        let shown = self.display_path();
        self.run(["worktree", "remove", "--force", "."], &shown).await?;
        Ok(())
    }

    fn display_path(&self) -> String {
        display_path(self.at)
    }
}

// A guest string git would read as an option, or the empty string it reads as
// a default, is nothing the repository knows.
fn validate_arg(value: &str) -> Result<()> {
    if value.is_empty() || value.starts_with('-') {
        return Err(Error::NotFound(value.to_owned()).into());
    }
    Ok(())
}

// The typed refusal for a failed process at `at`, which is what `exists`
// names: `missing` names what the repository lacks, and `label` the one a
// push sent, which is what a rejected push is `diverged` over; an operation
// that pushes nothing passes `None` and is never typed so, whatever git said.
fn refuse(output: &Output, at: &Place, missing: &str, label: Option<&str>) -> Error {
    let detail = output.stderr.trim().to_owned();
    match (refusal::classify(&output.stderr), label) {
        (Class::NotARepository, _) => Error::NotARepository,
        (Class::Exists, _) => Error::Exists(display_path(at)),
        (Class::NotFound, _) => Error::NotFound(missing.to_owned()),
        (Class::Access, _) => Error::Access(detail),
        (Class::Diverged, Some(label)) => Error::Diverged(label.to_owned()),
        (Class::Diverged | Class::Other, None) | (Class::Other, Some(_)) => Error::Other(detail),
    }
}

fn display_path(place: &Place) -> String {
    place.path().display().to_string()
}

// One entry per `-z` record of `log --format=%H%x1f%B`: the id, then the
// message as sealed, without the newline git ends a message with.
fn entries(output: &str) -> Vec<Entry> {
    output
        .split('\0')
        .filter_map(|record| record.split_once('\x1f'))
        .map(|(id, message)| Entry {
            id: id.to_owned(),
            message: message.trim_end_matches('\n').to_owned(),
        })
        .collect()
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

// The porcelain parsers are pure over text; the e2e suites drive every
// operation through a guest over real repositories, with git as the oracle.
#[cfg(test)]
mod tests {
    use omnia_wasi_vcs::ChangeKind;

    use super::{entries, pending};

    fn kinds(porcelain: &str) -> Vec<(String, ChangeKind)> {
        pending(porcelain).into_iter().map(|change| (change.path, change.kind)).collect()
    }

    fn logged(output: &str) -> Vec<(String, String)> {
        entries(output).into_iter().map(|entry| (entry.id, entry.message)).collect()
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

    #[test]
    fn log_records() {
        let output = "aa11\x1fmerge slice\n\nSlice: SLICE-001\n\0bb22\x1fours\n\0";
        assert_eq!(
            logged(output),
            [
                ("aa11".to_owned(), "merge slice\n\nSlice: SLICE-001".to_owned()),
                ("bb22".to_owned(), "ours".to_owned()),
            ]
        );
        assert_eq!(logged(""), []);
    }
}
