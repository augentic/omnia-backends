//! `WasiVcsCtx` over git: one process per operation, run in the place the
//! runtime opened, under the host-policy pins that keep the repository's own
//! configuration from choosing a program git runs, and what git says read
//! into the typed error.

mod policy;
pub mod refusal;

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path;

use anyhow::{Context as _, Result};
use futures::FutureExt as _;
use omnia_wasi_vcs::{
    Change, ChangeKind, CloneOptions, Error, FutureResult, Merged, Place, Rule, WasiVcsCtx,
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
            repo.run(["add", "-A"], &repo.shown(), "HEAD").await?;
            repo.run(["commit", "--quiet", "-m", &message], &repo.shown(), "HEAD").await?;
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
            vetted(&revision)?;
            let repo = Repo::open(&client, &at).await?;
            let head = repo.rev_parse(&revision).await?;
            // held before the commit, so the index is the backend's to settle
            // by policy rather than git's attributes
            let args = ["merge", "--no-ff", "--no-commit", "-m", &message, "--", &head];
            let output = repo.git(args, &[]).await?;
            if !repo.merging().await? {
                // nothing to merge — an ancestor — or a merge that never
                // started; a failure here is the refusal git gave
                if output.status.success() {
                    return Ok(Merged {
                        commit: Some(repo.rev_parse("HEAD").await?),
                        conflicts: Vec::new(),
                    });
                }
                return Err(refuse(&output, &repo.shown(), &revision).into());
            }

            // resolve what the policy covers; an unresolved conflict, or a step
            // that fails, abandons the merge and leaves the tree on its head
            match repo.resolve(&policy).await {
                Ok(Resolution::Complete) => {}
                Ok(Resolution::Unresolved(conflicts)) => {
                    repo.abort().await;
                    return Ok(Merged {
                        commit: None,
                        conflicts,
                    });
                }
                Err(error) => {
                    repo.abort().await;
                    return Err(error);
                }
            }
            if let Err(error) =
                repo.run(["commit", "--quiet", "--no-edit"], &repo.shown(), "HEAD").await
            {
                repo.abort().await;
                return Err(error);
            }
            Ok(Merged {
                commit: Some(repo.rev_parse("HEAD").await?),
                conflicts: Vec::new(),
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
            let repo = Repo::open(&client, &at).await?;
            repo.run(["init", "--quiet"], &repo.shown(), "HEAD").await?;
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
            vetted(&revision)?;
            // the one path handed to git: a working copy is laid at a path
            // git records in the repository, which no handle can stand for;
            // the runtime laid it empty and holds it open meanwhile
            let destination = path::absolute(at.path())
                .with_context(|| format!("resolving {}", at.path().display()))?;
            let repo = Repo::open(&client, &repo).await?;
            let args = [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("--detach"),
                OsStr::new("--"),
                destination.as_os_str(),
                OsStr::new(&revision),
            ];
            repo.run(args, &shown(&at), &revision).await?;
            Ok(())
        }
        .boxed()
    }

    fn remove(&self, at: Place) -> FutureResult<()> {
        tracing::trace!("removing the working copy at {}", at.path().display());
        let client = self.clone();
        async move {
            let repo = Repo::open(&client, &at).await?;
            repo.run(["worktree", "remove", "--force", "."], &repo.shown(), &repo.shown()).await?;
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
            vetted(&url)?;
            let repo = Repo::open(&client, &at).await?;
            let (protocol, pack) = transport(&client, &url, false);
            let depth = options.depth.map(|depth| depth.to_string());
            let mut args = vec!["clone".to_owned(), "--quiet".to_owned()];
            if let Some(pack) = pack {
                args.push(pack);
            }
            // depth cuts history, never the labels: git's implied --single-branch would
            if let Some(depth) = &depth {
                args.extend(["--depth".to_owned(), depth.clone(), "--no-single-branch".to_owned()]);
            }
            args.extend(["--".to_owned(), url.clone(), ".".to_owned()]);
            let output = repo.git(args, &[("GIT_ALLOW_PROTOCOL", protocol)]).await?;
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
            vetted(&remote)?;
            let repo = Repo::open(&client, &repo).await?;
            let address = resolve_remote(&repo, &remote, false).await;
            let (protocol, pack) = transport(&client, &address, false);
            let mut args = vec!["fetch".to_owned(), "--quiet".to_owned()];
            if let Some(pack) = pack {
                args.push(pack);
            }
            args.extend(["--".to_owned(), remote.clone()]);
            let output = repo.git(args, &[("GIT_ALLOW_PROTOCOL", protocol)]).await?;
            if !output.status.success() {
                return Err(refuse(&output, &repo.shown(), &remote).into());
            }
            Ok(())
        }
        .boxed()
    }

    fn label(&self, repo: Place, name: String, revision: String) -> FutureResult<()> {
        tracing::trace!("labelling {revision} as {name} in {}", repo.path().display());
        let client = self.clone();
        async move {
            vetted(&name)?;
            vetted(&revision)?;
            let repo = Repo::open(&client, &repo).await?;
            let head = repo.rev_parse(&revision).await?;
            // a label is a branch; git refuses to force the branch the
            // repository is on, so the working copy is detached onto its
            // sealed commit first — where the model says it already sits
            repo.detach_from(&name).await?;
            repo.run(["branch", "-f", "--", &name, &head], &repo.shown(), &revision).await?;
            Ok(())
        }
        .boxed()
    }

    fn push(&self, repo: Place, remote: String, label: String) -> FutureResult<()> {
        tracing::trace!("pushing {label} to {remote} from {}", repo.path().display());
        let client = self.clone();
        async move {
            vetted(&remote)?;
            vetted(&label)?;
            let repo = Repo::open(&client, &repo).await?;
            let address = resolve_remote(&repo, &remote, true).await;
            let (protocol, pack) = transport(&client, &address, true);
            let refspec = format!("refs/heads/{label}:refs/heads/{label}");
            let mut args = vec!["push".to_owned(), "--quiet".to_owned()];
            if let Some(pack) = pack {
                args.push(pack);
            }
            args.extend(["--".to_owned(), remote.clone(), refspec]);
            let output = repo.git(args, &[("GIT_ALLOW_PROTOCOL", protocol)]).await?;
            if !output.status.success() {
                // the label is what a refspec names; anything else missing is the remote
                let missing = if output.stderr.contains("src refspec") { &label } else { &remote };
                return Err(refuse(&output, &repo.shown(), missing).into());
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
        vetted(revision)?;
        let spec = format!("{revision}^{{commit}}");
        let args = ["rev-parse", "--verify", "--quiet", "--end-of-options", &spec];
        let output = self.git(Some(at), args, &[]).await?;
        if output.status.success() {
            return Ok(output.stdout.trim().to_owned());
        }
        Err(match refusal::classify(&output.stderr) {
            Class::NotARepository => Error::NotARepository,
            _ => Error::NotFound(revision.to_owned()),
        }
        .into())
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

    async fn git<I, S>(&self, args: I, env: &[(&str, &str)]) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut full: Vec<OsString> = self.pins.args().map(OsString::from).collect();
        full.extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        self.client.git(Some(self.at), full, env).await
    }

    // Run git for this repository and hold it to success, else the typed
    // refusal naming `exists` for a location taken and `missing` for what the
    // repository lacks.
    async fn run<I, S>(&self, args: I, exists: &str, missing: &str) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.git(args, &[]).await?;
        if output.status.success() {
            Ok(output)
        } else {
            Err(refuse(&output, exists, missing).into())
        }
    }

    async fn rev_parse(&self, revision: &str) -> Result<String> {
        self.client.rev_parse(self.at, revision).await
    }

    async fn status(&self) -> Result<Vec<Change>> {
        let args = ["status", "--porcelain", "-z", "--no-renames", "--untracked-files=all"];
        let output = self.run(args, &self.shown(), "HEAD").await?;
        Ok(pending(&output.stdout))
    }

    async fn conflicts(&self) -> Result<Vec<String>> {
        let args =
            ["diff", "--no-ext-diff", "--no-textconv", "--name-only", "--diff-filter=U", "-z"];
        let output = self.run(args, &self.shown(), "HEAD").await?;
        Ok(nul_separated(&output.stdout))
    }

    async fn resolve(&self, rules: &[Rule]) -> Result<Resolution> {
        policy::resolve(self, rules).await
    }

    // Whether a merge is in progress: a clean `--no-commit` merge and a
    // conflicted one both leave `MERGE_HEAD`, an ancestor leaves none.
    async fn merging(&self) -> Result<bool> {
        let args = ["rev-parse", "--verify", "--quiet", "--end-of-options", "MERGE_HEAD"];
        Ok(self.git(args, &[]).await?.status.success())
    }

    // The common ancestor of the merge's two sides, none when they share no
    // history.
    async fn merge_base(&self) -> Result<Option<String>> {
        let output = self.git(["merge-base", "HEAD", "MERGE_HEAD"], &[]).await?;
        Ok(output.status.success().then(|| output.stdout.trim().to_owned()))
    }

    // The paths the two sides differ on: every path the merge touched, where a
    // policy rule may apply.
    async fn touched(&self) -> Result<Vec<String>> {
        let args =
            ["diff", "--no-ext-diff", "--no-textconv", "--name-only", "-z", "HEAD", "MERGE_HEAD"];
        let output = self.run(args, &self.shown(), "HEAD").await?;
        Ok(nul_separated(&output.stdout))
    }

    // One commit's version of a path, or none when that commit lacks it.
    async fn show(&self, revision: &str, path: &str) -> Result<Option<String>> {
        let output = self.git(["show", &format!("{revision}:{path}")], &[]).await?;
        Ok(output.status.success().then_some(output.stdout))
    }

    // Best-effort unwind of a merge git left in progress; a repository with
    // none to abort is already on its head.
    async fn abort(&self) {
        let _ = self.git(["merge", "--abort"], &[]).await;
    }

    // Detach HEAD when the repository is on the branch `name`, so `branch -f`
    // can move it. An unborn branch cannot be detached and is refused.
    async fn detach_from(&self, name: &str) -> Result<()> {
        let head = self.git(["symbolic-ref", "--quiet", "HEAD"], &[]).await?;
        if head.stdout.trim() != format!("refs/heads/{name}") {
            return Ok(());
        }
        if self.git(["checkout", "--quiet", "--detach"], &[]).await?.status.success() {
            return Ok(());
        }
        Err(Error::Other(format!("cannot label the branch {name} the repository is on")).into())
    }

    fn shown(&self) -> String {
        shown(self.at)
    }

    fn write(&self, path: &str, bytes: &[u8]) -> Result<()> {
        self.at.dir().write(path, bytes).with_context(|| format!("writing the merge of {path}"))
    }

    fn remove_temp(&self, name: &str) -> Result<()> {
        self.at.dir().remove_file(name).with_context(|| format!("removing {name}"))
    }
}

// A guest string git would read as an option, or the empty string it reads as
// a default, is nothing the repository knows.
fn vetted(value: &str) -> Result<()> {
    if value.is_empty() || value.starts_with('-') {
        return Err(Error::NotFound(value.to_owned()).into());
    }
    Ok(())
}

// The protocol allow-list and, for a local transport, the wrapped pack
// command that runs the far side without its hooks: a bare repository beneath
// the mount is as much the guest's as the working copy.
fn transport(client: &Client, address: &str, push: bool) -> (&'static str, Option<String>) {
    let operation = if push { "receive-pack" } else { "upload-pack" };
    if is_local(address) {
        ("file", Some(wrap(client, operation)))
    } else {
        ("http:https:ssh", None)
    }
}

// git's own rule: a `file://` URL, or an address with no scheme and no
// scp-like `host:path`, is a local path; everything else crosses a network.
fn is_local(address: &str) -> bool {
    if let Some(scheme) = address.split_once("://") {
        return scheme.0.eq_ignore_ascii_case("file");
    }
    match (address.find(':'), address.find('/')) {
        (Some(colon), Some(slash)) => colon > slash,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

// `git -c core.hooksPath=/dev/null <operation>` as one `--upload-pack` or
// `--receive-pack` argument, so the far side — a bare repository as much
// beneath the mount as the working copy — runs none of its own hooks. git
// splits the value with shell quoting for a local transport, so the binary
// is single-quoted.
fn wrap(client: &Client, operation: &str) -> String {
    let binary = sq(&client.binary.display().to_string());
    format!("--{operation}={binary} -c core.hooksPath=/dev/null {operation}")
}

fn sq(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

// The URL a remote resolves to, for the transport rule; a name git does not
// know is read as the address the guest gave, which git will refuse in turn.
async fn resolve_remote(repo: &Repo<'_>, remote: &str, push: bool) -> String {
    let mut args = vec!["remote", "get-url"];
    if push {
        args.push("--push");
    }
    args.extend(["--", remote]);
    match repo.git(args, &[]).await {
        Ok(output) if output.status.success() => output.stdout.trim().to_owned(),
        _ => remote.to_owned(),
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

// Non-empty entries of a `-z` list, each `\0`-terminated.
fn nul_separated(output: &str) -> Vec<String> {
    output.split('\0').filter(|entry| !entry.is_empty()).map(str::to_owned).collect()
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

    use super::{is_local, pending};

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

    #[test]
    fn local_and_remote_addresses() {
        assert!(is_local("file:///srv/repo.git"));
        assert!(is_local("/srv/repo.git"));
        assert!(is_local("./repo"));
        assert!(is_local("../origin.git"));
        assert!(!is_local("https://host/repo.git"));
        assert!(!is_local("ssh://host/repo.git"));
        assert!(!is_local("git@host:owner/repo.git"));
        assert!(!is_local("ext::sh -c payload"));
    }
}
