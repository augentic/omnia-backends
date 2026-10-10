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
            // `--is-ancestor` answers by exit status alone, 1 writing
            // nothing, so the status is read rather than the stderr
            let args = ["merge-base", "--is-ancestor", &ancestor, &descendant];
            let output = repo.git(args, &[]).await?;
            match output.status.code() {
                Some(0) => Ok(true),
                Some(1) => Ok(false),
                _ => Err(refuse(&output, &repo.shown(), &descendant, None).into()),
            }
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
            let args = ["merge", "--no-ff", "--no-commit", "--", &head];
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
                return Err(refuse(&output, &repo.shown(), &revision, None).into());
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
            // the message on the command line, never read from `MERGE_MSG`:
            // that carries the `# Conflicts:` hint git appends to a merge it
            // left in conflict, and stripping it would strip the message's
            // own `#` lines too, to nothing where it has no others
            let args = ["commit", "--quiet", "-m", &message];
            if let Err(error) = repo.run(args, &repo.shown(), "HEAD").await {
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
            let output = repo.run(args, &repo.shown(), &revision).await?;
            Ok(entries(&output.text()))
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
            let output = repo.create(["init", "--quiet"], &[]).await?;
            if !output.status.success() {
                return Err(refuse(&output, &repo.shown(), "HEAD", None).into());
            }
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
            repo.transportable()?;
            let address = expand(&repo, &url, false).await;
            let (protocol, pack) = transport(&client, &address, false);
            let depth = options.depth.map(|depth| depth.to_string());
            let mut args = vec!["clone".to_owned(), "--quiet".to_owned(), pack];
            // depth cuts history, never the labels: git's implied --single-branch would
            if let Some(depth) = &depth {
                args.extend(["--depth".to_owned(), depth.clone(), "--no-single-branch".to_owned()]);
            }
            args.extend(["--".to_owned(), url.clone(), ".".to_owned()]);
            let output = repo.create(args, &[("GIT_ALLOW_PROTOCOL", protocol)]).await?;
            if !output.status.success() {
                return Err(refuse(&output, &shown(&at), &url, None).into());
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
            repo.transportable()?;
            let address = expand(&repo, &remote, false).await;
            let (protocol, pack) = transport(&client, &address, false);
            let args =
                ["fetch".to_owned(), "--quiet".to_owned(), pack, "--".to_owned(), remote.clone()];
            let output = repo.git(args, &[("GIT_ALLOW_PROTOCOL", protocol)]).await?;
            if !output.status.success() {
                return Err(refuse(&output, &repo.shown(), &remote, None).into());
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

    fn labelled(&self, repo: Place, name: String) -> FutureResult<String> {
        tracing::trace!("reading the label {name} in {}", repo.path().display());
        let client = self.clone();
        async move {
            vetted(&name)?;
            // the exact ref path, which `--verify` reads as a name and never
            // as revision syntax: a tag or a remote's branch of the same
            // spelling, which a bare name resolves to first, is never it, and
            // nor is the commit a `~`, `^`, or `@{}` in the name would walk
            // to; a branch holds a commit, so there is nothing to peel
            let full = format!("refs/heads/{name}");
            let args = ["show-ref", "--verify", "--hash", "--", &full];
            let output = client.git(Some(&repo), args, &[]).await?;
            if output.status.success() {
                return Ok(output.text().trim().to_owned());
            }
            Err(match refusal::classify(&output.stderr) {
                Class::NotARepository => Error::NotARepository,
                _ => Error::NotFound(name),
            }
            .into())
        }
        .boxed()
    }

    fn fetched(&self, repo: Place, remote: String, name: String) -> FutureResult<String> {
        tracing::trace!("reading {remote}'s label {name} in {}", repo.path().display());
        let client = self.clone();
        async move {
            vetted(&remote)?;
            vetted(&name)?;
            // the exact remote-tracking ref, read as `labelled` reads the
            // branch; what is missing is the remote when the repository has
            // none of that name, else the label
            let full = format!("refs/remotes/{remote}/{name}");
            let args = ["show-ref", "--verify", "--hash", "--", &full];
            let output = client.git(Some(&repo), args, &[]).await?;
            if output.status.success() {
                return Ok(output.text().trim().to_owned());
            }
            if refusal::classify(&output.stderr) == Class::NotARepository {
                return Err(Error::NotARepository.into());
            }
            let known = client.git(Some(&repo), ["remote", "get-url", "--", &remote], &[]).await?;
            Err(Error::NotFound(if known.status.success() { name } else { remote }).into())
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
            repo.transportable()?;
            let address = expand(&repo, &remote, true).await;
            let (protocol, pack) = transport(&client, &address, true);
            let refspec = format!("refs/heads/{label}:refs/heads/{label}");
            let args = [
                "push".to_owned(),
                "--quiet".to_owned(),
                pack,
                "--".to_owned(),
                remote.clone(),
                refspec,
            ];
            let output = repo.git(args, &[("GIT_ALLOW_PROTOCOL", protocol)]).await?;
            if !output.status.success() {
                // the label is what a refspec names; anything else missing is the remote
                let missing = if output.stderr.contains("src refspec") { &label } else { &remote };
                return Err(refuse(&output, &repo.shown(), missing, Some(&label)).into());
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
            return Ok(output.text().trim().to_owned());
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

    // Run git for the repository: the work tree is the place itself, since a
    // `core.worktree` the repository set would point git at any directory on
    // the host, and the pins lead the arguments.
    async fn git<I, S>(&self, args: I, env: &[(&str, &str)]) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut full = vec![OsString::from("--work-tree=.")];
        full.extend(self.pins.args().map(OsString::from));
        full.extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
        self.client.git(Some(self.at), full, env).await
    }

    // Run git to make the repository at the place: the pins lead, and the
    // work tree is left to `clone`, which lays it and refuses to be given one.
    async fn create<I, S>(&self, args: I, env: &[(&str, &str)]) -> Result<Output>
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
            Err(refuse(&output, exists, missing, None).into())
        }
    }

    async fn rev_parse(&self, revision: &str) -> Result<String> {
        self.client.rev_parse(self.at, revision).await
    }

    // How a transport runs — the URL it reaches, the proxy it crosses, the
    // TLS it trusts — is the host's configuration to set: a setting the
    // repository made is config no pin can put back, so a transport operation
    // refuses the repository whole rather than carry the operator's
    // credentials anywhere or over anything.
    fn transportable(&self) -> Result<()> {
        if let Some(key) = self.pins.shaping() {
            let shown = self.shown();
            let message = format!(
                "{shown}: the repository sets {key}, and how a transport runs is the host's to set"
            );
            return Err(Error::Other(message).into());
        }
        Ok(())
    }

    async fn status(&self) -> Result<Vec<Change>> {
        let args = ["status", "--porcelain", "-z", "--no-renames", "--untracked-files=all"];
        let output = self.run(args, &self.shown(), "HEAD").await?;
        Ok(pending(&output.text()))
    }

    async fn conflicts(&self) -> Result<Vec<String>> {
        let args =
            ["diff", "--no-ext-diff", "--no-textconv", "--name-only", "--diff-filter=U", "-z"];
        let output = self.run(args, &self.shown(), "HEAD").await?;
        Ok(nul_separated(&output.text()))
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
        Ok(output.status.success().then(|| output.text().trim().to_owned()))
    }

    // The paths both sides changed since their common ancestor, where a
    // policy rule may apply; a path one side alone changed is git's clean
    // merge and no rule's, whatever the rule would lay there. Without an
    // ancestor, every path the two sides differ on. A rename is its two
    // paths, each ruled on its own.
    async fn touched(&self, base: Option<&str>) -> Result<Vec<String>> {
        let Some(base) = base else {
            return self.changed("HEAD", "MERGE_HEAD").await;
        };
        let ours = self.changed(base, "HEAD").await?;
        let theirs = self.changed(base, "MERGE_HEAD").await?;
        Ok(theirs.into_iter().filter(|path| ours.contains(path)).collect())
    }

    async fn changed(&self, from: &str, to: &str) -> Result<Vec<String>> {
        let args = [
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--name-only",
            "-z",
            from,
            to,
        ];
        let output = self.run(args, &self.shown(), "HEAD").await?;
        Ok(nul_separated(&output.text()))
    }

    // One commit's version of a path, byte for byte, or none when that commit
    // lacks it.
    async fn show(&self, revision: &str, path: &str) -> Result<Option<Vec<u8>>> {
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
        if head.text().trim() != format!("refs/heads/{name}") {
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

    // Lay a merged path in the working tree, its directory first where the
    // merge removed one.
    fn write(&self, path: &str, bytes: &[u8]) -> Result<()> {
        if let Some((parent, _)) = path.rsplit_once('/') {
            self.at
                .dir()
                .create_dir_all(parent)
                .with_context(|| format!("creating {parent} for the merge of {path}"))?;
        }
        self.at.dir().write(path, bytes).with_context(|| format!("writing the merge of {path}"))
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

// The protocol allow-list and the pack command the far side runs, named on
// the command line so the repository's own `remote.<name>.uploadPack` or
// `receivePack` — which no `-c` outranks — is never the program the
// operator's ssh carries to a host: for a local transport, the wrapped
// command that holds the far side to `FAR_SIDE`; for a remote one, git's
// own default.
fn transport(client: &Client, address: &str, push: bool) -> (&'static str, String) {
    let operation = if push { "receive-pack" } else { "upload-pack" };
    if is_local(address) {
        ("file", wrap(client, operation))
    } else {
        ("http:https:ssh", format!("--{operation}=git-{operation}"))
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

// What the far side of a local transport is held to, since a repository
// beneath the mount is as much the guest's as the place: no hooks, no
// alternate-refs command, no file-system monitor, and no update of a branch
// a working copy has checked out, which `receive.denyCurrentBranch=
// updateInstead` would write through the copy's own filters.
const FAR_SIDE: [&str; 4] = [
    "core.hooksPath=/dev/null",
    "core.alternateRefsCommand=",
    "core.fsmonitor=false",
    "receive.denyCurrentBranch=refuse",
];

// `git -c … <operation>` as one `--upload-pack` or `--receive-pack`
// argument: the near side's `-c` pins reach no process the far side starts,
// so its own ride the command. git splits the value with shell quoting for a
// local transport, so the binary is single-quoted.
fn wrap(client: &Client, operation: &str) -> String {
    let binary = sq(&client.binary.display().to_string());
    let mut pins = String::new();
    for pin in FAR_SIDE {
        pins.push_str(" -c ");
        pins.push_str(pin);
    }
    format!("--{operation}={binary}{pins} {operation}")
}

fn sq(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

// The URL a remote name or address reaches as the host's configuration
// expands it, for the transport rule: a remote's own URL, else the address
// rewritten under `url.<base>.insteadOf`, else the address as given. A
// repository with transport settings of its own was refused before this, so
// the expansion is the host's alone.
async fn expand(repo: &Repo<'_>, remote: &str, push: bool) -> String {
    let mut named = vec!["remote", "get-url"];
    if push {
        named.push("--push");
    }
    named.extend(["--", remote]);
    if let Ok(output) = repo.git(named, &[]).await
        && output.status.success()
    {
        return output.text().trim().to_owned();
    }
    match repo.git(["ls-remote", "--get-url", "--", remote], &[]).await {
        Ok(output) if output.status.success() => output.text().trim().to_owned(),
        _ => remote.to_owned(),
    }
}

// The typed refusal for a failed process: `exists` names a location taken,
// `missing` what the repository lacks, and `label` the one a push sent,
// which is what a rejected push is `diverged` over; an operation that
// pushes nothing passes `None` and is never typed so, whatever git said.
fn refuse(output: &Output, exists: &str, missing: &str, label: Option<&str>) -> Error {
    let detail = output.stderr.trim().to_owned();
    match (refusal::classify(&output.stderr), label) {
        (Class::NotARepository, _) => Error::NotARepository,
        (Class::Exists, _) => Error::Exists(exists.to_owned()),
        (Class::NotFound, _) => Error::NotFound(missing.to_owned()),
        (Class::Access, _) => Error::Access(detail),
        (Class::Diverged, Some(label)) => Error::Diverged(label.to_owned()),
        (Class::Diverged | Class::Other, None) | (Class::Other, Some(_)) => Error::Other(detail),
    }
}

fn shown(place: &Place) -> String {
    place.path().display().to_string()
}

// Non-empty entries of a `-z` list, each `\0`-terminated.
fn nul_separated(output: &str) -> Vec<String> {
    output.split('\0').filter(|entry| !entry.is_empty()).map(str::to_owned).collect()
}

// One entry per `-z` record of `log --format=%H%x1f%B`: the id, then the
// message as sealed, without the newline git ends a message with.
fn entries(output: &str) -> Vec<Entry> {
    nul_separated(output)
        .iter()
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

#[cfg(test)]
mod tests {
    use omnia_wasi_vcs::ChangeKind;

    use super::{entries, is_local, pending};

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
