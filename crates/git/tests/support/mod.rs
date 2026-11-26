//! Hermetic git for the suites: a scratch global configuration applied by a
//! wrapper the backend runs as its `git`, and that same git as the oracle a
//! scenario reads a repository with.
//!
//! The wrapper exports `GIT_CONFIG_GLOBAL` and `GIT_CONFIG_NOSYSTEM`, and
//! clears the identity a shell exports, before handing over to the `git` on
//! `PATH`, so nothing of the developer's configuration — identity, signing,
//! hooks, default branch — reaches a scenario, and the backend is driven
//! through `GIT_BINARY` as a deployment drives it. Every wrapper sets that
//! configuration itself and hands over to the `git` on `PATH`, since a
//! bounded git reads no file of the suite's beside the program it runs.
//! Origins lie beneath an `origins` directory a scenario mounts, since a
//! guest dials no local repository outside its mounts. A network remote is
//! an ssh fake the host's configuration names, serving repositories outside
//! every mount and logging each dial.

#![allow(dead_code, reason = "each suite uses the part of the support it needs")]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use omnia::{Connect as _, Mount};
use omnia_git::{Client, ConnectOptions};
use tempfile::TempDir;

/// Connects a [`Client`] over `binary`, held to the kernel boundary.
pub async fn connect(binary: impl AsRef<Path>) -> anyhow::Result<Client> {
    connect_with(binary.as_ref(), false, None).await
}

/// Connects a [`Client`] over `binary` without the kernel boundary, as an operator who opts out.
pub async fn connect_unbounded(binary: impl AsRef<Path>) -> anyhow::Result<Client> {
    connect_with(binary.as_ref(), true, None).await
}

async fn connect_with(
    binary: &Path, unbounded: bool, mirrors: Option<&Path>,
) -> anyhow::Result<Client> {
    let utf8 = |path: &Path| path.to_str().expect("scratch path is UTF-8").to_owned();
    Client::connect_with(ConnectOptions {
        binary: utf8(binary),
        unbounded,
        mirrors: mirrors.map(utf8),
    })
    .await
}

/// One dial the ssh fake took: the directory git dialled from, and what it handed ssh.
pub struct Dial {
    pub from: PathBuf,
    pub argv: Vec<String>,
}

impl Dial {
    /// What git asked the far host to run.
    pub fn command(&self) -> &str {
        self.argv.last().map_or("", String::as_str)
    }
}

pub struct Hermetic {
    root: TempDir,
}

impl Hermetic {
    pub fn new() -> Self {
        let root = tempfile::Builder::new().prefix("omnia-git-").tempdir().expect("scratch root");
        fs::write(
            root.path().join("gitconfig"),
            "[user]\n\tname = Omnia Test\n\temail = test@omnia.invalid\n\
             [init]\n\tdefaultBranch = main\n\
             [commit]\n\tgpgsign = false\n\
             [core]\n\thooksPath = /dev/null\n",
        )
        .expect("writing gitconfig");
        fs::create_dir(root.path().join("origins")).expect("the origins directory");
        fs::create_dir(root.path().join("mirrors")).expect("the mirrors directory");
        let hermetic = Self { root };
        write_script(
            &hermetic.binary(),
            &format!("#!/bin/sh\n{}exec git \"$@\"\n", hermetic.preamble()),
        );
        hermetic
    }

    // What every wrapper runs first: the scratch global configuration, and
    // none of the identity a shell exports.
    fn preamble(&self) -> String {
        format!(
            "export GIT_CONFIG_GLOBAL='{}' GIT_CONFIG_NOSYSTEM=1\n\
             unset GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL\n",
            self.path("gitconfig").display()
        )
    }

    /// The writable `origins` mount the origins [`bare`](Self::bare) lays lie beneath.
    pub fn origins(&self) -> Mount {
        Mount {
            name: "origins".to_owned(),
            path: self.path("origins"),
            writable: true,
        }
    }

    /// The wrapper the backend and the oracle both run.
    pub fn binary(&self) -> PathBuf {
        self.root.path().join("git")
    }

    /// A fresh directory beneath the scratch root, not yet created.
    pub fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    pub async fn client(&self) -> Client {
        self.client_over(self.binary()).await
    }

    /// A [`Client`] over `binary`, held to the kernel boundary, its mirrors in [`mirrors`](Self::mirrors).
    pub async fn client_over(&self, binary: impl AsRef<Path>) -> Client {
        let mirrors = self.path("mirrors");
        connect_with(binary.as_ref(), false, Some(&mirrors)).await.expect("connecting over git")
    }

    /// Where every client the suite connects keeps its mirrors, resolved.
    pub fn mirrors(&self) -> PathBuf {
        fs::canonicalize(self.path("mirrors")).expect("the mirrors directory")
    }

    /// Names the ssh fake as the host's `core.sshCommand`.
    ///
    /// The fake serves what [`served`](Self::served) lays, answers nothing else, and logs every
    /// dial for [`dials`](Self::dials).
    pub fn ssh(&self) {
        let ssh = self.path("ssh");
        write_script(
            &ssh,
            &format!(
                "#!/bin/sh\n\
                 {{ printf '%s' \"$(pwd -P)\"; printf '\\t%s' \"$@\"; printf '\\n'; }} >> '{dials}'\n\
                 for command; do :; done\n\
                 eval \"set -- $command\"\n\
                 case $2 in\n\
                 '{served}'/*) exec '{git}' \"${{1#git-}}\" \"$2\" ;;\n\
                 esac\n\
                 exit 255\n",
                dials = self.path("dials").display(),
                served = self.path("served").display(),
                git = self.binary().display(),
            ),
        );
        self.host("core.sshCommand", &ssh.display().to_string());
    }

    /// A bare repository at `name` outside every mount, and the `ssh://` URL the fake serves it at.
    pub fn served(&self, name: &str) -> (PathBuf, String) {
        let path = self.path("served").join(name);
        fs::create_dir_all(&path).expect("creating the served repository");
        self.git(&path, &["init", "--quiet", "--bare"]);
        self.ssh();
        let url = format!("ssh://localhost{}", path.display());
        (path, url)
    }

    /// Every dial the ssh fake took, in order.
    pub fn dials(&self) -> Vec<Dial> {
        let Ok(log) = fs::read_to_string(self.path("dials")) else {
            return Vec::new();
        };
        let dial = |line: &str| {
            let mut fields = line.split('\t');
            let from = PathBuf::from(fields.next().unwrap_or_default());
            Dial {
                from,
                argv: fields.map(str::to_owned).collect(),
            }
        };
        log.lines().map(dial).collect()
    }

    /// A setting in the scratch global configuration: the host's, as an operator set it.
    pub fn host(&self, key: &str, value: &str) {
        let config = self.root.path().join("gitconfig");
        let output = Command::new(self.binary())
            .args(["config", "--file"])
            .arg(&config)
            .args([key, value])
            .output()
            .expect("running the oracle git");
        assert!(
            output.status.success(),
            "setting {key}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// The oracle: one git over the wrapper, held to success, its stdout trimmed.
    pub fn git(&self, at: &Path, args: &[&str]) -> String {
        let output = Command::new(self.binary())
            .arg("-C")
            .arg(at)
            .args(args)
            .output()
            .expect("running the oracle git");
        assert!(
            output.status.success(),
            "git {args:?} at {}: {}",
            at.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    /// A bare repository at `name` beneath the origins, and the `file://` URL a clone reaches it by.
    pub fn bare(&self, name: &str) -> (PathBuf, String) {
        let path = self.path("origins").join(name);
        fs::create_dir_all(&path).expect("creating the bare repository");
        self.git(&path, &["init", "--quiet", "--bare"]);
        let url = format!("file://{}", path.display());
        (path, url)
    }

    /// A `git` that only ever reports `version`, for the startup refusal.
    pub fn reporting(&self, version: &str) -> PathBuf {
        let script = self.path("old-git");
        write_script(&script, &format!("#!/bin/sh\necho 'git version {version}'\n"));
        script
    }

    /// A `git` that appends a repository's `.git/plant` to its configuration right after a
    /// listing returns and its `.git/plant-merge` right before a merge starts, fails a listing
    /// beside `.git/unlisted`, and lays every clone from `template`.
    pub fn planting(&self, template: &Path) -> PathBuf {
        let script = self.path("planting-git");
        write_script(
            &script,
            &format!(
                "#!/bin/sh\n\
                 {preamble}\
                 case \" $* \" in\n\
                 *' config --show-scope --list -z '*)\n\
                 \t[ -e .git/unlisted ] && {{ echo 'fatal: the listing failed' >&2; exit 1; }}\n\
                 \tgit \"$@\"\n\
                 \tstatus=$?\n\
                 \t[ -f .git/plant ] && cat .git/plant >> .git/config && rm .git/plant\n\
                 \texit $status\n\
                 \t;;\n\
                 *' clone '*)\n\
                 \texport GIT_TEMPLATE_DIR='{template}'\n\
                 \t;;\n\
                 *' merge '*)\n\
                 \t[ -f .git/plant-merge ] && cat .git/plant-merge >> .git/config && rm .git/plant-merge\n\
                 \t;;\n\
                 esac\n\
                 exec git \"$@\"\n",
                preamble = self.preamble(),
                template = template.display(),
            ),
        );
        script
    }

    /// A `git` that tries to leave the kernel boundary where a repository asks it to.
    ///
    /// - At a repository holding `.git/swap`, it swaps `.git` for a link to the path the file
    ///   names, once a listing returns.
    /// - At one holding `.git/probe`, a run that reads a mirror instead tries to write the
    ///   mirror, read the file the probe's second line names, and dial the local port its
    ///   first line names. It records in `.git/probed` whether the write and the read went
    ///   through, and fails the run.
    pub fn escaping(&self) -> PathBuf {
        let script = self.path("escaping-git");
        write_script(
            &script,
            &format!(
                "#!/bin/sh\n\
                 {preamble}\
                 case \" $* \" in\n\
                 *' config --show-scope --list -z '*)\n\
                 \tif [ -f .git/swap ]; then\n\
                 \t\tgit \"$@\"\n\
                 \t\tstatus=$?\n\
                 \t\tmv .git .git-swapped && ln -s \"$(cat .git-swapped/swap)\" .git\n\
                 \t\texit $status\n\
                 \tfi\n\
                 \t;;\n\
                 esac\n\
                 for arg; do\n\
                 \tcase $arg in\n\
                 \t*/mirrors/*.git)\n\
                 \t\t[ -f .git/probe ] || continue\n\
                 \t\t{{ read -r port; read -r outside; }} < .git/probe\n\
                 \t\tif touch \"$arg/probe\" 2>/dev/null; then echo 'mirror written'; else echo 'mirror refused'; fi >> .git/probed\n\
                 \t\tif cat \"$outside\" > /dev/null 2>&1; then echo 'outside read'; else echo 'outside refused'; fi >> .git/probed\n\
                 \t\tGIT_ALLOW_PROTOCOL=http GIT_HTTP_LOW_SPEED_LIMIT=1 GIT_HTTP_LOW_SPEED_TIME=1 \\\n\
                 \t\t\tgit ls-remote \"http://127.0.0.1:$port/\" > /dev/null 2>&1\n\
                 \t\texit 1\n\
                 \t\t;;\n\
                 \tesac\n\
                 done\n\
                 exec git \"$@\"\n",
                preamble = self.preamble(),
            ),
        );
        script
    }

    /// A current `git` that records `var` from its environment, and the file it records to.
    pub fn recording(&self, var: &str) -> (PathBuf, PathBuf) {
        let script = self.path("recording-git");
        let record = self.path("recorded");
        write_script(
            &script,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"${{{var}-unset}}\" > '{}'\necho 'git version 2.50.0'\n",
                record.display()
            ),
        );
        (script, record)
    }
}

fn write_script(path: &Path, body: &str) {
    fs::write(path, body).expect("writing a script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("marking it executable");
}
