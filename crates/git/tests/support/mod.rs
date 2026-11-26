//! Hermetic git for the suites: a scratch global configuration applied by a
//! wrapper the backend runs as its `git`, and that same git as the oracle a
//! scenario reads a repository with.
//!
//! The wrapper exports `GIT_CONFIG_GLOBAL` and `GIT_CONFIG_NOSYSTEM`, and
//! clears the identity a shell exports, before handing over to the `git` on
//! `PATH`, so nothing of the developer's configuration — identity, signing,
//! hooks, default branch — reaches a scenario, and the backend is driven
//! through `GIT_BINARY` as a deployment drives it.

#![allow(dead_code, reason = "each suite uses the part of the support it needs")]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use omnia::Connect as _;
use omnia_git::{Client, ConnectOptions};
use tempfile::TempDir;

/// Connects a [`Client`] over `binary`.
pub async fn connect(binary: impl AsRef<Path>) -> anyhow::Result<Client> {
    Client::connect_with(ConnectOptions {
        binary: binary.as_ref().to_str().expect("scratch path is UTF-8").to_owned(),
    })
    .await
}

pub struct Hermetic {
    root: TempDir,
}

impl Hermetic {
    pub fn new() -> Self {
        let root = tempfile::Builder::new().prefix("omnia-git-").tempdir().expect("scratch root");
        let config = root.path().join("gitconfig");
        fs::write(
            &config,
            "[user]\n\tname = Omnia Test\n\temail = test@omnia.invalid\n\
             [init]\n\tdefaultBranch = main\n\
             [commit]\n\tgpgsign = false\n\
             [core]\n\thooksPath = /dev/null\n",
        )
        .expect("writing gitconfig");
        write_script(
            &root.path().join("git"),
            &format!(
                "#!/bin/sh\n\
                 export GIT_CONFIG_GLOBAL='{}' GIT_CONFIG_NOSYSTEM=1\n\
                 unset GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL\n\
                 exec git \"$@\"\n",
                config.display()
            ),
        );
        Self { root }
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
        connect(self.binary()).await.expect("connecting over the hermetic git")
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

    /// A bare repository at `name`, and the `file://` URL a clone reaches it by.
    pub fn bare(&self, name: &str) -> (PathBuf, String) {
        let path = self.path(name);
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
                 git='{git}'\n\
                 case \" $* \" in\n\
                 *' config --show-scope --list -z '*)\n\
                 \t[ -e .git/unlisted ] && {{ echo 'fatal: the listing failed' >&2; exit 1; }}\n\
                 \t\"$git\" \"$@\"\n\
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
                 exec \"$git\" \"$@\"\n",
                git = self.binary().display(),
                template = template.display(),
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
