#![doc = include_str!("../README.md")]

mod command;
mod vcs;

use std::path::PathBuf;

use anyhow::{Context as _, Result, ensure};
use omnia::Backend;
use tracing::instrument;

/// The oldest git the backend runs over: `config --show-scope`, which the
/// host-policy pins are read through, arrived in 2.26.
pub const MIN_VERSION: (u32, u32) = (2, 26);

/// Git backend for `omnia:vcs`: one `git` process per operation.
#[derive(Debug, Clone)]
pub struct Client {
    binary: PathBuf,
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument(name = "Git::connect")]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        Self::connect(options.binary).await
    }
}

impl Client {
    /// Connects over the git at `binary`, a name on `PATH` or a path.
    ///
    /// # Errors
    ///
    /// Returns an error when the binary does not run, does not report a
    /// version, or is older than [`MIN_VERSION`].
    pub async fn connect(binary: impl Into<PathBuf>) -> Result<Self> {
        let client = Self {
            binary: binary.into(),
        };
        let output = client.git(None, ["--version"], &[]).await?;
        ensure!(output.status.success(), "`git --version` failed: {}", output.stderr.trim());
        let reported = output.text();
        let (major, minor, patch) = vcs::refusal::version(&reported)
            .with_context(|| format!("unrecognised `git --version`: {}", reported.trim()))?;
        ensure!(
            (major, minor) >= MIN_VERSION,
            "git {major}.{minor}.{patch} is older than {}.{}, the first with `config --show-scope`",
            MIN_VERSION.0,
            MIN_VERSION.1
        );
        Ok(client)
    }
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use fromenv::FromEnv;

    /// Connection options for the git backend.
    #[derive(Clone, Debug, FromEnv)]
    pub struct ConnectOptions {
        /// The git to run: a name on `PATH` or a path.
        #[env(from = "GIT_BINARY", default = "git")]
        pub binary: String,
    }
}
pub use config::ConnectOptions;

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}
