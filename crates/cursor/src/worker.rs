//! Spawn and supervise one `cursor-sdk-bridge` process — a worker — with
//! `sdk.v1` bound over it.
//!
//! The process is a sandboxed [`Child`]: it runs inside the [`Session`] the
//! completion prepared, leads a process group of its own, and is killed with
//! whatever it forked when it exits or the worker lets it go.

mod discovery;

use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use discovery::Discovery;
use omnia_sandbox::{Child, Exit, Session};
use tokio::time::{Instant, timeout};

use crate::endpoint::Registration;
use crate::protocol::Rpc;
use crate::{Failure, elapsed_ms};

const READY_WAIT: Duration = Duration::from_secs(30);
const CONNECT_WAIT: Duration = Duration::from_secs(10);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(1);
const SHUTDOWN_WAIT: Duration = Duration::from_secs(5);
const READY_PREFIX: &str = "cursor-sdk-bridge ready ";

pub const INHERIT: &[&str] = &[
    "SHELL",
    "USER",
    "LOGNAME",
    "CURSOR_WEBSITE_URL",
    "CURSOR_DATA_DIR",
    "CURSOR_RIPGREP_PATH",
    "CURSOR_TREE_SITTER_VENDOR_DIR",
    "CURSOR_SDK_BRIDGE_LOG",
    "CURSOR_SDK_BRIDGE_DEBUG_STARTUP",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "NODE_EXTRA_CA_CERTS",
];
pub const REFUSED: &[&str] = &["CURSOR_API_KEY", "CURSOR_BACKEND_URL"];

// A spawned `cursor-sdk-bridge` process this client watches, with `sdk.v1`
// bound on it. Dropping it asks the worker to go.
#[derive(Debug)]
pub struct Worker {
    child: Child,
    rpc: Rpc,
}

impl Worker {
    // The handshake is left to `Spawned::handshake`, so a pool lease can
    // occupy the slot first.
    pub fn spawn(callback: &Registration, relay: &str, session: Session) -> Result<Spawned> {
        let state_root = session.scratch().to_owned();
        let mut command = session.command("cursor-sdk-bridge")?;
        command
            .env("CURSOR_SDK_CLIENT_LANGUAGE", "rust")
            .env("CURSOR_BACKEND_URL", relay)
            .arg("--state-root")
            .arg(state_root)
            .args(["--tool-callback-url", callback.url()])
            .args(["--tool-callback-auth-token", callback.token()])
            .ready_prefix(READY_PREFIX);
        let child = command.spawn().context("issue spawning `cursor-sdk-bridge`")?;

        Ok(Spawned { child })
    }

    pub const fn rpc(&self) -> &Rpc {
        &self.rpc
    }

    pub fn live_rpc(&self) -> Option<&Rpc> {
        self.child.is_running().then_some(&self.rpc)
    }

    pub async fn fail_on_exit<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        fail_on_exit(&self.child, future).await
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let rpc = self.rpc.clone();
        let ask = async move {
            let _ = rpc.shutdown(SHUTDOWN_GRACE).await;
        };
        self.child.stop(ask, SHUTDOWN_WAIT);
    }
}

// A process `Worker::spawn` spawned, watched and killable, with its
// ready-line handshake still to run. Dropped, it is killed: nothing is
// bound to ask over.
pub struct Spawned {
    child: Child,
}

impl Spawned {
    // Resolves once the process exits, however the handshake goes.
    pub fn exited(&self) -> impl Future<Output = Exit> + Send + 'static {
        self.child.exited()
    }

    // Wait for the ready line and bind `sdk.v1` over it; a handshake that
    // fails kills the process with it.
    pub async fn handshake(mut self) -> Result<Worker> {
        let started_at = Instant::now();

        let ready = self.child.ready();
        let scanned = async {
            let payload = ready
                .await
                .ok_or_else(|| anyhow!("the stderr reader ended without a ready line"))?;
            payload.parse::<Discovery>()
        };
        let discovery = self.step("no ready line", READY_WAIT, scanned).await?;

        let bound = async {
            let base_url = discovery.base_url()?;
            let token = discovery.token().await?;
            Rpc::connect(&base_url, &token).await
        };
        let rpc = self.step("no answer to the sdk.v1 handshake", CONNECT_WAIT, bound).await?;

        tracing::debug!(
            pid = self.child.pid(),
            spawn_ms = elapsed_ms(started_at),
            "cursor-sdk-bridge spawned"
        );

        let Self { child } = self;
        Ok(Worker { child, rpc })
    }

    // Run one handshake step within `bound` and under the process's exit;
    // past the bound it fails as `missing`. The stderr tail behind a failure
    // is logged at DEBUG; it never reaches the error.
    async fn step<T>(
        &self, missing: &str, bound: Duration, future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let bounded = async {
            timeout(bound, future)
                .await
                .unwrap_or_else(|_elapsed| Err(anyhow!("{missing} within {}s", bound.as_secs())))
        };

        fail_on_exit(&self.child, bounded)
            .await
            .inspect_err(|_error| self.child.log_tail())
            .context("cursor sdk handshake failed")
    }
}

async fn fail_on_exit<T>(child: &Child, future: impl Future<Output = Result<T>>) -> Result<T> {
    match child.fail_on_exit(future).await {
        Ok(outcome) => outcome,
        Err(exit) => Err(Failure::WorkerExited(exit).into()),
    }
}
