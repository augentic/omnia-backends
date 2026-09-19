//! One `cursor-sdk-bridge` process per live agent, behind a fair semaphore.
//!
//! A completion takes a [`Lease`] before it creates its agent: a permit for
//! one of the `max_agents` slots plus the bridge the agent runs on — a
//! freshly spawned process, or the one external bridge every lease shares
//! in attach mode. Dropping the last handle to a lease closes a spawned
//! bridge and only then returns the permit, so a slot never reopens while
//! its process is still around.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context as _, Result, bail};
use omnia_wasi_model::ToolHost;
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::ConnectOptions;
use crate::bridge::{Bridge, elapsed_ms};
use crate::endpoint::{Attached, Endpoint};

#[derive(Debug)]
pub struct Pool {
    source: Source,
    permits: Arc<Semaphore>,
    endpoint: Endpoint,
    max_agents: usize,
}

#[derive(Debug)]
enum Source {
    /// A fresh process per lease.
    Spawn { bin: String },
    /// Every lease shares this externally managed bridge.
    Attached(Arc<Bridge>),
}

impl Pool {
    /// Bind the callback endpoint and prove the bridge is reachable — the
    /// attach handshake, or a probe spawn closed again — so a missing or
    /// broken binary fails here rather than at the first completion.
    pub async fn connect(options: &ConnectOptions) -> Result<Self> {
        let endpoint = Endpoint::bind().await?;
        let source = match (&options.bridge_url, &options.bridge_token) {
            (Some(url), Some(token)) => {
                Source::Attached(Arc::new(Bridge::attach(url.clone(), token).await?))
            }
            (None, None) => {
                Bridge::spawn(&options.bridge_bin, &endpoint).await?.close().await;
                Source::Spawn {
                    bin: options.bridge_bin.clone(),
                }
            }
            _ => bail!("bridge_url and bridge_token must be set together"),
        };
        Ok(Self {
            source,
            permits: Arc::new(Semaphore::new(options.max_agents.min(Semaphore::MAX_PERMITS))),
            endpoint,
            max_agents: options.max_agents,
        })
    }

    /// Wait for a slot, in arrival order, then for the bridge to run on.
    pub async fn lease(&self) -> Result<Arc<Lease>> {
        let queued = Instant::now();
        let permit =
            Arc::clone(&self.permits).acquire_owned().await.context("the agent pool is closed")?;
        tracing::info!(histogram.cursor_lease_wait_ms = elapsed_ms(queued), "agent slot acquired");

        match &self.source {
            Source::Attached(bridge) => Ok(Arc::new(Lease {
                permit: Some(permit),
                bridge: Arc::clone(bridge),
            })),
            Source::Spawn { bin } => {
                let started = match Bridge::start(bin, &self.endpoint) {
                    Ok(started) => started,
                    Err(error) => {
                        tracing::warn!(
                            monotonic_counter.cursor_bridge_spawn_failures = 1_u64,
                            "cursor-sdk-bridge failed to spawn"
                        );
                        return Err(error);
                    }
                };
                // Store the permit before the handshake: cancelling or failing
                // that wait drops this lease, which closes the process and
                // only then reopens the slot.
                let lease = Lease {
                    permit: Some(permit),
                    bridge: Arc::new(started.bridge),
                };
                if let Err(error) = started.handshake.complete().await {
                    tracing::warn!(
                        monotonic_counter.cursor_bridge_spawn_failures = 1_u64,
                        "cursor-sdk-bridge failed to spawn"
                    );
                    return Err(error);
                }
                tracing::info!(
                    pid = started.pid,
                    histogram.cursor_bridge_spawn_ms = elapsed_ms(started.at),
                    "cursor-sdk-bridge spawned"
                );
                Ok(Arc::new(lease))
            }
        }
    }

    /// Route callbacks for `agent_id` into `tool_host` until the returned
    /// guard drops.
    pub fn attach(
        &self, agent_id: String, tool_host: Arc<dyn ToolHost>, abort: mpsc::UnboundedSender<String>,
    ) -> Attached {
        self.endpoint.attach(agent_id, tool_host, abort)
    }

    pub const fn max_agents(&self) -> usize {
        self.max_agents
    }

    pub const fn is_attached(&self) -> bool {
        matches!(self.source, Source::Attached(_))
    }
}

/// One agent slot and the bridge it runs on.
#[derive(Debug)]
pub struct Lease {
    permit: Option<OwnedSemaphorePermit>,
    bridge: Arc<Bridge>,
}

impl Lease {
    pub fn bridge(&self) -> &Bridge {
        &self.bridge
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let permit = self.permit.take();
        // A shared bridge is not ours to close: the slot reopens now.
        if !self.bridge.is_owned() {
            return;
        }
        let bridge = Arc::clone(&self.bridge);
        if let Ok(handle) = Handle::try_current() {
            // The permit rides along so the slot reopens only once the
            // process is gone.
            handle.spawn(async move {
                bridge.close().await;
                drop(bridge);
                drop(permit);
            });
        }
        // Without a runtime both drop here; the bridge's own `Drop` still
        // asks the watcher for the shutdown.
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use super::*;

    impl Pool {
        async fn spawning(bin: String, max_agents: usize) -> Self {
            Self {
                source: Source::Spawn { bin },
                permits: Arc::new(Semaphore::new(max_agents)),
                endpoint: Endpoint::bind().await.expect("bind"),
                max_agents,
            }
        }

        fn available(&self) -> usize {
            self.permits.available_permits()
        }
    }

    /// A script that writes its pid and sleeps, so handshake never finishes.
    struct Hang {
        _dir: tempfile::TempDir,
        bin: String,
        pidfile: PathBuf,
    }

    impl Hang {
        fn new() -> Self {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let pidfile = dir.path().join("pid");
            let bin = dir.path().join("hang");
            std::fs::write(
                &bin,
                format!("#!/bin/sh\necho $$ >> '{}'\nexec sleep 86400\n", pidfile.display()),
            )
            .expect("write hang script");
            let mut permissions = std::fs::metadata(&bin).expect("metadata").permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&bin, permissions).expect("chmod");
            Self {
                bin: bin.to_str().expect("utf8 path").to_owned(),
                pidfile,
                _dir: dir,
            }
        }
    }

    async fn wait_path(path: &Path) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !path.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "hang script did not start: {}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn cancelled_handshake_holds_slot_until_process_exits() {
        let hang = Hang::new();
        let pool = Arc::new(Pool::spawning(hang.bin.clone(), 1).await);

        let mut first = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move { pool.lease().await }
        });
        let started = wait_path(&hang.pidfile);
        tokio::pin!(started);
        tokio::select! {
            biased;
            result = &mut first => panic!("lease finished before cancel: {result:?}"),
            () = &mut started => {}
        }

        first.abort();
        first.await.expect_err("the handshake task is cancelled");
        assert_eq!(
            pool.available(),
            0,
            "cancelling the handshake must not drop the permit at once"
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if pool.available() == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the slot reopens once the cancelled process has exited");
    }

    #[tokio::test]
    async fn failed_spawn_releases_slot_after_exit() {
        let pool = Pool::spawning("false".to_owned(), 1).await;
        pool.lease().await.expect_err("false never completes the handshake");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if pool.available() == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the slot reopens after the failed process exits");
    }
}
