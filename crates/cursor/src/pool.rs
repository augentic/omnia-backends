//! One `cursor-sdk-bridge` process per live agent, behind a fair semaphore.
//!
//! A completion takes a [`Lease`] before it creates its agent: a permit for
//! one of the `max_agents` slots plus the bridge the agent runs on — a
//! freshly spawned process, registered with the callback endpoint under its
//! own token so its callbacks route to its own agent. Dropping the last
//! handle to a lease closes the bridge and only then returns the permit, so
//! a slot never reopens while its process is still around.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context as _, Result};
use omnia_wasi_model::ToolHost;
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::bridge::{Bridge, elapsed_ms};
use crate::endpoint::{Attached, Endpoint, Registration};

#[derive(Debug)]
pub struct Pool {
    permits: Arc<Semaphore>,
    endpoint: Endpoint,
    max_agents: usize,
}

impl Pool {
    /// Bind the callback endpoint and prove the bridge is spawnable — a
    /// probe spawn, closed again — so a missing or broken binary fails here
    /// rather than at the first completion.
    pub async fn connect(max_agents: usize) -> Result<Self> {
        let endpoint = Endpoint::bind().await?;
        let probe = endpoint.register()?;
        Bridge::spawn(&probe).await?.close().await;
        Ok(Self {
            permits: Arc::new(Semaphore::new(max_agents.min(Semaphore::MAX_PERMITS))),
            endpoint,
            max_agents,
        })
    }

    /// Wait for a slot, in arrival order, then for the bridge to run on.
    pub async fn lease(&self) -> Result<Arc<Lease>> {
        let queued = Instant::now();
        let permit =
            Arc::clone(&self.permits).acquire_owned().await.context("the agent pool is closed")?;
        tracing::info!(histogram.cursor_lease_wait_ms = elapsed_ms(queued), "agent slot acquired");

        let registration = Arc::new(self.endpoint.register()?);
        let started = match Bridge::start(&registration) {
            Ok(started) => started,
            Err(error) => {
                tracing::warn!(
                    monotonic_counter.cursor_bridge_spawn_failures = 1_u64,
                    "cursor-sdk-bridge failed to spawn"
                );
                return Err(error);
            }
        };
        // Store the permit before the handshake: cancelling or failing that
        // wait drops this lease, which closes the process and only then
        // reopens the slot.
        let lease = Lease {
            permit: Some(permit),
            bridge: Arc::new(started.bridge),
            registration,
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

    pub const fn max_agents(&self) -> usize {
        self.max_agents
    }
}

/// One agent slot and the bridge it runs on.
#[derive(Debug)]
pub struct Lease {
    permit: Option<OwnedSemaphorePermit>,
    bridge: Arc<Bridge>,
    // The spawned process's own callback identity.
    registration: Arc<Registration>,
}

impl Lease {
    pub fn bridge(&self) -> &Bridge {
        &self.bridge
    }

    /// Route the bridge's callbacks for `agent_id` into `tool_host` until
    /// the returned guard drops.
    pub fn attach(
        &self, agent_id: String, tool_host: Arc<dyn ToolHost>, abort: mpsc::UnboundedSender<String>,
    ) -> Attached {
        self.registration.attach(agent_id, tool_host, abort)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let permit = self.permit.take();
        let bridge = Arc::clone(&self.bridge);
        let registration = Arc::clone(&self.registration);
        if let Ok(handle) = Handle::try_current() {
            // The token stays valid until the process is gone, and the
            // permit rides along so the slot reopens only then.
            handle.spawn(async move {
                bridge.close().await;
                drop(bridge);
                drop(registration);
                drop(permit);
            });
        }
        // Without a runtime all three drop here; the bridge's own `Drop`
        // still asks the watcher for the shutdown.
    }
}
