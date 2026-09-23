//! One `cursor-sdk-bridge` process per live agent, behind a fair semaphore.
//!
//! A completion takes a [`Lease`] before it creates its agent: a permit for
//! one of the `max_agents` slots plus the bridge the agent runs on — a
//! freshly spawned process, registered with the callback endpoint under its
//! own token so its callbacks route to its own agent. Dropping the last
//! handle to a lease closes the bridge and only then returns the permit, so
//! a slot never reopens while its process is still around.

use std::sync::Arc;

use anyhow::{Context as _, Result};
use omnia_wasi_model::ToolHost;
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::Instant;

use crate::bridge::{Bridge, Rpc};
use crate::elapsed_ms;
use crate::endpoint::{Attached, Endpoint, Registration};

#[derive(Debug)]
pub struct Pool {
    permits: Arc<Semaphore>,
    endpoint: Endpoint,
    max_agents: usize,
}

impl Pool {
    /// Bind the callback endpoint and prove the bridge is spawnable — one
    /// probe lease, closed again — so a missing or broken binary fails here
    /// rather than at the first completion.
    pub async fn connect(max_agents: usize) -> Result<Self> {
        let pool = Self {
            permits: Arc::new(Semaphore::new(max_agents.min(Semaphore::MAX_PERMITS))),
            endpoint: Endpoint::bind().await?,
            max_agents,
        };
        
        // closed here rather than on the lease's drop task, so the probe is
        // gone before the first completion queues for its slot
        pool.lease().await?.bridge().close().await;

        Ok(pool)
    }

    /// Wait for a slot, in arrival order, then for the bridge to run on.
    pub async fn lease(&self) -> Result<Arc<Lease>> {
        let queued = Instant::now();
        let permit =
            Arc::clone(&self.permits).acquire_owned().await.context("the agent pool is closed")?;
        tracing::info!(histogram.cursor_lease_wait_ms = elapsed_ms(queued), "agent slot acquired");

        let registration = Arc::new(self.endpoint.register()?);
        let spawned = async {
            let (bridge, handshake) = Bridge::spawn(&registration)?;
            let slot = Slot {
                permit: Some(permit),
                bridge: Arc::new(bridge),
                registration: Arc::clone(&registration),
            };
            let rpc = handshake.complete().await?;

            Ok(Arc::new(Lease { slot, rpc }))
        };

        spawned.await.inspect_err(|_error| {
            tracing::warn!(
                monotonic_counter.cursor_bridge_spawn_failures = 1_u64,
                "cursor-sdk-bridge failed to spawn"
            );
        })
    }

    pub const fn max_agents(&self) -> usize {
        self.max_agents
    }
}

/// One agent slot with its bridge handshaken: the `sdk.v1` client is bound
/// for as long as the lease lives.
#[derive(Debug)]
pub struct Lease {
    slot: Slot,
    rpc: Rpc,
}

impl Lease {
    pub fn bridge(&self) -> &Bridge {
        &self.slot.bridge
    }

    /// The bound `sdk.v1` client.
    pub const fn rpc(&self) -> &Rpc {
        &self.rpc
    }

    /// The bound `sdk.v1` client, while its bridge is still running.
    pub fn live_rpc(&self) -> Option<&Rpc> {
        self.slot.bridge.is_running().then_some(&self.rpc)
    }

    /// Route the bridge's callbacks for `agent_id` into `tool_host` until
    /// the returned guard drops.
    pub fn attach(
        &self, agent_id: String, tool_host: Arc<dyn ToolHost>, abort: mpsc::UnboundedSender<String>,
    ) -> Attached {
        self.slot.registration.attach(agent_id, tool_host, abort)
    }
}

// The permit, the process, and the process's own callback identity; held
// from before the handshake so a spawn that fails or is abandoned still
// closes the process before the permit returns.
#[derive(Debug)]
struct Slot {
    permit: Option<OwnedSemaphorePermit>,
    bridge: Arc<Bridge>,
    registration: Arc<Registration>,
}

impl Drop for Slot {
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
        // Without a runtime all three drop here; dropping the bridge still
        // asks the watcher for the shutdown.
    }
}
