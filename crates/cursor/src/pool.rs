//! One `cursor-sdk-bridge` process per live agent, behind a fair semaphore.
//!
//! A completion takes a [`Lease`] before it creates its agent: a permit for
//! one of the `max_agents` slots plus the worker the agent runs on — a
//! freshly spawned process, registered with the callback endpoint under its
//! own token so its callbacks route to its own agent. A task of the pool's
//! holds the permit and the token until the process has exited, so a slot
//! never reopens, and a token never routes, while its process is still
//! around; dropping the lease only asks the worker to go.

use std::sync::Arc;

use anyhow::{Context as _, Result};
use omnia_wasi_model::ToolHost;
use tokio::sync::{Semaphore, oneshot};
use tokio::time::Instant;

use crate::elapsed_ms;
use crate::endpoint::{Attached, Endpoint, Registration};
use crate::worker::Worker;

#[derive(Debug)]
pub struct Pool {
    permits: Arc<Semaphore>,
    endpoint: Endpoint,
    max_agents: usize,
}

impl Pool {
    /// Bind the callback endpoint and prove the worker is spawnable — one
    /// probe lease, closed again — so a missing or broken binary fails here
    /// rather than at the first completion.
    pub async fn connect(max_agents: usize) -> Result<Self> {
        let pool = Self {
            permits: Arc::new(Semaphore::new(max_agents.min(Semaphore::MAX_PERMITS))),
            endpoint: Endpoint::bind().await?,
            max_agents,
        };

        // closed here rather than left to the drop, so the probe is gone
        // before the first completion queues for its slot
        pool.lease().await?.worker().close().await;

        Ok(pool)
    }

    /// Wait for a slot, in arrival order, then for the worker to run on.
    pub async fn lease(&self) -> Result<Arc<Lease>> {
        let queued = Instant::now();
        let permit =
            Arc::clone(&self.permits).acquire_owned().await.context("the agent pool is closed")?;
        tracing::debug!(wait_ms = elapsed_ms(queued), "agent slot acquired");

        let registration = Arc::new(self.endpoint.register()?);
        let spawned = Worker::spawn(&registration)?;
        // The slot reopens, and the token is revoked, only once the
        // process is gone — however the lease ends, handshake included.
        let exited = spawned.exited();
        let token = Arc::clone(&registration);

        tokio::spawn(async move {
            exited.await;
            drop(permit);
            drop(token);
        });

        let worker = spawned.handshake().await?;
        Ok(Arc::new(Lease { worker, registration }))
    }

    pub const fn max_agents(&self) -> usize {
        self.max_agents
    }
}

/// One agent slot with its worker handshaken; dropping the lease asks the
/// worker to go.
#[derive(Debug)]
pub struct Lease {
    worker: Worker,
    registration: Arc<Registration>,
}

impl Lease {
    pub const fn worker(&self) -> &Worker {
        &self.worker
    }

    /// Route the worker's callbacks for `agent_id` into `tool_host` until
    /// the returned guard drops; the first hard tool failure is sent on
    /// `abort`.
    pub fn attach(
        &self, agent_id: String, tool_host: Arc<dyn ToolHost>, abort: oneshot::Sender<String>,
    ) -> Attached {
        self.registration.attach(agent_id, tool_host, abort)
    }
}
