//! One `cursor-sdk-bridge` process per live agent, behind a fair semaphore.
//!
//! A completion takes a [`Lease`] before it creates its agent: a permit for
//! one of the `max_agents` slots plus the worker the agent runs on — a
//! freshly spawned process inside the boundary the completion laid,
//! registered with the callback endpoint under its own token so its
//! callbacks route to its own agent, and pointed at the relay its API goes
//! through. A task of the pool's holds the permit and the token until the
//! process has exited, so a slot never reopens, and a token never routes,
//! while its process is still around; dropping the lease only asks the
//! worker to go.

use std::sync::Arc;

use anyhow::{Context as _, Result};
use omnia_confine::Boundary;
use omnia_wasi_model::ToolHost;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::elapsed_ms;
use crate::endpoint::{Attached, Endpoint, Registration};
use crate::relay::Relay;
use crate::worker::Worker;

#[derive(Debug)]
pub struct Pool {
    permits: Arc<Semaphore>,
    endpoint: Endpoint,
    relay: Relay,
    max_agents: usize,
}

impl Pool {
    // Nothing is spawned until the first lease, so a missing or broken
    // binary surfaces as that lease's failure.
    pub async fn connect(max_agents: usize, backend_url: &str) -> Result<Self> {
        Ok(Self {
            permits: Arc::new(Semaphore::new(max_agents.min(Semaphore::MAX_PERMITS))),
            endpoint: Endpoint::bind().await?,
            relay: Relay::bind(backend_url).await?,
            max_agents,
        })
    }

    // Wait for a slot, in arrival order, then for the worker to run on,
    // spawned inside `boundary`.
    pub async fn lease(&self, boundary: Boundary) -> Result<Lease> {
        let queued = Instant::now();
        let permit =
            Arc::clone(&self.permits).acquire_owned().await.context("the agent pool is closed")?;

        let wait_ms = elapsed_ms(queued);
        if wait_ms > 0 {
            tracing::debug!(wait_ms, "agent slot acquired");
        }

        let registration = Arc::new(self.endpoint.register()?);
        let spawned = Worker::spawn(&registration, self.relay.url(), boundary)?;

        // slot and token are held until the process is gone, however the lease ends
        let exited = spawned.exited();
        let token = Arc::clone(&registration);

        tokio::spawn(async move {
            exited.await;
            drop(permit);
            drop(token);
        });

        let worker = spawned.handshake().await?;
        Ok(Lease { worker, registration })
    }

    pub const fn max_agents(&self) -> usize {
        self.max_agents
    }
}

// One agent slot with its worker handshaken; dropping the lease asks the
// worker to go.
#[derive(Debug)]
pub struct Lease {
    worker: Worker,
    registration: Arc<Registration>,
}

impl Lease {
    pub const fn worker(&self) -> &Worker {
        &self.worker
    }

    pub fn attach(&self, agent_id: String, tool_host: Arc<dyn ToolHost>) -> Attached {
        self.registration.attach(agent_id, tool_host)
    }
}
