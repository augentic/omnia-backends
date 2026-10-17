//! One `cursor-sdk-bridge` process per live agent, behind a fair semaphore.
//!
//! The pool is the worker nursery: the sandbox every worker runs under, the
//! callback server its tool calls come back through, the relay its API goes
//! out through, and the `max_agents` slots. A completion prepares its
//! [`Session`] first, so a bad workspace or grant costs no wait, then takes
//! a [`Lease`] before it creates its agent: a permit for one slot plus the
//! worker the agent runs on — a freshly spawned process inside that session,
//! registered with the callback server under its own token so its callbacks
//! route to its own agent, and pointed at the relay. A task of the pool's
//! holds the permit and the token until the process has exited, so a slot
//! never reopens, and a token never routes, while its process is still
//! around; dropping the lease only asks the worker to go.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result};
use omnia_sandbox::{Grant, Options, Sandbox, Session};
use omnia_wasi_model::ToolHost;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::callback::{Callback, Registration, Route};
use crate::elapsed_ms;
use crate::relay::Relay;
use crate::worker::{self, Worker};

pub struct Pool {
    permits: Arc<Semaphore>,
    sandbox: Sandbox,
    callback: Callback,
    relay: Relay,
    max_agents: usize,
}

impl Pool {
    // Nothing is spawned until the first lease, so a missing or broken
    // binary surfaces as that lease's failure.
    pub async fn connect(
        max_agents: usize, backend_url: &str, shell_roots: Vec<PathBuf>,
    ) -> Result<Self> {
        let sandbox = Sandbox::connect(Options {
            shell_roots,
            inherit: worker::INHERIT,
            refused: worker::REFUSED,
        })
        .await?;

        Ok(Self {
            permits: Arc::new(Semaphore::new(max_agents.min(Semaphore::MAX_PERMITS))),
            sandbox,
            callback: Callback::bind().await?,
            relay: Relay::bind(backend_url).await?,
            max_agents,
        })
    }

    // The session a worker runs in, held to the sandbox before any slot is taken.
    pub async fn prepare(&self, workspace: Option<&Path>, grant: &Grant) -> Result<Session> {
        self.sandbox.prepare(workspace, grant).await
    }

    // Wait for a slot, in arrival order, then for the worker to run on,
    // spawned inside `session`.
    pub async fn acquire(&self, session: Session) -> Result<Lease> {
        let queued = Instant::now();
        let permit =
            Arc::clone(&self.permits).acquire_owned().await.context("the agent pool is closed")?;

        let wait_ms = elapsed_ms(queued);
        if wait_ms > 0 {
            tracing::debug!(wait_ms, "agent slot acquired");
        }

        let registration = Arc::new(self.callback.register()?);
        let spawned = Worker::spawn(&registration, self.relay.url(), session)?;

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
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("max_agents", &self.max_agents)
            .field("sandbox", self.sandbox.options())
            .finish_non_exhaustive()
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

    pub fn attach(&self, agent_id: String, tool_host: Arc<dyn ToolHost>) -> Route {
        self.registration.attach(agent_id, tool_host)
    }
}
