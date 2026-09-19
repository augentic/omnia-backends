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

        // A spawn failure drops the permit with it.
        let bridge = match &self.source {
            Source::Spawn { bin } => Arc::new(Bridge::spawn(bin, &self.endpoint).await?),
            Source::Attached(bridge) => Arc::clone(bridge),
        };
        Ok(Arc::new(Lease {
            permit: Some(permit),
            bridge,
        }))
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
        let bridge = Arc::clone(&self.bridge);
        if let Ok(handle) = Handle::try_current() {
            // The permit rides along so the slot reopens only once the
            // process is gone (an attached bridge closes as a no-op).
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
