use std::fmt;

use crate::bridge::Exit;

/// How a completion this backend ran came to fail, by variant rather than
/// by message.
///
/// `complete`'s error downcasts to one of these — or to a
/// [`TransportError`](crate::TransportError), or to the typed
/// `budget-exhausted` a rejected check ends on.
#[derive(Debug)]
#[non_exhaustive]
pub enum Failure {
    /// Absolute wall-clock cap exceeded while the stream was still active.
    Timeout {
        /// The cap in seconds, from connect options.
        cap_secs: u64,
    },
    /// No stream events within the inactivity window.
    Inactive {
        /// Observed idle span in seconds.
        idle_secs: u64,
        /// Configured inactivity limit in seconds.
        inactivity_secs: u64,
        /// Configured absolute cap in seconds.
        cap_secs: u64,
    },
    /// Hard tool-host failure (or a closed abort channel).
    Aborted(String),
    /// The spawned bridge process exited under the completion — during its
    /// handshake, or with a run on it.
    BridgeExited(Exit),
}

impl Failure {
    /// The `outcome` label the `cursor_completions` counter carries for
    /// this failure.
    #[must_use]
    pub const fn outcome(&self) -> &'static str {
        match self {
            Self::Timeout { .. } => "timeout",
            Self::Inactive { .. } => "inactive",
            Self::Aborted(_) => "abort",
            Self::BridgeExited(_) => "bridge_exit",
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { cap_secs } => write!(
                f,
                "cursor run timed out after {cap_secs}s (absolute cap exceeded while still active)"
            ),
            Self::Inactive {
                idle_secs,
                inactivity_secs,
                cap_secs,
            } => write!(
                f,
                "cursor run inactive for {idle_secs}s (no stream events; inactivity limit \
                 {inactivity_secs}s, absolute cap {cap_secs}s)"
            ),
            Self::Aborted(reason) => write!(f, "completion aborted: {reason}"),
            Self::BridgeExited(exit) => write!(f, "cursor-sdk-bridge exited ({exit})"),
        }
    }
}

impl std::error::Error for Failure {}
