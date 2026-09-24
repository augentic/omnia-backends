use crate::bridge::{Exit, RpcError, RunStatus};

/// How a completion this backend ran came to fail, by variant rather than
/// by message.
///
/// `complete`'s error downcasts to one of these — or to an [`RpcError`], or
/// to the typed `budget-exhausted` a rejected check ends on.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Failure {
    /// The run reached a terminal status other than `finished`: the bridge
    /// or the provider answered with an error.
    #[error("cursor run {status}: {}", .detail.as_deref().unwrap_or("<no detail>"))]
    Run {
        /// The status the stream's result carried.
        status: RunStatus,
        /// The result's error code, else the last status message; `None`
        /// when the stream carried neither.
        detail: Option<String>,
    },
    /// Absolute wall-clock cap exceeded while the stream was still active.
    #[error("cursor run timed out after {cap_secs}s (absolute cap exceeded while still active)")]
    Timeout {
        /// The cap in seconds, from connect options.
        cap_secs: u64,
    },
    /// No stream events within the inactivity window.
    #[error(
        "cursor run inactive for {idle_secs}s (no stream events; inactivity limit \
         {inactivity_secs}s, absolute cap {cap_secs}s)"
    )]
    Inactive {
        /// Observed idle span in seconds.
        idle_secs: u64,
        /// Configured inactivity limit in seconds.
        inactivity_secs: u64,
        /// Configured absolute cap in seconds.
        cap_secs: u64,
    },
    /// Hard tool-host failure (or a closed abort channel).
    #[error("completion aborted: {0}")]
    Aborted(String),
    /// The spawned bridge process exited under the completion — during its
    /// handshake, or with a run on it.
    #[error("cursor-sdk-bridge exited ({0})")]
    BridgeExited(Exit),
}

impl Failure {
    /// The `outcome` label the `cursor_completions` counter carries for
    /// this failure.
    #[must_use]
    pub const fn outcome(&self) -> &'static str {
        Outcome::of_failure(self).as_str()
    }
}

/// How one completion came out: the closed set of `outcome` labels the
/// `cursor_completions` counter carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Answered on the opening prompt.
    Ok,
    /// Answered after the guest's check rejected a candidate.
    Corrected,
    Timeout,
    Inactive,
    /// [`Failure::Aborted`], or a completion dropped before it finished.
    Abort,
    BridgeExit,
    /// An [`RpcError::Transport`]: the socket to the bridge failed below
    /// Connect.
    Transport,
    /// The guest's check rejected every candidate.
    Exhausted,
    /// The bridge or the provider answered with an error: a
    /// [`Failure::Run`], an [`RpcError::Connect`], or anything else.
    Error,
}

impl Outcome {
    /// Classify a failed `complete`.
    pub fn of(error: &anyhow::Error) -> Self {
        if let Some(failure) = error.downcast_ref::<Failure>() {
            return Self::of_failure(failure);
        }
        match error.downcast_ref::<RpcError>() {
            Some(RpcError::Transport { .. }) => return Self::Transport,
            Some(RpcError::Connect { .. }) => return Self::Error,
            None => {}
        }
        match error.downcast_ref::<omnia_wasi_model::Error>() {
            Some(omnia_wasi_model::Error::BudgetExhausted(_)) => Self::Exhausted,
            _ => Self::Error,
        }
    }

    const fn of_failure(failure: &Failure) -> Self {
        match failure {
            Failure::Run { .. } => Self::Error,
            Failure::Timeout { .. } => Self::Timeout,
            Failure::Inactive { .. } => Self::Inactive,
            Failure::Aborted(_) => Self::Abort,
            Failure::BridgeExited(_) => Self::BridgeExit,
        }
    }

    /// Whether the bridge, or the socket to it, was lost under the
    /// completion. Neither says anything about the prompt, so a fresh
    /// bridge may be given it again; every other outcome is the bridge
    /// answering — a Connect error, an end-stream error, a run that ended
    /// in a failing status — and is not.
    pub const fn lost_bridge(self) -> bool {
        matches!(self, Self::BridgeExit | Self::Transport)
    }

    /// The metric label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Corrected => "corrected",
            Self::Timeout => "timeout",
            Self::Inactive => "inactive",
            Self::Abort => "abort",
            Self::BridgeExit => "bridge_exit",
            Self::Transport => "transport",
            Self::Exhausted => "exhausted",
            Self::Error => "error",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Exit, Failure, Outcome, RpcError, RunStatus};

    // an exit whose status the wait never reported
    const EXITED: Exit = Exit { status: None, pid: 1 };

    fn inactive() -> anyhow::Error {
        Failure::Inactive {
            idle_secs: 120,
            inactivity_secs: 120,
            cap_secs: 600,
        }
        .into()
    }

    fn run_error(detail: Option<&str>) -> anyhow::Error {
        Failure::Run {
            status: RunStatus::Error,
            detail: detail.map(ToOwned::to_owned),
        }
        .into()
    }

    // The labels are what dashboards key on.
    #[test]
    fn labels() {
        let labels = [
            Outcome::Ok,
            Outcome::Corrected,
            Outcome::Timeout,
            Outcome::Inactive,
            Outcome::Abort,
            Outcome::BridgeExit,
            Outcome::Transport,
            Outcome::Exhausted,
            Outcome::Error,
        ]
        .map(Outcome::as_str);
        assert_eq!(
            labels,
            [
                "ok",
                "corrected",
                "timeout",
                "inactive",
                "abort",
                "bridge_exit",
                "transport",
                "exhausted",
                "error"
            ]
        );
        assert_eq!(Failure::Timeout { cap_secs: 600 }.outcome(), "timeout");
    }

    #[test]
    fn classified() {
        let timeout: anyhow::Error = Failure::Timeout { cap_secs: 600 }.into();
        assert_eq!(Outcome::of(&timeout), Outcome::Timeout);
        assert_eq!(Outcome::of(&inactive()), Outcome::Inactive);

        let aborted: anyhow::Error = Failure::Aborted("session closed".to_owned()).into();
        assert_eq!(Outcome::of(&aborted), Outcome::Abort);

        let exited: anyhow::Error = Failure::BridgeExited(EXITED).into();
        assert_eq!(Outcome::of(&exited), Outcome::BridgeExit);
        assert_eq!(exited.to_string(), "cursor-sdk-bridge exited (status unknown)");

        let transport: anyhow::Error = RpcError::truncated("SdkAgentService/Send", 3).into();
        assert_eq!(Outcome::of(&transport), Outcome::Transport);

        let rejected: anyhow::Error =
            omnia_wasi_model::Error::BudgetExhausted("say more".to_owned()).into();
        assert_eq!(Outcome::of(&rejected), Outcome::Exhausted);

        let failed = run_error(Some("model overloaded"));
        assert_eq!(Outcome::of(&failed), Outcome::Error);
        assert_eq!(failed.to_string(), "cursor run error: model overloaded");
        assert_eq!(run_error(None).to_string(), "cursor run error: <no detail>");

        assert_eq!(Outcome::of(&anyhow::anyhow!("bridge RPC failed")), Outcome::Error);
    }

    #[test]
    fn lost_bridge() {
        let exited: anyhow::Error = Failure::BridgeExited(EXITED).into();
        assert!(Outcome::of(&exited).lost_bridge());
        let reset = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
        let socket: anyhow::Error =
            RpcError::io("SdkAgentService/Send", "reading the stream", reset).into();
        assert!(Outcome::of(&socket).lost_bridge());
        let torn: anyhow::Error = RpcError::truncated("SdkAgentService/Send", 3).into();
        assert!(Outcome::of(&torn).lost_bridge());

        // The bridge answered, in one way or another.
        assert!(!Outcome::of(&inactive()).lost_bridge());
        let timeout: anyhow::Error = Failure::Timeout { cap_secs: 600 }.into();
        assert!(!Outcome::of(&timeout).lost_bridge());
        let aborted: anyhow::Error = Failure::Aborted("session closed".to_owned()).into();
        assert!(!Outcome::of(&aborted).lost_bridge());
        let rejected: anyhow::Error =
            omnia_wasi_model::Error::BudgetExhausted("say more".to_owned()).into();
        assert!(!Outcome::of(&rejected).lost_bridge());
        assert!(!Outcome::of(&anyhow::anyhow!(
            "bridge RPC `SdkAgentService/Send` failed (500 Internal Server Error, internal): boom"
        ))
        .lost_bridge());
        assert!(!Outcome::of(&run_error(Some("model overloaded"))).lost_bridge());
    }
}
