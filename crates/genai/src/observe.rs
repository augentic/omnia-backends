//! Completion telemetry, phase timing, and token usage accounting.

use std::fmt;
use std::time::Duration;

use anyhow::Result;
use omnia_wasi_model::{Answer, Error, Usage};
use tokio::sync::watch;
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};

use crate::elapsed_ms;
use crate::tools::SHELL_TOOL;

pub const PROGRESS: Duration = Duration::from_secs(15);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Model,
    Tool(String),
    Shell,
    Check,
}

impl Phase {
    pub fn of_tool(name: &str) -> Self {
        if name == SHELL_TOOL { Self::Shell } else { Self::Tool(name.to_owned()) }
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model => f.write_str("the model"),
            Self::Tool(tool) => write!(f, "tool `{tool}`"),
            Self::Shell => f.write_str("the shell"),
            Self::Check => f.write_str("the guest's check"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Corrected,
    Timeout,
    Abort,
    Exhausted,
    Provider,
    Transport,
    Error,
}

impl Outcome {
    pub fn of(error: &anyhow::Error) -> Self {
        if error.downcast_ref::<Timeout>().is_some() {
            return Self::Timeout;
        }
        if matches!(error.downcast_ref::<Error>(), Some(Error::BudgetExhausted(_))) {
            return Self::Exhausted;
        }
        match error.downcast_ref::<genai::Error>() {
            Some(
                genai::Error::WebModelCall { webc_error, .. }
                | genai::Error::WebAdapterCall { webc_error, .. },
            ) => Self::of_web(webc_error),
            _ => Self::Error,
        }
    }

    const fn of_web(error: &genai::webc::Error) -> Self {
        match error {
            genai::webc::Error::ResponseFailedStatus { .. } => Self::Provider,
            genai::webc::Error::ResponseFailedNotJson { .. }
            | genai::webc::Error::ResponseFailedInvalidJson { .. }
            | genai::webc::Error::Reqwest(_) => Self::Transport,
            genai::webc::Error::JsonValueExt(_) => Self::Error,
        }
    }

    pub const fn answered(self) -> bool {
        matches!(self, Self::Ok | Self::Corrected)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Corrected => "corrected",
            Self::Timeout => "timeout",
            Self::Abort => "abort",
            Self::Exhausted => "exhausted",
            Self::Provider => "provider",
            Self::Transport => "transport",
            Self::Error => "error",
        }
    }
}

#[derive(Debug)]
pub struct Timeout {
    pub after: Duration,
    pub waiting: Phase,
}

impl fmt::Display for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "completion timed out after {}s waiting on {} (absolute cap on one reply exceeded)",
            self.after.as_secs(),
            self.waiting
        )
    }
}

impl std::error::Error for Timeout {}

impl Timeout {
    // Surface `BudgetExhausted` to the guest while keeping `Timeout` in the chain for `Outcome::of`.
    pub fn into_error(self) -> anyhow::Error {
        let budget = Error::BudgetExhausted(self.to_string());
        anyhow::Error::new(self).context(budget)
    }
}

#[derive(Debug)]
struct Activity {
    at: Instant,
    phase: Phase,
    rounds: u32,
    tool_calls: u32,
}

// Emits `Outcome::Abort` on drop if `finish` was not called.
pub struct Completion {
    started: Instant,
    attempts: u32,
    retries: u32,
    cuts: u32,
    corrected: bool,
    bill: Bill,
    model: Duration,
    tool: Duration,
    check: Duration,
    activity: watch::Sender<Activity>,
    emitted: bool,
}

impl Completion {
    pub fn start(prompt_bytes: u64, tools: usize) -> Self {
        tracing::debug!(prompt_bytes, tools, "completion started");

        Self::new()
    }

    fn new() -> Self {
        let started = Instant::now();
        Self {
            started,
            attempts: 0,
            retries: 0,
            cuts: 0,
            corrected: false,
            bill: Bill::default(),
            model: Duration::ZERO,
            tool: Duration::ZERO,
            check: Duration::ZERO,
            activity: watch::Sender::new(Activity {
                at: started,
                phase: Phase::Model,
                rounds: 0,
                tool_calls: 0,
            }),
            emitted: false,
        }
    }

    pub fn enter(&mut self, phase: Phase) {
        self.book();
        self.activity.send_modify(|activity| {
            activity.at = Instant::now();
            match &phase {
                Phase::Model => activity.rounds += 1,
                Phase::Tool(_) | Phase::Shell => activity.tool_calls += 1,
                Phase::Check => {}
            }
            activity.phase = phase;
        });
    }

    pub fn phase(&self) -> Phase {
        self.activity.borrow().phase.clone()
    }

    fn book(&mut self) {
        let activity = self.activity.borrow();
        let bucket = match &activity.phase {
            Phase::Model => &mut self.model,
            Phase::Tool(_) | Phase::Shell => &mut self.tool,
            Phase::Check => &mut self.check,
        };
        *bucket = bucket.saturating_add(activity.at.elapsed());
    }

    pub fn answered(
        &mut self, usage: &genai::chat::Usage, reply_bytes: Option<usize>, tool_calls: usize,
        cut: bool,
    ) {
        if reply_bytes.is_some() {
            self.attempts = self.attempts.saturating_add(1);
        }
        if cut {
            self.cuts = self.cuts.saturating_add(1);
        }
        let bill = Bill::from(usage);
        tracing::debug!(
            reply_bytes,
            tool_calls,
            cut,
            input_tokens = bill.input,
            cache_read_tokens = bill.cache_read,
            cache_write_tokens = bill.cache_write,
            output_tokens = bill.output,
            reasoning_tokens = bill.reasoning,
            total_tokens = bill.total,
            "round answered"
        );
        self.bill.add(&bill);
    }

    pub const fn rejected(&mut self) {
        self.corrected = true;
    }

    pub const fn retried(&mut self) {
        self.retries = self.retries.saturating_add(1);
    }

    pub fn heartbeat(&self) -> Heartbeat {
        let mut ticks = interval_at(Instant::now() + PROGRESS, PROGRESS);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Heartbeat {
            started: self.started,
            activity: self.activity.subscribe(),
            ticks,
        }
    }

    pub fn finish(&mut self, result: &Result<Answer>) {
        let outcome = match result {
            Ok(_) if self.corrected => Outcome::Corrected,
            Ok(_) => Outcome::Ok,
            Err(error) => Outcome::of(error),
        };
        self.emit(outcome);
    }

    fn emit(&mut self, outcome: Outcome) {
        if self.emitted {
            return;
        }
        self.emitted = true;
        self.book();
        let duration_ms = elapsed_ms(self.started);

        macro_rules! completion {
            ($level:ident) => {
                tracing::$level!(
                    outcome = outcome.as_str(),
                    attempts = self.attempts,
                    retries = self.retries,
                    cut = self.cuts,
                    duration_ms,
                    model_ms = millis(self.model),
                    tool_ms = millis(self.tool),
                    check_ms = millis(self.check),
                    input_tokens = self.bill.input,
                    cache_read_tokens = self.bill.cache_read,
                    cache_write_tokens = self.bill.cache_write,
                    output_tokens = self.bill.output,
                    reasoning_tokens = self.bill.reasoning,
                    total_tokens = self.bill.total,
                    "completion"
                )
            };
        }
        if outcome.answered() { completion!(info) } else { completion!(warn) }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.emit(Outcome::Abort);
    }
}

pub struct Heartbeat {
    started: Instant,
    activity: watch::Receiver<Activity>,
    ticks: Interval,
}

impl Heartbeat {
    pub async fn tick(&mut self) {
        self.ticks.tick().await;
        let current = self.activity.borrow();
        tracing::info!(
            elapsed_s = self.started.elapsed().as_secs(),
            round = current.rounds,
            tool_calls = current.tool_calls,
            silent_s = current.at.elapsed().as_secs(),
            waiting = %current.phase,
            "in progress"
        );
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Bill {
    input: u64,
    cache_read: u64,
    cache_write: u64,
    output: u64,
    reasoning: u64,
    total: Option<u64>,
}

impl From<&genai::chat::Usage> for Bill {
    fn from(usage: &genai::chat::Usage) -> Self {
        let prompt = usage.prompt_tokens_details.as_ref();
        let completion = usage.completion_tokens_details.as_ref();
        Self {
            input: count(usage.prompt_tokens),
            cache_read: count(prompt.and_then(|details| details.cached_tokens)),
            cache_write: count(prompt.and_then(|details| details.cache_creation_tokens)),
            output: count(usage.completion_tokens),
            reasoning: count(completion.and_then(|details| details.reasoning_tokens)),
            total: usage.total_tokens.and_then(|total| u64::try_from(total).ok()),
        }
    }
}

impl Bill {
    const fn add(&mut self, other: &Self) {
        self.input = self.input.saturating_add(other.input);
        self.cache_read = self.cache_read.saturating_add(other.cache_read);
        self.cache_write = self.cache_write.saturating_add(other.cache_write);
        self.output = self.output.saturating_add(other.output);
        self.reasoning = self.reasoning.saturating_add(other.reasoning);
        self.total = match (self.total, other.total) {
            (None, None) => None,
            (Some(total), None) | (None, Some(total)) => Some(total),
            (Some(booked), Some(total)) => Some(booked.saturating_add(total)),
        };
    }
}

fn count(value: Option<i32>) -> u64 {
    value.map_or(0, |value| u64::from(clamp(value)))
}

fn clamp(value: i32) -> u32 {
    u32::try_from(value).unwrap_or(0)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub fn to_usage(usage: &genai::chat::Usage) -> Option<Usage> {
    if usage.prompt_tokens.is_none() && usage.completion_tokens.is_none() {
        return None;
    }
    Some(Usage {
        input_tokens: usage.prompt_tokens.map_or(0, clamp),
        output_tokens: usage.completion_tokens.map_or(0, clamp),
        reasoning_tokens: usage
            .completion_tokens_details
            .as_ref()
            .and_then(|details| details.reasoning_tokens)
            .map(clamp),
    })
}

// Unit tests for outcome classification, phase timing, and token accounting.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use genai::adapter::AdapterKind;
    use genai::chat::{CompletionTokensDetails, PromptTokensDetails, Usage as ProviderUsage};
    use genai::{ModelIden, webc};
    use http::{HeaderMap, StatusCode};
    use omnia_wasi_model::Answer;

    use super::{Bill, Completion, Error, Outcome, Phase, Timeout, to_usage};

    fn web(webc_error: webc::Error) -> anyhow::Error {
        genai::Error::WebModelCall {
            model_iden: ModelIden::new(AdapterKind::OpenAI, "gpt-4o-mini"),
            webc_error,
        }
        .into()
    }

    fn answer() -> Answer {
        Answer {
            answer: String::new(),
            usage: None,
            transcript: None,
        }
    }

    #[test]
    fn classify_errors() {
        let exhausted: anyhow::Error = Error::BudgetExhausted("say more".to_owned()).into();
        assert_eq!(Outcome::of(&exhausted), Outcome::Exhausted);

        let status = web(webc::Error::ResponseFailedStatus {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: "slow down".to_owned(),
            headers: Box::new(HeaderMap::new()),
        });
        assert_eq!(Outcome::of(&status), Outcome::Provider);

        let truncated = web(webc::Error::ResponseFailedInvalidJson {
            body: "{".to_owned(),
            cause: "EOF".to_owned(),
        });
        assert_eq!(Outcome::of(&truncated), Outcome::Transport);
        let wrapped = truncated.context("genai exec_chat failed");
        assert_eq!(Outcome::of(&wrapped), Outcome::Transport, "context hides nothing");

        let backend: anyhow::Error = Error::Backend("down".to_owned()).into();
        assert_eq!(Outcome::of(&backend), Outcome::Error);
        assert_eq!(Outcome::of(&anyhow::anyhow!("provider unreachable")), Outcome::Error);
    }

    #[test]
    fn timeout_budget() {
        let timeout = Timeout {
            after: Duration::from_secs(600),
            waiting: Phase::Tool("read".to_owned()),
        }
        .into_error();
        assert!(matches!(
            timeout.downcast_ref::<Error>(),
            Some(Error::BudgetExhausted(detail))
                if detail == "completion timed out after 600s waiting on tool `read` (absolute cap on one reply exceeded)"
        ));
        assert_eq!(Outcome::of(&timeout), Outcome::Timeout);
    }

    #[test]
    fn bill() {
        let mut completion = Completion::new();
        completion.answered(
            &ProviderUsage {
                prompt_tokens: Some(30),
                completion_tokens: Some(10),
                ..ProviderUsage::default()
            },
            None,
            1,
            true,
        );
        assert_eq!(completion.bill.total, None, "no round has reported a total");
        completion.answered(
            &ProviderUsage {
                prompt_tokens: Some(70),
                prompt_tokens_details: Some(PromptTokensDetails {
                    cached_tokens: Some(50),
                    cache_creation_tokens: Some(-1),
                    ..PromptTokensDetails::default()
                }),
                completion_tokens: Some(20),
                completion_tokens_details: Some(CompletionTokensDetails {
                    reasoning_tokens: Some(15),
                    ..CompletionTokensDetails::default()
                }),
                total_tokens: Some(140),
            },
            Some(10),
            0,
            false,
        );
        completion.answered(
            &ProviderUsage {
                total_tokens: Some(-1),
                ..ProviderUsage::default()
            },
            Some(10),
            0,
            false,
        );
        assert_eq!(
            completion.bill,
            Bill {
                input: 100,
                cache_read: 50,
                cache_write: 0,
                output: 30,
                reasoning: 15,
                total: Some(140),
            }
        );
        assert_eq!(completion.attempts, 2, "the replies are the attempts, the tool round none");
        assert_eq!(completion.cuts, 1, "the cut tool round is counted");
        completion.finish(&Ok(answer()));
    }

    #[tokio::test(start_paused = true)]
    async fn phase_buckets() {
        const TICK: Duration = Duration::from_millis(2);
        let mut completion = Completion::new();
        completion.enter(Phase::Tool("read".to_owned()));
        tokio::time::advance(TICK).await;
        completion.enter(Phase::Shell);
        tokio::time::advance(TICK).await;
        completion.enter(Phase::Model);
        completion.enter(Phase::Check);
        tokio::time::advance(TICK).await;

        let (rounds, tool_calls) = {
            let activity = completion.activity.borrow();
            (activity.rounds, activity.tool_calls)
        };
        assert_eq!((rounds, tool_calls), (1, 2));
        assert_eq!(completion.phase(), Phase::Check);
        assert_eq!(completion.tool, 2 * TICK);
        assert_eq!(completion.model, Duration::ZERO);
        assert_eq!(completion.check, Duration::ZERO, "the open wait is booked at the finish");
        completion.finish(&Ok(answer()));
        assert_eq!(completion.check, TICK);
    }

    #[test]
    fn usage_counts() {
        let counted = ProviderUsage {
            prompt_tokens: Some(3),
            completion_tokens: Some(1),
            completion_tokens_details: Some(CompletionTokensDetails {
                reasoning_tokens: Some(2),
                ..CompletionTokensDetails::default()
            }),
            ..ProviderUsage::default()
        };
        let usage = to_usage(&counted).expect("counts surfaced");
        assert_eq!(
            (usage.input_tokens, usage.output_tokens, usage.reasoning_tokens),
            (3, 1, Some(2))
        );

        let clamped = ProviderUsage {
            prompt_tokens: Some(-1),
            completion_tokens: Some(4),
            completion_tokens_details: Some(CompletionTokensDetails {
                reasoning_tokens: Some(-1),
                ..CompletionTokensDetails::default()
            }),
            ..ProviderUsage::default()
        };
        let usage = to_usage(&clamped).expect("counts surfaced");
        assert_eq!(
            (usage.input_tokens, usage.output_tokens, usage.reasoning_tokens),
            (0, 4, Some(0))
        );

        let unreported = ProviderUsage {
            prompt_tokens: Some(3),
            completion_tokens: Some(1),
            ..ProviderUsage::default()
        };
        let usage = to_usage(&unreported).expect("counts surfaced");
        assert_eq!(usage.reasoning_tokens, None, "no reasoning count, none reported");

        assert!(to_usage(&ProviderUsage::default()).is_none(), "no counts, no usage");
    }
}
