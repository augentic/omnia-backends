//! Completion telemetry: the one line per completion, its bill, and where
//! its time went.
//!
//! [`Completion`] emits the start event at DEBUG and the finish event at
//! INFO for an answer, WARN for anything else, carrying the [`Outcome`],
//! the bill, and how the wait split across the [`Phase`]s; its
//! [`Heartbeat`] says where an open completion is every [`PROGRESS`]. The
//! provider's token counts become the guest's [`Usage`] here too.

use std::fmt;
use std::time::Duration;

use anyhow::Result;
use omnia_wasi_model::{Answer, Error, Usage};
use tokio::sync::watch;
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};

use crate::elapsed_ms;
use crate::tools::SHELL_TOOL;

// How often an open completion says where it is.
pub const PROGRESS: Duration = Duration::from_secs(15);

// What a completion is waiting on: the provider composing its next turn, a
// tool call outstanding, the shell running a command, or the guest's check
// of a candidate.
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

// How one completion came out: the closed set of `outcome` values the
// `completion` event carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    // Answered after the guest's check rejected a candidate.
    Corrected,
    // The timeout passed before the model's reply.
    Timeout,
    // The completion was dropped before it finished.
    Abort,
    // The rounds or the candidates spent.
    Exhausted,
    // The provider answered a request with an error status.
    Provider,
    // A request got no well-formed answer: the connection failed, or the
    // body was not the provider's JSON.
    Transport,
    // Anything else: the host refusing a tool call, a reply with nothing in
    // it, a request the SDK could not shape.
    Error,
}

impl Outcome {
    // A failed `complete` by its class, read off the error's type, never
    // its text.
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

// The timeout passed before the model's reply, while the completion waited
// on `waiting`.
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
    // The error `complete` ends on. The timeout is the completion's time
    // budget, so the guest sees the typed `budget-exhausted` carrying this
    // text: the same request put again would take as long, where a backend
    // failure invites a retry. The timeout stays beneath for `Outcome::of`.
    pub fn into_error(self) -> anyhow::Error {
        let budget = Error::BudgetExhausted(self.to_string());
        anyhow::Error::new(self).context(budget)
    }
}

// The wait a completion is in: on what, since when, and how many provider
// rounds and tool calls it has made.
#[derive(Debug)]
struct Activity {
    at: Instant,
    phase: Phase,
    rounds: u32,
    tool_calls: u32,
}

// One completion's start/finish events: the outcome, what it cost, and
// where its time went, on the `complete` span that names the model and
// format. Drop without `finish` records `Outcome::Abort` (a cancelled
// future).
pub struct Completion {
    started: Instant,
    attempts: u32,
    // set by the check's rejection alone, so a nudged reply makes no `Corrected`
    corrected: bool,
    bill: Bill,
    // the wait booked per phase, over every round
    model: Duration,
    tool: Duration,
    check: Duration,
    activity: watch::Sender<Activity>,
    emitted: bool,
}

impl Completion {
    // The start event: the clock runs from here, waiting on the model.
    pub fn start(prompt_bytes: u64, tools: usize) -> Self {
        tracing::debug!(prompt_bytes, tools, "completion started");

        Self::new()
    }

    fn new() -> Self {
        let started = Instant::now();
        Self {
            started,
            attempts: 0,
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

    // A wait begins on `phase`: the one before it is booked to its bucket.
    // A wait on the model is a provider round, one on a tool or the shell a
    // tool call.
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

    // What the completion is waiting on right now.
    pub fn phase(&self) -> Phase {
        self.activity.borrow().phase.clone()
    }

    // The wait so far on the current phase, into its bucket.
    fn book(&mut self) {
        let activity = self.activity.borrow();
        let bucket = match &activity.phase {
            Phase::Model => &mut self.model,
            Phase::Tool(_) | Phase::Shell => &mut self.tool,
            Phase::Check => &mut self.check,
        };
        *bucket = bucket.saturating_add(activity.at.elapsed());
    }

    // One provider round answered, its tokens on the bill: a reply of
    // `reply_bytes` is an attempt, a round asking for `tool_calls` is not.
    pub fn answered(
        &mut self, usage: &genai::chat::Usage, reply_bytes: Option<usize>, tool_calls: usize,
    ) {
        if reply_bytes.is_some() {
            self.attempts = self.attempts.saturating_add(1);
        }
        let bill = Bill::from(usage);
        tracing::debug!(
            reply_bytes,
            tool_calls,
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

    // The guest's check rejected a candidate and a correction follows.
    pub const fn rejected(&mut self) {
        self.corrected = true;
    }

    // The `in progress` line, written every `PROGRESS` while awaited.
    pub fn heartbeat(&self) -> Heartbeat {
        let mut ticks = interval_at(Instant::now() + PROGRESS, PROGRESS);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Heartbeat {
            started: self.started,
            activity: self.activity.subscribe(),
            ticks,
        }
    }

    // The one line per completion: INFO for an answer, WARN for anything
    // else, so a bare `info` run sees failed or aborted completions too.
    // Emitted once, so a later `Drop` says nothing.
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

        // an event's level is fixed at its callsite, so each level is its own event
        macro_rules! completion {
            ($level:ident) => {
                tracing::$level!(
                    outcome = outcome.as_str(),
                    attempts = self.attempts,
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

// Where an open completion is, said every `PROGRESS`: how long it has run,
// how far it has got, and what it has been waiting on for how long.
pub struct Heartbeat {
    started: Instant,
    activity: watch::Receiver<Activity>,
    ticks: Interval,
}

impl Heartbeat {
    // The next beat, written once it is due.
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

// The tokens a completion has been billed so far, every round together.
// The total is the provider's own sum, absent until a round reports one.
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

// A wire count booked on the bill: absent or negative is 0.
fn count(value: Option<i32>) -> u64 {
    value.map_or(0, |value| u64::from(clamp(value)))
}

// A wire count as the guest reads it: negative is 0.
fn clamp(value: i32) -> u32 {
    u32::try_from(value).unwrap_or(0)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

// The guest's `Usage`: `None` when the provider did not surface any counts.
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

// The completion's books, the classes a failure falls under, and the
// provider's counts onto the boundary's `Usage`; the events themselves are
// read off a capture in the `model` and `provider` suites.
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

    // The timeout is the time budget: the guest's typed error is the budget,
    // the completion event still reads `timeout`.
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

    // Two rounds add up, cache and total included; an absent or negative
    // wire count books nothing, a negative total included. The provider's
    // total stays absent until a round reports one.
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
        );
        completion.answered(
            &ProviderUsage {
                total_tokens: Some(-1),
                ..ProviderUsage::default()
            },
            Some(10),
            0,
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
        completion.finish(&Ok(answer()));
    }

    // Every wait lands in its phase's bucket, tool calls and the shell
    // together, the guest's checks in their own, the open one at the
    // finish; the counts follow the waits, the opening wait on the model
    // being no round yet.
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

        // Reasoning is optional; a negative count is clamped, not an error,
        // whichever count it is.
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
