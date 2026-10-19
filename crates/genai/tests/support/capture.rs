//! The backend's own log events, read back by a test.
//!
//! A test that installs a [`Captured`] owns the process's subscriber, which
//! nextest grants by running each test in a process of its own.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use tracing_subscriber::layer::SubscriberExt as _;

/// Every event's fields, flattened to `name=value ` text.
#[derive(Clone, Default)]
pub struct Captured(Arc<Mutex<Vec<String>>>);

impl Captured {
    /// Installs a fresh capture as the process's global subscriber.
    pub fn install() -> Self {
        let captured = Self::default();
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(captured.clone()),
        )
        .expect("this test owns the process's subscriber");
        captured
    }

    /// Every event captured so far.
    pub fn events(&self) -> Vec<String> {
        self.0.lock().expect("captured lock").clone()
    }

    /// The one `completion` finish line the capture saw.
    pub fn completion(&self) -> String {
        let events = self.events();
        let mut completions =
            events.iter().filter(|event| event.contains("message=completion outcome="));
        let (Some(completion), None) = (completions.next(), completions.next()) else {
            panic!("one completion event: {events:#?}");
        };
        completion.clone()
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Captured {
    fn on_event(
        &self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut text = String::new();
        event.record(&mut Flatten(&mut text));
        self.0.lock().expect("captured lock").push(text);
    }
}

struct Flatten<'a>(&'a mut String);

impl tracing::field::Visit for Flatten<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let _ = write!(self.0, "{}={value:?} ", field.name());
    }
}
