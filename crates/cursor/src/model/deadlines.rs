//! The two bounds on one agent run: an inactivity deadline that real stream
//! progress rearms, and an absolute wall-clock cap.

use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};

/// Inactivity and absolute bounds on one run, from the connect options.
#[derive(Clone, Copy, Debug)]
pub struct Deadlines {
    /// Kill a run after this long with no stream events.
    pub inactivity: Duration,
    /// Kill a run after this long, streaming or not.
    pub cap: Duration,
}

impl Deadlines {
    /// Resolve when a run breaches its inactivity or absolute bound.
    pub async fn watch(&self, mut activity: watch::Receiver<Instant>) -> anyhow::Error {
        let cap = sleep_until(Instant::now() + self.cap);
        tokio::pin!(cap);
        let mut activity_closed = false;

        loop {
            let last_activity = *activity.borrow_and_update();
            let inactive = sleep_until(last_activity + self.inactivity);
            tokio::pin!(inactive);

            tokio::select! {
                () = &mut cap => {
                    return anyhow::anyhow!(
                        "cursor run timed out after {}s (absolute cap exceeded while still active)",
                        self.cap.as_secs()
                    );
                }
                () = &mut inactive => {
                    let idle = Instant::now().saturating_duration_since(last_activity).as_secs();
                    return anyhow::anyhow!(
                        "cursor run inactive for {idle}s (no stream events; inactivity limit {}s, \
                         absolute cap {}s)",
                        self.inactivity.as_secs(),
                        self.cap.as_secs()
                    );
                }
                changed = activity.changed(), if !activity_closed => {
                    activity_closed = changed.is_err();
                }
            }
        }
    }
}

// Deliberate unit tests: pure deadline logic under a paused clock (CI floor);
// `tests/live.rs` is the acceptance gate proving a real bridge-driven run
// works end-to-end.
#[cfg(test)]
mod tests {
    use tokio::sync::watch;
    use tokio::time::{Duration, Instant, sleep};

    use super::Deadlines;

    const DEADLINES: Deadlines = Deadlines {
        inactivity: Duration::from_mins(2),
        cap: Duration::from_mins(10),
    };

    #[tokio::test(start_paused = true)]
    async fn silent_stream_hits_inactivity_deadline() {
        let (_activity, receiver) = watch::channel(Instant::now());
        let started = Instant::now();
        let error = DEADLINES.watch(receiver).await;
        assert_eq!(started.elapsed(), Duration::from_mins(2));
        assert!(
            error.to_string().contains("inactive for 120s"),
            "the inactivity kill names the idle span: {error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn steady_activity_hits_absolute_cap() {
        let (activity, receiver) = watch::channel(Instant::now());
        let started = Instant::now();
        let toucher = async {
            loop {
                sleep(Duration::from_mins(1)).await;
                activity.send_replace(Instant::now());
            }
        };
        let error = tokio::select! {
            error = DEADLINES.watch(receiver) => error,
            () = toucher => unreachable!("the toucher never finishes"),
        };
        assert_eq!(started.elapsed(), Duration::from_mins(10));
        assert!(
            error.to_string().contains("timed out after 600s"),
            "the cap kill names the absolute bound: {error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn late_activity_rearms_inactivity_deadline() {
        let (activity, receiver) = watch::channel(Instant::now());
        let started = Instant::now();
        let toucher = async {
            sleep(Duration::from_secs(100)).await;
            activity.send_replace(Instant::now());
            std::future::pending::<()>().await;
        };
        let error = tokio::select! {
            error = DEADLINES.watch(receiver) => error,
            () = toucher => unreachable!("the toucher never finishes"),
        };
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(220),
            "one touch at 100s moves the kill to 100s + the 120s window"
        );
        assert!(error.to_string().contains("inactive for 120s"), "unexpected: {error}");
    }
}
