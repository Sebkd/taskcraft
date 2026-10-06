//! Poll strategies: how long a worker sleeps after an empty poll, and a sleep
//! that ends at once on shutdown or on a wake-up from the source
//! (spec 2.3.24).

use std::future::pending;
use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::error::ConfigError;
use crate::source::WakeSignal;

/// How long to sleep after the source answered "empty for now".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollStrategy {
    /// Always the same pause.
    Interval(Duration),
    /// Starts at `min`, doubles after every empty poll up to `max`, and
    /// returns to `min` once a task arrives.
    Backoff {
        /// The first pause.
        min: Duration,
        /// The longest pause.
        max: Duration,
    },
    /// Wait until the source signals new work.
    ///
    /// A source without a wake-up signal cannot do that; on its own this
    /// strategy then falls back to growing pauses of 100 ms to 30 s, so that
    /// the worker never sleeps forever.
    Wake,
    /// Whichever of these ends first.
    FirstOf(Vec<Self>),
}

impl PollStrategy {
    /// The shortest pause of the default strategy.
    pub const DEFAULT_MIN: Duration = Duration::from_millis(100);
    /// The longest pause of the default strategy.
    pub const DEFAULT_MAX: Duration = Duration::from_secs(30);

    /// Checks the strategy (spec 2.10).
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidPollStrategy`] for a zero interval or minimum, a
    /// minimum above the maximum, or an empty composition.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |reason| Err(ConfigError::InvalidPollStrategy { reason });
        match self {
            Self::Interval(d) if d.is_zero() => invalid("duration must be positive"),
            Self::Backoff { min, .. } if min.is_zero() => invalid("duration must be positive"),
            Self::Backoff { min, max } if min > max => invalid("base delay exceeds max delay"),
            Self::FirstOf(list) if list.is_empty() => invalid("composition must not be empty"),
            Self::FirstOf(list) => list.iter().try_for_each(Self::validate),
            Self::Interval(_) | Self::Backoff { .. } | Self::Wake => Ok(()),
        }
    }
}

impl Default for PollStrategy {
    /// A wake-up from the source, or growing pauses of 100 ms to 30 s
    /// (spec 2.8).
    fn default() -> Self {
        Self::FirstOf(vec![
            Self::Wake,
            Self::Backoff {
                min: Self::DEFAULT_MIN,
                max: Self::DEFAULT_MAX,
            },
        ])
    }
}

/// Why [`Poller::wait`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wakeup {
    /// The pause ran out: poll again.
    Elapsed,
    /// The source signalled new work: poll again.
    Woken,
    /// Shutdown was signalled: stop polling.
    Stopped,
}

#[derive(Debug, Clone, Copy)]
enum Timer {
    Interval(Duration),
    Backoff {
        min: Duration,
        max: Duration,
        next: Duration,
    },
}

impl Timer {
    fn backoff(min: Duration, max: Duration) -> Self {
        Self::Backoff {
            min,
            max,
            next: min,
        }
    }

    fn current(&self) -> Duration {
        match *self {
            Self::Interval(d) => d,
            Self::Backoff { next, .. } => next,
        }
    }

    fn advance(&mut self) {
        if let Self::Backoff { max, next, .. } = self {
            *next = next.saturating_mul(2).min(*max);
        }
    }

    fn reset(&mut self) {
        if let Self::Backoff { min, next, .. } = self {
            *next = *min;
        }
    }
}

/// The poll strategy state of one worker.
///
/// Protocol: call `mark_seen` on the wake-up signal before each poll; after an
/// "empty" answer call [`wait`](Self::wait); after a task call
/// [`reset`](Self::reset).
#[derive(Debug, Clone)]
pub struct Poller {
    timers: Vec<Timer>,
    wake: bool,
    fallback: Timer,
}

impl Poller {
    /// The state for `strategy`.
    #[must_use]
    pub fn new(strategy: &PollStrategy) -> Self {
        let mut poller = Self {
            timers: Vec::new(),
            wake: false,
            fallback: Timer::backoff(PollStrategy::DEFAULT_MIN, PollStrategy::DEFAULT_MAX),
        };
        poller.add(strategy);
        poller
    }

    fn add(&mut self, strategy: &PollStrategy) {
        match strategy {
            PollStrategy::Interval(d) => self.timers.push(Timer::Interval(*d)),
            PollStrategy::Backoff { min, max } => self.timers.push(Timer::backoff(*min, *max)),
            PollStrategy::Wake => self.wake = true,
            PollStrategy::FirstOf(list) => list.iter().for_each(|s| self.add(s)),
        }
    }

    /// Whether the strategy waits for a wake-up from the source.
    #[must_use]
    pub fn wants_wake(&self) -> bool {
        self.wake
    }

    /// The pause before the next poll, advancing every growing pause; `None`
    /// when the strategy only waits for a wake-up.
    pub fn next_sleep(&mut self) -> Option<Duration> {
        let next = self.timers.iter().map(Timer::current).min()?;
        self.timers.iter_mut().for_each(Timer::advance);
        Some(next)
    }

    /// Returns every growing pause to its minimum; call after a task.
    pub fn reset(&mut self) {
        self.timers.iter_mut().for_each(Timer::reset);
        self.fallback.reset();
    }

    /// Sleeps until the pause runs out, the source signals new work or `stop`
    /// fires, whichever comes first. Shutdown wins a tie.
    pub async fn wait(
        &mut self,
        wake: Option<&mut WakeSignal>,
        stop: &CancellationToken,
    ) -> Wakeup {
        if stop.is_cancelled() {
            return Wakeup::Stopped;
        }
        let wake = wake.filter(|_| self.wake);
        let pause = match self.next_sleep() {
            Some(d) => Some(d),
            None if wake.is_some() => None,
            None => {
                let d = self.fallback.current();
                self.fallback.advance();
                Some(d)
            }
        };
        let woken = async {
            match wake {
                Some(signal) => {
                    // `false` means the source is gone; the next poll shows
                    // what happened to it.
                    let _ = signal.changed().await;
                }
                None => pending::<()>().await,
            }
        };
        let elapsed = async {
            match pause {
                Some(d) => sleep(d).await,
                None => pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            () = stop.cancelled() => Wakeup::Stopped,
            () = woken => Wakeup::Woken,
            () = elapsed => Wakeup::Elapsed,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::Instant;

    use super::*;
    use crate::memory::InMemorySource;
    use crate::source::Source;
    use crate::task::{Task, TaskId};

    const MS: Duration = Duration::from_millis(1);

    fn backoff() -> PollStrategy {
        PollStrategy::Backoff {
            min: 100 * MS,
            max: Duration::from_secs(30),
        }
    }

    #[test]
    fn backoff_doubles_and_resets() {
        let mut poller = Poller::new(&backoff());
        let sleeps: Vec<_> = (0..5).map(|_| poller.next_sleep().unwrap()).collect();
        assert_eq!(sleeps, [100 * MS, 200 * MS, 400 * MS, 800 * MS, 1600 * MS]);
        poller.reset();
        assert_eq!(poller.next_sleep(), Some(100 * MS));
    }

    #[test]
    fn backoff_stops_at_max() {
        let mut poller = Poller::new(&backoff());
        let last = (0..20).map(|_| poller.next_sleep().unwrap()).last();
        assert_eq!(last, Some(Duration::from_secs(30)));
    }

    #[test]
    fn first_of_takes_the_shortest_and_keeps_growing() {
        let mut poller = Poller::new(&PollStrategy::FirstOf(vec![
            PollStrategy::Interval(Duration::from_secs(1)),
            backoff(),
        ]));
        let sleeps: Vec<_> = (0..6).map(|_| poller.next_sleep().unwrap()).collect();
        assert_eq!(
            sleeps,
            [100 * MS, 200 * MS, 400 * MS, 800 * MS, 1000 * MS, 1000 * MS]
        );
    }

    #[test]
    fn validation() {
        let bad = [
            PollStrategy::Interval(Duration::ZERO),
            PollStrategy::Backoff {
                min: Duration::ZERO,
                max: MS,
            },
            PollStrategy::Backoff {
                min: 2 * MS,
                max: MS,
            },
            PollStrategy::FirstOf(vec![]),
            PollStrategy::FirstOf(vec![PollStrategy::Wake, PollStrategy::FirstOf(vec![])]),
        ];
        for strategy in bad {
            assert!(strategy.validate().is_err(), "{strategy:?}");
        }
        assert!(PollStrategy::default().validate().is_ok());
        assert_eq!(
            PollStrategy::Interval(Duration::ZERO)
                .validate()
                .unwrap_err()
                .to_string(),
            "invalid poll strategy: duration must be positive"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn wait_sleeps_exactly_the_pause() {
        let mut poller = Poller::new(&backoff());
        let stop = CancellationToken::new();
        for expected in [100 * MS, 200 * MS, 400 * MS] {
            let start = Instant::now();
            assert_eq!(poller.wait(None, &stop).await, Wakeup::Elapsed);
            assert_eq!(start.elapsed(), expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn push_wakes_mid_sleep() {
        let source = Arc::new(InMemorySource::new(10));
        let mut signal = source.subscribe().unwrap();
        signal.mark_seen();
        let mut poller = Poller::new(&PollStrategy::FirstOf(vec![
            PollStrategy::Wake,
            PollStrategy::Interval(Duration::from_secs(30)),
        ]));
        let pusher = Arc::clone(&source);
        tokio::spawn(async move {
            sleep(Duration::from_secs(2)).await;
            let id = TaskId::new("t");
            let _ = pusher.push(&id, Task::new(1_u32).with_id(id.clone())).await;
        });
        let start = Instant::now();
        let woke = poller
            .wait(Some(&mut signal), &CancellationToken::new())
            .await;
        assert_eq!(
            (woke, start.elapsed()),
            (Wakeup::Woken, Duration::from_secs(2))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stop_ends_a_long_sleep_at_once() {
        let mut poller = Poller::new(&PollStrategy::Interval(Duration::from_secs(30)));
        let stop = CancellationToken::new();
        let trigger = stop.clone();
        tokio::spawn(async move {
            sleep(MS).await;
            trigger.cancel();
        });
        let start = Instant::now();
        assert_eq!(poller.wait(None, &stop).await, Wakeup::Stopped);
        assert_eq!(start.elapsed(), MS);
        // Already stopped: no sleep at all.
        assert_eq!(poller.wait(None, &stop).await, Wakeup::Stopped);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_wins_a_tie_with_a_wake_up() {
        let source = InMemorySource::<u32>::new(10);
        let mut signal = source.subscribe().unwrap();
        signal.mark_seen();
        source.close(); // wakes subscribers
        let stop = CancellationToken::new();
        stop.cancel();
        let mut poller = Poller::new(&PollStrategy::Wake);
        assert_eq!(poller.wait(Some(&mut signal), &stop).await, Wakeup::Stopped);
    }

    #[tokio::test(start_paused = true)]
    async fn wake_without_a_signal_falls_back_to_growing_pauses() {
        let mut poller = Poller::new(&PollStrategy::Wake);
        let stop = CancellationToken::new();
        for expected in [100 * MS, 200 * MS] {
            let start = Instant::now();
            assert_eq!(poller.wait(None, &stop).await, Wakeup::Elapsed);
            assert_eq!(start.elapsed(), expected);
        }
        poller.reset();
        let start = Instant::now();
        poller.wait(None, &stop).await;
        assert_eq!(start.elapsed(), 100 * MS);
    }
}
