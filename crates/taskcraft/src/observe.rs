//! Observers: every library event, for metrics and the consumer's own
//! registries (rule 2.3.22, spec 4.4.2).

use std::any::type_name;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tracing::warn;

use crate::monitor::StopReason;
use crate::state::TaskState;
use crate::status::FinishReason;
use crate::task::TaskId;

#[cfg(feature = "metrics")]
mod metrics;
#[cfg(feature = "metrics")]
pub use self::metrics::MetricsObserver;

/// How an attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AttemptEnd {
    /// The task reached this state: final, or deferred to its source.
    Finished(TaskState),
    /// The task waits to run again: a retry, or a defer handled in process.
    Retry,
}

impl AttemptEnd {
    /// The `outcome` label: the state's name, or `retry`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Finished(state) => state.as_str(),
            Self::Retry => "retry",
        }
    }
}

/// Something that happened in the library (rule 2.3.22 p. 1).
///
/// Values borrow from the library: copy what you keep. Events never carry
/// task arguments or metadata (invariant 1.3.18).
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Event<'a> {
    /// A task was pushed into the queue's source.
    Pushed {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
    },
    /// A task was accepted (transition 2.4.1.1).
    Accepted {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
    },
    /// A task was rejected for lack of a slot.
    Rejected {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
        /// Why.
        reason: &'a FinishReason,
    },
    /// A delivery with the id of a live task was acknowledged and dropped.
    Duplicate {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
    },
    /// An attempt started.
    AttemptStarted {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
        /// Its number, from 1.
        attempt: u32,
    },
    /// An attempt ended.
    AttemptFinished {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
        /// Its number.
        attempt: u32,
        /// What it led to.
        outcome: AttemptEnd,
        /// How long it ran.
        duration: Duration,
    },
    /// A task will be retried after a pause (transition 2.4.1.5).
    Retry {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
        /// The attempt that asked to retry.
        attempt: u32,
        /// The pause before the next one.
        pause: Duration,
    },
    /// A task reached a final state, or was deferred to its source.
    Finished {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
        /// Its last attempt.
        attempt: u32,
        /// Succeeded, failed, panicked, cancelled or deferred.
        state: TaskState,
        /// Why, for failed, panicked and cancelled tasks.
        reason: Option<&'a FinishReason>,
    },
    /// A message could not be decoded (a poison message).
    DecodeFailed {
        /// The queue.
        queue: &'a str,
    },
    /// The source failed: a poll or an ack.
    SourceFailed {
        /// The queue.
        queue: &'a str,
    },
    /// The intake loop restarted after a source error.
    WorkerRestarted {
        /// The queue.
        queue: &'a str,
    },
    /// The source closed.
    SourceClosed {
        /// The queue.
        queue: &'a str,
    },
    /// The worker stopped.
    WorkerStopped {
        /// The queue.
        queue: &'a str,
        /// Why.
        reason: &'a StopReason,
    },
    /// This process took over a task whose lease had expired.
    LeaseTakenOver {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
    },
    /// Another process took a task of this one over; it stops here.
    LeaseLost {
        /// The queue.
        queue: &'a str,
        /// The task.
        task_id: &'a TaskId,
    },
    /// How many tasks run and wait now.
    Occupancy {
        /// The queue.
        queue: &'a str,
        /// Running.
        running: usize,
        /// Waiting for a slot or a pool.
        waiting_slot: usize,
        /// Waiting to retry.
        waiting_retry: usize,
    },
    /// How many permits of a pool are taken now.
    PoolUsage {
        /// The pool.
        pool: &'a str,
        /// Taken.
        in_use: u32,
        /// The pool size.
        total: u32,
    },
}

/// Receives every event of the queues of a monitor (rule 2.3.22).
///
/// Called on the library's own tasks: keep it quick and never block. A panic
/// is caught and logged, and only that observer loses that event.
pub trait Observer: Send + Sync + 'static {
    /// One event.
    fn on_event(&self, event: &Event<'_>);

    /// The name used in the log when the observer panics.
    fn name(&self) -> &str {
        type_name::<Self>()
    }
}

/// A shared observer: keep one handle to read what it gathered, give the
/// monitor another.
impl<T: Observer + ?Sized> Observer for Arc<T> {
    fn on_event(&self, event: &Event<'_>) {
        (**self).on_event(event);
    }

    fn name(&self) -> &str {
        (**self).name()
    }
}

/// The observers of a monitor.
#[derive(Clone, Default)]
pub(crate) struct Observers(Arc<[Arc<dyn Observer>]>);

impl Observers {
    pub(crate) fn new(observers: Vec<Arc<dyn Observer>>) -> Self {
        Self(observers.into())
    }

    pub(crate) fn emit(&self, event: &Event<'_>) {
        for observer in self.0.iter() {
            let delivered = catch_unwind(AssertUnwindSafe(|| observer.on_event(event)));
            if let Err(payload) = delivered {
                let error = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "panic with a non-string payload".to_owned());
                warn!(
                    event = "observer",
                    action = "failed",
                    "observer failed: observer={}, error={:?}",
                    observer.name(),
                    error
                );
            }
        }
    }
}

impl fmt::Debug for Observers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Observers")
            .field("count", &self.0.len())
            .finish()
    }
}

/// The observers of a queue, known once it is registered; the queue handle
/// shares it.
pub(crate) type ObserverCell = Arc<OnceLock<Observers>>;

pub(crate) fn observers_of(cell: &ObserverCell) -> Observers {
    cell.get().cloned().unwrap_or_default()
}

/// Which occupancy count a guard holds.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Waiting {
    Running,
    Slot,
    Retry,
}

/// Running and waiting tasks of a queue.
#[derive(Debug)]
pub(crate) struct Occupancy {
    queue: Arc<str>,
    observers: Observers,
    running: AtomicUsize,
    slot: AtomicUsize,
    retry: AtomicUsize,
}

impl Occupancy {
    pub(crate) fn new(queue: Arc<str>, observers: Observers) -> Arc<Self> {
        Arc::new(Self {
            queue,
            observers,
            running: AtomicUsize::new(0),
            slot: AtomicUsize::new(0),
            retry: AtomicUsize::new(0),
        })
    }

    fn counter(&self, which: Waiting) -> &AtomicUsize {
        match which {
            Waiting::Running => &self.running,
            Waiting::Slot => &self.slot,
            Waiting::Retry => &self.retry,
        }
    }

    fn report(&self) {
        self.observers.emit(&Event::Occupancy {
            queue: &self.queue,
            running: self.running.load(Ordering::Relaxed),
            waiting_slot: self.slot.load(Ordering::Relaxed),
            waiting_retry: self.retry.load(Ordering::Relaxed),
        });
    }

    /// Counts a task in `which` until the guard drops — on every path,
    /// aborts included.
    pub(crate) fn enter(self: &Arc<Self>, which: Waiting) -> Occupied {
        self.counter(which).fetch_add(1, Ordering::Relaxed);
        self.report();
        Occupied {
            occupancy: Arc::clone(self),
            which,
        }
    }
}

pub(crate) struct Occupied {
    occupancy: Arc<Occupancy>,
    which: Waiting,
}

impl Drop for Occupied {
    fn drop(&mut self) {
        self.occupancy
            .counter(self.which)
            .fetch_sub(1, Ordering::Relaxed);
        self.occupancy.report();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct Seen(Mutex<Vec<String>>);

    impl Observer for Seen {
        fn on_event(&self, event: &Event<'_>) {
            self.0.lock().unwrap().push(format!("{event:?}"));
        }
    }

    struct Panicky;

    impl Observer for Panicky {
        fn on_event(&self, _: &Event<'_>) {
            panic!("observer bug");
        }
    }

    /// Change criterion 3: a panicking observer does not stop the others.
    #[test]
    fn a_panicking_observer_loses_only_its_event() {
        let seen = Arc::new(Seen::default());
        let observers = Observers::new(vec![Arc::new(Panicky), seen.clone()]);
        observers.emit(&Event::SourceClosed { queue: "q" });
        assert_eq!(seen.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn occupancy_guards_count_and_report() {
        let seen = Arc::new(Seen::default());
        let occupancy = Occupancy::new("q".into(), Observers::new(vec![seen.clone()]));
        let running = occupancy.enter(Waiting::Running);
        let waiting = occupancy.enter(Waiting::Slot);
        drop(running);
        drop(waiting);
        let events = seen.0.lock().unwrap().clone();
        assert_eq!(events.len(), 4);
        assert!(events[1].contains("running: 1, waiting_slot: 1"));
        assert!(events[3].contains("running: 0, waiting_slot: 0"));
    }

    #[test]
    fn attempt_end_labels() {
        assert_eq!(AttemptEnd::Retry.as_str(), "retry");
        assert_eq!(
            AttemptEnd::Finished(TaskState::Succeeded).as_str(),
            "succeeded"
        );
    }
}
