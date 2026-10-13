//! An observer behind a bounded buffer, for slow receivers (rule 2.3.22
//! p. 6, change async-observer).

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use tracing::warn;

use super::{AttemptEnd, Event, Observer};
use crate::error::ConfigError;
use crate::monitor::StopReason;
use crate::state::TaskState;
use crate::status::FinishReason;
use crate::task::TaskId;

/// How often a buffer that keeps overflowing logs its losses.
const WARN_EVERY: Duration = Duration::from_secs(10);

/// Gives another observer its events on a thread of its own, so that a slow
/// one — writing to a network, say — does not slow the tasks down.
///
/// Each event is copied into a buffer of `capacity` events and handed to
/// the inner observer in order. When the buffer is full the event is lost:
/// [`dropped`](Self::dropped) counts the losses, and a WARN
/// `observer/dropped` reports them at most every 10 s. A panic of the inner
/// observer is logged and loses that event only. Events still in the buffer
/// when the process exits are lost.
///
/// ```
/// use taskcraft::observe::{Buffered, Event, Observer};
///
/// struct Audit;
/// impl Observer for Audit {
///     fn on_event(&self, event: &Event<'_>) {
///         // a slow write somewhere else
///         let _ = event;
///     }
/// }
///
/// let audit = Buffered::new(Audit, 10_000)?;
/// let monitor = taskcraft::Monitor::new().observer(audit);
/// # let _ = monitor;
/// # Ok::<(), taskcraft::error::ConfigError>(())
/// ```
pub struct Buffered {
    sender: SyncSender<OwnedEvent>,
    name: String,
    dropped: AtomicU64,
    /// Losses since the last warning, and when that warning was.
    unreported: Mutex<(u64, Option<Instant>)>,
}

impl Buffered {
    /// `inner` gets the events on a thread of its own; at most `capacity`
    /// of them wait for it.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidCapacity`] when `capacity` is zero.
    pub fn new(inner: impl Observer, capacity: usize) -> Result<Self, ConfigError> {
        if capacity == 0 {
            return Err(ConfigError::InvalidCapacity);
        }
        let name = format!("Buffered({})", inner.name());
        let (sender, receiver) = sync_channel(capacity);
        // The thread ends once the buffer is gone and its events delivered.
        // Without it nothing receives: every event counts as dropped.
        let _ = std::thread::Builder::new()
            .name("taskcraft-observer".to_owned())
            .spawn(move || deliver(&inner, &receiver));
        Ok(Self {
            sender,
            name,
            dropped: AtomicU64::new(0),
            unreported: Mutex::new((0, None)),
        })
    }

    /// Events lost to a full buffer so far.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn lost(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut unreported = self
            .unreported
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        unreported.0 += 1;
        if unreported.1.is_some_and(|last| now < last + WARN_EVERY) {
            return;
        }
        warn!(
            event = "observer",
            action = "dropped",
            "observer events dropped: observer={}, count={}",
            self.name,
            unreported.0
        );
        *unreported = (0, Some(now));
    }
}

impl Observer for Buffered {
    fn on_event(&self, event: &Event<'_>) {
        // Full, or nothing receives (the thread could not start): lost.
        if self.sender.try_send(OwnedEvent::from(event)).is_err() {
            self.lost();
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for Buffered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buffered")
            .field("name", &self.name)
            .field("dropped", &self.dropped())
            .finish_non_exhaustive()
    }
}

/// Hands the buffered events to `inner`, one by one, until the buffer is
/// gone; a panic loses that event only (rule 2.3.22 p. 5).
fn deliver(inner: &impl Observer, receiver: &Receiver<OwnedEvent>) {
    while let Ok(event) = receiver.recv() {
        let delivered = catch_unwind(AssertUnwindSafe(|| inner.on_event(&event.as_event())));
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
                inner.name(),
                error
            );
        }
    }
}

/// An [`Event`] that owns its data, to cross to the delivery thread. Every
/// variant of `Event` has one here: the matches below are exhaustive, so a
/// new event cannot be forgotten.
#[derive(Debug)]
enum OwnedEvent {
    Pushed {
        queue: String,
        task_id: TaskId,
    },
    Accepted {
        queue: String,
        task_id: TaskId,
    },
    Rejected {
        queue: String,
        task_id: TaskId,
        reason: FinishReason,
    },
    Duplicate {
        queue: String,
        task_id: TaskId,
    },
    AttemptStarted {
        queue: String,
        task_id: TaskId,
        attempt: u32,
    },
    AttemptFinished {
        queue: String,
        task_id: TaskId,
        attempt: u32,
        outcome: AttemptEnd,
        duration: Duration,
    },
    Retry {
        queue: String,
        task_id: TaskId,
        attempt: u32,
        pause: Duration,
    },
    Finished {
        queue: String,
        task_id: TaskId,
        attempt: u32,
        state: TaskState,
        reason: Option<FinishReason>,
    },
    DecodeFailed {
        queue: String,
    },
    SourceFailed {
        queue: String,
    },
    WorkerRestarted {
        queue: String,
    },
    SourceClosed {
        queue: String,
    },
    WorkerStopped {
        queue: String,
        reason: StopReason,
    },
    LeaseTakenOver {
        queue: String,
        task_id: TaskId,
    },
    LeaseLost {
        queue: String,
        task_id: TaskId,
    },
    Occupancy {
        queue: String,
        running: usize,
        waiting_slot: usize,
        waiting_retry: usize,
    },
    PoolUsage {
        pool: String,
        in_use: u32,
        total: u32,
    },
}

impl From<&Event<'_>> for OwnedEvent {
    fn from(event: &Event<'_>) -> Self {
        match *event {
            Event::Pushed { queue, task_id } => Self::Pushed {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
            },
            Event::Accepted { queue, task_id } => Self::Accepted {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
            },
            Event::Rejected {
                queue,
                task_id,
                reason,
            } => Self::Rejected {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
                reason: reason.clone(),
            },
            Event::Duplicate { queue, task_id } => Self::Duplicate {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
            },
            Event::AttemptStarted {
                queue,
                task_id,
                attempt,
            } => Self::AttemptStarted {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
                attempt,
            },
            Event::AttemptFinished {
                queue,
                task_id,
                attempt,
                outcome,
                duration,
            } => Self::AttemptFinished {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
                attempt,
                outcome,
                duration,
            },
            Event::Retry {
                queue,
                task_id,
                attempt,
                pause,
            } => Self::Retry {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
                attempt,
                pause,
            },
            Event::Finished {
                queue,
                task_id,
                attempt,
                state,
                reason,
            } => Self::Finished {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
                attempt,
                state,
                reason: reason.cloned(),
            },
            Event::DecodeFailed { queue } => Self::DecodeFailed {
                queue: queue.to_owned(),
            },
            Event::SourceFailed { queue } => Self::SourceFailed {
                queue: queue.to_owned(),
            },
            Event::WorkerRestarted { queue } => Self::WorkerRestarted {
                queue: queue.to_owned(),
            },
            Event::SourceClosed { queue } => Self::SourceClosed {
                queue: queue.to_owned(),
            },
            Event::WorkerStopped { queue, reason } => Self::WorkerStopped {
                queue: queue.to_owned(),
                reason: reason.clone(),
            },
            Event::LeaseTakenOver { queue, task_id } => Self::LeaseTakenOver {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
            },
            Event::LeaseLost { queue, task_id } => Self::LeaseLost {
                queue: queue.to_owned(),
                task_id: task_id.clone(),
            },
            Event::Occupancy {
                queue,
                running,
                waiting_slot,
                waiting_retry,
            } => Self::Occupancy {
                queue: queue.to_owned(),
                running,
                waiting_slot,
                waiting_retry,
            },
            Event::PoolUsage {
                pool,
                in_use,
                total,
            } => Self::PoolUsage {
                pool: pool.to_owned(),
                in_use,
                total,
            },
        }
    }
}

impl OwnedEvent {
    fn as_event(&self) -> Event<'_> {
        match self {
            Self::Pushed { queue, task_id } => Event::Pushed { queue, task_id },
            Self::Accepted { queue, task_id } => Event::Accepted { queue, task_id },
            Self::Rejected {
                queue,
                task_id,
                reason,
            } => Event::Rejected {
                queue,
                task_id,
                reason,
            },
            Self::Duplicate { queue, task_id } => Event::Duplicate { queue, task_id },
            Self::AttemptStarted {
                queue,
                task_id,
                attempt,
            } => Event::AttemptStarted {
                queue,
                task_id,
                attempt: *attempt,
            },
            Self::AttemptFinished {
                queue,
                task_id,
                attempt,
                outcome,
                duration,
            } => Event::AttemptFinished {
                queue,
                task_id,
                attempt: *attempt,
                outcome: *outcome,
                duration: *duration,
            },
            Self::Retry {
                queue,
                task_id,
                attempt,
                pause,
            } => Event::Retry {
                queue,
                task_id,
                attempt: *attempt,
                pause: *pause,
            },
            Self::Finished {
                queue,
                task_id,
                attempt,
                state,
                reason,
            } => Event::Finished {
                queue,
                task_id,
                attempt: *attempt,
                state: *state,
                reason: reason.as_ref(),
            },
            Self::DecodeFailed { queue } => Event::DecodeFailed { queue },
            Self::SourceFailed { queue } => Event::SourceFailed { queue },
            Self::WorkerRestarted { queue } => Event::WorkerRestarted { queue },
            Self::SourceClosed { queue } => Event::SourceClosed { queue },
            Self::WorkerStopped { queue, reason } => Event::WorkerStopped { queue, reason },
            Self::LeaseTakenOver { queue, task_id } => Event::LeaseTakenOver { queue, task_id },
            Self::LeaseLost { queue, task_id } => Event::LeaseLost { queue, task_id },
            Self::Occupancy {
                queue,
                running,
                waiting_slot,
                waiting_retry,
            } => Event::Occupancy {
                queue,
                running: *running,
                waiting_slot: *waiting_slot,
                waiting_retry: *waiting_retry,
            },
            Self::PoolUsage {
                pool,
                in_use,
                total,
            } => Event::PoolUsage {
                pool,
                in_use: *in_use,
                total: *total,
            },
        }
    }
}
