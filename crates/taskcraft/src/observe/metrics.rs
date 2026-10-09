//! The `metrics` adapter: events as the standard series of spec 4.4.2.
//!
//! Series handles are kept per queue and per pool: an event finds its
//! handle by name under a read lock, with no allocation. A handle is
//! registered with the recorder on the first event of its series, so a
//! series appears only once something happened in it, as before.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock, PoisonError, RwLock};

use metrics::{Counter, Gauge, Histogram, SharedString, counter, gauge, histogram};

use super::{AttemptEnd, Event, Observer};
use crate::state::TaskState;

const STATES: usize = TaskState::ALL.len();

/// Publishes the standard series (spec 4.4.2) through the `metrics` facade,
/// to whatever exporter the application installed. Register it with
/// [`Monitor::observer`](crate::Monitor::observer).
///
/// Install the recorder before the monitor runs: the adapter keeps the
/// handle of each series from its first event, and a recorder installed
/// later does not get them. Clones share the handles.
#[derive(Clone, Default)]
pub struct MetricsObserver {
    series: Arc<Series>,
}

impl MetricsObserver {
    /// The adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl fmt::Debug for MetricsObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsObserver").finish_non_exhaustive()
    }
}

/// The handles of every queue and pool seen so far.
#[derive(Default)]
struct Series {
    queues: RwLock<HashMap<Box<str>, QueueSeries>>,
    pools: RwLock<HashMap<Box<str>, PoolSeries>>,
}

impl Series {
    /// Runs `f` with the handles of queue `name`, adding them on its first
    /// event.
    fn queue(&self, name: &str, f: impl FnOnce(&QueueSeries)) {
        let queues = self.queues.read().unwrap_or_else(PoisonError::into_inner);
        if let Some(series) = queues.get(name) {
            return f(series);
        }
        drop(queues);
        let mut queues = self.queues.write().unwrap_or_else(PoisonError::into_inner);
        f(queues
            .entry(name.into())
            .or_insert_with(|| QueueSeries::new(name)));
    }

    /// Runs `f` with the handles of pool `name`, adding them on its first
    /// event.
    fn pool(&self, name: &str, f: impl FnOnce(&PoolSeries)) {
        let pools = self.pools.read().unwrap_or_else(PoisonError::into_inner);
        if let Some(series) = pools.get(name) {
            return f(series);
        }
        drop(pools);
        let mut pools = self.pools.write().unwrap_or_else(PoisonError::into_inner);
        f(pools
            .entry(name.into())
            .or_insert_with(|| PoolSeries::new(name)));
    }
}

/// The series of one queue, each registered on its first event.
struct QueueSeries {
    queue: SharedString,
    accepted: OnceLock<Counter>,
    rejected: OnceLock<Counter>,
    duplicate: OnceLock<Counter>,
    retries: OnceLock<Counter>,
    panics: OnceLock<Counter>,
    decode_errors: OnceLock<Counter>,
    source_errors: OnceLock<Counter>,
    restarts: OnceLock<Counter>,
    /// By the outcome's index in [`TaskState::ALL`].
    finished: [OnceLock<Counter>; STATES],
    /// By the outcome's index in [`TaskState::ALL`]; the last one is "retry".
    durations: [OnceLock<Histogram>; STATES + 1],
    running: OnceLock<Gauge>,
    waiting_slot: OnceLock<Gauge>,
    waiting_retry: OnceLock<Gauge>,
}

impl QueueSeries {
    fn new(queue: &str) -> Self {
        Self {
            queue: SharedString::from(queue.to_owned()),
            accepted: OnceLock::new(),
            rejected: OnceLock::new(),
            duplicate: OnceLock::new(),
            retries: OnceLock::new(),
            panics: OnceLock::new(),
            decode_errors: OnceLock::new(),
            source_errors: OnceLock::new(),
            restarts: OnceLock::new(),
            finished: std::array::from_fn(|_| OnceLock::new()),
            durations: std::array::from_fn(|_| OnceLock::new()),
            running: OnceLock::new(),
            waiting_slot: OnceLock::new(),
            waiting_retry: OnceLock::new(),
        }
    }

    /// A counter labelled only with the queue.
    fn count(&self, cell: &OnceLock<Counter>, name: &'static str) {
        cell.get_or_init(|| counter!(name, "queue" => self.queue.clone()))
            .increment(1);
    }

    fn finished(&self, state: TaskState) {
        let Some(cell) = index(state).and_then(|i| self.finished.get(i)) else {
            return;
        };
        cell.get_or_init(|| {
            counter!(
                "taskcraft_tasks_finished_total",
                "queue" => self.queue.clone(),
                "outcome" => state.as_str()
            )
        })
        .increment(1);
    }

    fn duration(&self, outcome: AttemptEnd, seconds: f64) {
        let slot = match outcome {
            AttemptEnd::Finished(state) => index(state),
            AttemptEnd::Retry => Some(STATES),
        };
        let Some(cell) = slot.and_then(|i| self.durations.get(i)) else {
            return;
        };
        cell.get_or_init(|| {
            histogram!(
                "taskcraft_attempt_duration_seconds",
                "queue" => self.queue.clone(),
                "outcome" => outcome.as_str()
            )
        })
        .record(seconds);
    }

    #[allow(clippy::cast_precision_loss)] // counts of tasks
    fn occupancy(&self, running: usize, waiting_slot: usize, waiting_retry: usize) {
        self.running
            .get_or_init(|| gauge!("taskcraft_tasks_running", "queue" => self.queue.clone()))
            .set(running as f64);
        self.waiting_slot
            .get_or_init(|| {
                gauge!("taskcraft_tasks_waiting", "queue" => self.queue.clone(), "state" => "slot")
            })
            .set(waiting_slot as f64);
        self.waiting_retry
            .get_or_init(|| {
                gauge!("taskcraft_tasks_waiting", "queue" => self.queue.clone(), "state" => "retry")
            })
            .set(waiting_retry as f64);
    }
}

/// The series of one pool.
struct PoolSeries {
    pool: SharedString,
    in_use: OnceLock<Gauge>,
    total: OnceLock<Gauge>,
}

impl PoolSeries {
    fn new(pool: &str) -> Self {
        Self {
            pool: SharedString::from(pool.to_owned()),
            in_use: OnceLock::new(),
            total: OnceLock::new(),
        }
    }

    fn usage(&self, in_use: u32, total: u32) {
        self.in_use
            .get_or_init(|| gauge!("taskcraft_pool_permits_in_use", "pool" => self.pool.clone()))
            .set(f64::from(in_use));
        self.total
            .get_or_init(|| gauge!("taskcraft_pool_permits_total", "pool" => self.pool.clone()))
            .set(f64::from(total));
    }
}

/// The position of a state in [`TaskState::ALL`].
fn index(state: TaskState) -> Option<usize> {
    TaskState::ALL.iter().position(|s| *s == state)
}

impl Observer for MetricsObserver {
    fn on_event(&self, event: &Event<'_>) {
        let series = &self.series;
        match *event {
            Event::Accepted { queue, .. } => series.queue(queue, |q| {
                q.count(&q.accepted, "taskcraft_tasks_accepted_total");
            }),
            Event::Rejected { queue, .. } => series.queue(queue, |q| {
                q.rejected
                    .get_or_init(|| {
                        counter!(
                            "taskcraft_tasks_rejected_total",
                            "queue" => q.queue.clone(),
                            "reason" => "overflow"
                        )
                    })
                    .increment(1);
            }),
            Event::Duplicate { queue, .. } => series.queue(queue, |q| {
                q.count(&q.duplicate, "taskcraft_tasks_duplicate_total");
            }),
            Event::Retry { queue, .. } => series.queue(queue, |q| {
                q.count(&q.retries, "taskcraft_task_retries_total");
            }),
            Event::Finished { queue, state, .. } => series.queue(queue, |q| {
                q.finished(state);
                if state == TaskState::Panicked {
                    q.count(&q.panics, "taskcraft_task_panics_total");
                }
            }),
            Event::AttemptFinished {
                queue,
                outcome,
                duration,
                ..
            } => series.queue(queue, |q| q.duration(outcome, duration.as_secs_f64())),
            Event::DecodeFailed { queue } => series.queue(queue, |q| {
                q.count(&q.decode_errors, "taskcraft_decode_errors_total");
            }),
            Event::SourceFailed { queue } => series.queue(queue, |q| {
                q.count(&q.source_errors, "taskcraft_source_errors_total");
            }),
            Event::WorkerRestarted { queue } => series.queue(queue, |q| {
                q.count(&q.restarts, "taskcraft_worker_restarts_total");
            }),
            Event::Occupancy {
                queue,
                running,
                waiting_slot,
                waiting_retry,
            } => series.queue(queue, |q| {
                q.occupancy(running, waiting_slot, waiting_retry);
            }),
            Event::PoolUsage {
                pool,
                in_use,
                total,
            } => series.pool(pool, |p| p.usage(in_use, total)),
            Event::Pushed { .. }
            | Event::AttemptStarted { .. }
            | Event::SourceClosed { .. }
            | Event::WorkerStopped { .. }
            | Event::LeaseTakenOver { .. }
            | Event::LeaseLost { .. } => {}
        }
    }

    fn name(&self) -> &str {
        "metrics"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskId;

    /// Events of one queue and one pool make one entry each; clones share
    /// the handles.
    #[test]
    fn handles_are_kept_per_queue_and_pool() {
        let observer = MetricsObserver::new();
        let clone = observer.clone();
        let id = TaskId::new("t");
        for _ in 0..3 {
            observer.on_event(&Event::Accepted {
                queue: "q",
                task_id: &id,
            });
            clone.on_event(&Event::PoolUsage {
                pool: "p",
                in_use: 1,
                total: 2,
            });
        }
        assert!(Arc::ptr_eq(&observer.series, &clone.series));
        let queues = observer.series.queues.read().unwrap();
        assert_eq!(queues.len(), 1);
        assert!(queues["q"].accepted.get().is_some());
        assert!(queues["q"].duplicate.get().is_none(), "no event, no series");
        assert_eq!(observer.series.pools.read().unwrap().len(), 1);
    }
}
