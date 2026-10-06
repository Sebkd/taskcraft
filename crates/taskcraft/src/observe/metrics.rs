//! The `metrics` adapter: events as the standard series of spec 4.4.2.

use metrics::{counter, gauge, histogram};

use super::{AttemptEnd, Event, Observer};
use crate::state::TaskState;

/// Publishes the standard series (spec 4.4.2) through the `metrics` facade,
/// to whatever exporter the application installed. Register it with
/// [`Monitor::observer`](crate::Monitor::observer).
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsObserver;

impl MetricsObserver {
    /// The adapter.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

fn queue(queue: &str) -> [(&'static str, String); 1] {
    [("queue", queue.to_owned())]
}

impl Observer for MetricsObserver {
    fn on_event(&self, event: &Event<'_>) {
        match *event {
            Event::Accepted { queue: q, .. } => {
                counter!("taskcraft_tasks_accepted_total", &queue(q)).increment(1);
            }
            Event::Rejected { queue: q, .. } => {
                counter!(
                    "taskcraft_tasks_rejected_total",
                    "queue" => q.to_owned(),
                    "reason" => "overflow"
                )
                .increment(1);
            }
            Event::Duplicate { queue: q, .. } => {
                counter!("taskcraft_tasks_duplicate_total", &queue(q)).increment(1);
            }
            Event::Retry { queue: q, .. } => {
                counter!("taskcraft_task_retries_total", &queue(q)).increment(1);
            }
            Event::Finished {
                queue: q, state, ..
            } => {
                counter!(
                    "taskcraft_tasks_finished_total",
                    "queue" => q.to_owned(),
                    "outcome" => state.as_str()
                )
                .increment(1);
                if state == TaskState::Panicked {
                    counter!("taskcraft_task_panics_total", &queue(q)).increment(1);
                }
            }
            Event::AttemptFinished {
                queue: q,
                outcome,
                duration,
                ..
            } => {
                histogram!(
                    "taskcraft_attempt_duration_seconds",
                    "queue" => q.to_owned(),
                    "outcome" => AttemptEnd::as_str(outcome)
                )
                .record(duration.as_secs_f64());
            }
            Event::DecodeFailed { queue: q } => {
                counter!("taskcraft_decode_errors_total", &queue(q)).increment(1);
            }
            Event::SourceFailed { queue: q } => {
                counter!("taskcraft_source_errors_total", &queue(q)).increment(1);
            }
            Event::WorkerRestarted { queue: q } => {
                counter!("taskcraft_worker_restarts_total", &queue(q)).increment(1);
            }
            Event::Occupancy {
                queue: q,
                running,
                waiting_slot,
                waiting_retry,
            } => {
                #[allow(clippy::cast_precision_loss)] // counts of tasks
                {
                    gauge!("taskcraft_tasks_running", &queue(q)).set(running as f64);
                    gauge!("taskcraft_tasks_waiting", "queue" => q.to_owned(), "state" => "slot")
                        .set(waiting_slot as f64);
                    gauge!("taskcraft_tasks_waiting", "queue" => q.to_owned(), "state" => "retry")
                        .set(waiting_retry as f64);
                }
            }
            Event::PoolUsage {
                pool,
                in_use,
                total,
            } => {
                gauge!("taskcraft_pool_permits_in_use", "pool" => pool.to_owned())
                    .set(f64::from(in_use));
                gauge!("taskcraft_pool_permits_total", "pool" => pool.to_owned())
                    .set(f64::from(total));
            }
            Event::Pushed { .. }
            | Event::AttemptStarted { .. }
            | Event::SourceClosed { .. }
            | Event::WorkerStopped { .. } => {}
        }
    }

    fn name(&self) -> &str {
        "metrics"
    }
}
