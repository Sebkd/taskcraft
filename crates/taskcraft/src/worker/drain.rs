//! The drain after intake ends (rule 2.3.14, transition 2.4.2.13).

use std::collections::HashMap;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::TaskEnd;
use super::execute::log_outcome;
use super::intake::Intake;
use crate::monitor::{QueueReport, StopReason};
use crate::observe::{Event, Observers};
use crate::registry::TaskRegistry;
use crate::source::Source;
use crate::state::TaskState;
use crate::status::FinishReason;
use crate::task::TaskId;

/// Intake ended: refuse pushes, drain the running tasks, report the ack
/// failures left and how the worker stopped.
pub(super) async fn stop_worker<S: Source, C, Svc, Args>(
    intake: Intake<S, C, Svc, Args>,
    reason: StopReason,
    listener: Option<JoinHandle<()>>,
    grace: Duration,
) -> QueueReport {
    // Pushes are refused from now on (rule 2.3.14 p. 3).
    intake.tasks.set_closing();
    intake.exec.draining.cancel();
    if let Some(listener) = &listener {
        listener.abort();
    }
    let Intake {
        ctx,
        name,
        observers,
        tasks,
        exec,
        mut running,
        mut ids,
        drain_cancel,
        ack_errors,
        mut ack_failures,
        ..
    } = intake;
    let (completed, cancelled, aborted) = drain(
        &mut running,
        &mut ids,
        Drain {
            tasks: &tasks,
            observers: &observers,
            queue: &name,
            cancel: &drain_cancel,
            stop: &ctx.stop,
            stopped: matches!(reason, StopReason::Shutdown),
            timeout: ctx.shutdown_timeout,
            grace,
        },
    )
    .await;
    drop(exec);
    drop(ack_errors);
    while let Ok(error) = ack_failures.try_recv() {
        error!(
            event = "source",
            action = "failed",
            "source failed: queue={}, error={:?}, restart_in=none",
            name,
            error
        );
        observers.emit(&Event::SourceFailed { queue: &name });
    }
    info!(
        event = "worker",
        action = "stopped",
        "worker stopped: queue={}, reason={:?}",
        name,
        reason.to_string()
    );
    observers.emit(&Event::WorkerStopped {
        queue: &name,
        reason: &reason,
    });
    QueueReport {
        queue: name.to_string(),
        reason,
        completed,
        cancelled,
        aborted,
    }
}

pub(super) fn joined_id<T>(
    joined: &Result<(tokio::task::Id, T), tokio::task::JoinError>,
) -> tokio::task::Id {
    match joined {
        Ok((id, _)) => *id,
        Err(e) => e.id(),
    }
}

/// What the drain needs besides the running tasks.
struct Drain<'a> {
    tasks: &'a TaskRegistry,
    observers: &'a Observers,
    queue: &'a str,
    /// The parent of every task's cancel flag.
    cancel: &'a CancellationToken,
    /// The shutdown signal.
    stop: &'a CancellationToken,
    /// Intake ended by the shutdown signal rather than a closed source.
    stopped: bool,
    timeout: Duration,
    grace: Duration,
}

/// Waits for running tasks (rule 2.3.14). After a closed source it waits
/// for them to end on their own — until the shutdown signal, if one comes
/// (transition 2.4.2.13). From the signal on: the shutdown timeout, then
/// the cancel flag, then the cancel grace, then the abort. Returns
/// (completed, cancelled, aborted).
async fn drain(
    running: &mut JoinSet<TaskEnd>,
    ids: &mut HashMap<tokio::task::Id, TaskId>,
    d: Drain<'_>,
) -> (u32, u32, u32) {
    let mut counts = Counts::default();
    if !d.stopped && wait_all_or_stop(running, ids, d.stop, &mut counts).await {
        return (counts.completed, counts.cancelled, 0);
    }
    if wait_all(running, ids, Instant::now() + d.timeout, &mut counts).await {
        return (counts.completed, counts.cancelled, 0);
    }
    let cancelled = counts.cancelled + u32::try_from(running.len()).unwrap_or(u32::MAX);
    let completed = counts.completed;
    d.cancel.cancel();
    // Tasks ending within the grace are already counted as cancelled.
    if wait_all(
        running,
        ids,
        Instant::now() + d.grace,
        &mut Counts::default(),
    )
    .await
    {
        return (completed, cancelled, 0);
    }
    let aborted = u32::try_from(running.len()).unwrap_or(u32::MAX);
    // The aborted futures cannot report their outcome: report it for them
    // (rule 2.3.14 p. 6) — no `await` until `abort_all`.
    for task_id in ids.values() {
        warn!(
            event = "task",
            action = "aborted",
            "task aborted after cancel grace: queue={}, task_id={}",
            d.queue,
            task_id
        );
        let attempt = d.tasks.status(task_id).map_or(0, |s| s.attempt());
        let reason = FinishReason::CancelledByShutdown;
        log_outcome(
            d.queue,
            task_id,
            attempt,
            TaskState::Cancelled,
            Some(&reason),
        );
        d.observers.emit(&Event::Finished {
            queue: d.queue,
            task_id,
            attempt,
            state: TaskState::Cancelled,
            reason: Some(&reason),
        });
        d.tasks.remove(task_id);
    }
    running.abort_all();
    while running.join_next().await.is_some() {}
    (completed, cancelled, aborted)
}

/// Joins tasks until none is left (`true`) or the shutdown signal (`false`).
async fn wait_all_or_stop(
    running: &mut JoinSet<TaskEnd>,
    ids: &mut HashMap<tokio::task::Id, TaskId>,
    stop: &CancellationToken,
    counts: &mut Counts,
) -> bool {
    loop {
        tokio::select! {
            biased;
            joined = running.join_next_with_id() => match joined {
                Some(joined) => {
                    ids.remove(&joined_id(&joined));
                    counts.add(&joined);
                }
                None => return true,
            },
            () = stop.cancelled() => return false,
        }
    }
}

#[derive(Default)]
struct Counts {
    completed: u32,
    cancelled: u32,
}

impl Counts {
    fn add(&mut self, joined: &Result<(tokio::task::Id, TaskEnd), tokio::task::JoinError>) {
        match joined {
            Ok((_, TaskEnd::CancelledWaiting)) => self.cancelled += 1,
            Ok((_, TaskEnd::Before)) => {}
            Ok((_, TaskEnd::Finished)) | Err(_) => self.completed += 1,
        }
    }
}

/// Joins tasks until none is left (`true`) or the deadline passes (`false`).
async fn wait_all(
    running: &mut JoinSet<TaskEnd>,
    ids: &mut HashMap<tokio::task::Id, TaskId>,
    deadline: Instant,
    counts: &mut Counts,
) -> bool {
    loop {
        tokio::select! {
            biased;
            joined = running.join_next_with_id() => match joined {
                Some(joined) => {
                    ids.remove(&joined_id(&joined));
                    counts.add(&joined);
                }
                None => return true,
            },
            () = tokio::time::sleep_until(deadline) => return false,
        }
    }
}
