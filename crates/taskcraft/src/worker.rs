//! The worker of one queue: intake loop, supervision, execution and drain
//! (spec 2.4.2, 2.3.12–2.3.14).

use std::collections::HashMap;
use std::future::pending;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, debug_span, error, info, warn};

use crate::attempt::run_attempt;
use crate::codec::Codec;
use crate::handler::{SharedData, TaskRequest};
use crate::metadata::{MetadataRegistry, TraceParent};
use crate::monitor::{QueueReport, StopReason, WorkerContext};
use crate::observe::{AttemptEnd, Event, Observers, Occupancy, Waiting, observers_of};
use crate::outcome::{BoxError, Outcome};
use crate::poll::{Poller, Wakeup};
use crate::queue::{DeadLetter, OverflowPolicy, Queue, RejectFn, TimeoutOutcome};
use crate::registry::TaskRegistry;
use crate::retry::RetryPolicy;
use crate::source::{AckPointSupport, Completion, Notice, Polled, Progress, Source};
use crate::state::{Lifecycle, TaskState};
use crate::status::FinishReason;
use crate::task::{AckPoint, Task, TaskId};

/// Permits of one monitor pool taken by every attempt of a queue.
#[derive(Debug, Clone)]
pub(crate) struct PoolClaim {
    pub(crate) name: Arc<str>,
    pub(crate) size: u32,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) permits: u32,
}

/// Permits of one pool held by an attempt; reports the pool's usage when
/// taken and when given back.
struct PoolHeld {
    permit: Option<OwnedSemaphorePermit>,
    claim: PoolClaim,
    observers: Observers,
}

impl PoolHeld {
    fn new(claim: &PoolClaim, permit: OwnedSemaphorePermit, observers: &Observers) -> Self {
        let held = Self {
            permit: Some(permit),
            claim: claim.clone(),
            observers: observers.clone(),
        };
        held.report();
        held
    }

    fn report(&self) {
        let free = u32::try_from(self.claim.semaphore.available_permits()).unwrap_or(u32::MAX);
        self.observers.emit(&Event::PoolUsage {
            pool: &self.claim.name,
            in_use: self.claim.size.saturating_sub(free),
            total: self.claim.size,
        });
    }
}

impl Drop for PoolHeld {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.report();
    }
}

/// How a task left the worker, for the shutdown report.
enum TaskEnd {
    /// Reached its outcome after intake ended: the shutdown or the closed
    /// source found it at work.
    Finished,
    /// Reached its outcome before intake ended; not in the report (spec
    /// 2.7.6).
    Before,
    /// Cancelled by shutdown while waiting for a slot or a pool.
    CancelledWaiting,
}

/// The slot an accepted task runs with.
enum Slot {
    /// Taken already.
    Held(OwnedSemaphorePermit),
    /// To be waited for, holding a place in the waiting room meanwhile.
    Wait {
        place: Option<OwnedSemaphorePermit>,
        slots: Arc<Semaphore>,
    },
}

/// What the intake took before polling.
enum Gate<Args> {
    Slot(OwnedSemaphorePermit),
    Waiting(OwnedSemaphorePermit),
    /// Reject policy: poll without reserving anything; refuse with the hook
    /// when no slot is free.
    Free(RejectFn<Args>),
}

/// What every execution of a queue shares.
struct ExecCtx<S: Source, C> {
    queue: Arc<str>,
    source: Arc<S>,
    codec: Arc<C>,
    registry: Arc<MetadataRegistry>,
    shared: Arc<SharedData>,
    supports_defer: bool,
    ack_errors: mpsc::UnboundedSender<String>,
    stop: CancellationToken,
    pools: Vec<PoolClaim>,
    slots: Arc<Semaphore>,
    retry: RetryPolicy,
    tasks: Arc<TaskRegistry>,
    cancel_grace: Duration,
    attempt_timeout: Option<Duration>,
    timeout_outcome: TimeoutOutcome,
    occupancy: Arc<Occupancy>,
    observers: Observers,
    /// The source keeps task history: progress and final states are
    /// recorded (rule 2.3.9 p. 6).
    records: bool,
    /// Set when intake ends, by the shutdown signal or a closed source:
    /// outcomes from then on count in the shutdown report.
    draining: CancellationToken,
}

/// Sleeps the restart delay unless stopped; returns `true` when stopped.
async fn restart_pause(ctx: &WorkerContext, delay: Duration) -> bool {
    tokio::select! {
        biased;
        () = ctx.stop.cancelled() => true,
        () = sleep(delay) => false,
    }
}

/// Logs a source failure and waits out the restart delay (transitions
/// 2.4.2.5 / 2.4.2.12 and 2.4.2.9). Returns `true` when stopped meanwhile.
async fn restart(
    ctx: &WorkerContext,
    observers: &Observers,
    queue: &str,
    failures: u32,
    error: &str,
) -> bool {
    observers.emit(&Event::SourceFailed { queue });
    let delay = ctx.restart.delay(failures);
    error!(
        event = "source",
        action = "failed",
        "source failed: queue={}, error={:?}, restart_in={:?}",
        queue,
        error,
        delay
    );
    if restart_pause(ctx, delay).await {
        return true;
    }
    info!(
        event = "worker",
        action = "resumed",
        "worker resumed: queue={}",
        queue
    );
    observers.emit(&Event::WorkerRestarted { queue });
    false
}

pub(crate) async fn run_worker<S, C, Svc, Args>(
    queue: Queue<S, C, Svc, Args>,
    recovered: Vec<Task<Args>>,
    pools: Vec<PoolClaim>,
    ctx: WorkerContext,
) -> QueueReport
where
    S: Source,
    C: Codec<Args, S::Message>,
    Svc: tower::Service<TaskRequest<Args>, Response = Outcome> + Clone + Send + 'static,
    Svc::Error: Into<BoxError>,
    Svc::Future: Send,
    Args: Clone + Send + 'static,
{
    let Queue {
        config,
        source,
        codec,
        service,
        registry,
        shared,
        dead_letter,
        overflow,
        tasks,
        observers: observer_cell,
        ..
    } = queue;
    let name: Arc<str> = config.name.into();
    let observers = observers_of(&observer_cell);
    let store = source.capabilities().ack_point_support() == AckPointSupport::Fixed;
    info!(
        event = "worker",
        action = "started",
        "worker started: queue={}",
        name
    );

    let (ack_errors, mut ack_failures) = mpsc::unbounded_channel();
    let slots = Arc::new(Semaphore::new(config.concurrency));
    let exec = Arc::new(ExecCtx {
        queue: Arc::clone(&name),
        source: Arc::clone(&source),
        codec: Arc::clone(&codec),
        registry,
        shared,
        supports_defer: source.capabilities().supports_defer(),
        records: store,
        draining: CancellationToken::new(),
        ack_errors: ack_errors.clone(),
        stop: ctx.stop.clone(),
        pools,
        slots: Arc::clone(&slots),
        retry: config.retry,
        tasks: Arc::clone(&tasks),
        cancel_grace: config.cancel_grace,
        attempt_timeout: config.attempt_timeout,
        timeout_outcome: config.timeout_outcome,
        occupancy: Occupancy::new(Arc::clone(&name), observers.clone()),
        observers: observers.clone(),
    });
    let (reject, wait_limit) = match overflow {
        OverflowPolicy::Wait { limit } => (None, limit.unwrap_or(config.concurrency)),
        OverflowPolicy::Reject(hook) => (Some(hook), 0),
    };
    let waiting_room = Arc::new(Semaphore::new(wait_limit));
    let mut running = JoinSet::new();
    let mut ids: HashMap<tokio::task::Id, TaskId> = HashMap::new();
    let drain_cancel = CancellationToken::new();
    let mut poller = Poller::new(&config.poll);
    let mut wake = source.subscribe();
    let mut failures: u32 = 0;
    // Cancel requests and lost leases from the source (rule 2.3.20 pp. 5, 7).
    let listener = source.notices().map(|notices| {
        tokio::spawn(listen(
            notices,
            Arc::clone(&tasks),
            Arc::clone(&name),
            observers.clone(),
        ))
    });
    let stop = &ctx.stop;

    // Recovered tasks are accepted before the first poll, never rejected and
    // never acknowledged: they are not in the source (rule 2.3.18).
    for mut task in recovered {
        let accepted_at = SystemTime::now();
        let task_cancel = drain_cancel.child_token();
        if tasks
            .try_accept(task.id(), accepted_at, task_cancel.clone())
            .is_err()
        {
            debug!(
                event = "task",
                action = "duplicate",
                "duplicate task: queue={}, task_id={}",
                name,
                task.id()
            );
            observers.emit(&Event::Duplicate {
                queue: &name,
                task_id: task.id(),
            });
            continue;
        }
        task.mark_accepted(accepted_at);
        debug!(
            event = "task",
            action = "accepted",
            "task accepted: queue={}, task_id={}",
            name,
            task.id()
        );
        observers.emit(&Event::Accepted {
            queue: &name,
            task_id: task.id(),
        });
        let slot = match Arc::clone(&slots).try_acquire_owned() {
            Ok(permit) => Slot::Held(permit),
            Err(_) => Slot::Wait {
                place: None,
                slots: Arc::clone(&slots),
            },
        };
        let task_id = task.id().clone();
        let handle = running.spawn(execute(
            task,
            None,
            false,
            service.clone(),
            task_cancel,
            slot,
            Arc::clone(&exec),
        ));
        ids.insert(handle.id(), task_id);
    }

    let reason = 'intake: loop {
        while let Some(joined) = running.try_join_next_with_id() {
            ids.remove(&joined_id(&joined));
        }
        if let Ok(error) = ack_failures.try_recv() {
            failures += 1;
            if restart(&ctx, &observers, &name, failures, &error).await {
                break StopReason::Shutdown;
            }
            continue;
        }

        // Rule 2.3.7: with "wait", poll only when a slot or a place in the
        // waiting room is free; with "reject", always poll.
        // The semaphores are never closed; if one were, the worker stops as
        // failed instead of panicking (invariant 1.3.19).
        let gate = if let Some(hook) = &reject {
            Gate::Free(Arc::clone(hook))
        } else {
            tokio::select! {
                biased;
                () = stop.cancelled() => break 'intake StopReason::Shutdown,
                permit = Arc::clone(&slots).acquire_owned() => match permit {
                    Ok(permit) => Gate::Slot(permit),
                    Err(_) => break 'intake StopReason::Failed("slot semaphore closed".to_owned()),
                },
                place = Arc::clone(&waiting_room).acquire_owned() => match place {
                    Ok(place) => Gate::Waiting(place),
                    Err(_) => break 'intake StopReason::Failed("waiting room closed".to_owned()),
                },
            }
        };
        if let Some(signal) = wake.as_mut() {
            signal.mark_seen();
        }
        let polled = tokio::select! {
            biased;
            () = stop.cancelled() => break 'intake StopReason::Shutdown,
            polled = source.poll() => polled,
        };

        match polled {
            Err(e) => {
                drop(gate);
                failures += 1;
                if restart(&ctx, &observers, &name, failures, &e.to_string()).await {
                    break StopReason::Shutdown;
                }
            }
            Ok(Polled::Empty) => {
                failures = 0;
                drop(gate);
                let woke = tokio::select! {
                    woke = poller.wait(wake.as_mut(), stop) => woke,
                    Some(error) = ack_failures.recv() => {
                        failures += 1;
                        if restart(&ctx, &observers, &name, failures, &error).await {
                            break StopReason::Shutdown;
                        }
                        continue;
                    }
                };
                if woke == Wakeup::Stopped {
                    break StopReason::Shutdown;
                }
            }
            Ok(Polled::Closed(reason)) => {
                warn!(
                    event = "source",
                    action = "closed",
                    "source closed: queue={}, reason={:?}",
                    name,
                    reason.as_str()
                );
                observers.emit(&Event::SourceClosed { queue: &name });
                break StopReason::SourceClosed(reason);
            }
            Ok(Polled::Task { message, receipt }) => {
                failures = 0;
                poller.reset();
                let copy = dead_letter.as_ref().map(|h| (h.copy)(&message));
                let mut task = match codec.decode(message) {
                    Ok(task) => task,
                    Err(error) => {
                        drop(gate);
                        observers.emit(&Event::DecodeFailed { queue: &name });
                        let poison = FinishReason::Handler(error.to_string());
                        if let (Some(h), Some(message)) = (&dead_letter, copy) {
                            let letter = DeadLetter {
                                queue: name.to_string(),
                                message,
                                error,
                            };
                            if catch_unwind(AssertUnwindSafe(|| (h.hook)(letter))).is_err() {
                                error!(
                                    event = "source",
                                    action = "dead_letter_failed",
                                    "dead-letter hook panicked: queue={}",
                                    name
                                );
                            }
                        } else {
                            error!(
                                event = "source",
                                action = "message_dropped",
                                "message dropped: queue={}, reason={:?}",
                                name,
                                error.to_string()
                            );
                        }
                        let end = Completion {
                            state: TaskState::Failed,
                            reason: Some(&poison),
                        };
                        if let Err(e) = settle(&*source, receipt, store, end).await {
                            let _ = ack_errors.send(e.to_string());
                        }
                        continue;
                    }
                };
                // Rule 2.3.11: a busy id is a duplicate delivery, acked and
                // not run (scenario 2.2.3).
                let accepted_at = SystemTime::now();
                let task_cancel = drain_cancel.child_token();
                if tasks
                    .try_accept(task.id(), accepted_at, task_cancel.clone())
                    .is_err()
                {
                    drop(gate);
                    debug!(
                        event = "task",
                        action = "duplicate",
                        "duplicate task: queue={}, task_id={}",
                        name,
                        task.id()
                    );
                    observers.emit(&Event::Duplicate {
                        queue: &name,
                        task_id: task.id(),
                    });
                    if let Err(e) = source.ack(receipt).await {
                        let _ = ack_errors.send(e.to_string());
                    }
                    continue;
                }
                let slot = match gate {
                    Gate::Slot(permit) => Slot::Held(permit),
                    Gate::Waiting(place) => match Arc::clone(&slots).try_acquire_owned() {
                        Ok(permit) => Slot::Held(permit),
                        Err(_) => Slot::Wait {
                            place: Some(place),
                            slots: Arc::clone(&slots),
                        },
                    },
                    Gate::Free(hook) => {
                        if let Ok(permit) = Arc::clone(&slots).try_acquire_owned() {
                            Slot::Held(permit)
                        } else {
                            tasks.remove(task.id());
                            observers.emit(&Event::Rejected {
                                queue: &name,
                                task_id: task.id(),
                                reason: &FinishReason::RejectedOverflow,
                            });
                            reject_task(&name, &*hook, task);
                            let end = Completion {
                                state: TaskState::Cancelled,
                                reason: Some(&FinishReason::RejectedOverflow),
                            };
                            if let Err(e) = settle(&*source, receipt, store, end).await {
                                let _ = ack_errors.send(e.to_string());
                            }
                            continue;
                        }
                    }
                };
                task.mark_accepted(accepted_at);
                debug!(
                    event = "task",
                    action = "accepted",
                    "task accepted: queue={}, task_id={}",
                    name,
                    task.id()
                );
                observers.emit(&Event::Accepted {
                    queue: &name,
                    task_id: task.id(),
                });
                // A task store records every final state; its ack point is
                // not configurable (rule 2.3.9 p. 6).
                let ack_point = if store {
                    AckPoint::OnCompletion
                } else {
                    task.ack_point().unwrap_or(config.ack_point)
                };
                let ack_later = match ack_point {
                    AckPoint::OnAccept => {
                        if let Err(e) = source.ack(receipt.clone()).await {
                            let _ = ack_errors.send(e.to_string());
                        }
                        false
                    }
                    AckPoint::OnCompletion => true,
                };
                let task_id = task.id().clone();
                let handle = running.spawn(execute(
                    task,
                    Some(receipt),
                    ack_later,
                    service.clone(),
                    task_cancel,
                    slot,
                    Arc::clone(&exec),
                ));
                ids.insert(handle.id(), task_id);
            }
        }
    };

    // Pushes are refused from now on (rule 2.3.14 p. 3).
    tasks.set_closing();
    exec.draining.cancel();
    if let Some(listener) = &listener {
        listener.abort();
    }
    let (completed, cancelled, aborted) = drain(
        &mut running,
        &mut ids,
        Drain {
            tasks: &tasks,
            observers: &observers,
            queue: &name,
            cancel: &drain_cancel,
            stop,
            stopped: matches!(reason, StopReason::Shutdown),
            timeout: ctx.shutdown_timeout,
            grace: config.cancel_grace,
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

/// Refuses a task for lack of a slot (scenario 2.2.1).
fn reject_task<Args>(queue: &str, hook: &(dyn Fn(Task<Args>) + Send + Sync), task: Task<Args>) {
    let id = task.id().clone();
    warn!(
        event = "task",
        action = "rejected",
        "task rejected: queue={}, task_id={}, reason=overflow",
        queue,
        id
    );
    if catch_unwind(AssertUnwindSafe(|| hook(task))).is_err() {
        error!(
            event = "task",
            action = "reject_failed",
            "reject hook panicked: queue={}, task_id={}",
            queue,
            id
        );
    }
}

fn joined_id<T>(joined: &Result<(tokio::task::Id, T), tokio::task::JoinError>) -> tokio::task::Id {
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

/// What follows an attempt.
enum Next {
    /// A final state with its reason.
    Finish(TaskState, Option<FinishReason>),
    /// Wait, then run again (rules 2.3.4, 2.3.6).
    Pause(Duration),
}

/// Runs one task to its final state, retrying as the queue's policy allows
/// (rules 2.3.2–2.3.6, 2.3.9, 2.3.15).
async fn execute<S, C, Svc, Args>(
    mut task: Task<Args>,
    receipt: Option<S::Receipt>,
    ack_later: bool,
    mut service: Svc,
    cancel: CancellationToken,
    slot: Slot,
    ctx: Arc<ExecCtx<S, C>>,
) -> TaskEnd
where
    S: Source,
    C: Codec<Args, S::Message>,
    Svc: tower::Service<TaskRequest<Args>, Response = Outcome> + Send + 'static,
    Svc::Error: Into<BoxError>,
    Svc::Future: Send,
    Args: Clone + Send + 'static,
{
    let id = task.id().clone();
    let mut life = Lifecycle::accepted();
    let mut first = Some(slot);
    let mut held: Option<OwnedSemaphorePermit> = None;
    loop {
        // Accepted: the slot (from intake, kept through the pause, or taken
        // again after it), then the pools in name order. Shutdown cancels a
        // task that is still waiting (rule 2.3.14 p. 4), and so does a
        // cancel request (spec 2.1.2.15 p. 2).
        let slot = match (held.take(), first.take()) {
            (Some(permit), _) | (None, Some(Slot::Held(permit))) => Slot::Held(permit),
            (None, Some(waiting)) => waiting,
            (None, None) => Slot::Wait {
                place: None,
                slots: Arc::clone(&ctx.slots),
            },
        };
        // Counted as waiting while it waits for a slot or a pool.
        let waiting = (matches!(slot, Slot::Wait { .. }) || !ctx.pools.is_empty())
            .then(|| ctx.occupancy.enter(Waiting::Slot));
        let permit = match slot {
            Slot::Held(permit) => permit,
            Slot::Wait { place, slots } => {
                let permit = tokio::select! {
                    biased;
                    () = ctx.stop.cancelled() => {
                        return cancel_waiting(&ctx, &mut life, &id, task.attempt());
                    }
                    () = cancel.cancelled() => {
                        let reason = ctx.tasks.cancel_reason(&id);
                        let end = (TaskState::Cancelled, Some(reason));
                        return finish(&ctx, &mut life, &id, task.attempt(), receipt, ack_later, end)
                            .await;
                    }
                    // Never closed; if it were, the task is cancelled as on
                    // shutdown and delivered again (invariant 1.3.19).
                    permit = slots.acquire_owned() => match permit {
                        Ok(permit) => permit,
                        Err(_) => return cancel_waiting(&ctx, &mut life, &id, task.attempt()),
                    },
                };
                drop(place);
                permit
            }
        };
        let mut pool_permits = Vec::with_capacity(ctx.pools.len());
        for claim in &ctx.pools {
            let permits = tokio::select! {
                biased;
                () = ctx.stop.cancelled() => {
                    return cancel_waiting(&ctx, &mut life, &id, task.attempt());
                }
                () = cancel.cancelled() => {
                    let reason = ctx.tasks.cancel_reason(&id);
                    let end = (TaskState::Cancelled, Some(reason));
                    return finish(&ctx, &mut life, &id, task.attempt(), receipt, ack_later, end)
                        .await;
                }
                permits = Arc::clone(&claim.semaphore).acquire_many_owned(claim.permits) => {
                    match permits {
                        Ok(permits) => permits,
                        Err(_) => return cancel_waiting(&ctx, &mut life, &id, task.attempt()),
                    }
                }
            };
            pool_permits.push(PoolHeld::new(claim, permits, &ctx.observers));
        }
        drop(waiting);

        advance(&mut life, TaskState::Running);
        task.begin_attempt();
        let (number, retries) = (task.attempt(), task.retries());
        ctx.tasks.update(&id, |e| {
            e.state = TaskState::Running;
            e.attempt = number;
            e.retries = retries;
            e.attempt_started_at = Some(SystemTime::now());
            e.next_attempt_at = None;
        });
        record(&ctx, receipt.clone(), TaskState::Running, number, retries).await;
        // The attempt's own flag: set by the task's flag and by the attempt
        // timeout, which must not cancel the retries that follow.
        let attempt_cancel = cancel.child_token();
        let request = TaskRequest::new(
            task.clone(),
            Arc::clone(&ctx.registry),
            Arc::clone(&ctx.shared),
        )
        .with_cancel(attempt_cancel.clone());
        let timed_out = AtomicBool::new(false);
        let running = ctx.occupancy.enter(Waiting::Running);
        let started = Instant::now();
        let outcome = tokio::select! {
            outcome = attempt(&mut service, request, &ctx) => Some(outcome),
            () = forced_abort(&cancel, &ctx.tasks, &id, ctx.cancel_grace) => None,
            () = expire(&ctx, &attempt_cancel, &timed_out, &id, number) => None,
        };
        if outcome.is_none() {
            warn!(
                event = "task",
                action = "aborted",
                "task aborted after cancel grace: queue={}, task_id={}",
                ctx.queue,
                id
            );
        }
        // With the cancel flag set, the reason of the flag decides, not what
        // the handler returned; a panic stays a panic (rule 2.3.15 p. 4).
        let next = match outcome {
            Some(outcome @ Outcome::Panic { .. }) => {
                decide(outcome, &mut task, receipt.clone(), &ctx).await
            }
            _ if cancel.is_cancelled() => {
                Next::Finish(TaskState::Cancelled, Some(ctx.tasks.cancel_reason(&id)))
            }
            _ if timed_out.load(Ordering::Relaxed) => match ctx.timeout_outcome {
                TimeoutOutcome::Abort => {
                    Next::Finish(TaskState::Failed, Some(FinishReason::AttemptTimeout))
                }
                TimeoutOutcome::Retry => {
                    let retry = Outcome::Retry {
                        reason: FinishReason::AttemptTimeout,
                        delay: None,
                    };
                    decide(retry, &mut task, receipt.clone(), &ctx).await
                }
            },
            Some(outcome) => decide(outcome, &mut task, receipt.clone(), &ctx).await,
            None => Next::Finish(TaskState::Cancelled, Some(FinishReason::CancelledByUser)),
        };
        drop(running);
        ctx.observers.emit(&Event::AttemptFinished {
            queue: &ctx.queue,
            task_id: &id,
            attempt: number,
            outcome: match next {
                Next::Finish(state, _) => AttemptEnd::Finished(state),
                Next::Pause(_) => AttemptEnd::Retry,
            },
            duration: started.elapsed(),
        });

        match next {
            Next::Finish(state, reason) => {
                let end = (state, reason);
                let ended = finish(
                    &ctx,
                    &mut life,
                    &id,
                    task.attempt(),
                    receipt,
                    ack_later,
                    end,
                );
                let ended = ended.await;
                drop(pool_permits);
                drop(permit);
                return ended;
            }
            Next::Pause(pause) => {
                // Waiting to retry: pools go back always, the slot unless the
                // policy keeps it (rule 2.3.5).
                advance(&mut life, TaskState::RetryWaiting);
                let due = SystemTime::now() + pause;
                let retries = task.retries();
                ctx.tasks.update(&id, |e| {
                    e.state = TaskState::RetryWaiting;
                    e.retries = retries;
                    e.attempt_started_at = None;
                    e.next_attempt_at = Some(due);
                });
                let number = task.attempt();
                record(
                    &ctx,
                    receipt.clone(),
                    TaskState::RetryWaiting,
                    number,
                    retries,
                )
                .await;
                drop(pool_permits);
                if ctx.retry.hold_slot {
                    held = Some(permit);
                } else {
                    drop(permit);
                }
                let retrying = ctx.occupancy.enter(Waiting::Retry);
                tokio::select! {
                    biased;
                    () = ctx.stop.cancelled() => {
                        return cancel_waiting(&ctx, &mut life, &id, task.attempt());
                    }
                    () = cancel.cancelled() => {
                        let reason = ctx.tasks.cancel_reason(&id);
                        let end = (TaskState::Cancelled, Some(reason));
                        return finish(&ctx, &mut life, &id, task.attempt(), receipt, ack_later, end)
                            .await;
                    }
                    () = sleep(pause) => {}
                }
                drop(retrying);
                advance(&mut life, TaskState::Accepted);
                ctx.tasks.update(&id, |e| {
                    e.state = TaskState::Accepted;
                    e.next_attempt_at = None;
                });
            }
        }
    }
}

/// Acks a delivery that ends before it runs; a task store records how.
async fn settle<S: Source>(
    source: &S,
    receipt: S::Receipt,
    store: bool,
    completion: Completion<'_>,
) -> Result<(), S::Error> {
    if store {
        source.complete(receipt, completion).await
    } else {
        source.ack(receipt).await
    }
}

/// Tells a source that keeps task history where the task stands.
async fn record<S: Source, C>(
    ctx: &ExecCtx<S, C>,
    receipt: Option<S::Receipt>,
    state: TaskState,
    attempt: u32,
    retries: u32,
) {
    if !ctx.records {
        return;
    }
    let Some(receipt) = receipt else {
        return;
    };
    let progress = Progress {
        state,
        attempt,
        retries,
    };
    if let Err(e) = ctx.source.progress(receipt, progress).await {
        let _ = ctx.ack_errors.send(e.to_string());
    }
}

/// Applies the source's notices to the registry: a cancel request or a lost
/// lease sets the task's cancel flag with its reason.
async fn listen(
    mut notices: crate::source::Notices,
    tasks: Arc<TaskRegistry>,
    queue: Arc<str>,
    observers: Observers,
) {
    while let Some(notice) = notices.recv().await {
        match notice {
            Notice::CancelRequested(id) => {
                if tasks.cancel(&id).is_some() {
                    info!(
                        event = "task",
                        action = "cancel_requested",
                        "task cancel requested: queue={}, task_id={}",
                        queue,
                        id
                    );
                }
            }
            Notice::LeaseLost(id) => {
                if tasks.cancel_with(&id, FinishReason::LeaseLost).is_some() {
                    warn!(
                        event = "lease",
                        action = "lost",
                        "lease lost: queue={}, task_id={}",
                        queue,
                        id
                    );
                    observers.emit(&Event::LeaseLost {
                        queue: &queue,
                        task_id: &id,
                    });
                }
            }
            Notice::TakenOver {
                task_id,
                previous_owner,
            } => {
                warn!(
                    event = "lease",
                    action = "taken_over",
                    "task taken over after lease expiry: queue={}, task_id={}, previous_owner={}",
                    queue,
                    task_id,
                    previous_owner
                );
                observers.emit(&Event::LeaseTakenOver {
                    queue: &queue,
                    task_id: &task_id,
                });
            }
        }
    }
}

/// Completes `grace` after a cancel request; never for shutdown, whose
/// timeout and grace the drain enforces (rule 2.3.14).
async fn forced_abort(
    cancel: &CancellationToken,
    tasks: &TaskRegistry,
    id: &TaskId,
    grace: Duration,
) {
    cancel.cancelled().await;
    if tasks.cancel_reason(id) == FinishReason::CancelledByShutdown {
        pending::<()>().await;
    }
    sleep(grace).await;
}

/// Completes `grace` after the attempt timeout expired; never without a
/// timeout (rule 2.3.16, spec 2.1.2.13).
async fn expire<S: Source, C>(
    ctx: &ExecCtx<S, C>,
    attempt_cancel: &CancellationToken,
    timed_out: &AtomicBool,
    id: &TaskId,
    attempt: u32,
) {
    let Some(timeout) = ctx.attempt_timeout else {
        return pending().await;
    };
    sleep(timeout).await;
    timed_out.store(true, Ordering::Relaxed);
    warn!(
        event = "task",
        action = "timed_out",
        "task attempt timed out: queue={}, task_id={}, attempt={}",
        ctx.queue,
        id,
        attempt
    );
    attempt_cancel.cancel();
    sleep(ctx.cancel_grace).await;
}

/// The final state or defer: logged, taken out of the registry and acked
/// unless deferred or cancelled by shutdown (rule 2.3.9, spec 2.1.2.15 p. 8).
async fn finish<S: Source, C>(
    ctx: &ExecCtx<S, C>,
    life: &mut Lifecycle,
    id: &TaskId,
    attempt: u32,
    receipt: Option<S::Receipt>,
    ack_later: bool,
    (state, reason): (TaskState, Option<FinishReason>),
) -> TaskEnd {
    advance(life, state);
    log_outcome(&ctx.queue, id, attempt, state, reason.as_ref());
    ctx.observers.emit(&Event::Finished {
        queue: &ctx.queue,
        task_id: id,
        attempt,
        state,
        reason: reason.as_ref(),
    });
    ctx.tasks.remove(id);
    let acknowledged_by_defer = state == TaskState::Deferred;
    // Shutdown leaves the task to be delivered again; a lost lease leaves it
    // to its new owner (rule 2.3.20 p. 5).
    let left_to_others = matches!(
        reason,
        Some(FinishReason::CancelledByShutdown | FinishReason::LeaseLost)
    );
    let completion = Completion {
        state,
        reason: reason.as_ref(),
    };
    if ack_later
        && !acknowledged_by_defer
        && !left_to_others
        && let Some(receipt) = receipt
        && let Err(e) = ctx.source.complete(receipt, completion).await
    {
        let _ = ctx.ack_errors.send(e.to_string());
    }
    if ctx.draining.is_cancelled() {
        TaskEnd::Finished
    } else {
        TaskEnd::Before
    }
}

/// One attempt in its span; panics and service errors are outcomes already.
async fn attempt<S, C, Svc, Args>(
    service: &mut Svc,
    request: TaskRequest<Args>,
    ctx: &ExecCtx<S, C>,
) -> Outcome
where
    S: Source,
    Svc: tower::Service<TaskRequest<Args>, Response = Outcome>,
    Svc::Error: Into<BoxError>,
{
    let id = request.task().id().clone();
    let number = request.task().attempt();
    let span = debug_span!(
        "taskcraft.attempt",
        queue = %ctx.queue,
        task_id = %id,
        attempt = number,
        trace_parent = tracing::field::Empty,
    );
    // Spec 4.4.1: the attempt span carries the trace context of the push,
    // for the tracing layer that links it to its parent.
    if !span.is_disabled()
        && let Ok(Some(TraceParent(parent))) = request
            .task()
            .metadata()
            .resolve::<TraceParent>(request.registry())
    {
        span.record("trace_parent", parent.as_str());
    }
    ctx.observers.emit(&Event::AttemptStarted {
        queue: &ctx.queue,
        task_id: &id,
        attempt: number,
    });
    async {
        debug!(
            event = "task",
            action = "started",
            "task started: queue={}, task_id={}, attempt={}",
            ctx.queue,
            id,
            number
        );
        run_attempt(service, request).await
    }
    .instrument(span)
    .await
}

/// What the outcome of an attempt leads to (rules 2.3.2, 2.3.3, 2.3.6).
async fn decide<S, C, Args>(
    outcome: Outcome,
    task: &mut Task<Args>,
    receipt: Option<S::Receipt>,
    ctx: &ExecCtx<S, C>,
) -> Next
where
    S: Source,
    C: Codec<Args, S::Message>,
    Args: Clone,
{
    match outcome {
        Outcome::Success => Next::Finish(TaskState::Succeeded, None),
        Outcome::Abort { reason } => Next::Finish(TaskState::Failed, Some(reason)),
        Outcome::Panic { message } => {
            Next::Finish(TaskState::Panicked, Some(FinishReason::Panic(message)))
        }
        Outcome::Retry { reason, delay } => {
            if !ctx.retry.allows_retry(task.retries()) {
                return Next::Finish(TaskState::Failed, Some(FinishReason::AttemptsExhausted));
            }
            task.count_retry();
            let pause = ctx.retry.pause(task.retries(), delay);
            warn!(
                event = "task",
                action = "retry",
                "task will be retried: queue={}, task_id={}, attempt={}, pause={:?}, reason={:?}",
                ctx.queue,
                task.id(),
                task.attempt(),
                pause,
                reason.to_string()
            );
            ctx.observers.emit(&Event::Retry {
                queue: &ctx.queue,
                task_id: task.id(),
                attempt: task.attempt(),
                pause,
            });
            Next::Pause(pause)
        }
        Outcome::Defer { delay, .. } => {
            // A recovered task has no delivery to hand back.
            if ctx.supports_defer
                && let Some(receipt) = receipt
            {
                match defer(ctx, task.clone(), receipt, delay).await {
                    Ok(()) => return Next::Finish(TaskState::Deferred, None),
                    Err(error) => warn!(
                        event = "task",
                        action = "defer_failed",
                        "defer failed, handled in process: queue={}, task_id={}, error={:?}",
                        ctx.queue,
                        task.id(),
                        error
                    ),
                }
            } else {
                debug!(
                    event = "task",
                    action = "defer_in_process",
                    "defer handled in process: queue={}, task_id={}",
                    ctx.queue,
                    task.id()
                );
            }
            // Not now rather than failed: no retry is counted and the
            // pause is not capped (rule 2.3.6 p. 2).
            Next::Pause(delay)
        }
    }
}

/// A task cancelled by shutdown while still waiting for a slot or a pool:
/// "Accepted" → "Cancelled", no ack (scenario 2.2.7).
fn cancel_waiting<S: Source, C>(
    ctx: &ExecCtx<S, C>,
    life: &mut Lifecycle,
    id: &TaskId,
    attempt: u32,
) -> TaskEnd {
    advance(life, TaskState::Cancelled);
    log_outcome(
        &ctx.queue,
        id,
        attempt,
        TaskState::Cancelled,
        Some(&FinishReason::CancelledByShutdown),
    );
    ctx.observers.emit(&Event::Finished {
        queue: &ctx.queue,
        task_id: id,
        attempt,
        state: TaskState::Cancelled,
        reason: Some(&FinishReason::CancelledByShutdown),
    });
    ctx.tasks.remove(id);
    TaskEnd::CancelledWaiting
}

async fn defer<S, C, Args>(
    ctx: &ExecCtx<S, C>,
    task: Task<Args>,
    receipt: S::Receipt,
    delay: Duration,
) -> Result<(), String>
where
    S: Source,
    C: Codec<Args, S::Message>,
{
    let message = ctx.codec.encode(task).map_err(|e| e.to_string())?;
    ctx.source
        .defer(receipt, message, Instant::now() + delay)
        .await
        .map_err(|e| format!("defer failed: {e}"))
}

fn advance(life: &mut Lifecycle, to: TaskState) {
    // The worker only takes transitions of the lifecycle table; a failure
    // here is a bug in the worker, not in the task.
    let from = life.state();
    let result = life.advance(to);
    debug_assert!(result.is_ok(), "{from} -> {to}");
}

fn log_outcome(
    queue: &str,
    id: &TaskId,
    attempt: u32,
    state: TaskState,
    reason: Option<&FinishReason>,
) {
    let reason_text = reason.map(ToString::to_string).unwrap_or_default();
    match state {
        TaskState::Panicked => error!(
            event = "task",
            action = "panicked",
            "task panicked: queue={}, task_id={}, attempt={}, message={:?}",
            queue,
            id,
            attempt,
            reason_text
        ),
        TaskState::Failed => error!(
            event = "task",
            action = "failed",
            "task failed: queue={}, task_id={}, attempt={}, reason={:?}",
            queue,
            id,
            attempt,
            reason_text
        ),
        _ => {}
    }
    debug!(
        event = "task",
        action = "finished",
        "task finished: queue={}, task_id={}, attempt={}, outcome={}",
        queue,
        id,
        attempt,
        state
    );
}
