//! Execution of one task: slot and pools, attempts, retries, the final state
//! (rules 2.3.2–2.3.6, 2.3.9, 2.3.15, 2.3.16).

use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, debug_span, error, warn};

use super::pools::PoolHeld;
use super::{ExecCtx, Slot, TaskEnd};
use crate::attempt::run_attempt;
use crate::codec::Codec;
use crate::handler::TaskRequest;
use crate::metadata::TraceParent;
use crate::observe::{AttemptEnd, Event, Waiting};
use crate::outcome::{BoxError, Outcome};
use crate::queue::TimeoutOutcome;
use crate::registry::TaskRegistry;
use crate::source::{Completion, Progress, Source};
use crate::state::{Lifecycle, TaskState};
use crate::status::FinishReason;
use crate::task::{Task, TaskId};

/// What follows an attempt.
enum Next {
    /// A final state with its reason.
    Finish(TaskState, Option<FinishReason>),
    /// Wait, then run again (rules 2.3.4, 2.3.6).
    Pause(Duration),
}

/// One task on its way through the worker.
struct Run<S: Source> {
    id: TaskId,
    life: Lifecycle,
    receipt: Option<S::Receipt>,
    ack_later: bool,
    cancel: CancellationToken,
}

impl<S: Source> Run<S> {
    /// Cancelled by shutdown while waiting: no ack (scenario 2.2.7).
    fn cancelled_waiting<C>(&mut self, ctx: &ExecCtx<S, C>, attempt: u32) -> TaskEnd {
        cancel_waiting(ctx, &mut self.life, &self.id, attempt)
    }

    /// Cancelled on request while waiting (spec 2.1.2.15 p. 2).
    async fn cancelled_on_request<C>(&mut self, ctx: &ExecCtx<S, C>, attempt: u32) -> TaskEnd {
        let reason = ctx.tasks.cancel_reason(&self.id);
        let end = (TaskState::Cancelled, Some(reason));
        let receipt = self.receipt.take();
        finish(
            ctx,
            &mut self.life,
            &self.id,
            attempt,
            receipt,
            self.ack_later,
            end,
        )
        .await
    }
}

/// Runs one task to its final state, retrying as the queue's policy allows
/// (rules 2.3.2–2.3.6, 2.3.9, 2.3.15).
pub(super) async fn execute<S, C, Svc, Args>(
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
    let mut run = Run {
        id: task.id().clone(),
        life: Lifecycle::accepted(),
        receipt,
        ack_later,
        cancel,
    };
    let mut slot = slot;
    loop {
        let (permit, pool_permits) = match acquire(&ctx, &mut run, slot, task.attempt()).await {
            Ok(held) => held,
            Err(end) => return end,
        };
        match run_once(&ctx, &mut run, &mut task, &mut service).await {
            Next::Finish(state, reason) => {
                let end = (state, reason);
                let receipt = run.receipt.take();
                let ended = finish(
                    &ctx,
                    &mut run.life,
                    &run.id,
                    task.attempt(),
                    receipt,
                    run.ack_later,
                    end,
                );
                let ended = ended.await;
                drop(pool_permits);
                drop(permit);
                return ended;
            }
            Next::Pause(pause) => {
                let (attempt, retries) = (task.attempt(), task.retries());
                let held = wait_retry(
                    &ctx,
                    &mut run,
                    attempt,
                    retries,
                    pause,
                    pool_permits,
                    permit,
                );
                let held = held.await;
                slot = match held {
                    Ok(Some(permit)) => Slot::Held(permit),
                    Ok(None) => Slot::Wait {
                        place: None,
                        slots: Arc::clone(&ctx.slots),
                    },
                    Err(end) => return end,
                };
            }
        }
    }
}

/// Accepted: the slot (from intake, kept through the pause, or taken again
/// after it), then the pools in name order. Shutdown cancels a task that is
/// still waiting (rule 2.3.14 p. 4), and so does a cancel request (spec
/// 2.1.2.15 p. 2): then the task's end comes back as the error.
async fn acquire<S: Source, C>(
    ctx: &ExecCtx<S, C>,
    run: &mut Run<S>,
    slot: Slot,
    attempt: u32,
) -> Result<(OwnedSemaphorePermit, Vec<PoolHeld>), TaskEnd> {
    // Counted as waiting while it waits for a slot or a pool.
    let waiting = (matches!(slot, Slot::Wait { .. }) || !ctx.pools.is_empty())
        .then(|| ctx.occupancy.enter(Waiting::Slot));
    let permit = match slot {
        Slot::Held(permit) => permit,
        Slot::Wait { place, slots } => {
            let permit = tokio::select! {
                biased;
                () = ctx.stop.cancelled() => return Err(run.cancelled_waiting(ctx, attempt)),
                () = run.cancel.cancelled() => {
                    return Err(run.cancelled_on_request(ctx, attempt).await);
                }
                // Never closed; if it were, the task is cancelled as on
                // shutdown and delivered again (invariant 1.3.19).
                permit = slots.acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => return Err(run.cancelled_waiting(ctx, attempt)),
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
            () = ctx.stop.cancelled() => return Err(run.cancelled_waiting(ctx, attempt)),
            () = run.cancel.cancelled() => {
                return Err(run.cancelled_on_request(ctx, attempt).await);
            }
            permits = Arc::clone(&claim.semaphore).acquire_many_owned(claim.permits) => {
                match permits {
                    Ok(permits) => permits,
                    Err(_) => return Err(run.cancelled_waiting(ctx, attempt)),
                }
            }
        };
        pool_permits.push(PoolHeld::new(claim, permits, &ctx.observers));
    }
    drop(waiting);
    Ok((permit, pool_permits))
}

/// One attempt: the handler against the forced abort and the attempt
/// timeout, then what follows it.
async fn run_once<S, C, Svc, Args>(
    ctx: &ExecCtx<S, C>,
    run: &mut Run<S>,
    task: &mut Task<Args>,
    service: &mut Svc,
) -> Next
where
    S: Source,
    C: Codec<Args, S::Message>,
    Svc: tower::Service<TaskRequest<Args>, Response = Outcome> + Send + 'static,
    Svc::Error: Into<BoxError>,
    Svc::Future: Send,
    Args: Clone + Send + 'static,
{
    let id = &run.id;
    advance(&mut run.life, TaskState::Running);
    task.begin_attempt();
    let (number, retries) = (task.attempt(), task.retries());
    ctx.tasks.update(id, |e| {
        e.state = TaskState::Running;
        e.attempt = number;
        e.retries = retries;
        e.attempt_started_at = Some(SystemTime::now());
        e.next_attempt_at = None;
    });
    record(
        ctx,
        run.receipt.clone(),
        TaskState::Running,
        number,
        retries,
    )
    .await;
    // The attempt's own flag: set by the task's flag and by the attempt
    // timeout, which must not cancel the retries that follow.
    let attempt_cancel = run.cancel.child_token();
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
        outcome = attempt(service, request, ctx) => Some(outcome),
        () = forced_abort(&run.cancel, &ctx.tasks, id, ctx.cancel_grace) => None,
        () = expire(ctx, &attempt_cancel, &timed_out, id, number) => None,
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
            decide(outcome, task, run.receipt.clone(), ctx).await
        }
        _ if run.cancel.is_cancelled() => {
            Next::Finish(TaskState::Cancelled, Some(ctx.tasks.cancel_reason(id)))
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
                decide(retry, task, run.receipt.clone(), ctx).await
            }
        },
        Some(outcome) => decide(outcome, task, run.receipt.clone(), ctx).await,
        None => Next::Finish(TaskState::Cancelled, Some(FinishReason::CancelledByUser)),
    };
    drop(running);
    ctx.observers.emit(&Event::AttemptFinished {
        queue: &ctx.queue,
        task_id: id,
        attempt: number,
        outcome: match next {
            Next::Finish(state, _) => AttemptEnd::Finished(state),
            Next::Pause(_) => AttemptEnd::Retry,
        },
        duration: started.elapsed(),
    });
    next
}

/// Waiting to retry: pools go back always, the slot unless the policy keeps
/// it (rule 2.3.5). Returns the kept slot, or the task's end when shutdown or
/// a cancel request comes first.
async fn wait_retry<S: Source, C>(
    ctx: &ExecCtx<S, C>,
    run: &mut Run<S>,
    attempt: u32,
    retries: u32,
    pause: Duration,
    pool_permits: Vec<PoolHeld>,
    permit: OwnedSemaphorePermit,
) -> Result<Option<OwnedSemaphorePermit>, TaskEnd> {
    advance(&mut run.life, TaskState::RetryWaiting);
    let due = SystemTime::now() + pause;
    ctx.tasks.update(&run.id, |e| {
        e.state = TaskState::RetryWaiting;
        e.retries = retries;
        e.attempt_started_at = None;
        e.next_attempt_at = Some(due);
    });
    record(
        ctx,
        run.receipt.clone(),
        TaskState::RetryWaiting,
        attempt,
        retries,
    )
    .await;
    drop(pool_permits);
    let held = if ctx.retry.hold_slot {
        Some(permit)
    } else {
        drop(permit);
        None
    };
    let retrying = ctx.occupancy.enter(Waiting::Retry);
    tokio::select! {
        biased;
        () = ctx.stop.cancelled() => return Err(run.cancelled_waiting(ctx, attempt)),
        () = run.cancel.cancelled() => {
            return Err(run.cancelled_on_request(ctx, attempt).await);
        }
        () = sleep(pause) => {}
    }
    drop(retrying);
    advance(&mut run.life, TaskState::Accepted);
    ctx.tasks.update(&run.id, |e| {
        e.state = TaskState::Accepted;
        e.next_attempt_at = None;
    });
    Ok(held)
}

/// Acks a delivery that ends before it runs; a task store records how.
pub(super) async fn settle<S: Source>(
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

pub(super) fn log_outcome(
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
