//! The intake loop of a worker: polls the source, accepts tasks and starts
//! them; restarts after source failures (spec 2.4.2, rules 2.3.7, 2.3.11).

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::drain::joined_id;
use super::execute::{execute, settle};
use super::{ExecCtx, Slot, TaskEnd};
use crate::codec::{Codec, CodecError};
use crate::handler::TaskRequest;
use crate::monitor::{StopReason, WorkerContext};
use crate::observe::{Event, Observers};
use crate::outcome::{BoxError, Outcome};
use crate::poll::{Poller, Wakeup};
use crate::queue::{DeadLetter, DeadLetterHook, RejectFn};
use crate::registry::TaskRegistry;
use crate::source::{Completion, Polled, Source, WakeSignal};
use crate::state::TaskState;
use crate::status::FinishReason;
use crate::task::{AckPoint, Task, TaskId};

/// What the intake took before polling.
enum Gate<Args> {
    Slot(OwnedSemaphorePermit),
    Waiting(OwnedSemaphorePermit),
    /// Reject policy: poll without reserving anything; refuse with the hook
    /// when no slot is free.
    Free(RejectFn<Args>),
}

/// The state of one worker's intake loop.
pub(super) struct Intake<S: Source, C, Svc, Args> {
    pub(super) ctx: WorkerContext,
    pub(super) name: Arc<str>,
    pub(super) observers: Observers,
    pub(super) source: Arc<S>,
    pub(super) codec: Arc<C>,
    pub(super) service: Svc,
    pub(super) dead_letter: Option<DeadLetterHook<S::Message>>,
    pub(super) tasks: Arc<TaskRegistry>,
    pub(super) exec: Arc<ExecCtx<S, C>>,
    /// The task store records every final state (rule 2.3.9 p. 6).
    pub(super) store: bool,
    pub(super) ack_point: AckPoint,
    pub(super) slots: Arc<Semaphore>,
    pub(super) waiting_room: Arc<Semaphore>,
    pub(super) reject: Option<RejectFn<Args>>,
    pub(super) running: JoinSet<TaskEnd>,
    pub(super) ids: HashMap<tokio::task::Id, TaskId>,
    /// The parent of every task's cancel flag.
    pub(super) drain_cancel: CancellationToken,
    pub(super) poller: Poller,
    pub(super) wake: Option<WakeSignal>,
    pub(super) failures: u32,
    pub(super) ack_errors: mpsc::UnboundedSender<String>,
    pub(super) ack_failures: mpsc::UnboundedReceiver<String>,
}

impl<S, C, Svc, Args> Intake<S, C, Svc, Args>
where
    S: Source,
    C: Codec<Args, S::Message>,
    Svc: tower::Service<TaskRequest<Args>, Response = Outcome> + Clone + Send + 'static,
    Svc::Error: Into<BoxError>,
    Svc::Future: Send,
    Args: Clone + Send + 'static,
{
    /// Recovered tasks are accepted before the first poll, never rejected
    /// and never acknowledged: they are not in the source (rule 2.3.18).
    pub(super) fn accept_recovered(&mut self, recovered: Vec<Task<Args>>) {
        for mut task in recovered {
            let accepted_at = SystemTime::now();
            let task_cancel = self.drain_cancel.child_token();
            if self
                .tasks
                .try_accept(task.id(), accepted_at, task_cancel.clone())
                .is_err()
            {
                self.duplicate(task.id());
                continue;
            }
            self.accepted(&mut task, accepted_at);
            let slot = match Arc::clone(&self.slots).try_acquire_owned() {
                Ok(permit) => Slot::Held(permit),
                Err(_) => Slot::Wait {
                    place: None,
                    slots: Arc::clone(&self.slots),
                },
            };
            self.start(task, None, false, task_cancel, slot);
        }
    }

    /// Runs the loop until the shutdown signal, a closed source or a broken
    /// semaphore.
    pub(super) async fn run(&mut self) -> StopReason {
        loop {
            if let ControlFlow::Break(reason) = self.step().await {
                return reason;
            }
        }
    }

    /// One pass: collect ended tasks, take a gate, poll, handle the answer.
    async fn step(&mut self) -> ControlFlow<StopReason> {
        while let Some(joined) = self.running.try_join_next_with_id() {
            self.ids.remove(&joined_id(&joined));
        }
        if let Ok(error) = self.ack_failures.try_recv() {
            return self.source_failed(&error).await;
        }
        let gate = self.gate().await?;
        if let Some(signal) = self.wake.as_mut() {
            signal.mark_seen();
        }
        let polled = tokio::select! {
            biased;
            () = self.ctx.stop.cancelled() => return ControlFlow::Break(StopReason::Shutdown),
            polled = self.source.poll() => polled,
        };

        match polled {
            Err(e) => {
                drop(gate);
                self.source_failed(&e.to_string()).await
            }
            Ok(Polled::Empty) => {
                self.failures = 0;
                drop(gate);
                self.on_empty().await
            }
            Ok(Polled::Closed(reason)) => {
                warn!(
                    event = "source",
                    action = "closed",
                    "source closed: queue={}, reason={:?}",
                    self.name,
                    reason.as_str()
                );
                self.observers
                    .emit(&Event::SourceClosed { queue: &self.name });
                ControlFlow::Break(StopReason::SourceClosed(reason))
            }
            Ok(Polled::Task { message, receipt }) => {
                self.failures = 0;
                self.poller.reset();
                self.on_message(message, receipt, gate).await;
                ControlFlow::Continue(())
            }
        }
    }

    /// Rule 2.3.7: with "wait", poll only when a slot or a place in the
    /// waiting room is free; with "reject", always poll.
    /// The semaphores are never closed; if one were, the worker stops as
    /// failed instead of panicking (invariant 1.3.19).
    // `&mut self`: the worker's future must be `Send`, and `Intake` is not `Sync`.
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn gate(&mut self) -> ControlFlow<StopReason, Gate<Args>> {
        if let Some(hook) = &self.reject {
            return ControlFlow::Continue(Gate::Free(Arc::clone(hook)));
        }
        let failed = |what: &str| ControlFlow::Break(StopReason::Failed(what.to_owned()));
        tokio::select! {
            biased;
            () = self.ctx.stop.cancelled() => ControlFlow::Break(StopReason::Shutdown),
            permit = Arc::clone(&self.slots).acquire_owned() => match permit {
                Ok(permit) => ControlFlow::Continue(Gate::Slot(permit)),
                Err(_) => failed("slot semaphore closed"),
            },
            place = Arc::clone(&self.waiting_room).acquire_owned() => match place {
                Ok(place) => ControlFlow::Continue(Gate::Waiting(place)),
                Err(_) => failed("waiting room closed"),
            },
        }
    }

    /// Nothing to do: wait for a wake-up, the next poll or an ack failure.
    async fn on_empty(&mut self) -> ControlFlow<StopReason> {
        let woke = tokio::select! {
            woke = self.poller.wait(self.wake.as_mut(), &self.ctx.stop) => woke,
            Some(error) = self.ack_failures.recv() => return self.source_failed(&error).await,
        };
        if woke == Wakeup::Stopped {
            ControlFlow::Break(StopReason::Shutdown)
        } else {
            ControlFlow::Continue(())
        }
    }

    /// A delivered message: decoded, checked for a busy id, given a slot or
    /// refused, acknowledged as its ack point says and started.
    async fn on_message(&mut self, message: S::Message, receipt: S::Receipt, gate: Gate<Args>) {
        let copy = self.dead_letter.as_ref().map(|h| (h.copy)(&message));
        let mut task = match self.codec.decode(message) {
            Ok(task) => task,
            Err(error) => {
                drop(gate);
                self.on_poison(error, copy, receipt).await;
                return;
            }
        };
        // Rule 2.3.11: a busy id is a duplicate delivery, acked and not run
        // (scenario 2.2.3).
        let accepted_at = SystemTime::now();
        let task_cancel = self.drain_cancel.child_token();
        if self
            .tasks
            .try_accept(task.id(), accepted_at, task_cancel.clone())
            .is_err()
        {
            drop(gate);
            self.duplicate(task.id());
            if let Err(e) = self.source.ack(receipt).await {
                let _ = self.ack_errors.send(e.to_string());
            }
            return;
        }
        let slot = match gate {
            Gate::Slot(permit) => Slot::Held(permit),
            Gate::Waiting(place) => match Arc::clone(&self.slots).try_acquire_owned() {
                Ok(permit) => Slot::Held(permit),
                Err(_) => Slot::Wait {
                    place: Some(place),
                    slots: Arc::clone(&self.slots),
                },
            },
            Gate::Free(hook) => {
                if let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() {
                    Slot::Held(permit)
                } else {
                    self.reject(&*hook, task, receipt).await;
                    return;
                }
            }
        };
        self.accepted(&mut task, accepted_at);
        // A task store records every final state; its ack point is not
        // configurable (rule 2.3.9 p. 6).
        let ack_point = if self.store {
            AckPoint::OnCompletion
        } else {
            task.ack_point().unwrap_or(self.ack_point)
        };
        let ack_later = match ack_point {
            AckPoint::OnAccept => {
                if let Err(e) = self.source.ack(receipt.clone()).await {
                    let _ = self.ack_errors.send(e.to_string());
                }
                false
            }
            AckPoint::OnCompletion => true,
        };
        self.start(task, Some(receipt), ack_later, task_cancel, slot);
    }

    /// A message that cannot be decoded: to the dead-letter hook or the log,
    /// then settled as failed.
    // `&mut self`: the worker's future must be `Send`, and `Intake` is not `Sync`.
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn on_poison(
        &mut self,
        error: CodecError,
        copy: Option<S::Message>,
        receipt: S::Receipt,
    ) {
        let name = &self.name;
        self.observers.emit(&Event::DecodeFailed { queue: name });
        let poison = FinishReason::Handler(error.to_string());
        if let (Some(h), Some(message)) = (&self.dead_letter, copy) {
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
        if let Err(e) = settle(&*self.source, receipt, self.store, end).await {
            let _ = self.ack_errors.send(e.to_string());
        }
    }

    /// No slot under the reject policy: the hook answers the sender and the
    /// delivery is settled (scenario 2.2.1).
    // `&mut self`: the worker's future must be `Send`, and `Intake` is not `Sync`.
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn reject(
        &mut self,
        hook: &(dyn Fn(Task<Args>) + Send + Sync),
        task: Task<Args>,
        receipt: S::Receipt,
    ) {
        self.tasks.remove(task.id());
        self.observers.emit(&Event::Rejected {
            queue: &self.name,
            task_id: task.id(),
            reason: &FinishReason::RejectedOverflow,
        });
        reject_task(&self.name, hook, task);
        let end = Completion {
            state: TaskState::Cancelled,
            reason: Some(&FinishReason::RejectedOverflow),
        };
        if let Err(e) = settle(&*self.source, receipt, self.store, end).await {
            let _ = self.ack_errors.send(e.to_string());
        }
    }

    fn duplicate(&self, id: &TaskId) {
        debug!(
            event = "task",
            action = "duplicate",
            "duplicate task: queue={}, task_id={}",
            self.name,
            id
        );
        self.observers.emit(&Event::Duplicate {
            queue: &self.name,
            task_id: id,
        });
    }

    fn accepted(&self, task: &mut Task<Args>, at: SystemTime) {
        task.mark_accepted(at);
        debug!(
            event = "task",
            action = "accepted",
            "task accepted: queue={}, task_id={}",
            self.name,
            task.id()
        );
        self.observers.emit(&Event::Accepted {
            queue: &self.name,
            task_id: task.id(),
        });
    }

    fn start(
        &mut self,
        task: Task<Args>,
        receipt: Option<S::Receipt>,
        ack_later: bool,
        cancel: CancellationToken,
        slot: Slot,
    ) {
        let task_id = task.id().clone();
        let handle = self.running.spawn(execute(
            task,
            receipt,
            ack_later,
            self.service.clone(),
            cancel,
            slot,
            Arc::clone(&self.exec),
        ));
        self.ids.insert(handle.id(), task_id);
    }

    /// Counts a source failure and waits out the restart delay; breaks when
    /// stopped meanwhile.
    async fn source_failed(&mut self, error: &str) -> ControlFlow<StopReason> {
        self.failures += 1;
        if restart(&self.ctx, &self.observers, &self.name, self.failures, error).await {
            ControlFlow::Break(StopReason::Shutdown)
        } else {
            ControlFlow::Continue(())
        }
    }
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
