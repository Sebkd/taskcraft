//! Runnables: a handler hands its attempt over to a longer process, such as
//! a state machine, and the process's outcome becomes the attempt's (rule
//! 2.3.21).

use std::fmt;
use std::future::{Future, ready};
use std::sync::Arc;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::handler::BoxFuture;
use crate::outcome::{IntoOutcome, Outcome, TaskError};
use crate::status::FinishReason;

/// A process a handler can return instead of an outcome (rule 2.3.21).
///
/// The contract has four parts:
///
/// - **start and wait** — [`run`](Self::run) starts the process and completes
///   with its own result;
/// - **soft stop** — `stop` is set when the task is cancelled: by a cancel
///   request, the attempt timeout or shutdown. The process should wind down
///   and finish; if it does not within the queue's cancel grace, its future
///   is dropped;
/// - **translate** — [`into_outcome`](Self::into_outcome) turns its result
///   into the task's outcome.
///
/// A panic inside `run` is the outcome "panic", as for a handler.
///
/// Any type may implement it; [`SpawnedMachine`] adapts a state machine
/// running in its own task.
pub trait Runnable: Send + 'static {
    /// What the process ends with.
    type Output: Send;

    /// Starts the process and waits for its result.
    fn run(self, stop: CancellationToken) -> impl Future<Output = Self::Output> + Send;

    /// The task's outcome for the process's result.
    fn into_outcome(output: Self::Output) -> Outcome;
}

/// A handler's answer: run this process and take its outcome.
///
/// ```
/// use taskcraft::{CancellationToken, Outcome, Run, Runnable, task_fn};
///
/// struct Export {
///     rows: u32,
/// }
///
/// impl Runnable for Export {
///     type Output = u32;
///
///     async fn run(self, stop: CancellationToken) -> u32 {
///         let mut done = 0;
///         while done < self.rows && !stop.is_cancelled() {
///             done += 1;
///         }
///         done
///     }
///
///     fn into_outcome(done: u32) -> Outcome {
///         if done > 0 { Outcome::Success } else { Outcome::retry("nothing exported") }
///     }
/// }
///
/// async fn export(rows: u32) -> Run<Export> {
///     Run(Export { rows })
/// }
///
/// let service = task_fn(export);
/// # let _ = service;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run<R>(pub R);

/// What a handler may return: an outcome now, or a process that gives one
/// later.
///
/// Implemented for every [`IntoOutcome`] type, for [`Run<R>`](Run) and for
/// `Result<Run<R>, TaskError>`.
#[diagnostic::on_unimplemented(
    message = "a handler cannot return `{Self}`",
    label = "not a handler outcome",
    note = "return (), Outcome, Result<(), TaskError>, Result<Outcome, TaskError>, Run<R> or Result<Run<R>, TaskError>; `?` turns any error into TaskError"
)]
pub trait HandlerOutput: Send + 'static {
    /// The outcome, running the process if there is one. `stop` is the
    /// attempt's cancel flag.
    fn finish(self, stop: CancellationToken) -> BoxFuture<'static, Outcome>;
}

impl<T: IntoOutcome + Send + 'static> HandlerOutput for T {
    fn finish(self, _: CancellationToken) -> BoxFuture<'static, Outcome> {
        Box::pin(ready(self.into_outcome()))
    }
}

impl<R: Runnable> HandlerOutput for Run<R> {
    fn finish(self, stop: CancellationToken) -> BoxFuture<'static, Outcome> {
        Box::pin(async move { R::into_outcome(self.0.run(stop).await) })
    }
}

impl<R: Runnable> HandlerOutput for Result<Run<R>, TaskError> {
    fn finish(self, stop: CancellationToken) -> BoxFuture<'static, Outcome> {
        match self {
            Ok(run) => run.finish(stop),
            Err(error) => Box::pin(ready(Outcome::from(error))),
        }
    }
}

/// Where a state machine leaves its outcome: put a clone into the machine's
/// context and [`set`](Self::set) it in the final states.
#[derive(Clone)]
pub struct OutcomeSlot(Arc<watch::Sender<Option<Outcome>>>);

impl OutcomeSlot {
    /// An empty slot.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(watch::Sender::new(None)))
    }

    /// Leaves the outcome; a later call replaces it.
    pub fn set(&self, outcome: Outcome) {
        self.0.send_replace(Some(outcome));
    }

    /// The outcome left so far.
    #[must_use]
    pub fn get(&self) -> Option<Outcome> {
        self.0.borrow().clone()
    }

    /// Completes with the outcome once one is left.
    pub async fn wait(&self) -> Outcome {
        let mut seen = self.0.subscribe();
        loop {
            if let Some(outcome) = seen.borrow_and_update().clone() {
                return outcome;
            }
            // The sender lives in `self`, so the channel never closes here.
            let _ = seen.changed().await;
        }
    }
}

impl Default for OutcomeSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for OutcomeSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutcomeSlot")
            .field("set", &self.0.borrow().is_some())
            .finish()
    }
}

type StopFn = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// A state machine running in its own tokio task — such as a statecraft-fsm
/// machine started with `spawn` — as a [`Runnable`].
///
/// - The machine leaves its outcome in an [`OutcomeSlot`] from its context;
///   that ends the run, and the machine's task with it. Its task ending
///   without an outcome is "abort".
/// - On the task's cancel flag the `stop` function runs once: send the
///   machine its stop event or call its graceful shutdown.
/// - A panic in the machine's task is "panic". Dropping the runnable — the
///   attempt aborted after the cancel grace — aborts the machine's task.
///
/// ```ignore
/// let slot = OutcomeSlot::new();
/// let (handle, join) = Export::spawn(ExportContext { slot: slot.clone(), ..ctx });
/// handle.send(ExportEvent::Start).await?;
/// let stopper = handle.clone();
/// Run(SpawnedMachine::new(join, slot, move || async move { stopper.shutdown() }))
/// ```
pub struct SpawnedMachine {
    join: JoinHandle<()>,
    slot: OutcomeSlot,
    stop: StopFn,
}

impl SpawnedMachine {
    /// The machine's task, its outcome slot, and how to ask it to stop.
    pub fn new<F, Fut>(join: JoinHandle<()>, slot: OutcomeSlot, stop: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self {
            join,
            slot,
            stop: Box::new(move || Box::pin(stop())),
        }
    }
}

impl fmt::Debug for SpawnedMachine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnedMachine")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

/// Aborts the machine's task unless it already finished.
struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The machine's task ended: a panic, or whatever outcome it left.
fn ended(joined: Result<(), tokio::task::JoinError>, slot: &OutcomeSlot) -> MachineEnd {
    match joined {
        Err(error) if error.is_panic() => {
            let payload = error.into_panic();
            let message = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic with a non-string payload".to_owned());
            MachineEnd::Panicked(message)
        }
        _ => MachineEnd::Finished(slot.get()),
    }
}

/// How a spawned machine ended.
#[derive(Debug)]
pub enum MachineEnd {
    /// Its task finished; the outcome it left, if any.
    Finished(Option<Outcome>),
    /// Its task panicked with this message.
    Panicked(String),
}

impl Runnable for SpawnedMachine {
    type Output = MachineEnd;

    async fn run(self, stop: CancellationToken) -> MachineEnd {
        let Self {
            mut join,
            slot,
            stop: stop_machine,
        } = self;
        // Ends the machine's task once its outcome is in, and on abort.
        let _guard = AbortOnDrop(join.abort_handle());
        tokio::select! {
            outcome = slot.wait() => return MachineEnd::Finished(Some(outcome)),
            joined = &mut join => return ended(joined, &slot),
            () = stop.cancelled() => stop_machine().await,
        }
        tokio::select! {
            outcome = slot.wait() => MachineEnd::Finished(Some(outcome)),
            joined = join => ended(joined, &slot),
        }
    }

    fn into_outcome(end: MachineEnd) -> Outcome {
        match end {
            MachineEnd::Finished(Some(outcome)) => outcome,
            MachineEnd::Finished(None) => Outcome::Abort {
                reason: FinishReason::Handler("machine stopped without an outcome".to_owned()),
            },
            MachineEnd::Panicked(message) => Outcome::Panic { message },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn machine(
        body: impl Future<Output = ()> + Send + 'static,
        slot: &OutcomeSlot,
    ) -> SpawnedMachine {
        SpawnedMachine::new(tokio::spawn(body), slot.clone(), || async {})
    }

    #[tokio::test]
    async fn outcome_comes_from_the_slot() {
        let slot = OutcomeSlot::new();
        let inner = slot.clone();
        let run = machine(async move { inner.set(Outcome::retry("later")) }, &slot);
        let outcome = Run(run).finish(CancellationToken::new()).await;
        assert!(matches!(outcome, Outcome::Retry { .. }));
    }

    #[tokio::test]
    async fn no_outcome_is_abort_and_panic_is_panic() {
        let slot = OutcomeSlot::new();
        let outcome = Run(machine(async {}, &slot))
            .finish(CancellationToken::new())
            .await;
        assert!(matches!(outcome, Outcome::Abort { .. }));

        let outcome = Run(machine(async { panic!("boom") }, &slot))
            .finish(CancellationToken::new())
            .await;
        assert_eq!(
            outcome,
            Outcome::Panic {
                message: "boom".to_owned()
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_flag_asks_the_machine_to_stop() {
        let slot = OutcomeSlot::new();
        let stopping = CancellationToken::new();
        let (inner, seen) = (slot.clone(), stopping.clone());
        let join = tokio::spawn(async move {
            seen.cancelled().await;
            inner.set(Outcome::Success);
        });
        let ask = stopping.clone();
        let run = SpawnedMachine::new(join, slot, move || async move { ask.cancel() });
        let stop = CancellationToken::new();
        let finished = tokio::spawn(Run(run).finish(stop.clone()));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!stopping.is_cancelled());
        stop.cancel();
        assert_eq!(finished.await.unwrap(), Outcome::Success);
        assert!(stopping.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_run_aborts_the_machine() {
        let slot = OutcomeSlot::new();
        let join = tokio::spawn(tokio::time::sleep(Duration::from_secs(3600)));
        let abort = join.abort_handle();
        let run = SpawnedMachine::new(join, slot, || async {});
        let attempt = tokio::spawn(Run(run).finish(CancellationToken::new()));
        tokio::time::sleep(Duration::from_secs(1)).await;
        attempt.abort();
        let _ = attempt.await;
        tokio::task::yield_now().await;
        assert!(abort.is_finished());
    }

    #[tokio::test]
    async fn result_of_run_carries_errors() {
        let failed: Result<Run<SpawnedMachine>, TaskError> = Err(TaskError::abort("bad input"));
        let outcome = failed.finish(CancellationToken::new()).await;
        assert!(matches!(outcome, Outcome::Abort { .. }));
    }
}
