//! The source contract: where a queue takes tasks from and acknowledges them
//! to (spec 2.3.1, 2.3.9, 2.3.10, 2.5).
//!
//! Three traits, by what a source can do:
//!
//! - [`Source`] — a stream to consume, such as a Kafka topic: poll and ack.
//!   A queue on it is built with [`Queue::consumer`](crate::Queue::consumer);
//!   its handle cannot push.
//! - [`PushSource`] — a source tasks are pushed into from code, such as the
//!   [`InMemorySource`](crate::InMemorySource):
//!   [`Queue::builder`](crate::Queue::builder).
//! - [`TaskStore`] — a durable store that records every task's state, such
//!   as the PostgreSQL store: [`Queue::on_store`](crate::Queue::on_store),
//!   with the built-in JSON codec.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

pub use crate::memory::Delivery;
pub use crate::offset::OffsetTracker;
pub use crate::poll::{Poller, Wakeup};
use crate::state::TaskState;
use crate::status::{FinishReason, TaskStatus};
use crate::task::{AckPoint, TaskId};

/// The answer to one poll of a source.
///
/// "Empty for now" and "closed" are separate answers: an empty source never
/// ends a worker, only [`Closed`](Self::Closed) does.
#[derive(Debug)]
pub enum Polled<M, R> {
    /// A message to turn into a task, with the receipt to acknowledge it by.
    Task {
        /// The raw message; the queue's codec decodes it.
        message: M,
        /// What the source needs to recognise this delivery on ack or defer.
        receipt: R,
    },
    /// Nothing to hand out right now. Poll again later.
    Empty,
    /// The source is closed for good and will not be polled again.
    Closed(CloseReason),
}

/// Why a source closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseReason(String);

impl CloseReason {
    /// A reason with this description.
    pub fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }

    /// The description.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// How a source treats the ack point (spec 2.3.9, 2.3.10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AckPointSupport {
    /// Set per queue and overridable per task. Sources with per-task ack and
    /// no durable accept, such as the in-memory source.
    PerTask,
    /// Set per queue only. Log sources: acking one task commits every earlier
    /// offset of its partition.
    QueueOnly,
    /// Not configurable: the source records accept durably and acks on every
    /// final transition. Task stores.
    Fixed,
}

/// What a source can do, beyond polling and acking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Capabilities {
    ack_point: AckPointSupport,
    defer: bool,
    transactional_ack: bool,
}

impl Capabilities {
    /// A source that only polls and acks, with the given ack point support.
    #[must_use]
    pub const fn new(ack_point: AckPointSupport) -> Self {
        Self {
            ack_point,
            defer: false,
            transactional_ack: false,
        }
    }

    /// The source can take a task back with a delivery delay: it implements
    /// [`PushSource::defer`].
    #[must_use]
    pub const fn with_defer(mut self) -> Self {
        self.defer = true;
        self
    }

    /// The source can commit a result and its ack in one transaction. No
    /// source implements this in the first version (spec 4.3).
    #[must_use]
    pub const fn with_transactional_ack(mut self) -> Self {
        self.transactional_ack = true;
        self
    }

    /// How the ack point is configured.
    #[must_use]
    pub const fn ack_point_support(&self) -> AckPointSupport {
        self.ack_point
    }

    /// Whether the source supports deferred redelivery.
    #[must_use]
    pub const fn supports_defer(&self) -> bool {
        self.defer
    }

    /// Whether the source supports transactional ack.
    #[must_use]
    pub const fn transactional_ack(&self) -> bool {
        self.transactional_ack
    }

    /// Checks a task's own ack point against the source (spec 2.3.10).
    ///
    /// # Errors
    ///
    /// [`AckOverrideUnsupported`] when the task overrides the ack point and the
    /// source does not support [`AckPointSupport::PerTask`].
    pub fn check_ack_override(
        &self,
        task_ack_point: Option<AckPoint>,
    ) -> Result<(), AckOverrideUnsupported> {
        match (task_ack_point, self.ack_point) {
            (None, _) | (Some(_), AckPointSupport::PerTask) => Ok(()),
            (Some(_), AckPointSupport::QueueOnly | AckPointSupport::Fixed) => {
                Err(AckOverrideUnsupported)
            }
        }
    }
}

/// A task overrides the ack point, and its source does not allow it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("ack point override is not supported by this source")]
pub struct AckOverrideUnsupported;

/// What a source did with a pushed task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum PushResult {
    /// Stored; it will be handed out by a poll.
    Stored,
    /// The source already holds a task with this id; nothing was stored.
    Duplicate,
    /// The source is at capacity; nothing was stored.
    Full,
    /// A task store still remembers a finished task with this id; nothing
    /// was stored (rule 2.3.11 p. 4).
    Finished(TaskState),
}

/// What a source did with a request to take a task back (spec 2.1.2.15).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Withdrawal {
    /// The task was stored and not handed out; it is removed.
    Removed,
    /// Another process holds the task; the request is recorded for it.
    CancelRequested,
    /// A task store remembers the task as finished.
    Finished,
    /// The source holds no such task, or cannot take tasks back.
    NotFound,
}

/// What a task store did with a request to run a failed task again (rule
/// 2.3.26).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[must_use]
pub enum Requeue {
    /// The failed or panicked task is queued again.
    Requeued,
    /// The task is not failed or panicked: it is live, succeeded or
    /// cancelled.
    NotFailed(TaskState),
    /// The store holds no such task.
    NotFound,
}

/// How a task ended, for a source that records it (spec 2.3.9 p. 6).
#[derive(Debug, Clone, Copy)]
pub struct Completion<'a> {
    /// Succeeded, failed, panicked or cancelled.
    pub state: TaskState,
    /// Why, for failed, panicked and cancelled tasks.
    pub reason: Option<&'a FinishReason>,
}

/// Where a task stands, for a source that records it.
#[derive(Debug, Clone, Copy)]
pub struct Progress {
    /// Accepted, running or waiting to retry.
    pub state: TaskState,
    /// The attempt number.
    pub attempt: u32,
    /// The retries so far.
    pub retries: u32,
}

/// What a source tells the worker about tasks this process holds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Notice {
    /// Another process asked to cancel the task (spec 2.1.2.15 p. 5).
    CancelRequested(TaskId),
    /// Another process took the task over: stop it without writing anything
    /// (rule 2.3.20 p. 5).
    LeaseLost(TaskId),
    /// This process took over a task whose lease had expired (spec
    /// 2.1.2.18).
    TakenOver {
        /// The task.
        task_id: TaskId,
        /// Its previous owner.
        previous_owner: String,
    },
    /// A background operation of the source failed, such as an offset
    /// commit. The source logs the details; the worker counts it as a
    /// source error and does not restart its intake.
    SourceError(String),
}

/// The receiving end of a source's notices; one worker takes it.
#[derive(Debug)]
pub struct Notices(mpsc::UnboundedReceiver<Notice>);

impl Notices {
    /// A channel: the sender stays with the source.
    #[must_use]
    pub fn channel() -> (mpsc::UnboundedSender<Notice>, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (sender, Self(receiver))
    }

    /// The next notice; `None` once the source is gone.
    pub async fn recv(&mut self) -> Option<Notice> {
        self.0.recv().await
    }
}

/// A push the source could not perform.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PushError<E> {
    /// The source cannot hold a task back until a moment of delivery.
    #[error("source does not support delayed push")]
    DelayUnsupported,
    /// The source is closed.
    #[error("source is closed")]
    Closed,
    /// The source failed.
    #[error(transparent)]
    Source(E),
}

/// A defer the source could not perform.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DeferError<E> {
    /// The source does not support deferred redelivery.
    #[error("source does not support defer")]
    Unsupported,
    /// The source failed.
    #[error(transparent)]
    Source(E),
}

/// A wake-up signal from a source: something was pushed or the source closed.
///
/// Call [`mark_seen`](Self::mark_seen) before polling and
/// [`changed`](Self::changed) after an empty answer: a push in between is not
/// lost.
#[derive(Debug, Clone)]
pub struct WakeSignal(watch::Receiver<u64>);

impl WakeSignal {
    /// Remembers the current state of the signal.
    pub fn mark_seen(&mut self) {
        self.0.borrow_and_update();
    }

    /// Completes once the signal changed since the last
    /// [`mark_seen`](Self::mark_seen), immediately if it already has.
    /// Returns `false` when the source is gone.
    pub async fn changed(&mut self) -> bool {
        self.0.changed().await.is_ok()
    }
}

/// The sending side of a [`WakeSignal`], for source implementations.
#[derive(Debug)]
pub struct WakeHandle(watch::Sender<u64>);

impl WakeHandle {
    /// A new waker with no subscribers yet.
    #[must_use]
    pub fn new() -> Self {
        Self(watch::Sender::new(0))
    }

    /// Wakes every subscriber.
    pub fn wake(&self) {
        self.0.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// A new subscription.
    #[must_use]
    pub fn subscribe(&self) -> WakeSignal {
        WakeSignal(self.0.subscribe())
    }
}

impl Default for WakeHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// The message of a [`TaskStore`]: a task in the JSON envelope of
/// [`JsonCodec`](crate::codec::JsonCodec), as bytes.
///
/// A store keeps these bytes as they are; only the built-in codec of
/// [`Queue::on_store`](crate::Queue::on_store) reads and writes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreMessage(Vec<u8>);

impl StoreMessage {
    /// A message of these bytes, as the store read them.
    #[must_use]
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The bytes, to store.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// The bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A source of tasks: a stream the queue polls and acknowledges to.
///
/// Methods take `&self`: acks arrive from tasks finishing concurrently with
/// polling, so a source synchronises itself and is shared behind an `Arc`.
pub trait Source: Send + Sync + 'static {
    /// The raw message the queue's codec turns into a task.
    type Message: Send + 'static;
    /// What identifies one delivery on ack and defer.
    type Receipt: Clone + Send + 'static;
    /// A source failure such as a lost connection. Transient: the worker
    /// restarts its loop after it.
    type Error: std::error::Error + Send + Sync + 'static;

    /// What the source can do.
    fn capabilities(&self) -> Capabilities;

    /// Asks for the next message.
    fn poll(
        &self,
    ) -> impl Future<Output = Result<Polled<Self::Message, Self::Receipt>, Self::Error>> + Send;

    /// Acknowledges a delivery. For a log source this marks the offset done;
    /// the commit itself follows the boundary rule (see `OffsetTracker`).
    fn ack(&self, receipt: Self::Receipt) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// A wake-up signal, if the source can tell when work arrives. Without one
    /// the worker relies on its poll strategy.
    fn subscribe(&self) -> Option<WakeSignal> {
        None
    }

    /// Notices about tasks this process holds and background failures of
    /// the source. Taken once, by the queue's worker. The default has none.
    fn notices(&self) -> Option<Notices> {
        None
    }
}

/// A source that tasks are pushed into from code; a queue on it is built
/// with [`Queue::builder`](crate::Queue::builder) and its handle pushes.
pub trait PushSource: Source {
    /// Stores a pushed task, checking its id.
    fn push(
        &self,
        id: &TaskId,
        message: Self::Message,
    ) -> impl Future<Output = Result<PushResult, PushError<Self::Error>>> + Send;

    /// Takes back a task that is stored but not handed out yet — ready or
    /// deferred — and frees its id.
    fn remove(&self, id: &TaskId) -> impl Future<Output = Result<Withdrawal, Self::Error>> + Send;

    /// Stores a pushed task, checking its id, to be handed out not before
    /// `at` (spec 2.1.2.1). A source with delayed delivery declares
    /// [`Capabilities::with_defer`]; the default does not support it.
    fn push_at(
        &self,
        id: &TaskId,
        message: Self::Message,
        at: SystemTime,
    ) -> impl Future<Output = Result<PushResult, PushError<Self::Error>>> + Send {
        let _ = (id, message, at);
        async { Err(PushError::DelayUnsupported) }
    }

    /// Takes a delivered task back, to be handed out again at `at`. Declare
    /// it with [`Capabilities::with_defer`]; the default does not support it.
    fn defer(
        &self,
        receipt: Self::Receipt,
        message: Self::Message,
        at: Instant,
    ) -> impl Future<Output = Result<(), DeferError<Self::Error>>> + Send {
        let _ = (receipt, message, at);
        async { Err(DeferError::Unsupported) }
    }
}

/// A durable task store: it records accept, progress and every final state,
/// keeps finished tasks for status requests and hands tasks out under leases
/// (rule 2.3.9 p. 6). A queue on it is built with
/// [`Queue::on_store`](crate::Queue::on_store).
///
/// Not a [`Source`]: a store's outcome is recorded by
/// [`complete`](Self::complete), never by a plain ack, so it cannot be
/// consumed as a stream by mistake. Its ack point is fixed
/// ([`AckPointSupport::Fixed`]); push and defer are always supported.
pub trait TaskStore: Send + Sync + 'static {
    /// What identifies one delivery.
    type Receipt: Clone + Send + 'static;
    /// A store failure such as a lost connection.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Asks for the next task, taking it under this process's lease.
    fn poll(
        &self,
    ) -> impl Future<Output = Result<Polled<StoreMessage, Self::Receipt>, Self::Error>> + Send;

    /// Stores a pushed task, checking its id against live and finished
    /// tasks.
    fn push(
        &self,
        id: &TaskId,
        message: StoreMessage,
    ) -> impl Future<Output = Result<PushResult, PushError<Self::Error>>> + Send;

    /// Stores a pushed task to be handed out not before `at` (spec
    /// 2.1.2.1). The default does not support it.
    fn push_at(
        &self,
        id: &TaskId,
        message: StoreMessage,
        at: SystemTime,
    ) -> impl Future<Output = Result<PushResult, PushError<Self::Error>>> + Send {
        let _ = (id, message, at);
        async { Err(PushError::DelayUnsupported) }
    }

    /// Takes back a task that is stored but not handed out yet, or records
    /// a request to cancel a task another process holds.
    fn remove(&self, id: &TaskId) -> impl Future<Output = Result<Withdrawal, Self::Error>> + Send;

    /// Takes a delivered task back, to be handed out again at `at`.
    fn defer(
        &self,
        receipt: Self::Receipt,
        message: StoreMessage,
        at: Instant,
    ) -> impl Future<Output = Result<(), DeferError<Self::Error>>> + Send;

    /// Records how a task ended.
    fn complete(
        &self,
        receipt: Self::Receipt,
        completion: Completion<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Records where a task stands: accepted, running, waiting to retry.
    fn progress(
        &self,
        receipt: Self::Receipt,
        progress: Progress,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// The status of a task, including finished ones.
    fn status(
        &self,
        id: &TaskId,
    ) -> impl Future<Output = Result<Option<TaskStatus>, Self::Error>> + Send;

    /// Queues a failed or panicked task again (rule 2.3.26): its retries
    /// start over, its attempt number goes on. The default holds no such
    /// task.
    fn requeue(&self, id: &TaskId) -> impl Future<Output = Result<Requeue, Self::Error>> + Send {
        let _ = id;
        async { Ok(Requeue::NotFound) }
    }

    /// A wake-up signal, if the store can tell when work arrives.
    fn subscribe(&self) -> Option<WakeSignal> {
        None
    }

    /// Notices about tasks this process holds: cancel requests, lost and
    /// taken-over leases. Taken once, by the queue's worker.
    fn notices(&self) -> Option<Notices> {
        None
    }
}

/// A [`Source`] as a queue consumes it; the type of a queue built with
/// [`Queue::consumer`](crate::Queue::consumer).
#[derive(Debug)]
pub struct Consumed<S>(pub(crate) Arc<S>);

/// A [`PushSource`] as a queue uses it; the type of a queue built with
/// [`Queue::builder`](crate::Queue::builder).
#[derive(Debug)]
pub struct Pushed<S>(pub(crate) Arc<S>);

/// A [`TaskStore`] as a queue uses it; the type of a queue built with
/// [`Queue::on_store`](crate::Queue::on_store).
#[derive(Debug)]
pub struct Stored<S>(pub(crate) Arc<S>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_override_follows_the_source() {
        let any = [None, Some(AckPoint::OnAccept), Some(AckPoint::OnCompletion)];
        let per_task = Capabilities::new(AckPointSupport::PerTask);
        for point in any {
            assert_eq!(per_task.check_ack_override(point), Ok(()));
        }
        for support in [AckPointSupport::QueueOnly, AckPointSupport::Fixed] {
            let caps = Capabilities::new(support);
            assert_eq!(caps.check_ack_override(None), Ok(()));
            assert_eq!(
                caps.check_ack_override(Some(AckPoint::OnCompletion)),
                Err(AckOverrideUnsupported)
            );
        }
        assert_eq!(
            AckOverrideUnsupported.to_string(),
            "ack point override is not supported by this source"
        );
    }

    #[test]
    fn capability_builders() {
        let caps = Capabilities::new(AckPointSupport::PerTask).with_defer();
        assert!(caps.supports_defer() && !caps.transactional_ack());
    }

    #[tokio::test]
    async fn wake_between_poll_and_wait_is_not_lost() {
        let waker = WakeHandle::new();
        let mut signal = waker.subscribe();
        signal.mark_seen();
        // A push lands after the poll answered "empty" but before waiting.
        waker.wake();
        assert!(signal.changed().await);
    }

    #[tokio::test]
    async fn changed_reports_a_gone_source() {
        let waker = WakeHandle::new();
        let mut signal = waker.subscribe();
        signal.mark_seen();
        drop(waker);
        assert!(!signal.changed().await);
    }
}
