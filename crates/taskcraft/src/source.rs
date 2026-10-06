//! The source contract: where a queue takes tasks from and acknowledges them
//! to (spec 2.3.1, 2.3.9, 2.3.10, 2.5).

use std::fmt;
use std::future::Future;

use tokio::sync::watch;
use tokio::time::Instant;

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
    push: bool,
    defer: bool,
    transactional_ack: bool,
}

impl Capabilities {
    /// A source that only polls and acks, with the given ack point support.
    #[must_use]
    pub const fn new(ack_point: AckPointSupport) -> Self {
        Self {
            ack_point,
            push: false,
            defer: false,
            transactional_ack: false,
        }
    }

    /// The source accepts tasks pushed from code.
    #[must_use]
    pub const fn with_push(mut self) -> Self {
        self.push = true;
        self
    }

    /// The source can take a task back with a delivery delay.
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

    /// Whether tasks can be pushed from code.
    #[must_use]
    pub const fn accepts_push(&self) -> bool {
        self.push
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
}

/// A push the source could not perform.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PushError<E> {
    /// The source does not accept pushes.
    #[error("source does not accept pushes")]
    Unsupported,
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

/// A source of tasks.
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

    /// Stores a pushed task, checking its id. Only for sources that
    /// [accept pushes](Capabilities::accepts_push).
    fn push(
        &self,
        id: &TaskId,
        message: Self::Message,
    ) -> impl Future<Output = Result<PushResult, PushError<Self::Error>>> + Send {
        let _ = (id, message);
        async { Err(PushError::Unsupported) }
    }

    /// Takes a delivered task back, to be handed out again at `at`. Only for
    /// sources that [support defer](Capabilities::supports_defer).
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
        let caps = Capabilities::new(AckPointSupport::PerTask)
            .with_push()
            .with_defer();
        assert!(caps.accepts_push() && caps.supports_defer() && !caps.transactional_ack());
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
