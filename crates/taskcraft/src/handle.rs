//! The handles of a queue: push, status and cancel by id (spec 2.1.2.1,
//! 2.1.2.14, 2.1.2.15).

use std::fmt;
use std::sync::Arc;

use tracing::{info, warn};

use crate::backend::Backend;
use crate::codec::{Codec, CodecError};
use crate::handler::BoxFuture;
use crate::observe::{Event, ObserverCell, observers_of};
use crate::outcome::BoxError;
use crate::registry::TaskRegistry;
use crate::source::{
    AckOverrideUnsupported, AckPointSupport, Capabilities, PushError, PushResult, Withdrawal,
};
use crate::state::TaskState;
use crate::status::{PushOutcome, RejectReason, TaskStatus};
use crate::task::{Task, TaskId};

/// A push that could not be performed (spec 2.11.2).
///
/// A busy id and a full source are not errors but [`PushOutcome`]s.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PushTaskError {
    /// The queue got the shutdown signal (rule 2.3.14 p. 3).
    #[error("queue is stopping")]
    Stopping,
    /// The task sets its own ack point and the source does not allow it.
    #[error(transparent)]
    AckOverride(#[from] AckOverrideUnsupported),
    /// The task could not be encoded, for example metadata of an
    /// unregistered type (scenario 2.2.10).
    #[error(transparent)]
    Encode(#[from] CodecError),
    /// The source failed to store the task; try again later.
    #[error("source failed: {0}")]
    Source(BoxError),
}

/// The answer to a cancel request (spec 2.1.2.15).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[must_use]
pub enum CancelOutcome {
    /// The task is running: its cancel flag is set, and it ends cancelled
    /// on its own or when the cancel grace runs out.
    CancelRequested,
    /// The task was waiting — in the source, for a slot or to retry — and is
    /// cancelled.
    Cancelled,
    /// The task already finished; only a task store remembers that.
    AlreadyFinished,
    /// No such task.
    Unknown,
}

/// What a handle asks of its queue's source and codec, with their types
/// erased.
pub(crate) trait Port<Args>: Send + Sync {
    fn capabilities(&self) -> Capabilities;

    fn status<'a>(&'a self, id: &'a TaskId) -> BoxFuture<'a, Result<Option<TaskStatus>, BoxError>>;

    fn remove<'a>(&'a self, id: &'a TaskId) -> BoxFuture<'a, Result<Withdrawal, BoxError>>;

    /// Encodes the task and stores it.
    fn push<'a>(
        &'a self,
        id: &'a TaskId,
        task: Task<Args>,
    ) -> BoxFuture<'a, Result<PushResult, PushTaskError>>;
}

/// A queue's source and codec behind a [`Port`].
pub(crate) struct Ports<B, C> {
    pub(crate) backend: Arc<B>,
    pub(crate) codec: Arc<C>,
}

impl<B, C, Args> Port<Args> for Ports<B, C>
where
    B: Backend,
    C: Codec<Args, B::Message>,
    Args: Send + 'static,
{
    fn capabilities(&self) -> Capabilities {
        self.backend.capabilities()
    }

    fn status<'a>(&'a self, id: &'a TaskId) -> BoxFuture<'a, Result<Option<TaskStatus>, BoxError>> {
        Box::pin(async move { self.backend.status(id).await.map_err(Into::into) })
    }

    fn remove<'a>(&'a self, id: &'a TaskId) -> BoxFuture<'a, Result<Withdrawal, BoxError>> {
        Box::pin(async move { self.backend.remove(id).await.map_err(Into::into) })
    }

    fn push<'a>(
        &'a self,
        id: &'a TaskId,
        task: Task<Args>,
    ) -> BoxFuture<'a, Result<PushResult, PushTaskError>> {
        let message = self.codec.encode(task);
        Box::pin(async move {
            match self.backend.push(id, message?).await {
                Ok(result) => Ok(result),
                Err(PushError::Closed) => Err(PushTaskError::Stopping),
                Err(PushError::Source(e)) => Err(PushTaskError::Source(Box::new(e))),
            }
        })
    }
}

/// What both handles share: the queue's name, its live tasks, its source
/// and codec. Not nameable outside the crate.
#[doc(hidden)]
pub struct HandleCore<Args> {
    name: Arc<str>,
    port: Arc<dyn Port<Args>>,
    tasks: Arc<TaskRegistry>,
    observers: ObserverCell,
}

impl<Args> HandleCore<Args> {
    pub(crate) fn new(
        name: &str,
        port: Arc<dyn Port<Args>>,
        tasks: Arc<TaskRegistry>,
        observers: ObserverCell,
    ) -> Self {
        Self {
            name: name.into(),
            port,
            tasks,
            observers,
        }
    }

    async fn fetch_status(&self, id: &TaskId) -> Result<Option<TaskStatus>, BoxError> {
        if let Some(status) = self.tasks.status(id) {
            return Ok(Some(status));
        }
        self.port.status(id).await
    }

    async fn cancel(&self, id: &TaskId) -> CancelOutcome {
        let outcome = match self.tasks.cancel(id) {
            Some(TaskState::Running) => CancelOutcome::CancelRequested,
            Some(_) => CancelOutcome::Cancelled,
            None => match self.port.remove(id).await {
                Ok(Withdrawal::Removed) => CancelOutcome::Cancelled,
                Ok(Withdrawal::CancelRequested) => CancelOutcome::CancelRequested,
                Ok(Withdrawal::Finished) => CancelOutcome::AlreadyFinished,
                Ok(Withdrawal::NotFound) => CancelOutcome::Unknown,
                Err(e) => {
                    warn!(
                        event = "source",
                        action = "remove_failed",
                        "source could not remove task: queue={}, task_id={}, error={:?}",
                        self.name,
                        id,
                        e.to_string()
                    );
                    CancelOutcome::Unknown
                }
            },
        };
        if matches!(
            outcome,
            CancelOutcome::CancelRequested | CancelOutcome::Cancelled
        ) {
            info!(
                event = "task",
                action = "cancel_requested",
                "task cancel requested: queue={}, task_id={}",
                self.name,
                id
            );
        }
        outcome
    }

    async fn push(&self, task: Task<Args>) -> Result<PushOutcome, PushTaskError> {
        if self.tasks.is_closing() {
            return Err(PushTaskError::Stopping);
        }
        let caps = self.port.capabilities();
        caps.check_ack_override(task.ack_point())?;
        let id = task.id().clone();
        if let Some(state) = self.tasks.state(&id) {
            return Ok(PushOutcome::AlreadyRunning { id, state });
        }
        match self.port.push(&id, task).await? {
            PushResult::Stored => {
                // The worker may have stopped intake during the write: a
                // source that does not outlive the process would keep the
                // task where nobody polls it (spec 2.1.2.1 step 1). A task
                // store keeps it for the next run.
                let durable = caps.ack_point_support() == AckPointSupport::Fixed;
                if self.tasks.is_closing()
                    && !durable
                    && matches!(self.port.remove(&id).await, Ok(Withdrawal::Removed))
                {
                    return Err(PushTaskError::Stopping);
                }
                observers_of(&self.observers).emit(&Event::Pushed {
                    queue: &self.name,
                    task_id: &id,
                });
                Ok(PushOutcome::Enqueued { id })
            }
            PushResult::Duplicate => {
                let state = self.tasks.state(&id).unwrap_or(TaskState::Queued);
                Ok(PushOutcome::AlreadyRunning { id, state })
            }
            PushResult::Full => Ok(PushOutcome::Rejected {
                id,
                reason: RejectReason::SourceFull,
            }),
            PushResult::Finished(state) => Ok(PushOutcome::AlreadyFinished { id, state }),
        }
    }
}

impl<Args> fmt::Debug for HandleCore<Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandleCore")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<Args> Clone for HandleCore<Args> {
    fn clone(&self) -> Self {
        Self {
            name: Arc::clone(&self.name),
            port: Arc::clone(&self.port),
            tasks: Arc::clone(&self.tasks),
            observers: Arc::clone(&self.observers),
        }
    }
}

/// The methods both handles have.
macro_rules! common_methods {
    () => {
        /// The queue name.
        #[must_use]
        pub fn name(&self) -> &str {
            &self.core.name
        }

        /// How many tasks are live: accepted, running or waiting to retry.
        #[must_use]
        pub fn live_tasks(&self) -> usize {
            self.core.tasks.len()
        }

        /// The status of a live task: accepted, running or waiting to
        /// retry. `None` means unknown: not accepted yet, finished or
        /// deferred.
        #[must_use]
        pub fn status(&self, id: &TaskId) -> Option<TaskStatus> {
            self.core.tasks.status(id)
        }

        /// The status of a task wherever it is: this process's live tasks
        /// first, then the source — a task store also knows tasks of other
        /// processes and finished ones (spec 2.1.2.14).
        ///
        /// # Errors
        ///
        /// The source failed, for example the database is unreachable
        /// (spec 2.11.3).
        pub async fn fetch_status(&self, id: &TaskId) -> Result<Option<TaskStatus>, BoxError> {
            self.core.fetch_status(id).await
        }

        /// Cancels a task: at once when it waits, cooperatively when it runs
        /// (rule 2.3.15). Repeating the request changes nothing.
        pub async fn cancel(&self, id: &TaskId) -> CancelOutcome {
            self.core.cancel(id).await
        }
    };
}

/// The handle of a queue that tasks are pushed into: push, status and
/// cancel by id. [`Monitor::register`](crate::Monitor::register) returns it
/// for queues built with [`Queue::builder`](crate::Queue::builder) and
/// [`Queue::on_store`](crate::Queue::on_store); it is cheap to clone.
pub struct QueueHandle<Args> {
    core: HandleCore<Args>,
}

impl<Args> QueueHandle<Args> {
    pub(crate) fn new(core: HandleCore<Args>) -> Self {
        Self { core }
    }

    common_methods!();

    /// Pushes a task into the queue's source (spec 2.1.2.1). Idempotent by
    /// id: a live task with the same id is not touched.
    ///
    /// # Errors
    ///
    /// [`PushTaskError`] when the queue is stopping, the source failed, the
    /// task's ack point cannot be overridden, or the task cannot be
    /// encoded.
    pub async fn push(&self, task: Task<Args>) -> Result<PushOutcome, PushTaskError> {
        self.core.push(task).await
    }
}

/// The handle of a queue that consumes a stream, such as a Kafka topic:
/// status and cancel by id, no push — tasks come from the stream.
/// [`Monitor::register`](crate::Monitor::register) returns it for queues
/// built with [`Queue::consumer`](crate::Queue::consumer).
///
/// ```compile_fail,E0599
/// # async fn f(handle: taskcraft::ConsumerHandle<String>) {
/// // A consumed stream takes no pushes: this does not compile.
/// let _ = handle.push(taskcraft::Task::new("x".to_owned())).await;
/// # }
/// ```
pub struct ConsumerHandle<Args> {
    core: HandleCore<Args>,
}

impl<Args> ConsumerHandle<Args> {
    pub(crate) fn new(core: HandleCore<Args>) -> Self {
        Self { core }
    }

    common_methods!();
}

impl<Args> Clone for QueueHandle<Args> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}

impl<Args> Clone for ConsumerHandle<Args> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}

impl<Args> fmt::Debug for QueueHandle<Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueHandle")
            .field("name", &self.core.name)
            .field("live_tasks", &self.core.tasks.len())
            .finish_non_exhaustive()
    }
}

impl<Args> fmt::Debug for ConsumerHandle<Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerHandle")
            .field("name", &self.core.name)
            .field("live_tasks", &self.core.tasks.len())
            .finish_non_exhaustive()
    }
}
