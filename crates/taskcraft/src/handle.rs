//! The handle of a queue: push, status and cancel by id (spec 2.1.2.1,
//! 2.1.2.14, 2.1.2.15).

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use tracing::{info, warn};

use crate::codec::{Codec, CodecError};
use crate::observe::{Event, ObserverCell, observers_of};
use crate::outcome::BoxError;
use crate::registry::TaskRegistry;
use crate::source::{AckOverrideUnsupported, PushError, PushResult, Source};
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
    /// The source does not accept pushes; write to it directly.
    #[error("source does not accept pushes")]
    Unsupported,
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

/// A handle to a queue for pushing tasks and asking about them by id. Get it
/// with [`Queue::handle`](crate::Queue::handle) before registering the queue;
/// it is cheap to clone and works while the monitor runs.
pub struct QueueHandle<S: Source, C, Args> {
    name: Arc<str>,
    source: Arc<S>,
    codec: Arc<C>,
    tasks: Arc<TaskRegistry>,
    observers: ObserverCell,
    _args: PhantomData<fn(Args)>,
}

impl<S: Source, C, Args> QueueHandle<S, C, Args> {
    pub(crate) fn new(
        name: &str,
        source: Arc<S>,
        codec: Arc<C>,
        tasks: Arc<TaskRegistry>,
        observers: ObserverCell,
    ) -> Self {
        Self {
            name: name.into(),
            source,
            codec,
            tasks,
            observers,
            _args: PhantomData,
        }
    }

    /// The queue name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// How many tasks are live: accepted, running or waiting to retry.
    #[must_use]
    pub fn live_tasks(&self) -> usize {
        self.tasks.len()
    }

    /// The status of a live task: accepted, running or waiting to retry.
    /// `None` means unknown: not accepted yet, finished or deferred.
    #[must_use]
    pub fn status(&self, id: &TaskId) -> Option<TaskStatus> {
        self.tasks.status(id)
    }

    /// Cancels a task: at once when it waits, cooperatively when it runs
    /// (rule 2.3.15). Repeating the request changes nothing.
    pub async fn cancel(&self, id: &TaskId) -> CancelOutcome {
        let outcome = match self.tasks.cancel(id) {
            Some(TaskState::Running) => CancelOutcome::CancelRequested,
            Some(_) => CancelOutcome::Cancelled,
            None => match self.source.remove(id).await {
                Ok(true) => CancelOutcome::Cancelled,
                Ok(false) => CancelOutcome::Unknown,
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
        if outcome != CancelOutcome::Unknown {
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
}

impl<S: Source, C: Codec<Args, S::Message>, Args> QueueHandle<S, C, Args> {
    /// Pushes a task into the queue's source (spec 2.1.2.1). Idempotent by
    /// id: a live task with the same id is not touched.
    ///
    /// # Errors
    ///
    /// [`PushTaskError`] when the queue is stopping, the source does not
    /// accept pushes or failed, the task's ack point cannot be overridden, or
    /// the task cannot be encoded.
    pub async fn push(&self, task: Task<Args>) -> Result<PushOutcome, PushTaskError> {
        if self.tasks.is_closing() {
            return Err(PushTaskError::Stopping);
        }
        let caps = self.source.capabilities();
        if !caps.accepts_push() {
            return Err(PushTaskError::Unsupported);
        }
        caps.check_ack_override(task.ack_point())?;
        let id = task.id().clone();
        if let Some(state) = self.tasks.state(&id) {
            return Ok(PushOutcome::AlreadyRunning { id, state });
        }
        let message = self.codec.encode(task)?;
        match self.source.push(&id, message).await {
            Ok(PushResult::Stored) => {
                observers_of(&self.observers).emit(&Event::Pushed {
                    queue: &self.name,
                    task_id: &id,
                });
                Ok(PushOutcome::Enqueued { id })
            }
            Ok(PushResult::Duplicate) => {
                let state = self.tasks.state(&id).unwrap_or(TaskState::Queued);
                Ok(PushOutcome::AlreadyRunning { id, state })
            }
            Ok(PushResult::Full) => Ok(PushOutcome::Rejected {
                id,
                reason: RejectReason::SourceFull,
            }),
            Err(PushError::Unsupported) => Err(PushTaskError::Unsupported),
            Err(PushError::Closed) => Err(PushTaskError::Stopping),
            Err(PushError::Source(e)) => Err(PushTaskError::Source(Box::new(e))),
        }
    }
}

impl<S: Source, C, Args> Clone for QueueHandle<S, C, Args> {
    fn clone(&self) -> Self {
        Self {
            name: Arc::clone(&self.name),
            source: Arc::clone(&self.source),
            codec: Arc::clone(&self.codec),
            tasks: Arc::clone(&self.tasks),
            observers: Arc::clone(&self.observers),
            _args: PhantomData,
        }
    }
}

impl<S: Source, C, Args> fmt::Debug for QueueHandle<S, C, Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueHandle")
            .field("name", &self.name)
            .field("live_tasks", &self.tasks.len())
            .finish_non_exhaustive()
    }
}
