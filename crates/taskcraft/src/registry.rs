//! The registry of a queue's live tasks: id check on accept, status and
//! cancellation (rule 2.3.11, invariant 1.3.9).

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use tokio_util::sync::CancellationToken;

use crate::state::TaskState;
use crate::status::{FinishReason, TaskStatus};
use crate::task::TaskId;

/// One live task.
#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) state: TaskState,
    pub(crate) attempt: u32,
    pub(crate) retries: u32,
    pub(crate) accepted_at: SystemTime,
    pub(crate) attempt_started_at: Option<SystemTime>,
    pub(crate) next_attempt_at: Option<SystemTime>,
    cancel: CancellationToken,
    cancel_reason: Option<FinishReason>,
}

#[derive(Debug, Default)]
struct Inner {
    tasks: HashMap<TaskId, Entry>,
    closing: bool,
}

/// Live tasks of one queue, from accept to their final state or defer. Only
/// they take memory: finished tasks are removed (invariant 1.3.9).
#[derive(Debug, Default)]
pub(crate) struct TaskRegistry {
    inner: Mutex<Inner>,
}

impl TaskRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Every critical section is a few infallible map calls.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records an accepted task; checking and recording are one step (rule
    /// 2.3.11 p. 1). A busy id answers with the state of the live task.
    pub(crate) fn try_accept(
        &self,
        id: &TaskId,
        at: SystemTime,
        cancel: CancellationToken,
    ) -> Result<(), TaskState> {
        let mut inner = self.lock();
        if let Some(entry) = inner.tasks.get(id) {
            return Err(entry.state);
        }
        inner.tasks.insert(
            id.clone(),
            Entry {
                state: TaskState::Accepted,
                attempt: 0,
                retries: 0,
                accepted_at: at,
                attempt_started_at: None,
                next_attempt_at: None,
                cancel,
                cancel_reason: None,
            },
        );
        Ok(())
    }

    pub(crate) fn update(&self, id: &TaskId, f: impl FnOnce(&mut Entry)) {
        if let Some(entry) = self.lock().tasks.get_mut(id) {
            f(entry);
        }
    }

    pub(crate) fn remove(&self, id: &TaskId) {
        self.lock().tasks.remove(id);
    }

    pub(crate) fn state(&self, id: &TaskId) -> Option<TaskState> {
        self.lock().tasks.get(id).map(|e| e.state)
    }

    pub(crate) fn status(&self, id: &TaskId) -> Option<TaskStatus> {
        self.lock().tasks.get(id).map(|e| {
            TaskStatus::new(id.clone(), e.state, e.attempt, e.retries).with_times(
                Some(e.accepted_at),
                e.attempt_started_at,
                e.next_attempt_at,
            )
        })
    }

    /// Sets the task's cancel flag with "cancelled by request" (rule 2.3.15)
    /// and returns its state; `None` when the task is not live.
    pub(crate) fn cancel(&self, id: &TaskId) -> Option<TaskState> {
        let mut inner = self.lock();
        let entry = inner.tasks.get_mut(id)?;
        entry
            .cancel_reason
            .get_or_insert(FinishReason::CancelledByUser);
        entry.cancel.cancel();
        Some(entry.state)
    }

    /// Why the task was cancelled; without a recorded reason its flag was
    /// set by shutdown.
    pub(crate) fn cancel_reason(&self, id: &TaskId) -> FinishReason {
        self.lock()
            .tasks
            .get(id)
            .and_then(|e| e.cancel_reason.clone())
            .unwrap_or(FinishReason::CancelledByShutdown)
    }

    /// The queue is stopping: pushes are refused from now on.
    pub(crate) fn set_closing(&self) {
        self.lock().closing = true;
    }

    pub(crate) fn is_closing(&self) -> bool {
        self.lock().closing
    }

    pub(crate) fn len(&self) -> usize {
        self.lock().tasks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accept(registry: &TaskRegistry, id: &str) -> Result<(), TaskState> {
        registry.try_accept(
            &TaskId::new(id),
            SystemTime::UNIX_EPOCH,
            CancellationToken::new(),
        )
    }

    #[test]
    fn a_busy_id_answers_with_its_state() {
        let registry = TaskRegistry::new();
        assert_eq!(accept(&registry, "a"), Ok(()));
        registry.update(&TaskId::new("a"), |e| e.state = TaskState::Running);
        assert_eq!(accept(&registry, "a"), Err(TaskState::Running));
        registry.remove(&TaskId::new("a"));
        assert_eq!(registry.len(), 0);
        assert_eq!(accept(&registry, "a"), Ok(()));
    }

    #[test]
    fn cancel_sets_the_flag_and_the_reason() {
        let registry = TaskRegistry::new();
        let id = TaskId::new("a");
        let token = CancellationToken::new();
        registry
            .try_accept(&id, SystemTime::UNIX_EPOCH, token.clone())
            .unwrap();
        assert_eq!(
            registry.cancel_reason(&id),
            FinishReason::CancelledByShutdown
        );
        assert_eq!(registry.cancel(&id), Some(TaskState::Accepted));
        assert!(token.is_cancelled());
        assert_eq!(registry.cancel_reason(&id), FinishReason::CancelledByUser);
        assert_eq!(registry.cancel(&TaskId::new("other")), None);
    }

    #[test]
    fn status_reports_counters_and_times() {
        let registry = TaskRegistry::new();
        let id = TaskId::new("a");
        assert!(registry.status(&id).is_none());
        accept(&registry, "a").unwrap();
        registry.update(&id, |e| {
            e.state = TaskState::Running;
            e.attempt = 2;
            e.retries = 1;
            e.attempt_started_at = Some(SystemTime::UNIX_EPOCH);
        });
        let status = registry.status(&id).unwrap();
        assert_eq!(
            (status.state(), status.attempt(), status.retries()),
            (TaskState::Running, 2, 1)
        );
        assert_eq!(status.accepted_at(), Some(SystemTime::UNIX_EPOCH));
        assert_eq!(status.attempt_started_at(), Some(SystemTime::UNIX_EPOCH));
        assert!(!registry.is_closing());
        registry.set_closing();
        assert!(registry.is_closing());
    }
}
