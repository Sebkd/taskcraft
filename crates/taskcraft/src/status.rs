//! Push results, task status and the reasons behind outcomes (spec 2.7.3,
//! 2.7.4, 2.11.4).

use std::fmt;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::state::TaskState;
use crate::task::TaskId;

/// The result of pushing a task into a queue.
///
/// Only [`Enqueued`](Self::Enqueued) means a new task was created. A busy id
/// or a full source are answers, not errors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum PushOutcome {
    /// Written to the source (transition 2.4.1.12).
    Enqueued {
        /// Id of the new task.
        id: TaskId,
    },
    /// A live task with this id already exists; nothing was created.
    AlreadyRunning {
        /// The id.
        id: TaskId,
        /// State of the existing task.
        state: TaskState,
    },
    /// A finished task with this id is still remembered by the task store;
    /// nothing was created.
    AlreadyFinished {
        /// The id.
        id: TaskId,
        /// Final state of that task.
        state: TaskState,
    },
    /// The source refused the task.
    Rejected {
        /// The id of the refused task.
        id: TaskId,
        /// Why.
        reason: RejectReason,
    },
}

impl PushOutcome {
    /// The task id the answer is about.
    #[must_use]
    pub fn id(&self) -> &TaskId {
        match self {
            Self::Enqueued { id }
            | Self::AlreadyRunning { id, .. }
            | Self::AlreadyFinished { id, .. }
            | Self::Rejected { id, .. } => id,
        }
    }
}

/// Why a push was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RejectReason {
    /// The in-memory source is at capacity.
    SourceFull,
}

/// Why a task ended up failed, panicked or cancelled, or why it is being
/// retried.
///
/// `Display` gives a short phrase; it never includes task arguments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
#[non_exhaustive]
pub enum FinishReason {
    /// The handler gave this reason with its outcome.
    Handler(String),
    /// The last allowed retry also asked to retry.
    AttemptsExhausted,
    /// The handler requires metadata the task does not carry.
    MissingMetadata {
        /// The required type.
        type_name: String,
    },
    /// Stored metadata did not parse into its registered type.
    UnparsableMetadata {
        /// The stable name of the value.
        name: String,
        /// The registered type.
        type_name: String,
    },
    /// An attempt ran past the task timeout.
    AttemptTimeout,
    /// The handler panicked with this message.
    Panic(String),
    /// Cancelled by a cancel request.
    CancelledByUser,
    /// Cancelled by shutdown.
    CancelledByShutdown,
    /// Another process took the task over after the lease expired.
    LeaseLost,
    /// Rejected on accept because no slot was free.
    RejectedOverflow,
}

impl fmt::Display for FinishReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Handler(reason) => f.write_str(reason),
            Self::AttemptsExhausted => f.write_str("attempts exhausted"),
            Self::MissingMetadata { type_name } => write!(f, "missing metadata {type_name}"),
            Self::UnparsableMetadata { name, type_name } => {
                write!(f, "metadata {name} could not be parsed as {type_name}")
            }
            Self::AttemptTimeout => f.write_str("attempt timed out"),
            Self::Panic(message) => write!(f, "panicked: {message}"),
            Self::CancelledByUser => f.write_str("cancelled by request"),
            Self::CancelledByShutdown => f.write_str("cancelled by shutdown"),
            Self::LeaseLost => f.write_str("lease lost"),
            Self::RejectedOverflow => f.write_str("rejected: overflow"),
        }
    }
}

/// A snapshot of one task, as returned by a status request (spec 2.7.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatus {
    id: TaskId,
    state: TaskState,
    attempt: u32,
    retries: u32,
    accepted_at: Option<SystemTime>,
    attempt_started_at: Option<SystemTime>,
    next_attempt_at: Option<SystemTime>,
    reason: Option<FinishReason>,
    owner: Option<String>,
}

impl TaskStatus {
    /// A status with only the always-present fields set.
    #[allow(dead_code)] // built by the task registry in a later change
    pub(crate) fn new(id: TaskId, state: TaskState, attempt: u32, retries: u32) -> Self {
        Self {
            id,
            state,
            attempt,
            retries,
            accepted_at: None,
            attempt_started_at: None,
            next_attempt_at: None,
            reason: None,
            owner: None,
        }
    }

    /// The task id.
    #[must_use]
    pub fn id(&self) -> &TaskId {
        &self.id
    }

    /// The current state.
    #[must_use]
    pub fn state(&self) -> TaskState {
        self.state
    }

    /// The attempt number.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The number of retries so far.
    #[must_use]
    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// When the task was accepted.
    #[must_use]
    pub fn accepted_at(&self) -> Option<SystemTime> {
        self.accepted_at
    }

    /// When the current attempt started; only while running.
    #[must_use]
    pub fn attempt_started_at(&self) -> Option<SystemTime> {
        self.attempt_started_at
    }

    /// When the next attempt is due; only while waiting to retry or deferred.
    #[must_use]
    pub fn next_attempt_at(&self) -> Option<SystemTime> {
        self.next_attempt_at
    }

    /// The reason, for failed, panicked and cancelled tasks.
    #[must_use]
    pub fn reason(&self) -> Option<&FinishReason> {
        self.reason.as_ref()
    }

    /// The owning process; only with leases.
    #[must_use]
    pub fn owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_outcome_reports_its_id() {
        let id = TaskId::new("t");
        let outcomes = [
            PushOutcome::Enqueued { id: id.clone() },
            PushOutcome::AlreadyRunning {
                id: id.clone(),
                state: TaskState::Running,
            },
            PushOutcome::AlreadyFinished {
                id: id.clone(),
                state: TaskState::Succeeded,
            },
            PushOutcome::Rejected {
                id: id.clone(),
                reason: RejectReason::SourceFull,
            },
        ];
        for outcome in &outcomes {
            assert_eq!(outcome.id(), &id);
        }
    }

    #[test]
    fn reason_display_and_serialization() {
        assert_eq!(
            FinishReason::AttemptsExhausted.to_string(),
            "attempts exhausted"
        );
        assert_eq!(
            serde_json::to_value(FinishReason::Panic("boom".into())).unwrap(),
            serde_json::json!({ "kind": "panic", "detail": "boom" })
        );
    }

    #[test]
    fn fresh_status_has_no_optional_fields() {
        let status = TaskStatus::new(TaskId::new("t"), TaskState::Accepted, 0, 0);
        assert_eq!(status.state(), TaskState::Accepted);
        assert!(status.reason().is_none() && status.owner().is_none());
        assert!(status.next_attempt_at().is_none());
    }
}
