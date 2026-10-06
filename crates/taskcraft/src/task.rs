//! The task: one unit of work (spec 2.7.1).

use std::fmt;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::metadata::Metadata;

/// Identifier of a task, unique within its queue.
///
/// Status, cancellation and idempotent accept all work by this id. It is any
/// string the consumer chooses; when none is given, a UUID v4 is generated.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(String);

impl TaskId {
    /// An id with the given value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// A fresh random id: a UUID v4 in its canonical text form.
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// The id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TaskId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for TaskId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for TaskId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

/// When a task is acknowledged to its source (spec 2.3.9).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckPoint {
    /// Right after the task is accepted into the registry. The default.
    #[default]
    OnAccept,
    /// After the task reaches a final state.
    OnCompletion,
}

/// One unit of work: id, arguments, metadata and attempt counters.
///
/// Build it from its arguments and add what else it needs:
///
/// ```
/// use taskcraft::{AckPoint, Task};
///
/// #[derive(Clone)]
/// struct Priority(u8);
///
/// let task = Task::new("report-2026-10")
///     .with_id("report-2026-10")
///     .with_meta(Priority(5))
///     .with_ack_point(AckPoint::OnCompletion);
///
/// assert_eq!(task.id().as_str(), "report-2026-10");
/// assert_eq!(task.attempt(), 0);
/// assert_eq!(task.metadata().get::<Priority>().map(|p| p.0), Some(5));
/// ```
///
/// Attempt counters and the accept time are kept by the engine and cannot be
/// set from outside.
#[derive(Debug, Clone)]
pub struct Task<Args> {
    id: TaskId,
    args: Args,
    metadata: Metadata,
    attempt: u32,
    retries: u32,
    ack_point: Option<AckPoint>,
    accepted_at: Option<SystemTime>,
}

/// Every field of a [`Task`], for codecs and sources that store tasks and
/// must restore them exactly, attempt counters included.
#[derive(Debug, Clone)]
pub struct TaskParts<Args> {
    /// The task id.
    pub id: TaskId,
    /// The arguments.
    pub args: Args,
    /// The metadata.
    pub metadata: Metadata,
    /// The attempt number: 0 before the first attempt.
    pub attempt: u32,
    /// How many retries happened.
    pub retries: u32,
    /// The per-task ack point, if any.
    pub ack_point: Option<AckPoint>,
    /// When the task was accepted, if it has been.
    pub accepted_at: Option<SystemTime>,
}

impl<Args> Task<Args> {
    /// Rebuilds a task from all of its fields. Meant for codecs and sources;
    /// application code builds tasks with [`Task::new`].
    pub fn from_parts(parts: TaskParts<Args>) -> Self {
        let TaskParts {
            id,
            args,
            metadata,
            attempt,
            retries,
            ack_point,
            accepted_at,
        } = parts;
        Self {
            id,
            args,
            metadata,
            attempt,
            retries,
            ack_point,
            accepted_at,
        }
    }

    /// Splits the task into all of its fields.
    pub fn into_parts(self) -> TaskParts<Args> {
        TaskParts {
            id: self.id,
            args: self.args,
            metadata: self.metadata,
            attempt: self.attempt,
            retries: self.retries,
            ack_point: self.ack_point,
            accepted_at: self.accepted_at,
        }
    }

    /// A task with these arguments and a generated id.
    pub fn new(args: Args) -> Self {
        Self {
            id: TaskId::generate(),
            args,
            metadata: Metadata::default(),
            attempt: 0,
            retries: 0,
            ack_point: None,
            accepted_at: None,
        }
    }

    /// Replaces the generated id.
    #[must_use]
    pub fn with_id(mut self, id: impl Into<TaskId>) -> Self {
        self.id = id.into();
        self
    }

    /// Adds a metadata value, replacing any earlier value of the same type.
    #[must_use]
    pub fn with_meta<T>(mut self, value: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.metadata.insert(value);
        self
    }

    /// Overrides the queue's ack point for this task. Only sources with
    /// per-task ack honour it (spec 2.3.10).
    #[must_use]
    pub fn with_ack_point(mut self, ack_point: AckPoint) -> Self {
        self.ack_point = Some(ack_point);
        self
    }

    /// The task id.
    #[must_use]
    pub fn id(&self) -> &TaskId {
        &self.id
    }

    /// The arguments.
    #[must_use]
    pub fn args(&self) -> &Args {
        &self.args
    }

    /// Consumes the task and returns its arguments.
    pub fn into_args(self) -> Args {
        self.args
    }

    /// The metadata.
    #[must_use]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Mutable access to the metadata.
    pub fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Number of the current attempt: 0 before the first one, then 1, 2, …
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// How many times the task was retried after a "retry" outcome.
    #[must_use]
    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// The per-task ack point, if the task overrides the queue's.
    #[must_use]
    pub fn ack_point(&self) -> Option<AckPoint> {
        self.ack_point
    }

    /// When the task was accepted into the registry, once it has been.
    #[must_use]
    pub fn accepted_at(&self) -> Option<SystemTime> {
        self.accepted_at
    }

    /// Starts a new attempt: the attempt number grows by one.
    #[allow(dead_code)] // used by the worker in a later change
    pub(crate) fn begin_attempt(&mut self) {
        self.attempt = self.attempt.saturating_add(1);
    }

    /// Counts a retry after a "retry" outcome.
    #[allow(dead_code)] // used by the retry policy in a later change
    pub(crate) fn count_retry(&mut self) {
        self.retries = self.retries.saturating_add(1);
    }

    /// Records the moment of accept (transition 2.4.1.1).
    #[allow(dead_code)] // used by the worker in a later change
    pub(crate) fn mark_accepted(&mut self, at: SystemTime) {
        self.accepted_at = Some(at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_uuid_v4_and_differ() {
        let a = TaskId::generate();
        let b = TaskId::generate();
        assert_ne!(a, b);
        let parsed = uuid::Uuid::parse_str(a.as_str()).unwrap();
        assert_eq!(parsed.get_version_num(), 4);
        assert_eq!(parsed.hyphenated().to_string(), a.as_str());
    }

    #[test]
    fn new_task_defaults() {
        let task = Task::new(42_u32);
        assert!(uuid::Uuid::parse_str(task.id().as_str()).is_ok());
        assert_eq!(task.attempt(), 0);
        assert_eq!(task.retries(), 0);
        assert_eq!(task.ack_point(), None);
        assert_eq!(task.accepted_at(), None);
        assert!(task.metadata().is_empty());
        assert_eq!(*task.args(), 42);
    }

    #[test]
    fn engine_counters() {
        let mut task = Task::new(()).with_id("t-1");
        task.begin_attempt();
        task.count_retry();
        task.begin_attempt();
        let at = SystemTime::UNIX_EPOCH;
        task.mark_accepted(at);
        assert_eq!(
            (task.attempt(), task.retries(), task.accepted_at()),
            (2, 1, Some(at))
        );
    }

    #[test]
    fn id_serializes_as_plain_string() {
        let id = TaskId::new("abc");
        assert_eq!(serde_json::to_value(&id).unwrap(), "abc");
        assert_eq!(format!("{id} {id:?}"), r#"abc "abc""#);
    }

    #[test]
    fn default_ack_point_is_on_accept() {
        assert_eq!(AckPoint::default(), AckPoint::OnAccept);
    }
}
