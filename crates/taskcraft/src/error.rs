//! Error types of the task model.

pub use crate::codec::CodecError;
pub use crate::handle::PushTaskError;
use crate::outcome::BoxError;
use crate::state::TaskState;

/// A configuration mistake caught while building queues or registries.
///
/// The messages match section 2.10 of the specification.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The stable name is already registered for another metadata type.
    #[error("duplicate metadata name: {name}")]
    DuplicateMetadataName {
        /// The repeated name.
        name: String,
    },
    /// The metadata type is already registered under another name.
    #[error("duplicate metadata name: type {type_name} is already registered")]
    DuplicateMetadataType {
        /// The repeated type.
        type_name: &'static str,
    },
    /// The name is reserved by the library.
    #[error("reserved metadata name: {name}")]
    ReservedMetadataName {
        /// The reserved name.
        name: String,
    },
    /// A queue without a name.
    #[error("queue name must not be empty")]
    EmptyQueueName,
    /// Two queues of one monitor share a name.
    #[error("duplicate queue name: {name}")]
    DuplicateQueueName {
        /// The repeated name.
        name: String,
    },
    /// A source capacity below 1.
    #[error("capacity must be at least 1")]
    InvalidCapacity,
    /// A concurrency limit below 1.
    #[error("concurrency must be at least 1")]
    InvalidConcurrency,
    /// A duration out of range.
    #[error("invalid duration: {reason}")]
    InvalidDuration {
        /// What is wrong, worded as in the configuration rules.
        reason: &'static str,
    },
    /// A queue requires a pool the monitor does not have.
    #[error("unknown pool: {name}")]
    UnknownPool {
        /// The pool name.
        name: String,
    },
    /// A queue requires more permits than the pool has.
    #[error("permits exceed pool size: {name}")]
    PermitsExceedPool {
        /// The pool name.
        name: String,
    },
    /// A pool declared with size 0, declared twice, or required with 0
    /// permits.
    #[error("invalid pool {name}: {reason}")]
    InvalidPool {
        /// The pool name.
        name: String,
        /// What is wrong.
        reason: &'static str,
    },
    /// A retry policy out of range (spec 2.10).
    #[error("invalid retry policy: {reason}")]
    InvalidRetryPolicy {
        /// What is wrong, worded as in the configuration rules.
        reason: &'static str,
    },
    /// A poll strategy with a zero duration, a minimum above its maximum or
    /// an empty composition.
    #[error("invalid poll strategy: {reason}")]
    InvalidPollStrategy {
        /// What is wrong, worded as in the configuration rules.
        reason: &'static str,
    },
    /// The queue acks on accept and its source is not a task store, but no
    /// recovery hook is set: a crash would lose accepted tasks (rule 2.3.9
    /// p. 4).
    #[error("ack-on-accept without a store requires a recovery hook")]
    RecoveryRequired,
}

/// A failure to encode or parse task metadata.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// The task carries a metadata type the registry does not know, so it
    /// cannot be stored outside the process.
    #[error("unregistered metadata type: {type_name}")]
    UnregisteredType {
        /// The unregistered type.
        type_name: &'static str,
    },
    /// A typed value could not be encoded.
    #[error("metadata {name} could not be encoded: {reason}")]
    Encode {
        /// The stable name of the value.
        name: String,
        /// Why encoding failed.
        reason: String,
    },
    /// A stored value could not be parsed into its registered type. No
    /// default is substituted.
    #[error("metadata {name} could not be parsed as {type_name}: {reason}")]
    Unparsable {
        /// The stable name of the value.
        name: String,
        /// The registered type.
        type_name: &'static str,
        /// Why parsing failed.
        reason: String,
    },
}

/// A task state change that the lifecycle table (spec 2.4.1) does not allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("transition {from} -> {to} is not allowed")]
pub struct InvalidTransition {
    /// The state the task is in; it is left unchanged.
    pub from: TaskState,
    /// The requested state.
    pub to: TaskState,
}

/// A recovery hook failed; the monitor did not start (scenario 2.2.8).
#[derive(Debug, thiserror::Error)]
#[error("recovery failed: queue={queue}: {source}")]
pub struct RecoveryError {
    queue: String,
    #[source]
    source: BoxError,
}

impl RecoveryError {
    pub(crate) fn new(queue: impl Into<String>, source: BoxError) -> Self {
        Self {
            queue: queue.into(),
            source,
        }
    }

    /// The queue whose hook failed.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }
}
