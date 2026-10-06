//! # taskcraft
//!
//! Background task queue for Rust on the tokio runtime.
//!
//! **Work in progress.** This version contains the task model and the source
//! contract: the task, its states and metadata, sources with an in-memory
//! implementation, and codecs. Workers and queues follow in later versions; the first working release
//! will be `0.1.0`.
//!
//! The target behaviour is specified in
//! [`openspec/specs/taskcraft/taskcraft.md`](https://github.com/Sebkd/taskcraft/blob/master/openspec/specs/taskcraft/taskcraft.md).
//!
//! ## Terms
//!
//! - **Task** ([`Task`]) — one unit of work: an id unique within its queue
//!   ([`TaskId`]), typed arguments, metadata and attempt counters.
//! - **Metadata** ([`Metadata`]) — values that accompany a task, keyed by
//!   their type: trace context, priority, details about the recipient.
//!   Arguments say what to do; metadata says what else is known.
//! - **Metadata registry** ([`MetadataRegistry`]) — stable names for metadata
//!   types, needed only when tasks are stored outside the process.
//! - **State** ([`TaskState`]) — where a task is in its lifecycle. Only the
//!   transitions of the lifecycle table are possible ([`Lifecycle`]).
//! - **Terminal state** — succeeded, failed, panicked or cancelled; nothing
//!   follows it.
//! - **Ack point** ([`AckPoint`]) — when the source is told a task is taken:
//!   on accept or on completion.
//! - **Push outcome** ([`PushOutcome`]) — the answer to a push: enqueued,
//!   already running, already finished, or rejected.
//! - **Status** ([`TaskStatus`]) and **finish reason** ([`FinishReason`]) —
//!   what a status request reports.
//! - **Source** ([`Source`]) — where a queue takes tasks from and acknowledges
//!   them to. A poll answers with a task, "empty for now" or "closed"
//!   ([`Polled`]); what else a source can do is in [`Capabilities`].
//! - **In-memory source** ([`InMemorySource`]) — a bounded source in process
//!   memory, also the reference implementation of the contract.
//! - **Codec** ([`Codec`]) — turns a source's messages into tasks and back;
//!   [`JsonCodec`] is the default for sources that store bytes.
//! - **Log partition, offset** — a log source's ordered sequence of messages
//!   and a message's position in it; [`OffsetTracker`] finds how far a
//!   partition may be committed.

mod codec;
mod error;
mod memory;
mod metadata;
mod offset;
mod source;
mod state;
mod status;
mod task;

pub use codec::{Codec, CodecError, IdentityCodec, JsonCodec};

pub use error::{ConfigError, InvalidTransition, MetadataError};
pub use memory::{Delivery, InMemorySource};
pub use metadata::{Metadata, MetadataRegistry, TRACE_PARENT};
pub use offset::OffsetTracker;
pub use source::{
    AckOverrideUnsupported, AckPointSupport, Capabilities, CloseReason, DeferError, Polled,
    PushError, PushResult, Source, WakeHandle, WakeSignal,
};
pub use state::{Lifecycle, TaskState};
pub use status::{FinishReason, PushOutcome, RejectReason, TaskStatus};
pub use task::{AckPoint, Task, TaskId, TaskParts};
