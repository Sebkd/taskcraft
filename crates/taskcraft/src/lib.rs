//! # taskcraft
//!
//! Background task queue for Rust on the tokio runtime.
//!
//! **Work in progress.** This version contains the task model and the source
//! contract, handlers, queues and workers with retries, overflow policies and
//! resource pools, push, status and cancellation by id, recovery on start and
//! attempt timeouts. The Kafka source and the task store follow in later
//! versions; the first working release will be `0.1.0`.
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
//! - **Handler** — a plain `async fn(Args, X1, …, Xn)` turned into a tower
//!   service by [`task_fn`]. The extra parameters are **extractable values**
//!   ([`FromTask`]): [`Meta<T>`](Meta), [`Attempt`], [`TaskId`],
//!   [`Data<T>`](Data), [`Cancel`]. Missing required metadata aborts the attempt before
//!   the handler runs.
//! - **Outcome** ([`Outcome`]) — success, retry, abort, defer or panic. The
//!   classification is data: `?` on any error gives a retry, and
//!   [`TaskError::abort`] or [`ResultExt::or_abort`] says "do not retry"
//!   however the error is wrapped. [`run_attempt`] turns panics into
//!   [`Outcome::Panic`].
//! - **Runnable** ([`Runnable`]) — a process a handler returns as
//!   [`Run`] instead of an outcome: started, softly stopped on the task's
//!   cancel flag, its result turned into the task's outcome.
//!   [`SpawnedMachine`] adapts a state machine running in its own task (for
//!   example statecraft-fsm), which leaves its outcome in an [`OutcomeSlot`].
//! - **Poll strategy** ([`PollStrategy`]) — how long a worker sleeps after an
//!   empty poll: a fixed interval, growing pauses, a wake-up from the source,
//!   or the first of several. [`Poller::wait`] ends the sleep at once on
//!   shutdown or on a wake-up.
//! - **Retry policy** ([`RetryPolicy`]) — how many attempts a task gets and
//!   how long to pause between them; "defer" does not use attempts up.
//! - **Queue** ([`Queue`]) — a source, a codec and a handler with their
//!   settings. **Worker** — the intake loop of one queue: polls, hands tasks
//!   over without waiting for them, restarts after source errors, drains on
//!   shutdown. **Monitor** ([`Monitor`]) — owns the workers and returns a
//!   [`ShutdownReport`].
//! - **Queue handle** ([`QueueHandle`]) — pushes tasks into a queue and asks
//!   about them by id: status ([`TaskStatus`]) and cancel
//!   ([`CancelOutcome`]). **Task registry** — the queue's live tasks, from
//!   accept to their final state: a busy id is never run twice, and finished
//!   tasks take no memory.
//! - **Recovery hook** ([`QueueBuilder::recover_with`]) — called when the
//!   monitor starts, before the first poll; the tasks it returns are accepted
//!   again. Required when a queue acks on accept and its source is not a task
//!   store; [`QueueBuilder::no_recovery`] declares such tasks may be lost.
//! - **Attempt timeout** ([`QueueBuilder::attempt_timeout`]) — the longest an
//!   attempt may run; it then ends as [`TimeoutOutcome`] says.
//! - **Cancel flag** ([`Cancel`]) — set by a cancel request or by shutdown; a
//!   running handler stops on it or is aborted after the cancel grace.
//! - **Test harness** (`taskcraft::testing`, feature `test-util`) — a source
//!   with scripted failures, a delivery ledger and reusable worker scenarios.
//! - **Log partition, offset** — a log source's ordered sequence of messages
//!   and a message's position in it; [`OffsetTracker`] finds how far a
//!   partition may be committed.

mod attempt;
mod codec;
mod error;
mod handle;
mod handler;
mod memory;
mod metadata;
mod monitor;
mod observe;
mod offset;
mod outcome;
mod poll;
mod queue;
mod registry;
mod retry;
mod runnable;
mod source;
mod state;
mod status;
mod task;
#[cfg(feature = "test-util")]
pub mod testing;
mod worker;

pub use attempt::{CatchPanic, catch_panic, outcome_of, run_attempt};
pub use codec::{Codec, CodecError, IdentityCodec, JsonCodec};

pub use error::{ConfigError, InvalidTransition, MetadataError, RecoveryError};
pub use handle::{CancelOutcome, PushTaskError, QueueHandle};
pub use handler::{
    Attempt, BoxFuture, Cancel, Data, FromTask, Handler, Meta, Rejection, SharedData, TaskFn,
    TaskRequest, task_fn,
};
pub use memory::{Delivery, InMemorySource};
pub use metadata::{Metadata, MetadataRegistry, TRACE_PARENT, TraceParent};
pub use monitor::{Monitor, QueueReport, ShutdownReport, StopReason};
#[cfg(feature = "metrics")]
pub use observe::MetricsObserver;
pub use observe::{AttemptEnd, Event, Observer};
pub use offset::OffsetTracker;
pub use outcome::{BoxError, ErrorKind, IntoOutcome, Outcome, ResultExt, TaskError};
pub use poll::{PollStrategy, Poller, Wakeup};
pub use queue::{DeadLetter, Queue, QueueBuilder, TimeoutOutcome};
pub use retry::RetryPolicy;
pub use runnable::{HandlerOutput, MachineEnd, OutcomeSlot, Run, Runnable, SpawnedMachine};
pub use source::{
    AckOverrideUnsupported, AckPointSupport, Capabilities, CloseReason, DeferError, Polled,
    PushError, PushResult, Source, WakeHandle, WakeSignal,
};
pub use state::{Lifecycle, TaskState};
pub use status::{FinishReason, PushOutcome, RejectReason, TaskStatus};
pub use task::{AckPoint, Task, TaskId, TaskParts};
/// The token that signals shutdown to a [`Monitor`].
pub use tokio_util::sync::CancellationToken;
