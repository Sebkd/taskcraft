//! # taskcraft
//!
//! Background task queue for Rust on the tokio runtime.
//!
//! Built for long, heavy and stateful work: tasks that run for hours, hold
//! scarce resources, survive a restart, and can be asked about — and
//! stopped — by id. A handler is a plain `async fn`; its result is data:
//! success, retry, abort or defer.
//!
//! ```
//! use std::sync::Arc;
//! use taskcraft::prelude::*;
//!
//! async fn send_report(month: String) {
//!     println!("report for {month} sent");
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let queue = Queue::builder("reports", Arc::new(InMemorySource::default()), IdentityCodec::new(), task_fn(send_report))
//!     .concurrency(4)
//!     .no_recovery()
//!     .build()?;
//! let (monitor, reports) = Monitor::new().register(queue)?;
//! let stop = CancellationToken::new();
//! let monitor = tokio::spawn(monitor.run(stop.clone()));
//!
//! let _ = reports.push(Task::new("2026-10".to_owned())).await?;
//! stop.cancel();
//! let report = monitor.await??;
//! # let _ = report;
//! # Ok(()) }
//! ```
//!
//! - **Start here:** [`prelude`], [`Queue::builder`], [`task_fn`],
//!   [`Monitor::register`], [`QueueHandle`].
//! - **Sources:** [`InMemorySource`] here; Kafka in
//!   [`taskcraft-kafka`](https://docs.rs/taskcraft-kafka), a PostgreSQL task
//!   store with leases in [`taskcraft-postgres`](https://docs.rs/taskcraft-postgres);
//!   your own through the traits of [`source`].
//! - **Modules:** [`source`], [`codec`], [`handler`], [`observe`],
//!   [`runnable`], [`error`]; `testing` with the `test-util` feature.
//! - **Features:** `metrics` (`observe::MetricsObserver`), `log`,
//!   `test-util`.
//! - **More:** the [README](https://github.com/Sebkd/taskcraft#readme) tours
//!   every capability; the
//!   [example catalog](https://github.com/Sebkd/taskcraft/tree/master/examples)
//!   has fifteen runnable examples; coming from 0.1, see the
//!   [migration guide](https://github.com/Sebkd/taskcraft/blob/master/MIGRATION.md).
//!
//! The behaviour is specified in
//! [`openspec/specs/taskcraft/taskcraft.md`](https://github.com/Sebkd/taskcraft/blob/master/openspec/specs/taskcraft/taskcraft.md)
//! (in Russian).
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
//! - **Source** ([`source`]) — where a queue takes tasks from and
//!   acknowledges them to, by what it can do: a stream to consume
//!   ([`source::Source`]), a source tasks are pushed into
//!   ([`source::PushSource`]) or a durable task store
//!   ([`source::TaskStore`]). A poll answers with a task, "empty for now" or
//!   "closed" ([`source::Polled`]).
//! - **In-memory source** ([`InMemorySource`]) — a bounded source in process
//!   memory, also the reference implementation of the contract.
//! - **Codec** ([`codec::Codec`]) — turns a source's messages into tasks and
//!   back; [`codec::JsonCodec`] is the default for sources that store bytes
//!   and the built-in codec of a task store.
//! - **Handler** — a plain `async fn(Args, X1, …, Xn)` turned into a tower
//!   service by [`task_fn`]. The extra parameters are **extractable values**
//!   ([`handler::FromTask`]): [`Meta<T>`](Meta), [`Attempt`], [`TaskId`],
//!   [`Data<T>`](Data), [`Cancel`]. Missing required metadata aborts the
//!   attempt before the handler runs.
//! - **Outcome** ([`Outcome`]) — success, retry, abort, defer or panic. The
//!   classification is data: `?` on any error gives a retry, and
//!   [`TaskError::abort`] or [`ResultExt::or_abort`] says "do not retry"
//!   however the error is wrapped. [`handler::run_attempt`] turns panics
//!   into [`Outcome::Panic`].
//! - **Runnable** ([`runnable::Runnable`]) — a process a handler returns as
//!   [`runnable::Run`] instead of an outcome: started, softly stopped on the
//!   task's cancel flag, its result turned into the task's outcome.
//!   [`runnable::SpawnedMachine`] adapts a state machine running in its own
//!   task (for example statecraft-fsm), which leaves its outcome in an
//!   [`runnable::OutcomeSlot`].
//! - **Poll strategy** ([`PollStrategy`]) — how long a worker sleeps after an
//!   empty poll: a fixed interval, growing pauses, a wake-up from the source,
//!   or the first of several. [`source::Poller::wait`] ends the sleep at once
//!   on shutdown or on a wake-up.
//! - **Retry policy** ([`RetryPolicy`]) — how many attempts a task gets and
//!   how long to pause between them; "defer" does not use attempts up.
//! - **Queue** ([`Queue`]) — a source, a codec and a handler with their
//!   settings. **Worker** — the intake loop of one queue: polls, hands tasks
//!   over without waiting for them, restarts after source errors, drains on
//!   shutdown. **Monitor** ([`Monitor`]) — owns the workers and returns a
//!   [`ShutdownReport`].
//! - **Queue handle** ([`QueueHandle`]) — returned by
//!   [`Monitor::register`]: pushes tasks into a queue and asks about them by
//!   id, status ([`TaskStatus`]) and cancel ([`CancelOutcome`]). A queue
//!   that consumes a stream has a [`ConsumerHandle`]: the same without push.
//!   **Task registry** — the queue's live tasks, from accept to their final
//!   state: a busy id is never run twice, and finished tasks take no memory.
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
//!   and a message's position in it; [`source::OffsetTracker`] finds how far
//!   a partition may be committed.

mod attempt;
mod backend;
pub mod codec;
pub mod error;
mod handle;
pub mod handler;
mod memory;
mod metadata;
mod monitor;
pub mod observe;
mod offset;
mod outcome;
mod poll;
mod queue;
mod registry;
mod retry;
pub mod runnable;
pub mod source;
mod state;
mod status;
mod task;
#[cfg(feature = "test-util")]
pub mod testing;
mod worker;

pub use handle::{CancelOutcome, ConsumerHandle, QueueHandle};
pub use handler::{Attempt, Cancel, Data, Meta, SharedData, task_fn};
pub use memory::InMemorySource;
pub use metadata::{Metadata, MetadataRegistry, TRACE_PARENT, TraceParent};
pub use monitor::{Monitor, QueueReport, ShutdownReport, StopReason};
pub use outcome::{BoxError, ErrorKind, Outcome, ResultExt, TaskError};
pub use poll::PollStrategy;
pub use queue::{DeadLetter, FailedTask, Queue, QueueBuilder, TimeoutOutcome};
pub use retry::RetryPolicy;
pub use state::{Lifecycle, TaskState};
pub use status::{FinishReason, PushOutcome, RejectReason, TaskStatus};
pub use task::{AckPoint, Task, TaskId, TaskParts};
/// The token that signals shutdown to a [`Monitor`].
pub use tokio_util::sync::CancellationToken;

/// What nearly every program needs: `use taskcraft::prelude::*;`.
pub mod prelude {
    pub use crate::codec::{IdentityCodec, JsonCodec};
    pub use crate::{
        Attempt, Cancel, CancellationToken, Data, InMemorySource, Meta, Monitor, Outcome, Queue,
        QueueHandle, ResultExt, RetryPolicy, Task, TaskError, TaskId, task_fn,
    };
}
