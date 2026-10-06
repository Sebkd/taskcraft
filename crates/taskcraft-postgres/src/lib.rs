//! # taskcraft-postgres
//!
//! A task store in PostgreSQL for [`taskcraft`] (spec 2.6, 2.7.5): queues
//! keep their tasks, statuses and history outside the process.
//!
//! - **Connection**: a connection string, or the application's
//!   [`sqlx::PgPool`] — also the one of a sea-orm 2.x connection.
//! - **Claim on poll**: a poll takes the next due task for this process in
//!   one statement; no two processes get one task.
//! - **Push and status** are checked against the store: "already finished"
//!   for a finished task until its retention ends, status of tasks of other
//!   processes and of finished ones.
//! - **Restart**: a process restarted with the same id gives its unfinished
//!   tasks back to their queues first. Two live processes cannot share an
//!   id.
//! - **Leases** ([`Lease`]): any process takes over a task whose owner
//!   stopped renewing it.
//!
//! ```no_run
//! use std::sync::Arc;
//! use taskcraft::{CancellationToken, JsonCodec, MetadataRegistry, Monitor, Queue, task_fn};
//! use taskcraft_postgres::{Lease, PgStore};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! async fn export(dataset: String) {}
//!
//! let store = PgStore::builder("exporter-1")
//!     .lease(Lease::default())
//!     .connect("postgres://app:secret@db:5432/app")
//!     .await?;
//! let source = Arc::new(store.queue("exports"));
//! let queue = Queue::builder("exports", source, JsonCodec::new(MetadataRegistry::new()), task_fn(export))
//!     .concurrency(4)
//!     .build()?;
//! let report = Monitor::new().register(queue)?.run(CancellationToken::new()).await?;
//! # let _ = report;
//! # Ok(()) }
//! ```
//!
//! More: the [README](https://github.com/Sebkd/taskcraft/tree/master/crates/taskcraft-postgres#readme)
//! and the [`durable-store`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft-postgres/examples/durable-store.rs)
//! example.

mod source;
mod store;

pub use source::{PgReceipt, PgSource};
pub use store::{Lease, PgStore, PgStoreBuilder, PgStoreError};
