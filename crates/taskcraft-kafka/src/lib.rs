//! # taskcraft-kafka
//!
//! A Kafka source for [`taskcraft`] (spec 2.6, 2.8).
//!
//! - **Commit boundary.** Auto commit is always off. An ack commits the
//!   partition's offset only up to the last task such that every earlier
//!   task of the partition reached its ack point too.
//! - **Task ids** come from the message key by default ([`KafkaSourceBuilder::task_id`]
//!   changes that). A message without one gets a generated id, and its
//!   redelivery is then not recognised as a duplicate.
//! - **Metadata** comes from headers named in the metadata registry
//!   ([`KafkaJsonCodec`]).
//! - **Consumer groups** share partitions between processes; no lease is
//!   needed per task.
//!
//! Queues on this source ack on accept by default, which needs a recovery
//! hook (`QueueBuilder::recover_with`), or ack on completion:
//!
//! ```no_run
//! use std::sync::Arc;
//! use taskcraft::{AckPoint, CancellationToken, MetadataRegistry, Monitor, Queue, task_fn};
//! use taskcraft_kafka::{KafkaJsonCodec, KafkaSource};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! async fn export(dataset: String) {}
//!
//! let source = KafkaSource::builder("localhost:9092", "exports", "export-workers").build()?;
//! let codec = KafkaJsonCodec::new(MetadataRegistry::new());
//! let queue = Queue::builder("exports", Arc::new(source), codec, task_fn(export))
//!     .ack_point(AckPoint::OnCompletion)
//!     .concurrency(4)
//!     .build()?;
//! let report = Monitor::new().register(queue)?.run(CancellationToken::new()).await?;
//! # let _ = report;
//! # Ok(()) }
//! ```
//!
//! More: the [README](https://github.com/Sebkd/taskcraft/tree/master/crates/taskcraft-kafka#readme)
//! and the [`kafka-consumer`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft-kafka/examples/kafka-consumer.rs)
//! example.

mod commits;
mod message;
mod source;

pub use commits::KafkaReceipt;
pub use message::{KafkaJsonCodec, KafkaMessage};
pub use source::{KafkaSource, KafkaSourceBuilder, KafkaSourceError, TaskIdFn};
