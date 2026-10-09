# taskcraft-kafka

[![crates.io](https://img.shields.io/crates/v/taskcraft-kafka.svg)](https://crates.io/crates/taskcraft-kafka)
[![docs.rs](https://img.shields.io/docsrs/taskcraft-kafka)](https://docs.rs/taskcraft-kafka)

A Kafka source for [taskcraft](https://crates.io/crates/taskcraft): a queue
reads one topic as a member of a consumer group.

```toml
[dependencies]
taskcraft = "0.3"
taskcraft-kafka = "0.3"
```

```rust,no_run
use std::sync::Arc;
use serde::Deserialize;
use taskcraft::{AckPoint, BoxError, CancellationToken, Meta, MetadataRegistry, Monitor, Queue, TaskId, task_fn};
use taskcraft_kafka::{KafkaJsonCodec, KafkaSource};

/// Metadata read from the `billing.region` header of a message.
#[derive(Clone, Deserialize, serde::Serialize)]
struct Region(String);

/// The body is the task's arguments as JSON; the key is its id.
async fn export(table: String, id: TaskId, Meta(Region(region)): Meta<Region>) {
    println!("{id}: export {table} for {region}");
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let registry = MetadataRegistry::new().register::<Region>("billing.region")?;
    let source = KafkaSource::builder("localhost:9092", "exports", "exporters")
        .property("client.id", "exporter-1") // any librdkafka setting
        .build()?;
    // The queue consumes the topic: producers write to it, the handle does not push.
    let queue = Queue::consumer("exports", Arc::new(source), KafkaJsonCodec::new(registry.clone()), task_fn(export))
        .metadata_registry(registry)
        .ack_point(AckPoint::OnCompletion)
        .concurrency(8)
        .build()?;
    let (monitor, _exports) = Monitor::new().register(queue)?;
    monitor.run(CancellationToken::new()).await?;
    Ok(())
}
```

## How it works

- **Commit boundary.** Auto commit is always off, whatever the properties
  say. An ack commits a partition's offset only past tasks that are all done:
  an unfinished task holds back the commit of every later task of its
  partition.
- **Order within a partition.** The source hands a partition's messages out
  in order; the queue runs them in order only with concurrency 1 *and* a
  retry policy that holds the slot (see below). Otherwise a task waiting for
  a retry or a defer (a defer waits in the process) frees the slot, and the
  next message runs first. With higher concurrency messages of one partition
  run in parallel and finish in any order; commits stay correct either way.
  Delivery is at least once: after a crash or a rebalance, messages from the
  last committed offset come again, including ones already handled.
- **Ack point.** On completion: a crash redelivers unfinished tasks. On
  accept: offsets move early, so the queue needs a recovery hook that brings
  unfinished tasks back from your own records.
- **Task ids** come from the message key ([`task_id`](https://docs.rs/taskcraft-kafka/latest/taskcraft_kafka/struct.KafkaSourceBuilder.html#method.task_id)
  takes another rule). A message without a key gets a generated id — and its
  redelivery is then not recognised as a duplicate.
- **Metadata** comes from headers named in the metadata registry.
- **Consumer groups.** Kafka shares partitions between the processes of a
  group. After a rebalance, unfinished deliveries of a revoked partition
  commit nothing and reach the new owner.
- **Broker down.** A poll fails, the worker restarts its intake with growing
  delays.
- Pushes and deferred redelivery are not supported: producers write to the
  topic; a "defer" outcome waits in the process.

A queue that keeps the order of each partition:

```rust,no_run
# use std::sync::Arc;
# use taskcraft::{AckPoint, MetadataRegistry, Queue, RetryPolicy, task_fn};
# use taskcraft_kafka::{KafkaJsonCodec, KafkaSource};
# async fn apply(payment: String) {}
# fn build() -> Result<(), Box<dyn std::error::Error>> {
# let source = Arc::new(KafkaSource::builder("localhost:9092", "payments", "payment-workers").build()?);
let queue = Queue::consumer("payments", source, KafkaJsonCodec::new(MetadataRegistry::new()), task_fn(apply))
    .concurrency(1)
    // A retry or a defer keeps the slot: the next message waits for it.
    .retry_policy(RetryPolicy { max_attempts: 5, hold_slot: true, ..RetryPolicy::default() })
    .ack_point(AckPoint::OnCompletion)
    .build()?;
# let _ = taskcraft::Monitor::new().register(queue)?;
# Ok(()) }
```

Holding the slot stops the queue for the length of a pause: that is the
price of the order.

Defaults: `auto.offset.reset=earliest`, a poll waits up to 1 s for a message
(`max_wait`).

## Building

`rdkafka` compiles librdkafka: a C compiler and `make` are needed.

## Testing

The integration tests need a broker: set `TASKCRAFT_KAFKA_BROKERS` (for
example `localhost:9092`), or they are skipped. The
[`kafka-consumer`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft-kafka/examples/kafka-consumer.rs)
example runs two processes of one group against a broker from the
[docker compose file](https://github.com/Sebkd/taskcraft/blob/master/examples/docker-compose.yml).

## License

MIT.
