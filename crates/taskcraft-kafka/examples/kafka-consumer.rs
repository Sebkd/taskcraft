//! E-12 `kafka-consumer` — a Kafka source: ack on accept with a recovery
//! hook, keys as task ids, partitions shared by two processes of one
//! consumer group (spec 2.6 "Kafka", 2.3.9, 2.3.18).
//!
//! Needs a broker: `docker compose -f examples/docker-compose.yml up -d kafka`.
//!
//! ```text
//! cargo run -p taskcraft-kafka --example kafka-consumer
//! ```
//!
//! `TASKCRAFT_KAFKA_BROKERS` overrides the broker address (default
//! `localhost:9092`).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use taskcraft::{
    BoxError, CancellationToken, Data, MetadataRegistry, Monitor, Queue, SharedData, Task, TaskId,
    task_fn,
};
use taskcraft_kafka::{KafkaJsonCodec, KafkaSource};

/// Which process ran which job — the consumer's own records.
type Ledger = Arc<Mutex<BTreeMap<String, String>>>;

async fn export(table: u32, id: TaskId, Data(seen): Data<(String, Ledger)>) {
    let (process, ledger) = &*seen;
    println!("  {process}: export of table {table} ({id})");
    let mut ledger = ledger.lock().unwrap_or_else(PoisonError::into_inner);
    ledger.insert(id.as_str().to_owned(), process.clone());
}

/// One process: a source in the group and its queue.
fn process(
    name: &str,
    brokers: &str,
    topic: &str,
    group: &str,
    ledger: &Ledger,
) -> Result<Monitor, BoxError> {
    let source = KafkaSource::builder(brokers, topic, group).build()?;
    let mut shared = SharedData::new();
    shared.insert((name.to_owned(), Arc::clone(ledger)));
    let queue = Queue::builder(
        "exports",
        Arc::new(source),
        KafkaJsonCodec::new(MetadataRegistry::new()),
        task_fn(export),
    )
    .concurrency(4)
    .shared_data(shared)
    // Ack on accept commits offsets early; after a crash, unfinished
    // exports come back from the consumer's own records.
    .recover_with(|| async { Ok::<Vec<Task<u32>>, BoxError>(Vec::new()) })
    .build()?;
    Ok(Monitor::new().register(queue)?)
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let brokers =
        std::env::var("TASKCRAFT_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let run = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let (topic, group) = (format!("exports-{run}"), format!("exporters-{run}"));

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()?;
    for result in admin
        .create_topics(
            &[NewTopic::new(&topic, 2, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await?
    {
        result.map_err(|(topic, error)| format!("topic {topic}: {error}"))?;
    }
    println!("topic {topic} with 2 partitions");

    let ledger = Ledger::default();
    let stop = CancellationToken::new();
    let mut running = Vec::new();
    for name in ["process-a", "process-b"] {
        let monitor = process(name, &brokers, &topic, &group, &ledger)?;
        running.push(tokio::spawn(monitor.run(stop.clone())));
    }
    println!("waiting for both processes to join the group");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()?;
    for n in 0..20 {
        let (key, payload) = (format!("export-{n}"), n.to_string());
        producer
            .send(
                FutureRecord::to(&topic).key(&key).payload(&payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(error, _)| error)?;
    }
    println!("20 exports written to the topic");

    for _ in 0..600 {
        if ledger.lock().unwrap_or_else(PoisonError::into_inner).len() == 20 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.cancel();
    for monitor in running {
        monitor.await??;
    }

    let ledger = ledger
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let by_a = ledger.values().filter(|p| *p == "process-a").count();
    println!(
        "done: {} exports, process-a ran {by_a}, process-b ran {}",
        ledger.len(),
        ledger.len() - by_a
    );
    if ledger.len() != 20 || by_a == 0 || by_a == 20 {
        return Err("expected 20 exports shared by both processes".into());
    }
    Ok(())
}
