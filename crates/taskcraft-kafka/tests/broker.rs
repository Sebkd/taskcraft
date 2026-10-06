//! The Kafka source against a real client.
//!
//! The unreachable-broker test needs no broker. The others need one: set
//! `TASKCRAFT_KAFKA_BROKERS` (for example `localhost:9092`), or they are
//! skipped.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use taskcraft::{
    AckPoint, CancellationToken, Meta, MetadataRegistry, Monitor, Polled, Queue, ShutdownReport,
    Source, TaskId, task_fn,
};
use taskcraft_kafka::{KafkaJsonCodec, KafkaSource, KafkaSourceError};
use tokio::task::JoinHandle;
use tokio::time::sleep;

const SEC: Duration = Duration::from_secs(1);

fn brokers() -> Option<String> {
    let brokers = std::env::var("TASKCRAFT_KAFKA_BROKERS").ok();
    if brokers.is_none() {
        eprintln!("TASKCRAFT_KAFKA_BROKERS is not set: skipped");
    }
    brokers
}

async fn until(limit: Duration, cond: impl Fn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok()
}

/// Change criterion 4: an unreachable broker is a source error.
#[tokio::test(flavor = "multi_thread")]
async fn unreachable_broker_is_a_source_error() {
    let source = KafkaSource::builder("127.0.0.1:1", "jobs", "workers")
        .max_wait(Duration::from_millis(200))
        .build()
        .unwrap();
    let failed = tokio::time::timeout(30 * SEC, async {
        loop {
            match source.poll().await {
                Err(KafkaSourceError::Broker(_) | KafkaSourceError::Client(_)) => break,
                Ok(Polled::Empty) => {}
                other => panic!("unexpected answer: {other:?}"),
            }
        }
    })
    .await;
    assert!(failed.is_ok(), "no broker error within 30 s");
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Region(String);

fn registry() -> MetadataRegistry {
    MetadataRegistry::new()
        .register::<Region>("billing.region")
        .unwrap()
}

async fn create_topic(brokers: &str, partitions: i32) -> String {
    let topic = format!("taskcraft-{}", uuid::Uuid::new_v4());
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    let new = NewTopic::new(&topic, partitions, TopicReplication::Fixed(1));
    let results = admin
        .create_topics(&[new], &AdminOptions::new())
        .await
        .unwrap();
    for result in results {
        result.unwrap();
    }
    topic
}

async fn produce(brokers: &str, topic: &str, messages: impl IntoIterator<Item = (String, u32)>) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    for (key, n) in messages {
        let payload = n.to_string();
        let headers = OwnedHeaders::new().insert(Header {
            key: "billing.region",
            value: Some("\"eu\""),
        });
        let record = FutureRecord::to(topic)
            .key(&key)
            .payload(&payload)
            .headers(headers);
        producer.send(record, 10 * SEC).await.unwrap();
    }
}

/// The committed offset of partition 0 for `group`.
fn committed(brokers: &str, topic: &str, group: &str) -> Option<i64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .create()
        .unwrap();
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, 0);
    let offsets = consumer.committed_offsets(partitions, 10 * SEC).ok()?;
    match offsets.find_partition(topic, 0)?.offset() {
        Offset::Offset(n) => Some(n),
        _ => None,
    }
}

type Seen = Arc<Mutex<Vec<(String, u32)>>>;

fn spawn(
    brokers: &str,
    topic: &str,
    group: &str,
    seen: &Seen,
    hold: Option<Arc<AtomicBool>>,
) -> (JoinHandle<ShutdownReport>, CancellationToken) {
    let source = KafkaSource::builder(brokers, topic, group)
        .max_wait(Duration::from_millis(200))
        .build()
        .unwrap();
    let record = Arc::clone(seen);
    let handler = task_fn(move |n: u32, id: TaskId, Meta(region): Meta<Region>| {
        let record = Arc::clone(&record);
        let hold = hold.clone();
        async move {
            assert_eq!(region, Region("eu".into()));
            // Task 1 waits while `hold` is set.
            while n == 1 && hold.as_ref().is_some_and(|h| h.load(Ordering::SeqCst)) {
                sleep(Duration::from_millis(20)).await;
            }
            record.lock().unwrap().push((id.as_str().to_owned(), n));
        }
    });
    let queue = Queue::builder(
        "jobs",
        Arc::new(source),
        KafkaJsonCodec::new(registry()),
        handler,
    )
    .ack_point(AckPoint::OnCompletion)
    .concurrency(4)
    .metadata_registry(registry())
    .build()
    .unwrap();
    let stop = CancellationToken::new();
    let monitor = Monitor::new().register(queue).unwrap();
    let run = tokio::spawn({
        let stop = stop.clone();
        async move { monitor.run(stop).await.unwrap() }
    });
    (run, stop)
}

/// Keys become ids, headers metadata; once all tasks finish, the offset of
/// the last one is committed.
#[tokio::test(flavor = "multi_thread")]
async fn tasks_run_and_offsets_are_committed() {
    let Some(brokers) = brokers() else { return };
    let topic = create_topic(&brokers, 1).await;
    produce(&brokers, &topic, (0..5).map(|n| (format!("job-{n}"), n))).await;
    let seen = Seen::default();
    let (run, stop) = spawn(&brokers, &topic, "group-a", &seen, None);

    assert!(until(60 * SEC, || seen.lock().unwrap().len() == 5).await);
    let mut ids: Vec<_> = seen.lock().unwrap().iter().map(|s| s.0.clone()).collect();
    ids.sort();
    assert_eq!(ids, ["job-0", "job-1", "job-2", "job-3", "job-4"]);
    assert!(
        until(30 * SEC, || committed(&brokers, &topic, "group-a")
            == Some(5))
        .await
    );
    stop.cancel();
    run.await.unwrap();
}

/// Criterion 23 on a broker: an unfinished task holds back the commit of the
/// later ones.
#[tokio::test(flavor = "multi_thread")]
async fn unfinished_task_holds_back_the_commit() {
    let Some(brokers) = brokers() else { return };
    let topic = create_topic(&brokers, 1).await;
    produce(&brokers, &topic, (0..3).map(|n| (format!("job-{n}"), n))).await;
    let seen = Seen::default();
    let hold = Arc::new(AtomicBool::new(true));
    let (run, stop) = spawn(&brokers, &topic, "group-b", &seen, Some(Arc::clone(&hold)));

    assert!(until(60 * SEC, || seen.lock().unwrap().len() == 2).await);
    assert!(
        until(30 * SEC, || committed(&brokers, &topic, "group-b")
            == Some(1))
        .await
    );
    sleep(2 * SEC).await;
    assert_eq!(committed(&brokers, &topic, "group-b"), Some(1), "held at 1");

    hold.store(false, Ordering::SeqCst);
    assert!(
        until(30 * SEC, || committed(&brokers, &topic, "group-b")
            == Some(3))
        .await
    );
    stop.cancel();
    run.await.unwrap();
}

/// Change criterion 5: two processes of one group share the partitions, and
/// every task runs in one of them only.
#[tokio::test(flavor = "multi_thread")]
async fn a_group_shares_partitions_between_processes() {
    let Some(brokers) = brokers() else { return };
    let topic = create_topic(&brokers, 2).await;
    let (first, second) = (Seen::default(), Seen::default());
    let (run_a, stop_a) = spawn(&brokers, &topic, "group-c", &first, None);
    let (run_b, stop_b) = spawn(&brokers, &topic, "group-c", &second, None);
    // Both members join the group before any message is there.
    sleep(15 * SEC).await;

    produce(&brokers, &topic, (0..40).map(|n| (format!("job-{n}"), n))).await;
    let total = || first.lock().unwrap().len() + second.lock().unwrap().len();
    assert!(until(60 * SEC, || total() >= 40).await);
    sleep(2 * SEC).await;

    let mut runs: HashMap<String, u32> = HashMap::new();
    for (id, _) in first
        .lock()
        .unwrap()
        .iter()
        .chain(second.lock().unwrap().iter())
    {
        *runs.entry(id.clone()).or_default() += 1;
    }
    assert_eq!(runs.len(), 40);
    assert!(runs.values().all(|&n| n == 1), "a task ran twice: {runs:?}");
    assert!(!first.lock().unwrap().is_empty() && !second.lock().unwrap().is_empty());
    stop_a.cancel();
    stop_b.cancel();
    run_a.await.unwrap();
    run_b.await.unwrap();
}
