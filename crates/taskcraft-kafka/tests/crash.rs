//! A real process crash (scenario 2.2.12 p. 2; change
//! concurrency-and-fuzz-tests): a child process — this test binary run again
//! — takes a message with ack on completion and is killed with SIGKILL; the
//! offset was never committed, so another member of the group gets the
//! message again. Set `TASKCRAFT_KAFKA_BROKERS`, or the test is skipped.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use taskcraft::{AckPoint, CancellationToken, MetadataRegistry, Monitor, Queue, TaskId, task_fn};
use taskcraft_kafka::{KafkaJsonCodec, KafkaSource};
use tokio::time::sleep;

const SEC: Duration = Duration::from_secs(1);
/// The child: its test name and the variable that makes it one.
const CHILD: &str = "child_takes_a_message";
const CHILD_ENV: &str = "TASKCRAFT_CRASH_CHILD";

fn brokers() -> Option<String> {
    let brokers = std::env::var("TASKCRAFT_KAFKA_BROKERS").ok();
    if brokers.is_none() {
        assert!(
            std::env::var_os("TASKCRAFT_REQUIRE_SERVICES").is_none(),
            "TASKCRAFT_KAFKA_BROKERS is required"
        );
        eprintln!("TASKCRAFT_KAFKA_BROKERS is not set: skipped");
    }
    brokers
}

/// A member of `group` acking on completion; short session, so the group
/// notices a killed member within seconds.
fn source(brokers: &str, topic: &str, group: &str) -> KafkaSource {
    KafkaSource::builder(brokers, topic, group)
        .property("session.timeout.ms", "6000")
        .max_wait(Duration::from_millis(200))
        .build()
        .unwrap()
}

/// As the child (with `TASKCRAFT_CRASH_CHILD=topic|group|marker`): marks the
/// start of the task in the marker file and never finishes it. Otherwise
/// does nothing.
#[tokio::test(flavor = "multi_thread")]
async fn child_takes_a_message() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let mut parts = spec.splitn(3, '|');
    let (topic, group, marker) = (
        parts.next().unwrap().to_owned(),
        parts.next().unwrap().to_owned(),
        PathBuf::from(parts.next().unwrap()),
    );
    let brokers = std::env::var("TASKCRAFT_KAFKA_BROKERS").unwrap();
    let queue = Queue::consumer(
        "jobs",
        Arc::new(source(&brokers, &topic, &group)),
        KafkaJsonCodec::new(MetadataRegistry::new()),
        task_fn(move |_: u32| {
            let marker = marker.clone();
            async move {
                std::fs::write(&marker, b"started").unwrap();
                std::future::pending::<()>().await;
            }
        }),
    )
    .ack_point(AckPoint::OnCompletion)
    .build()
    .unwrap();
    let (monitor, _handle) = Monitor::new().register(queue).unwrap();
    monitor.run(CancellationToken::new()).await.unwrap();
}

/// Kills the child however the test ends.
struct Killed(Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn until(limit: Duration, cond: impl Fn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond() {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn killed_member_message_is_delivered_again() {
    let Some(brokers) = brokers() else { return };
    let id = uuid::Uuid::new_v4();
    let (topic, group) = (format!("taskcraft-crash-{id}"), format!("crash-{id}"));
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    let new = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
    for result in admin
        .create_topics(&[new], &AdminOptions::new())
        .await
        .unwrap()
    {
        result.unwrap();
    }
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    producer
        .send(FutureRecord::to(&topic).key("job").payload("7"), 10 * SEC)
        .await
        .unwrap();

    let marker = std::env::temp_dir().join(format!("taskcraft-crash-{id}"));
    let child = Command::new(std::env::current_exe().unwrap())
        .args([CHILD, "--exact", "--nocapture"])
        // Killed mid-write, its coverage profile would spoil the merged
        // report: the child writes none.
        .env("LLVM_PROFILE_FILE", "/dev/null")
        .env(CHILD_ENV, format!("{topic}|{group}|{}", marker.display()))
        .env("TASKCRAFT_KAFKA_BROKERS", &brokers)
        .spawn()
        .unwrap();
    let mut child = Killed(child);
    assert!(
        until(90 * SEC, || marker.exists()).await,
        "the child started the task"
    );
    // kill -9: no ack, no commit, no leaving the group.
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let _ = std::fs::remove_file(&marker);

    let seen: Arc<Mutex<Vec<(String, u32)>>> = Arc::default();
    let record = Arc::clone(&seen);
    let queue = Queue::consumer(
        "jobs",
        Arc::new(source(&brokers, &topic, &group)),
        KafkaJsonCodec::new(MetadataRegistry::new()),
        task_fn(move |n: u32, id: TaskId| {
            record.lock().unwrap().push((id.as_str().to_owned(), n));
            async {}
        }),
    )
    .ack_point(AckPoint::OnCompletion)
    .build()
    .unwrap();
    let (monitor, _handle) = Monitor::new().register(queue).unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));
    assert!(
        until(90 * SEC, || !seen.lock().unwrap().is_empty()).await,
        "the message came again"
    );
    assert_eq!(*seen.lock().unwrap(), [("job".to_owned(), 7)]);
    stop.cancel();
    running.await.unwrap().unwrap();
}
