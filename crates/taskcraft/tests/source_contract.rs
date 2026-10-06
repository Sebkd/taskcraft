//! The source contract implemented from outside the crate, the way a user or
//! a backend crate would, and the in-memory source under concurrency.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use taskcraft::{
    AckPoint, AckPointSupport, Capabilities, CloseReason, Codec, InMemorySource, JsonCodec,
    MetadataRegistry, OffsetTracker, Polled, PushResult, Source, Task, TaskId,
};

/// A table: rows are handed out one by one and deleted on ack.
#[derive(Default)]
struct ToyTable {
    rows: Mutex<VecDeque<(u64, Vec<u8>)>>,
    deleted: Mutex<Vec<u64>>,
    drained: Mutex<bool>,
}

impl Source for ToyTable {
    type Message = Vec<u8>;
    type Receipt = u64;
    type Error = Infallible;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::PerTask)
    }

    async fn poll(&self) -> Result<Polled<Vec<u8>, u64>, Infallible> {
        let next = self.rows.lock().unwrap().pop_front();
        Ok(match next {
            Some((row, message)) => Polled::Task {
                message,
                receipt: row,
            },
            None if *self.drained.lock().unwrap() => {
                Polled::Closed(CloseReason::new("table drained"))
            }
            None => Polled::Empty,
        })
    }

    async fn ack(&self, row: u64) -> Result<(), Infallible> {
        self.deleted.lock().unwrap().push(row);
        Ok(())
    }
}

/// A one-partition log: acks mark offsets done, the commit follows the
/// boundary rule.
#[derive(Default)]
struct ToyLog {
    messages: Mutex<VecDeque<(i64, Vec<u8>)>>,
    tracker: Mutex<OffsetTracker>,
    committed: Mutex<Option<i64>>,
}

impl Source for ToyLog {
    type Message = Vec<u8>;
    type Receipt = i64;
    type Error = Infallible;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::QueueOnly)
    }

    async fn poll(&self) -> Result<Polled<Vec<u8>, i64>, Infallible> {
        let next = self.messages.lock().unwrap().pop_front();
        Ok(match next {
            Some((offset, message)) => {
                self.tracker.lock().unwrap().delivered(offset);
                Polled::Task {
                    message,
                    receipt: offset,
                }
            }
            None => Polled::Empty,
        })
    }

    async fn ack(&self, offset: i64) -> Result<(), Infallible> {
        let mut tracker = self.tracker.lock().unwrap();
        tracker.acked(offset);
        *self.committed.lock().unwrap() = tracker.commit_point();
        Ok(())
    }
}

fn encoded(codec: &JsonCodec, id: &str) -> Vec<u8> {
    codec.encode(Task::new(id.to_owned()).with_id(id)).unwrap()
}

async fn take<S: Source>(source: &S) -> (S::Message, S::Receipt) {
    match source.poll().await.unwrap() {
        Polled::Task { message, receipt } => (message, receipt),
        _ => panic!("expected a task"),
    }
}

#[tokio::test]
async fn table_source_on_the_contract() {
    let codec = JsonCodec::new(MetadataRegistry::new());
    let table = ToyTable::default();
    table
        .rows
        .lock()
        .unwrap()
        .extend([(1, encoded(&codec, "a")), (2, encoded(&codec, "b"))]);

    let (message, row) = take(&table).await;
    let first: Task<String> = codec.decode(message).unwrap();
    assert_eq!(first.id().as_str(), "a");
    table.ack(row).await.unwrap();
    let _ = take(&table).await;

    // Empty and closed are different answers.
    assert!(matches!(table.poll().await.unwrap(), Polled::Empty));
    *table.drained.lock().unwrap() = true;
    assert!(matches!(table.poll().await.unwrap(), Polled::Closed(_)));
    assert_eq!(*table.deleted.lock().unwrap(), [1]);
}

#[tokio::test]
async fn log_source_commits_up_to_the_boundary() {
    let codec = JsonCodec::new(MetadataRegistry::new());
    let log = ToyLog::default();
    log.messages
        .lock()
        .unwrap()
        .extend((1..=3).map(|o| (o, encoded(&codec, &o.to_string()))));

    let mut receipts = Vec::new();
    for _ in 0..3 {
        receipts.push(take(&log).await.1);
    }
    log.ack(receipts[0]).await.unwrap();
    log.ack(receipts[2]).await.unwrap();
    assert_eq!(*log.committed.lock().unwrap(), Some(1));
    log.ack(receipts[1]).await.unwrap();
    assert_eq!(*log.committed.lock().unwrap(), Some(3));
    assert!(matches!(log.poll().await.unwrap(), Polled::Empty));
}

#[test]
fn ack_override_is_refused_by_a_log_and_accepted_by_memory() {
    let task = Task::new(()).with_ack_point(AckPoint::OnCompletion);
    let log = ToyLog::default().capabilities();
    let memory = InMemorySource::<()>::new(1).unwrap().capabilities();
    assert!(log.check_ack_override(task.ack_point()).is_err());
    assert!(memory.check_ack_override(task.ack_point()).is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_source_under_many_producers_and_consumers() {
    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 500;
    const CONSUMERS: usize = 6;
    let total = PRODUCERS * PER_PRODUCER;

    let source = Arc::new(InMemorySource::<usize>::new(total).unwrap());
    let seen = Arc::new(Mutex::new(HashSet::new()));

    let mut producers = Vec::new();
    for p in 0..PRODUCERS {
        let source = Arc::clone(&source);
        producers.push(tokio::spawn(async move {
            for i in 0..PER_PRODUCER {
                let n = p * PER_PRODUCER + i;
                let id = TaskId::new(n.to_string());
                let task = Task::new(n).with_id(id.clone());
                assert_eq!(source.push(&id, task).await.unwrap(), PushResult::Stored);
            }
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..CONSUMERS {
        let source = Arc::clone(&source);
        let seen = Arc::clone(&seen);
        consumers.push(tokio::spawn(async move {
            loop {
                match source.poll().await.unwrap() {
                    Polled::Task { message, receipt } => {
                        assert!(
                            seen.lock().unwrap().insert(*message.args()),
                            "delivered twice"
                        );
                        source.ack(receipt).await.unwrap();
                    }
                    Polled::Empty => tokio::task::yield_now().await,
                    Polled::Closed(_) => break,
                }
            }
        }));
    }

    for producer in producers {
        producer.await.unwrap();
    }
    source.close();
    for consumer in consumers {
        consumer.await.unwrap();
    }

    assert_eq!(seen.lock().unwrap().len(), total, "no task lost");
    assert!(source.is_empty(), "nothing left behind: {source:?}");
}
