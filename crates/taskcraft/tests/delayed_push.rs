//! Delayed push: a task delivered not before its moment (change
//! delayed-push, criteria 1 and 3).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use taskcraft::codec::IdentityCodec;
use taskcraft::error::PushTaskError;
use taskcraft::source::{Polled, Source};
use taskcraft::testing::{FaultyCodec, FaultySource};
use taskcraft::{
    CancelOutcome, CancellationToken, InMemorySource, Monitor, PushOutcome, Queue, RejectReason,
    Task, TaskId, task_fn,
};
use tokio::time::{Instant, sleep};

const HOUR: Duration = Duration::from_secs(3600);

/// Criterion 1: a task pushed for an hour from now runs after that hour,
/// not before — within the queue's poll strategy (pauses up to 30 s).
#[tokio::test(start_paused = true)]
async fn delayed_task_runs_not_before_its_moment() {
    let started: Arc<Mutex<Vec<Instant>>> = Arc::default();
    let record = Arc::clone(&started);
    let queue = Queue::builder(
        "reminders",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(move |_: u32| {
            record.lock().unwrap().push(Instant::now());
            async {}
        }),
    )
    .no_recovery()
    .build()
    .unwrap();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));

    let pushed_at = Instant::now();
    let task = Task::new(1).with_id("remind").with_delay(HOUR);
    assert!(task.deliver_at().is_some());
    assert!(matches!(
        handle.push(task).await.unwrap(),
        PushOutcome::Enqueued { .. }
    ));
    sleep(HOUR - Duration::from_secs(1)).await;
    assert!(started.lock().unwrap().is_empty(), "not before the moment");

    sleep(Duration::from_secs(40)).await;
    let started = started.lock().unwrap().clone();
    assert_eq!(started.len(), 1);
    let waited = started[0] - pushed_at;
    assert!(
        waited >= HOUR - Duration::from_secs(1) && waited <= HOUR + Duration::from_secs(31),
        "{waited:?}"
    );
    stop.cancel();
    running.await.unwrap().unwrap();
}

/// Criterion 3: a source without delayed delivery refuses a delayed push and
/// stores nothing; a moment that has passed is an ordinary push.
#[tokio::test]
async fn source_without_delay_refuses_a_delayed_push() {
    let source = Arc::new(FaultySource::<u32>::new());
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        FaultyCodec::new(),
        task_fn(|_: u32| async {}),
    )
    .no_recovery()
    .build()
    .unwrap();
    let (_monitor, handle) = Monitor::new().register(queue).unwrap();

    let delayed = handle.push(Task::new(1).with_delay(HOUR)).await;
    assert!(
        matches!(delayed, Err(PushTaskError::DelayUnsupported)),
        "{delayed:?}"
    );
    assert_eq!(
        delayed.unwrap_err().to_string(),
        "source does not support delayed push"
    );

    let past = SystemTime::now() - Duration::from_secs(60);
    let pushed = handle
        .push(Task::new(2).with_deliver_at(past))
        .await
        .unwrap();
    assert!(matches!(pushed, PushOutcome::Enqueued { .. }));
    assert!(matches!(source.poll().await, Ok(Polled::Task { .. })));
    assert!(
        matches!(source.poll().await, Ok(Polled::Empty)),
        "only the ordinary push"
    );
}

/// A delayed task holds its place in the in-memory source and can be
/// cancelled before its moment.
#[tokio::test(start_paused = true)]
async fn delayed_task_counts_and_cancels() {
    let source = Arc::new(InMemorySource::<u32>::new(1).unwrap());
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(|_: u32| async {}),
    )
    .no_recovery()
    .build()
    .unwrap();
    let (_monitor, handle) = Monitor::new().register(queue).unwrap();

    let delayed = Task::new(1).with_id("later").with_delay(HOUR);
    assert!(matches!(
        handle.push(delayed).await.unwrap(),
        PushOutcome::Enqueued { .. }
    ));
    assert!(matches!(
        handle.push(Task::new(1).with_id("later")).await.unwrap(),
        PushOutcome::AlreadyRunning { .. }
    ));
    assert!(matches!(
        handle.push(Task::new(2)).await.unwrap(),
        PushOutcome::Rejected {
            reason: RejectReason::SourceFull,
            ..
        }
    ));
    assert_eq!(
        handle.cancel(&TaskId::new("later")).await,
        CancelOutcome::Cancelled
    );
    assert!(source.is_empty());
}
