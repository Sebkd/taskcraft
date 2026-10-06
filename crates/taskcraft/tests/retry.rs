//! Retries, pauses and defer, on virtual time.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![cfg(feature = "test-util")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::testing::{FaultyCodec, FaultySource};
use taskcraft::{
    AckPoint, Attempt, CancellationToken, IdentityCodec, InMemorySource, Monitor, Outcome, Queue,
    QueueReport, RetryPolicy, ShutdownReport, Source, Task, TaskId, task_fn,
};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

mod common;
use common::{Captured, run};

const SEC: Duration = Duration::from_secs(1);

type Faulty = Arc<FaultySource<u32>>;

fn policy(max_attempts: u32, base: Duration) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        base,
        factor: 2.0,
        max: Duration::from_secs(3600),
        jitter: 0.0,
        hold_slot: false,
    }
}

async fn until(limit: Duration, cond: impl Fn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

fn spawn(monitor: Monitor) -> (JoinHandle<ShutdownReport>, CancellationToken) {
    let stop = CancellationToken::new();
    (tokio::spawn(run(monitor, stop.clone())), stop)
}

/// (task argument, attempt number, virtual time since start) of every run.
type Runs = Arc<Mutex<Vec<(u32, u32, Duration)>>>;

fn record(runs: &Runs, start: Instant, args: u32, attempt: u32) {
    runs.lock().unwrap().push((args, attempt, start.elapsed()));
}

fn attempts(runs: &Runs) -> Vec<u32> {
    runs.lock().unwrap().iter().map(|r| r.1).collect()
}

fn only(report: &ShutdownReport) -> &QueueReport {
    &report.queues[0]
}

/// Criteria 4 and 39: retry, retry, success — attempts 1, 2, 3, paused 1 s
/// then 2 s; acked once, after the success and not during the pauses.
#[tokio::test(start_paused = true)]
async fn retries_until_success_and_acks_after() {
    let source: Faulty = Arc::default();
    let runs: Runs = Arc::default();
    let start = Instant::now();
    let r = Arc::clone(&runs);
    let handler = task_fn(move |n: u32, Attempt(attempt): Attempt| {
        record(&r, start, n, attempt);
        async move {
            if attempt < 3 {
                Outcome::retry("not yet")
            } else {
                Outcome::Success
            }
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), FaultyCodec::new(), handler)
        .retry_policy(policy(3, SEC))
        .ack_point(AckPoint::OnCompletion)
        .no_recovery()
        .build()
        .unwrap();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    source.enqueue(Task::new(7));

    assert!(until(2 * SEC, || attempts(&runs).len() == 2).await);
    assert_eq!(source.acks(), 0, "no ack during a retry pause");
    assert!(until(10 * SEC, || source.acks() == 1).await);
    let times: Vec<_> = runs.lock().unwrap().iter().map(|r| r.2.as_secs()).collect();
    assert_eq!(attempts(&runs), [1, 2, 3]);
    assert_eq!(times, [0, 1, 3]);
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 11: always "retry" — exactly M runs, then "attempts exhausted".
#[tokio::test(start_paused = true)]
async fn always_retry_runs_exactly_max_attempts() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Faulty = Arc::default();
    let runs: Runs = Arc::default();
    let start = Instant::now();
    let r = Arc::clone(&runs);
    let handler = task_fn(move |n: u32, Attempt(attempt): Attempt| {
        record(&r, start, n, attempt);
        async { Outcome::retry("still failing") }
    });
    let queue = Queue::builder("q", Arc::clone(&source), FaultyCodec::new(), handler)
        .retry_policy(policy(3, SEC))
        .no_recovery()
        .build()
        .unwrap();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    source.enqueue(Task::new(1));
    assert!(until(60 * SEC, || logs.count("task", "failed") == 1).await);
    sleep(60 * SEC).await;
    assert_eq!(attempts(&runs), [1, 2, 3]);
    assert_eq!(logs.count("task", "retry"), 2);
    let failed = logs
        .records()
        .into_iter()
        .find(|r| r.is("task", "failed"))
        .unwrap();
    assert!(failed.fields["message"].contains(r#"reason="attempts exhausted""#));
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 11, second half, and 7 b: without source support, defer runs
/// again in process after its delay and does not use up attempts.
#[tokio::test(start_paused = true)]
async fn defer_in_process_does_not_use_up_attempts() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Faulty = Arc::default();
    let runs: Runs = Arc::default();
    let start = Instant::now();
    let r = Arc::clone(&runs);
    let handler = task_fn(move |n: u32, Attempt(attempt): Attempt| {
        record(&r, start, n, attempt);
        async move {
            if attempt % 2 == 1 {
                Outcome::defer(30 * SEC, "not now")
            } else {
                Outcome::retry("failed")
            }
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), FaultyCodec::new(), handler)
        .retry_policy(policy(2, SEC))
        .no_recovery()
        .build()
        .unwrap();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    source.enqueue(Task::new(1));
    assert!(until(300 * SEC, || logs.count("task", "failed") == 1).await);

    // defer, retry (1 of 1 allowed), defer, retry → exhausted: 4 runs, not 2.
    assert_eq!(attempts(&runs), [1, 2, 3, 4]);
    let times: Vec<_> = runs.lock().unwrap().iter().map(|r| r.2.as_secs()).collect();
    assert_eq!(times, [0, 30, 31, 61], "defer waits its own delay");
    assert_eq!(logs.count("task", "defer_in_process"), 2);
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 7 a: a source that supports defer takes the task back and
/// delivers it again after the delay, with the attempt count kept.
#[tokio::test(start_paused = true)]
async fn defer_goes_to_a_source_that_supports_it() {
    let source = Arc::new(InMemorySource::<u32>::new(10));
    let runs: Runs = Arc::default();
    let start = Instant::now();
    let r = Arc::clone(&runs);
    let handler = task_fn(move |n: u32, Attempt(attempt): Attempt| {
        record(&r, start, n, attempt);
        async move {
            if attempt == 1 {
                Outcome::defer(60 * SEC, "later")
            } else {
                Outcome::Success
            }
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .no_recovery()
        .build()
        .unwrap();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    let id = TaskId::new("d");
    let _ = source
        .push(&id, Task::new(1).with_id(id.clone()))
        .await
        .unwrap();

    assert!(until(120 * SEC, || attempts(&runs).len() == 2).await);
    let rows = runs.lock().unwrap().clone();
    assert_eq!(rows.iter().map(|r| r.1).collect::<Vec<_>>(), [1, 2]);
    assert!(
        rows[1].2 >= 60 * SEC,
        "delivered again after the delay: {rows:?}"
    );
    assert!(until(SEC, || source.is_empty()).await);
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 12: shutdown during a 5 min pause ends it at once.
#[tokio::test(start_paused = true)]
async fn shutdown_ends_a_retry_pause_at_once() {
    let source: Faulty = Arc::default();
    let runs: Runs = Arc::default();
    let start = Instant::now();
    let r = Arc::clone(&runs);
    let handler = task_fn(move |n: u32, Attempt(attempt): Attempt| {
        record(&r, start, n, attempt);
        async { Outcome::retry("later") }
    });
    let queue = Queue::builder("q", Arc::clone(&source), FaultyCodec::new(), handler)
        .retry_policy(policy(3, 300 * SEC))
        .no_recovery()
        .build()
        .unwrap();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    source.enqueue(Task::new(1));
    assert!(until(SEC, || attempts(&runs).len() == 1).await);

    let at = Instant::now();
    stop.cancel();
    let report = monitor.await.unwrap();
    assert_eq!(at.elapsed(), Duration::ZERO);
    let q = only(&report);
    assert_eq!((q.completed, q.cancelled, q.aborted), (0, 1, 0), "{q:?}");
    assert_eq!(attempts(&runs), [1]);
}

/// Criterion 21 with a task in a retry pause: it is cancelled at once, the
/// running one after the timeout and grace.
#[tokio::test(start_paused = true)]
async fn shutdown_report_counts_a_task_in_a_pause() {
    let source: Faulty = Arc::default();
    let handler = task_fn(|n: u32| async move {
        if n == 0 {
            sleep(3600 * SEC).await;
            Outcome::Success
        } else {
            Outcome::retry("later")
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), FaultyCodec::new(), handler)
        .concurrency(2)
        .retry_policy(policy(3, 300 * SEC))
        .cancel_grace(SEC)
        .no_recovery()
        .build()
        .unwrap();
    let (monitor, stop) = spawn(
        Monitor::new()
            .shutdown_timeout(10 * SEC)
            .register(queue)
            .unwrap(),
    );
    source.enqueue(Task::new(0));
    source.enqueue(Task::new(1));
    assert!(until(SEC, || source.acks() == 2).await);
    sleep(SEC).await;

    let at = Instant::now();
    stop.cancel();
    let report = monitor.await.unwrap();
    assert_eq!(at.elapsed(), 11 * SEC);
    let q = only(&report);
    assert_eq!((q.completed, q.cancelled, q.aborted), (0, 2, 1), "{q:?}");
}

/// Change criterion 3: the slot is free during a pause unless the policy
/// holds it.
async fn second_task_start(hold_slot: bool) -> Duration {
    let source: Faulty = Arc::default();
    let runs: Runs = Arc::default();
    let start = Instant::now();
    let r = Arc::clone(&runs);
    let handler = task_fn(move |n: u32, Attempt(attempt): Attempt| {
        record(&r, start, n, attempt);
        async move {
            if n == 0 && attempt == 1 {
                Outcome::retry("once")
            } else {
                Outcome::Success
            }
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), FaultyCodec::new(), handler)
        .retry_policy(RetryPolicy {
            hold_slot,
            ..policy(2, 60 * SEC)
        })
        .no_recovery()
        .build()
        .unwrap();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    source.enqueue(Task::new(0));
    assert!(until(SEC, || attempts(&runs).len() == 1).await);
    source.enqueue(Task::new(1));
    assert!(until(120 * SEC, || runs.lock().unwrap().iter().any(|r| r.0 == 1)).await);
    let second = runs.lock().unwrap().iter().find(|r| r.0 == 1).unwrap().2;
    stop.cancel();
    monitor.await.unwrap();
    second
}

#[tokio::test(start_paused = true)]
async fn released_slot_lets_another_task_run_during_the_pause() {
    assert!(second_task_start(false).await < 60 * SEC);
}

#[tokio::test(start_paused = true)]
async fn held_slot_keeps_other_tasks_waiting_through_the_pause() {
    assert!(second_task_start(true).await >= 60 * SEC);
}
