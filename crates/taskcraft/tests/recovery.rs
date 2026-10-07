//! Recovery on start and attempt timeouts, on virtual time.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::codec::IdentityCodec;
use taskcraft::error::PushTaskError;
use taskcraft::{
    AckPoint, Attempt, BoxError, Cancel, CancellationToken, InMemorySource, Monitor, Outcome,
    Queue, RetryPolicy, StopReason, Task, TaskId, TimeoutOutcome, task_fn,
};
use tokio::time::{Instant, sleep};

mod common;
use common::{Captured, run};

const SEC: Duration = Duration::from_secs(1);

type Memory = Arc<InMemorySource<u32>>;
type Started = Arc<Mutex<Vec<(u32, u32)>>>;

async fn until(limit: Duration, cond: impl Fn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

fn started(log: &Started) -> Vec<u32> {
    log.lock().unwrap().iter().map(|r| r.0).collect()
}

fn recorded(log: &Started) -> impl Fn(u32, Attempt) -> std::future::Ready<()> + Clone + use<> {
    let log = Arc::clone(log);
    move |n, Attempt(attempt)| {
        log.lock().unwrap().push((n, attempt));
        std::future::ready(())
    }
}

fn three_tasks() -> Vec<Task<u32>> {
    (1..=3)
        .map(|n| Task::new(n).with_id(format!("r{n}")))
        .collect()
}

/// Criterion 26: the hook's tasks are accepted before the first poll and are
/// not acknowledged to the source.
#[tokio::test(start_paused = true)]
async fn recovered_tasks_run_before_polled_ones() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Memory = Arc::default();
    let log = Started::default();
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(recorded(&log)),
    )
    .recover_with(|| async { Ok::<_, BoxError>(three_tasks()) })
    .build()
    .unwrap();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let _ = handle.push(Task::new(10).with_id("polled")).await.unwrap();
    let stop = CancellationToken::new();
    let monitor = tokio::spawn(run(monitor, stop.clone()));

    assert!(until(SEC, || started(&log).len() == 4).await);
    assert_eq!(started(&log), [1, 2, 3, 10]);
    let recovered = logs
        .records()
        .into_iter()
        .find(|r| r.is("recovery", "recovered"))
        .unwrap();
    assert!(recovered.fields["message"].contains("count=3"));
    stop.cancel();
    // All four finished before the stop signal: not in the report (2.7.6).
    assert_eq!(monitor.await.unwrap().queues[0].completed, 0);
}

/// Criterion 26, second run: a failing hook keeps the monitor from starting.
#[tokio::test(start_paused = true)]
async fn failing_hook_keeps_the_monitor_from_starting() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Memory = Arc::default();
    let log = Started::default();
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(recorded(&log)),
    )
    .recover_with(|| async { Err::<Vec<Task<u32>>, _>("database is down") })
    .build()
    .unwrap();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let _ = handle.push(Task::new(1)).await.unwrap();

    let error = monitor.run(CancellationToken::new()).await.unwrap_err();
    assert_eq!(error.queue(), "q");
    assert_eq!(
        error.to_string(),
        "recovery failed: queue=q: database is down"
    );
    assert_eq!(logs.count("recovery", "failed"), 1);
    sleep(10 * SEC).await;
    assert!(started(&log).is_empty());
    assert_eq!(source.len(), 1, "the source was never polled");
    assert!(matches!(
        handle.push(Task::new(2)).await,
        Err(PushTaskError::Stopping)
    ));
}

/// Transition 2.4.2.11: the stop signal during the hook ends the run.
#[tokio::test(start_paused = true)]
async fn stop_during_the_hook_ends_the_run() {
    let source: Memory = Arc::default();
    let log = Started::default();
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(recorded(&log)),
    )
    .recover_with(|| async {
        sleep(3600 * SEC).await;
        Ok::<_, BoxError>(three_tasks())
    })
    .build()
    .unwrap();
    let stop = CancellationToken::new();
    let monitor = tokio::spawn(run(Monitor::new().register(queue).unwrap().0, stop.clone()));
    sleep(SEC).await;

    let at = Instant::now();
    stop.cancel();
    let report = monitor.await.unwrap();
    assert!(at.elapsed() < SEC);
    assert_eq!(report.queues.len(), 1);
    assert_eq!(report.queues[0].reason, StopReason::Shutdown);
    assert!(started(&log).is_empty());
}

/// Change criterion 2: with "reject" and no free slot, recovered tasks wait
/// for a slot instead of being rejected (rule 2.3.7 p. 6). A repeated id is
/// accepted once.
#[tokio::test(start_paused = true)]
async fn recovered_tasks_are_never_rejected() {
    let source: Memory = Arc::default();
    let log = Started::default();
    let rejected = Arc::new(AtomicU32::new(0));
    let seen = Arc::clone(&rejected);
    let record = Arc::clone(&log);
    let handler = task_fn(move |n: u32| {
        record.lock().unwrap().push((n, 1));
        async { sleep(10 * SEC).await }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .reject_with(move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
        })
        .recover_with(|| async {
            let mut tasks = three_tasks();
            tasks.push(Task::new(4).with_id("r1"));
            Ok::<_, BoxError>(tasks)
        })
        .build()
        .unwrap();
    let stop = CancellationToken::new();
    let monitor = tokio::spawn(run(Monitor::new().register(queue).unwrap().0, stop.clone()));

    assert!(until(60 * SEC, || started(&log).len() == 3).await);
    assert_eq!(started(&log), [1, 2, 3]);
    assert_eq!(rejected.load(Ordering::SeqCst), 0);
    stop.cancel();
    monitor.await.unwrap();
}

fn timeout_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        base: SEC,
        factor: 1.0,
        max: SEC,
        jitter: 0.0,
        hold_slot: false,
    }
}

/// Criterion 36: a 1 min timeout on a 2 min handler — the flag is set after
/// 1 min, the attempt is aborted after the grace, the task fails with "attempt
/// timed out".
#[tokio::test(start_paused = true)]
async fn attempt_timeout_fails_the_task() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Memory = Arc::default();
    let handler = task_fn(|_: u32| async {
        sleep(120 * SEC).await;
        Outcome::Success
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .attempt_timeout(60 * SEC)
        .cancel_grace(5 * SEC)
        .ack_point(AckPoint::OnCompletion)
        .build()
        .unwrap();
    let stop = CancellationToken::new();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let monitor = tokio::spawn(run(monitor, stop.clone()));
    let start = Instant::now();
    let _ = handle.push(Task::new(1)).await.unwrap();

    assert!(until(300 * SEC, || logs.count("task", "timed_out") == 1).await);
    assert_eq!(start.elapsed().as_secs(), 60);
    assert!(until(300 * SEC, || logs.count("task", "failed") == 1).await);
    assert_eq!(start.elapsed().as_secs(), 65);
    let failed = logs
        .records()
        .into_iter()
        .find(|r| r.is("task", "failed"))
        .unwrap();
    assert!(failed.fields["message"].contains(r#"reason="attempt timed out""#));
    assert_eq!(logs.count("task", "aborted"), 1);
    assert!(until(SEC, || source.is_empty()).await, "acked as failed");
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 46: a handler answering "retry" on the timeout flag still fails
/// with "attempt timed out", and is not retried.
#[tokio::test(start_paused = true)]
async fn timeout_decides_over_the_handler_answer() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Memory = Arc::default();
    let runs = Arc::new(AtomicU32::new(0));
    let count = Arc::clone(&runs);
    let handler = task_fn(move |_: u32, cancel: Cancel| {
        count.fetch_add(1, Ordering::SeqCst);
        async move {
            cancel.cancelled().await;
            Outcome::retry("interrupted")
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .attempt_timeout(60 * SEC)
        .retry_policy(timeout_policy())
        .no_recovery()
        .build()
        .unwrap();
    let stop = CancellationToken::new();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let monitor = tokio::spawn(run(monitor, stop.clone()));
    let start = Instant::now();
    let _ = handle.push(Task::new(1)).await.unwrap();

    assert!(until(300 * SEC, || logs.count("task", "failed") == 1).await);
    assert_eq!(start.elapsed().as_secs(), 60, "ended on its own, no grace");
    let failed = logs
        .records()
        .into_iter()
        .find(|r| r.is("task", "failed"))
        .unwrap();
    assert!(failed.fields["message"].contains(r#"reason="attempt timed out""#));
    sleep(60 * SEC).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(logs.count("task", "retry"), 0);
    stop.cancel();
    monitor.await.unwrap();
}

/// Timeout outcome "retry": the next attempt gets a fresh cancel flag and
/// its own timeout.
#[tokio::test(start_paused = true)]
async fn timeout_with_retry_runs_again() {
    let source: Memory = Arc::default();
    let log = Started::default();
    let record = Arc::clone(&log);
    let handler = task_fn(move |n: u32, Attempt(attempt): Attempt, cancel: Cancel| {
        record.lock().unwrap().push((n, attempt));
        async move {
            assert!(!cancel.is_cancelled(), "a fresh flag for every attempt");
            if attempt == 1 {
                sleep(3600 * SEC).await;
            }
            Outcome::Success
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .attempt_timeout(60 * SEC)
        .timeout_outcome(TimeoutOutcome::Retry)
        .cancel_grace(SEC)
        .retry_policy(timeout_policy())
        .no_recovery()
        .build()
        .unwrap();
    let stop = CancellationToken::new();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let monitor = tokio::spawn(run(monitor, stop.clone()));
    let id = TaskId::new("t");
    let _ = handle.push(Task::new(1).with_id("t")).await.unwrap();

    assert!(until(300 * SEC, || log.lock().unwrap().len() == 2).await);
    assert_eq!(*log.lock().unwrap(), [(1, 1), (1, 2)]);
    assert!(until(SEC, || handle.status(&id).is_none()).await);
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 22: with ack on accept, the task is acknowledged while it still
/// runs.
#[tokio::test(start_paused = true)]
async fn ack_on_accept_comes_before_completion() {
    let source: Memory = Arc::default();
    let handler = task_fn(|_: u32| async { sleep(60 * SEC).await });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .no_recovery()
        .build()
        .unwrap();
    let stop = CancellationToken::new();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let monitor = tokio::spawn(run(monitor, stop.clone()));
    let id = TaskId::new("a");
    let _ = handle.push(Task::new(1).with_id("a")).await.unwrap();

    assert!(until(SEC, || source.is_empty()).await, "acked");
    let status = handle.status(&id).unwrap();
    assert_eq!(status.state(), taskcraft::TaskState::Running);
    stop.cancel();
    monitor.await.unwrap();
}
