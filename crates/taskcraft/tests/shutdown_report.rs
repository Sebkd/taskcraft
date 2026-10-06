//! The shutdown report counts only the tasks the shutdown found at work
//! (spec 2.7.6), on virtual time.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use taskcraft::{
    Cancel, CancelOutcome, CancellationToken, IdentityCodec, InMemorySource, Monitor, PollStrategy,
    Queue, QueueReport, StopReason, Task, TaskId, task_fn,
};
use tokio::time::sleep;

const SEC: Duration = Duration::from_secs(1);

async fn until(limit: Duration, cond: impl Fn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

fn counts(report: &QueueReport) -> (u32, u32, u32) {
    (report.completed, report.cancelled, report.aborted)
}

/// Task 1 waits for its cancel flag; task 2 works for `n` seconds.
async fn job(n: u32, cancel: Cancel) {
    if n == 1 {
        cancel.cancelled().await;
    } else {
        sleep(n * SEC).await;
    }
}

/// Criterion 50: a task that finished, and one cancelled on request, before
/// the stop signal — while the worker sleeps between polls — are not in the
/// report.
#[tokio::test(start_paused = true)]
async fn tasks_ended_before_the_signal_are_not_counted() {
    let source = Arc::new(InMemorySource::<u32>::default());
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), task_fn(job))
        .concurrency(2)
        .poll_strategy(PollStrategy::Interval(30 * SEC))
        .no_recovery()
        .build()
        .unwrap();
    let handle = queue.handle();
    let stop = CancellationToken::new();
    let running = tokio::spawn(Monitor::new().register(queue).unwrap().run(stop.clone()));
    let _ = handle.push(Task::new(0)).await.unwrap();
    let _ = handle.push(Task::new(1).with_id("waits")).await.unwrap();
    assert!(
        until(SEC, || handle.live_tasks() == 1).await,
        "task 0 is done"
    );
    assert_eq!(
        handle.cancel(&TaskId::new("waits")).await,
        CancelOutcome::CancelRequested
    );
    assert!(until(SEC, || handle.live_tasks() == 0).await);
    sleep(SEC).await;

    stop.cancel();
    let report = running.await.unwrap().unwrap();
    assert_eq!(counts(&report.queues[0]), (0, 0, 0), "{report:?}");
}

/// Change criterion 2: when the source closes, a task finished before is
/// not counted, one finishing during the drain is.
#[tokio::test(start_paused = true)]
async fn closed_source_counts_only_the_drained_task() {
    let source = Arc::new(InMemorySource::<u32>::default());
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), task_fn(job))
        .concurrency(2)
        .no_recovery()
        .build()
        .unwrap();
    let handle = queue.handle();
    let running = tokio::spawn(
        Monitor::new()
            .register(queue)
            .unwrap()
            .run(CancellationToken::new()),
    );
    let _ = handle.push(Task::new(0)).await.unwrap();
    let _ = handle.push(Task::new(10)).await.unwrap();
    assert!(
        until(SEC, || handle.live_tasks() == 1).await,
        "task 0 is done"
    );
    source.close();

    let report = running.await.unwrap().unwrap();
    let queue = &report.queues[0];
    assert!(
        matches!(queue.reason, StopReason::SourceClosed(_)),
        "{queue:?}"
    );
    assert_eq!(counts(queue), (1, 0, 0), "{queue:?}");
}
