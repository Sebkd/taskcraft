//! Failed tasks: the failed-task hook and requeue (change failed-tasks,
//! criteria 1 and 2; criterion 4 for a source without history).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::codec::IdentityCodec;
use taskcraft::error::RequeueError;
use taskcraft::observe::{Event, Observer};
use taskcraft::testing::{FaultyCodec, FaultySource};
use taskcraft::{
    AckPoint, CancelOutcome, CancellationToken, FailedTask, FinishReason, InMemorySource, Monitor,
    Outcome, Queue, RetryPolicy, Task, TaskId, TaskState, task_fn,
};
use tokio::time::sleep;

mod common;

const SEC: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq)]
struct Customer(&'static str);

/// What the hook saw: id, args, state, reason, customer metadata.
type Seen = Arc<Mutex<Vec<(String, u32, TaskState, FinishReason, Option<Customer>)>>>;

async fn until(limit: Duration, cond: impl Fn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

/// 0 aborts, 1 retries until out of attempts, 2 panics, 3 succeeds, 4 waits
/// to be cancelled.
async fn work(n: u32, cancel: taskcraft::Cancel) -> Outcome {
    match n {
        0 => Outcome::abort("bad input"),
        1 => Outcome::retry("busy"),
        2 => panic!("boom"),
        4 => {
            cancel.cancelled().await;
            Outcome::Success
        }
        _ => Outcome::Success,
    }
}

/// Criterion 1: aborted, exhausted and panicked tasks reach the hook with
/// their arguments, metadata and reason; succeeded and cancelled ones do
/// not.
#[tokio::test(start_paused = true)]
async fn failed_and_panicked_tasks_reach_the_hook() {
    let seen: Seen = Arc::default();
    let record = Arc::clone(&seen);
    let queue = Queue::builder(
        "billing",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(work),
    )
    .concurrency(5)
    .retry_policy(RetryPolicy {
        max_attempts: 2,
        base: SEC,
        jitter: 0.0,
        ..RetryPolicy::default()
    })
    .failed_task(move |failed: FailedTask<u32>| {
        let customer = failed.task.metadata().get::<Customer>().cloned();
        record.lock().unwrap().push((
            failed.task.id().as_str().to_owned(),
            *failed.task.args(),
            failed.state,
            failed.reason,
            customer,
        ));
        assert_eq!(failed.queue, "billing");
    })
    .no_recovery()
    .build()
    .unwrap();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));

    for n in 0..5 {
        let task = Task::new(n)
            .with_id(format!("t{n}"))
            .with_meta(Customer("acme"));
        let _ = handle.push(task).await.unwrap();
    }
    assert!(until(10 * SEC, || handle.live_tasks() == 1).await);
    assert_eq!(
        handle.cancel(&TaskId::new("t4")).await,
        CancelOutcome::CancelRequested
    );
    assert!(until(10 * SEC, || handle.live_tasks() == 0).await);

    let mut seen = seen.lock().unwrap().clone();
    seen.sort_by(|a, b| a.0.cmp(&b.0));
    let acme = Some(Customer("acme"));
    assert_eq!(
        seen,
        [
            (
                "t0".to_owned(),
                0,
                TaskState::Failed,
                FinishReason::Handler("bad input".to_owned()),
                acme.clone()
            ),
            (
                "t1".to_owned(),
                1,
                TaskState::Failed,
                FinishReason::AttemptsExhausted,
                acme.clone()
            ),
            (
                "t2".to_owned(),
                2,
                TaskState::Panicked,
                FinishReason::Panic("boom".to_owned()),
                acme
            ),
        ]
    );
    stop.cancel();
    running.await.unwrap().unwrap();
}

/// Counts finished events by state.
#[derive(Default)]
struct Finals(Mutex<Vec<TaskState>>);

impl Observer for Finals {
    fn on_event(&self, event: &Event<'_>) {
        if let Event::Finished { state, .. } = event {
            self.0.lock().unwrap().push(*state);
        }
    }
}

/// Criterion 2: a panicking hook is logged; the task still ends failed and
/// is acknowledged.
#[tokio::test(start_paused = true)]
async fn panicking_hook_changes_nothing() {
    let logs = common::Captured::default();
    let _guard = logs.install();
    let source = Arc::new(FaultySource::<u32>::new());
    let queue = Queue::builder("q", Arc::clone(&source), FaultyCodec::new(), task_fn(work))
        .ack_point(AckPoint::OnCompletion)
        .failed_task(|_: FailedTask<u32>| panic!("the error topic is down"))
        .no_recovery()
        .build()
        .unwrap();
    let finals = Arc::new(Finals::default());
    let (monitor, _handle) = Monitor::new()
        .observer(Arc::clone(&finals))
        .register(queue)
        .unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));
    source.enqueue(Task::new(0).with_id("bad"));

    assert!(until(10 * SEC, || source.acks() == 1).await);
    assert_eq!(*finals.0.lock().unwrap(), [TaskState::Failed]);
    assert_eq!(logs.count("task", "failed_hook_failed"), 1);
    let record = logs
        .records()
        .into_iter()
        .find(|r| r.is("task", "failed_hook_failed"))
        .unwrap();
    assert_eq!(record.level, tracing::Level::ERROR);
    assert!(record.fields["message"].contains("task_id=bad"));
    stop.cancel();
    running.await.unwrap().unwrap();
}

/// Criterion 4 without a task store: a live task is not failed; a finished
/// one is unknown — the in-memory source keeps no history.
#[tokio::test(start_paused = true)]
async fn requeue_without_a_store() {
    let queue = Queue::builder(
        "q",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(work),
    )
    .concurrency(2)
    .no_recovery()
    .build()
    .unwrap();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));

    let _ = handle.push(Task::new(4).with_id("live")).await.unwrap();
    assert!(until(10 * SEC, || handle.live_tasks() == 1).await);
    let live = handle.requeue(&TaskId::new("live")).await;
    assert!(
        matches!(live, Err(RequeueError::NotFailed(TaskState::Running))),
        "{live:?}"
    );
    let _ = handle.push(Task::new(0).with_id("bad")).await.unwrap();
    sleep(SEC).await;
    let gone = handle.requeue(&TaskId::new("bad")).await;
    assert!(matches!(gone, Err(RequeueError::Unknown)), "{gone:?}");
    stop.cancel();
    running.await.unwrap().unwrap();
}
