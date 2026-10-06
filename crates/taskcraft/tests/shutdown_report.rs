//! The shutdown report counts only the tasks the shutdown found at work
//! (spec 2.7.6), on virtual time.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use taskcraft::{
    Cancel, CancelOutcome, CancellationToken, IdentityCodec, InMemorySource, Monitor, PollStrategy,
    Queue, QueueReport, Source, StopReason, Task, TaskId, task_fn,
};
use tokio::time::sleep;

mod common;

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

/// Criterion 51: after a closed source the drain waits for its tasks, but a
/// shutdown signal starts the shutdown timeout and the cancel grace.
#[tokio::test(start_paused = true)]
async fn shutdown_signal_ends_a_closed_source_drain() {
    let source = Arc::new(InMemorySource::<u32>::default());
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), task_fn(job))
        .cancel_grace(2 * SEC)
        .no_recovery()
        .build()
        .unwrap();
    let handle = queue.handle();
    let stop = CancellationToken::new();
    let monitor = Monitor::new().shutdown_timeout(5 * SEC);
    let running = tokio::spawn(monitor.register(queue).unwrap().run(stop.clone()));
    let _ = handle.push(Task::new(3600)).await.unwrap();
    assert!(until(SEC, || handle.live_tasks() == 1).await);
    source.close();
    sleep(10 * SEC).await;
    assert!(
        !running.is_finished(),
        "the closed source drain waits for the task"
    );

    let at = tokio::time::Instant::now();
    stop.cancel();
    let report = running.await.unwrap().unwrap();
    assert_eq!(at.elapsed(), 7 * SEC);
    assert_eq!(counts(&report.queues[0]), (0, 1, 1), "{report:?}");
}

/// The observer's view of final states.
#[derive(Default)]
struct Finals(
    std::sync::Mutex<
        Vec<(
            String,
            taskcraft::TaskState,
            Option<taskcraft::FinishReason>,
        )>,
    >,
);

impl taskcraft::Observer for Finals {
    fn on_event(&self, event: &taskcraft::Event<'_>) {
        if let taskcraft::Event::Finished {
            task_id,
            state,
            reason,
            ..
        } = event
        {
            let entry = (task_id.as_str().to_owned(), *state, reason.cloned());
            self.0.lock().unwrap().push(entry);
        }
    }
}

/// Criterion 52: a task aborted after the cancel grace has an outcome:
/// cancelled by shutdown, as an event and in the log.
#[tokio::test(start_paused = true)]
async fn aborted_task_has_an_outcome() {
    let logs = common::Captured::default();
    let _guard = logs.install();
    let source = Arc::new(InMemorySource::<u32>::default());
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), task_fn(job))
        .cancel_grace(SEC)
        .no_recovery()
        .build()
        .unwrap();
    let handle = queue.handle();
    let finals = Arc::new(Finals::default());
    let monitor = Monitor::new()
        .shutdown_timeout(SEC)
        .observer(Arc::clone(&finals))
        .register(queue)
        .unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));
    let _ = handle
        .push(Task::new(3600).with_id("stubborn"))
        .await
        .unwrap();
    assert!(until(SEC, || handle.live_tasks() == 1).await);
    stop.cancel();
    let report = running.await.unwrap().unwrap();
    assert_eq!(counts(&report.queues[0]), (0, 1, 1));

    let finals = finals.0.lock().unwrap().clone();
    assert_eq!(
        finals,
        [(
            "stubborn".to_owned(),
            taskcraft::TaskState::Cancelled,
            Some(taskcraft::FinishReason::CancelledByShutdown)
        )]
    );
    let finished = logs
        .records()
        .into_iter()
        .find(|r| r.is("task", "finished"))
        .unwrap();
    assert!(finished.fields["message"].contains("outcome=cancelled"));
    assert_eq!(logs.count("task", "aborted"), 1);
}

/// An in-memory source whose pushes wait for the test to let them through.
struct GatedSource {
    inner: InMemorySource<u32>,
    gate: tokio::sync::Semaphore,
}

impl Source for GatedSource {
    type Message = Task<u32>;
    type Receipt = taskcraft::Delivery;
    type Error = std::convert::Infallible;

    fn capabilities(&self) -> taskcraft::Capabilities {
        self.inner.capabilities()
    }

    async fn poll(&self) -> Result<taskcraft::Polled<Task<u32>, taskcraft::Delivery>, Self::Error> {
        self.inner.poll().await
    }

    async fn ack(&self, receipt: taskcraft::Delivery) -> Result<(), Self::Error> {
        self.inner.ack(receipt).await
    }

    fn subscribe(&self) -> Option<taskcraft::WakeSignal> {
        self.inner.subscribe()
    }

    async fn push(
        &self,
        id: &TaskId,
        message: Task<u32>,
    ) -> Result<taskcraft::PushResult, taskcraft::PushError<Self::Error>> {
        let _pass = self.gate.acquire().await.unwrap();
        self.inner.push(id, message).await
    }

    async fn remove(&self, id: &TaskId) -> Result<taskcraft::Withdrawal, Self::Error> {
        self.inner.remove(id).await
    }
}

/// Criterion 53: a push whose write races with the shutdown either reaches
/// the worker or fails with "queue is stopping" and leaves nothing behind.
#[tokio::test(start_paused = true)]
async fn push_racing_the_shutdown_leaves_nothing_behind() {
    let source = Arc::new(GatedSource {
        inner: InMemorySource::default(),
        gate: tokio::sync::Semaphore::new(0),
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), task_fn(job))
        .no_recovery()
        .build()
        .unwrap();
    let handle = queue.handle();
    let stop = CancellationToken::new();
    let running = tokio::spawn(Monitor::new().register(queue).unwrap().run(stop.clone()));
    sleep(SEC).await;

    // The push passes the "stopping?" check, then waits inside the write.
    let pushing = tokio::spawn({
        let handle = handle.clone();
        async move { handle.push(Task::new(0)).await }
    });
    sleep(SEC).await;
    stop.cancel();
    running.await.unwrap().unwrap();
    source.gate.add_permits(1);

    let pushed = pushing.await.unwrap();
    assert!(
        matches!(pushed, Err(taskcraft::PushTaskError::Stopping)),
        "{pushed:?}"
    );
    assert!(source.inner.is_empty(), "nothing left where nobody polls");
}
