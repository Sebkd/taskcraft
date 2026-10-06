//! The task registry through the queue handle: push, status and cancel by
//! id, on virtual time.

use std::convert::Infallible;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::{
    AckPoint, AckPointSupport, Attempt, Cancel, CancelOutcome, CancellationToken, Capabilities,
    CodecError, IdentityCodec, InMemorySource, JsonCodec, MetadataRegistry, Monitor, Outcome,
    Polled, PushError, PushOutcome, PushResult, PushTaskError, Queue, RetryPolicy, ShutdownReport,
    Source, Task, TaskId, TaskState, task_fn,
};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

mod common;
use common::Captured;

const SEC: Duration = Duration::from_secs(1);

type Memory = Arc<InMemorySource<u32>>;

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
    (tokio::spawn(monitor.run(stop.clone())), stop)
}

fn retry_policy(max_attempts: u32, base: Duration) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        base,
        factor: 2.0,
        max: Duration::from_secs(3600),
        jitter: 0.0,
        hold_slot: false,
    }
}

fn counter() -> (Arc<AtomicU32>, Arc<AtomicU32>) {
    let runs = Arc::new(AtomicU32::new(0));
    (Arc::clone(&runs), runs)
}

fn runs(counter: &AtomicU32) -> u32 {
    counter.load(Ordering::SeqCst)
}

fn task(id: &str, n: u32) -> Task<u32> {
    Task::new(n).with_id(id)
}

/// Criterion 17: two concurrent pushes with one id — one task, one run.
#[tokio::test(start_paused = true)]
async fn concurrent_pushes_with_one_id_create_one_task() {
    let source: Memory = Arc::default();
    let (count, runs_seen) = counter();
    let handler = task_fn(move |_: u32| {
        count.fetch_add(1, Ordering::SeqCst);
        async { sleep(10 * SEC).await }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .build()
        .unwrap();
    let handle = queue.handle();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());

    let (first, second) = tokio::join!(handle.push(task("x", 1)), handle.push(task("x", 2)));
    let mut answers = [first.unwrap(), second.unwrap()];
    answers.sort_by_key(|a| matches!(a, PushOutcome::AlreadyRunning { .. }));
    assert_eq!(answers[0], PushOutcome::Enqueued { id: "x".into() });
    assert!(matches!(
        answers[1],
        PushOutcome::AlreadyRunning {
            state: TaskState::Queued,
            ..
        }
    ));

    assert!(until(SEC, || runs(&runs_seen) == 1).await);
    // While it runs, the registry answers for the id.
    assert_eq!(
        handle.push(task("x", 3)).await.unwrap(),
        PushOutcome::AlreadyRunning {
            id: "x".into(),
            state: TaskState::Running
        }
    );
    assert!(until(20 * SEC, || handle.live_tasks() == 0).await);
    assert_eq!(runs(&runs_seen), 1);
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 18: after many tasks with every kind of outcome the registry
/// holds nothing.
#[tokio::test(start_paused = true)]
async fn registry_is_empty_after_many_tasks() {
    const TASKS: u32 = 10_000;
    let source: Memory = Arc::default();
    let handler = task_fn(|n: u32, Attempt(attempt): Attempt| async move {
        match (n % 4, attempt) {
            (0, _) => Outcome::Success,
            (1, _) => Outcome::abort("no"),
            (2, _) => Outcome::retry("again"),
            (_, 1) => Outcome::defer(SEC, "later"),
            _ => Outcome::Success,
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .concurrency(16)
        .retry_policy(retry_policy(2, SEC))
        .ack_point(AckPoint::OnCompletion)
        .build()
        .unwrap();
    let handle = queue.handle();
    for n in 0..TASKS {
        let pushed = handle.push(Task::new(n)).await.unwrap();
        assert!(matches!(pushed, PushOutcome::Enqueued { .. }));
    }
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    assert!(until(600 * SEC, || source.is_empty()).await);
    assert_eq!(handle.live_tasks(), 0);
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 19 a: a handler watching its cancel flag stops on its own; the
/// task ends cancelled by request and is acknowledged.
#[tokio::test(start_paused = true)]
async fn cooperative_handler_stops_on_cancel() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Memory = Arc::default();
    let handler = task_fn(|_: u32, cancel: Cancel| async move {
        cancel.cancelled().await;
        Outcome::Success
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .ack_point(AckPoint::OnCompletion)
        .cancel_grace(30 * SEC)
        .build()
        .unwrap();
    let handle = queue.handle();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    let id = TaskId::new("c");
    let _ = handle.push(task("c", 1)).await.unwrap();
    assert!(
        until(SEC, || handle
            .status(&id)
            .is_some_and(|s| s.state() == TaskState::Running))
        .await
    );

    let at = Instant::now();
    assert_eq!(handle.cancel(&id).await, CancelOutcome::CancelRequested);
    assert_eq!(
        handle.cancel(&id).await,
        CancelOutcome::CancelRequested,
        "idempotent"
    );
    assert!(until(SEC, || source.is_empty()).await, "acked as cancelled");
    assert!(at.elapsed() < SEC);
    assert_eq!(handle.live_tasks(), 0);
    assert_eq!(logs.count("task", "cancel_requested"), 2);
    assert_eq!(logs.count("task", "aborted"), 0);
    let finished = logs
        .records()
        .into_iter()
        .find(|r| r.is("task", "finished"))
        .unwrap();
    assert!(finished.fields["message"].contains("outcome=cancelled"));
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 19 b: a handler that ignores the flag is aborted when the cancel
/// grace runs out.
#[tokio::test(start_paused = true)]
async fn ignoring_handler_is_aborted_after_the_grace() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Memory = Arc::default();
    let handler = task_fn(|_: u32| async {
        sleep(3600 * SEC).await;
        Outcome::Success
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .cancel_grace(5 * SEC)
        .build()
        .unwrap();
    let handle = queue.handle();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    let id = TaskId::new("stuck");
    let _ = handle.push(task("stuck", 1)).await.unwrap();
    assert!(until(SEC, || handle.live_tasks() == 1).await);
    sleep(SEC).await;

    let at = Instant::now();
    assert_eq!(handle.cancel(&id).await, CancelOutcome::CancelRequested);
    assert!(until(60 * SEC, || handle.live_tasks() == 0).await);
    assert_eq!(at.elapsed().as_secs(), 5);
    assert_eq!(logs.count("task", "aborted"), 1);

    // The queue goes on taking tasks.
    let _ = handle.push(task("next", 2)).await.unwrap();
    assert!(until(SEC, || handle.live_tasks() == 1).await);
    stop.cancel();
    monitor.await.unwrap();
}

/// Change criterion 1: a queued task of the in-memory source is removed and
/// never runs.
#[tokio::test(start_paused = true)]
async fn queued_task_is_removed_from_the_source() {
    let source: Memory = Arc::default();
    let (count, runs_seen) = counter();
    let handler = task_fn(move |_: u32| {
        count.fetch_add(1, Ordering::SeqCst);
        async {}
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .build()
        .unwrap();
    let handle = queue.handle();
    let id = TaskId::new("q1");
    let _ = handle.push(task("q1", 1)).await.unwrap();
    assert_eq!(handle.cancel(&id).await, CancelOutcome::Cancelled);
    assert!(source.is_empty());
    assert_eq!(handle.cancel(&id).await, CancelOutcome::Unknown);

    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    sleep(10 * SEC).await;
    assert_eq!(runs(&runs_seen), 0);
    stop.cancel();
    monitor.await.unwrap();
}

/// A task waiting to retry is cancelled at once and acknowledged.
#[tokio::test(start_paused = true)]
async fn cancel_during_a_retry_pause() {
    let source: Memory = Arc::default();
    let (count, runs_seen) = counter();
    let handler = task_fn(move |_: u32| {
        count.fetch_add(1, Ordering::SeqCst);
        async { Outcome::retry("later") }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .retry_policy(retry_policy(3, 300 * SEC))
        .ack_point(AckPoint::OnCompletion)
        .build()
        .unwrap();
    let handle = queue.handle();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    let id = TaskId::new("r");
    let _ = handle.push(task("r", 1)).await.unwrap();
    assert!(
        until(SEC, || {
            handle
                .status(&id)
                .is_some_and(|s| s.state() == TaskState::RetryWaiting)
        })
        .await
    );
    let status = handle.status(&id).unwrap();
    assert_eq!((status.attempt(), status.retries()), (1, 1));
    assert!(status.next_attempt_at().is_some() && status.attempt_started_at().is_none());

    let at = Instant::now();
    assert_eq!(handle.cancel(&id).await, CancelOutcome::Cancelled);
    assert!(until(SEC, || source.is_empty()).await);
    assert!(at.elapsed() < SEC);
    assert_eq!(handle.live_tasks(), 0);
    assert_eq!(runs(&runs_seen), 1);
    stop.cancel();
    monitor.await.unwrap();
}

/// A task waiting for a slot is cancelled at once and never runs.
#[tokio::test(start_paused = true)]
async fn cancel_while_waiting_for_a_slot() {
    let source: Memory = Arc::default();
    let started = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&started);
    let handler = task_fn(move |n: u32| {
        seen.lock().unwrap().push(n);
        async { sleep(60 * SEC).await }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .wait_limit(1)
        .build()
        .unwrap();
    let handle = queue.handle();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    let _ = handle.push(task("busy", 1)).await.unwrap();
    let _ = handle.push(task("waiting", 2)).await.unwrap();
    let waiting = TaskId::new("waiting");
    assert!(until(SEC, || handle.live_tasks() == 2).await);
    assert_eq!(
        handle.status(&waiting).unwrap().state(),
        TaskState::Accepted
    );

    assert_eq!(handle.cancel(&waiting).await, CancelOutcome::Cancelled);
    assert!(until(SEC, || handle.live_tasks() == 1).await);
    sleep(120 * SEC).await;
    assert_eq!(*started.lock().unwrap(), [1]);
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 38: the status of a running task, then unknown once finished.
#[tokio::test(start_paused = true)]
async fn status_of_a_running_task() {
    let source: Memory = Arc::default();
    let handler = task_fn(|_: u32| async { sleep(10 * SEC).await });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .build()
        .unwrap();
    let handle = queue.handle();
    let id = TaskId::new("s");
    assert!(handle.status(&id).is_none());
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    let _ = handle.push(task("s", 1)).await.unwrap();
    assert!(until(SEC, || handle.status(&id).is_some()).await);
    sleep(SEC).await;

    let status = handle.status(&id).unwrap();
    assert_eq!(status.id(), &id);
    assert_eq!((status.state(), status.attempt()), (TaskState::Running, 1));
    assert!(status.accepted_at().is_some() && status.attempt_started_at().is_some());
    assert!(status.next_attempt_at().is_none() && status.reason().is_none());

    assert!(until(20 * SEC, || handle.status(&id).is_none()).await);
    assert_eq!(handle.cancel(&id).await, CancelOutcome::Unknown);
    stop.cancel();
    monitor.await.unwrap();
}

/// Duplicate delivery of a live task's id: acknowledged, not run (scenario
/// 2.2.3).
#[tokio::test(start_paused = true)]
async fn duplicate_from_a_poll_is_acked_and_not_run() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Memory = Arc::default();
    let (count, runs_seen) = counter();
    let handler = task_fn(move |_: u32| {
        count.fetch_add(1, Ordering::SeqCst);
        async { sleep(10 * SEC).await }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .concurrency(2)
        .build()
        .unwrap();
    let handle = queue.handle();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    let id = TaskId::new("d");
    let _ = handle.push(task("d", 1)).await.unwrap();
    assert!(until(SEC, || runs(&runs_seen) == 1).await);

    // Acked on accept, so the source takes the same id again directly.
    let stored = source.push(&id, task("d", 2)).await.unwrap();
    assert_eq!(stored, PushResult::Stored);
    assert!(until(SEC, || source.is_empty()).await);
    assert_eq!(logs.count("task", "duplicate"), 1);
    sleep(30 * SEC).await;
    assert_eq!(runs(&runs_seen), 1);
    stop.cancel();
    monitor.await.unwrap();
}

/// A source of JSON bytes, with or without push support.
#[derive(Default)]
struct BytesSource {
    push: bool,
    stored: Mutex<Vec<Vec<u8>>>,
}

impl Source for BytesSource {
    type Message = Vec<u8>;
    type Receipt = ();
    type Error = Infallible;

    fn capabilities(&self) -> Capabilities {
        let caps = Capabilities::new(AckPointSupport::QueueOnly);
        if self.push { caps.with_push() } else { caps }
    }

    async fn poll(&self) -> Result<Polled<Vec<u8>, ()>, Infallible> {
        Ok(Polled::Empty)
    }

    async fn ack(&self, (): ()) -> Result<(), Infallible> {
        Ok(())
    }

    async fn push(
        &self,
        _: &TaskId,
        message: Vec<u8>,
    ) -> Result<PushResult, PushError<Infallible>> {
        self.stored.lock().unwrap().push(message);
        Ok(PushResult::Stored)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Recipient(String);

fn bytes_queue(source: &Arc<BytesSource>) -> Queue<BytesSource, JsonCodec, impl Clone + Send, u32> {
    Queue::builder(
        "bytes",
        Arc::clone(source),
        JsonCodec::new(MetadataRegistry::new()),
        task_fn(|_: u32| async {}),
    )
    .build()
    .unwrap()
}

/// Criterion 28: metadata of an unregistered type fails the push and stores
/// nothing.
#[tokio::test]
async fn unregistered_metadata_fails_the_push() {
    let source = Arc::new(BytesSource {
        push: true,
        ..BytesSource::default()
    });
    let handle = bytes_queue(&source).handle();
    let pushed = handle
        .push(Task::new(1).with_meta(Recipient("r".into())))
        .await;
    assert!(
        matches!(pushed, Err(PushTaskError::Encode(CodecError::Metadata(_)))),
        "{pushed:?}"
    );
    assert!(source.stored.lock().unwrap().is_empty());
    assert!(handle.push(Task::new(1)).await.is_ok());
    assert_eq!(source.stored.lock().unwrap().len(), 1);
}

/// Criterion 42: a source without push support, a task overriding the ack
/// point of a log source, and a push after shutdown.
#[tokio::test(start_paused = true)]
async fn push_errors() {
    let source = Arc::new(BytesSource::default());
    let handle = bytes_queue(&source).handle();
    assert!(matches!(
        handle.push(Task::new(1)).await,
        Err(PushTaskError::Unsupported)
    ));

    let source = Arc::new(BytesSource {
        push: true,
        ..BytesSource::default()
    });
    let handle = bytes_queue(&source).handle();
    let overriding = Task::new(1).with_ack_point(AckPoint::OnCompletion);
    assert!(matches!(
        handle.push(overriding).await,
        Err(PushTaskError::AckOverride(_))
    ));

    let source: Memory = Arc::default();
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(|_: u32| async {}),
    )
    .build()
    .unwrap();
    let handle = queue.handle();
    let (monitor, stop) = spawn(Monitor::new().register(queue).unwrap());
    stop.cancel();
    monitor.await.unwrap();
    let pushed = handle.push(Task::new(1)).await;
    assert!(matches!(pushed, Err(PushTaskError::Stopping)), "{pushed:?}");
    assert_eq!(pushed.unwrap_err().to_string(), "queue is stopping");
    assert!(source.is_empty());
}
