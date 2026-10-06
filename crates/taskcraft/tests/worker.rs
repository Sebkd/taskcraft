//! The worker end to end, on virtual time.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![cfg(feature = "test-util")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::testing::{
    FaultyCodec, FaultySource, InjectedError, Runner, RunnerSetup, Scripted, scenarios,
};
use taskcraft::{
    AckPoint, BoxFuture, CancellationToken, Capabilities, Monitor, Outcome, PollStrategy, Polled,
    PushError, PushResult, QueueReport, RetryPolicy, ShutdownReport, Source, StopReason, Task,
    TaskError, TaskId, TaskRequest, WakeSignal, task_fn,
};
use taskcraft::{Queue, QueueBuilder};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

mod common;
use common::Captured;

const SEC: Duration = Duration::from_secs(1);

// ------------------------------------------------------------------- helpers

type Faulty = Arc<FaultySource<u32>>;

fn start<Svc>(
    builder: QueueBuilder<FaultySource<u32>, FaultyCodec<u32>, Svc, u32>,
    monitor: Monitor,
) -> (JoinHandle<ShutdownReport>, CancellationToken)
where
    Svc: tower::Service<TaskRequest<u32>, Response = Outcome> + Clone + Send + 'static,
    Svc::Error: Into<taskcraft::BoxError>,
    Svc::Future: Send,
{
    let queue = builder.no_recovery().build().unwrap();
    let stop = CancellationToken::new();
    let monitor = monitor.register(queue).unwrap();
    let handle = tokio::spawn(common::run(monitor, stop.clone()));
    (handle, stop)
}

fn queue_of<Svc>(
    source: &Faulty,
    service: Svc,
) -> QueueBuilder<FaultySource<u32>, FaultyCodec<u32>, Svc, u32> {
    Queue::builder("q", Arc::clone(source), FaultyCodec::new(), service)
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

fn counter() -> Arc<AtomicU32> {
    Arc::new(AtomicU32::new(0))
}

fn get(c: &AtomicU32) -> u32 {
    c.load(Ordering::SeqCst)
}

fn only(report: &ShutdownReport) -> &QueueReport {
    assert_eq!(report.queues.len(), 1);
    &report.queues[0]
}

// --------------------------------------------------------------------- tests

/// Criterion 1: a thousand "empty" answers, then a task.
#[tokio::test(start_paused = true)]
async fn many_empty_polls_never_end_the_worker() {
    let source: Faulty = Arc::default();
    let runs = counter();
    let r = Arc::clone(&runs);
    let builder = queue_of(
        &source,
        task_fn(move |_: u32| {
            let r = Arc::clone(&r);
            async move {
                r.fetch_add(1, Ordering::SeqCst);
            }
        }),
    )
    .poll_strategy(PollStrategy::Interval(Duration::from_millis(10)));
    let (worker, stop) = start(builder, Monitor::new());

    assert!(until(60 * SEC, || source.polls() >= 1000).await);
    assert!(!worker.is_finished());
    source.enqueue(Task::new(1));
    assert!(until(SEC, || get(&runs) == 1).await);
    stop.cancel();
    assert_eq!(only(&worker.await.unwrap()).reason, StopReason::Shutdown);
}

/// Criterion 2: a closed source stops the worker with its reason, logged.
#[tokio::test(start_paused = true)]
async fn closed_source_stops_the_worker_with_its_reason() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Faulty = Arc::default();
    source.close();
    let (worker, _stop) = start(
        queue_of(&source, task_fn(|_: u32| async {})),
        Monitor::new(),
    );
    let report = worker.await.unwrap();
    assert!(matches!(only(&report).reason, StopReason::SourceClosed(_)));
    assert_eq!(logs.count("source", "closed"), 1);
    assert_eq!(logs.count("worker", "stopped"), 1);
    let closed = logs
        .records()
        .into_iter()
        .find(|r| r.is("source", "closed"))
        .unwrap();
    assert_eq!(closed.level, tracing::Level::WARN);
}

/// Criteria 3 and 39: success; with ack on completion the ack comes after the
/// outcome and not before.
#[tokio::test(start_paused = true)]
async fn ack_on_completion_follows_the_outcome() {
    let source: Faulty = Arc::default();
    let builder = queue_of(&source, task_fn(|_: u32| async { sleep(10 * SEC).await }))
        .ack_point(AckPoint::OnCompletion);
    let (worker, stop) = start(builder, Monitor::new());
    source.enqueue(Task::new(1));

    assert!(until(SEC, || source.in_flight() == 1).await);
    sleep(5 * SEC).await;
    assert_eq!(source.acks(), 0, "no ack while the task runs");
    assert!(until(10 * SEC, || source.acks() == 1).await);
    assert_eq!(source.in_flight(), 0);
    stop.cancel();
    worker.await.unwrap();
}

/// Ack on accept comes before the task finishes.
#[tokio::test(start_paused = true)]
async fn ack_on_accept_comes_first() {
    let source: Faulty = Arc::default();
    let (worker, stop) = start(
        queue_of(&source, task_fn(|_: u32| async { sleep(10 * SEC).await })),
        Monitor::new(),
    );
    source.enqueue(Task::new(1));
    assert!(until(SEC, || source.acks() == 1).await);
    stop.cancel();
    worker.await.unwrap();
}

/// Criterion 8: a poison message goes to the dead-letter hook and is acked;
/// the next task runs.
#[tokio::test(start_paused = true)]
async fn poison_goes_to_dead_letter_and_work_goes_on() {
    let source: Faulty = Arc::default();
    let letters = Arc::new(Mutex::new(Vec::new()));
    let runs = counter();
    let (l, r) = (Arc::clone(&letters), Arc::clone(&runs));
    let builder = queue_of(
        &source,
        task_fn(move |_: u32| {
            let r = Arc::clone(&r);
            async move {
                r.fetch_add(1, Ordering::SeqCst);
            }
        }),
    )
    .dead_letter(move |letter| l.lock().unwrap().push(letter.error.to_string()));
    let (worker, stop) = start(builder, Monitor::new());

    source.inject_poison("not json");
    source.enqueue(Task::new(1));
    assert!(until(SEC, || get(&runs) == 1).await);
    assert_eq!(
        *letters.lock().unwrap(),
        ["message could not be decoded: not json"]
    );
    assert_eq!(source.acks(), 2);
    stop.cancel();
    worker.await.unwrap();
}

/// A source that records when it was polled.
#[derive(Default)]
struct Timed {
    inner: FaultySource<u32>,
    polls: Mutex<Vec<Instant>>,
}

impl Source for Timed {
    type Message = Scripted<u32>;
    type Receipt = u64;
    type Error = InjectedError;

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn poll(&self) -> Result<Polled<Scripted<u32>, u64>, InjectedError> {
        self.polls.lock().unwrap().push(Instant::now());
        self.inner.poll().await
    }

    async fn ack(&self, receipt: u64) -> Result<(), InjectedError> {
        self.inner.ack(receipt).await
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        self.inner.subscribe()
    }

    async fn push(
        &self,
        id: &TaskId,
        message: Scripted<u32>,
    ) -> Result<PushResult, PushError<InjectedError>> {
        self.inner.push(id, message).await
    }
}

/// Criteria 9 and 10: source errors restart the intake after 1, 2, 4 s; a
/// success resets the count; a running task is not interrupted.
#[tokio::test(start_paused = true)]
async fn source_errors_restart_with_growing_delays() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source = Arc::new(Timed::default());
    let done = counter();
    let d = Arc::clone(&done);
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        FaultyCodec::new(),
        task_fn(move |_: u32| {
            let d = Arc::clone(&d);
            async move {
                sleep(30 * SEC).await;
                d.fetch_add(1, Ordering::SeqCst);
            }
        }),
    )
    .concurrency(2)
    .poll_strategy(PollStrategy::Interval(Duration::from_millis(100)))
    .no_recovery()
    .build()
    .unwrap();
    let stop = CancellationToken::new();
    let monitor = Monitor::new().register(queue).unwrap();
    let worker = tokio::spawn(common::run(monitor, stop.clone()));

    source.inner.enqueue(Task::new(1));
    // Acked on accept: the task is running from here on.
    assert!(until(SEC, || source.inner.acks() == 1).await);
    source.polls.lock().unwrap().clear();
    source.inner.fail_next_polls(3);
    sleep(20 * SEC).await;

    let polls = source.polls.lock().unwrap().clone();
    let gaps: Vec<_> = polls
        .windows(2)
        .take(3)
        .map(|w| (w[1] - w[0]).as_secs())
        .collect();
    assert_eq!(gaps, [1, 2, 4], "restart delays");
    assert_eq!(logs.count("source", "failed"), 3);
    assert_eq!(logs.count("worker", "resumed"), 3);

    // After a successful poll the count starts over.
    source.polls.lock().unwrap().clear();
    source.inner.fail_next_polls(1);
    sleep(5 * SEC).await;
    let polls = source.polls.lock().unwrap().clone();
    let failed_at = polls[0];
    assert_eq!((polls[1] - failed_at).as_secs(), 1);

    assert!(
        until(30 * SEC, || get(&done) == 1).await,
        "the running task finished"
    );
    stop.cancel();
    worker.await.unwrap();
}

/// Criterion 21 (running tasks): shutdown waits the timeout, then cancels,
/// then aborts after the grace; the report counts it.
#[tokio::test(start_paused = true)]
async fn shutdown_cancels_then_aborts_a_task_that_ignores_cancellation() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Faulty = Arc::default();
    let builder = queue_of(&source, task_fn(|_: u32| async { sleep(3600 * SEC).await }))
        .cancel_grace(5 * SEC);
    let (worker, stop) = start(builder, Monitor::new().shutdown_timeout(10 * SEC));
    source.enqueue(Task::new(1).with_id("hour"));
    assert!(until(SEC, || source.acks() == 1).await);

    let started = Instant::now();
    stop.cancel();
    let report = worker.await.unwrap();
    assert_eq!(started.elapsed(), 15 * SEC);
    let q = only(&report);
    assert_eq!((q.completed, q.cancelled, q.aborted), (0, 1, 1));
    assert_eq!(logs.count("task", "aborted"), 1);
    assert_eq!(logs.count("shutdown", "started"), 1);
    assert_eq!(logs.count("shutdown", "finished"), 1);
    let aborted = logs
        .records()
        .into_iter()
        .find(|r| r.is("task", "aborted"))
        .unwrap();
    assert!(aborted.fields["message"].contains("task_id=hour"));
}

/// A task that finishes within the shutdown timeout counts as completed.
#[tokio::test(start_paused = true)]
async fn shutdown_lets_running_tasks_finish_in_time() {
    let source: Faulty = Arc::default();
    let builder = queue_of(&source, task_fn(|_: u32| async { sleep(3 * SEC).await }))
        .ack_point(AckPoint::OnCompletion);
    let (worker, stop) = start(builder, Monitor::new().shutdown_timeout(10 * SEC));
    source.enqueue(Task::new(1));
    assert!(until(SEC, || source.in_flight() == 1).await);
    stop.cancel();
    let q = only(&worker.await.unwrap()).clone();
    assert_eq!((q.completed, q.cancelled, q.aborted), (1, 0, 0));
    assert_eq!(source.acks(), 1, "acked on completion during drain");
}

/// Criterion 25 and change criterion 2: an ack that fails after completion
/// does not change the outcome; the intake restarts, also when idle.
#[tokio::test(start_paused = true)]
async fn failed_ack_restarts_the_intake_without_changing_the_outcome() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Faulty = Arc::default();
    let runs = counter();
    let r = Arc::clone(&runs);
    let builder = queue_of(
        &source,
        task_fn(move |_: u32| {
            let r = Arc::clone(&r);
            async move {
                r.fetch_add(1, Ordering::SeqCst);
            }
        }),
    )
    .ack_point(AckPoint::OnCompletion);
    let (worker, stop) = start(builder, Monitor::new());

    source.fail_next_acks(1);
    source.enqueue(Task::new(1).with_id("t"));
    // The worker is idle once the only task ran; the failed ack arrives then.
    assert!(until(10 * SEC, || logs.count("worker", "resumed") == 1).await);
    assert_eq!(
        logs.count("task", "failed"),
        0,
        "the outcome stays a success"
    );
    assert_eq!(logs.count("source", "failed"), 1);
    // The failed ack put the task back: at-least-once redelivery.
    assert!(until(10 * SEC, || get(&runs) == 2).await);
    assert_eq!(source.redeliveries(), 1);
    stop.cancel();
    worker.await.unwrap();
}

/// Panic, abort and retry end the task; the intake goes on.
#[tokio::test(start_paused = true)]
async fn final_outcomes_are_logged_and_intake_goes_on() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Faulty = Arc::default();
    let service = task_fn(|n: u32| async move {
        match n {
            0 => panic!("boom"),
            1 => Err(TaskError::abort(std::io::Error::other("bad"))),
            2 => Ok(Outcome::retry("later")),
            _ => Ok(Outcome::Success),
        }
    });
    let (worker, stop) = start(queue_of(&source, service), Monitor::new());
    for n in 0..4 {
        source.enqueue(Task::new(n));
    }
    assert!(until(10 * SEC, || source.acks() == 4).await);
    assert_eq!(logs.count("task", "panicked"), 1);
    assert_eq!(logs.count("task", "failed"), 2);
    let failed: Vec<_> = logs
        .records()
        .into_iter()
        .filter(|r| r.is("task", "failed"))
        .map(|r| r.fields["message"].clone())
        .collect();
    assert!(
        failed.iter().any(|m| m.contains(r#"reason="bad""#)),
        "{failed:?}"
    );
    assert!(
        failed
            .iter()
            .any(|m| m.contains(r#"reason="attempts exhausted""#)),
        "{failed:?}"
    );
    assert!(!worker.is_finished());
    stop.cancel();
    worker.await.unwrap();
}

/// The intake does not wait for running tasks.
#[tokio::test(start_paused = true)]
async fn intake_does_not_wait_for_running_tasks() {
    let source: Faulty = Arc::default();
    let running = counter();
    let r = Arc::clone(&running);
    let builder = queue_of(
        &source,
        task_fn(move |_: u32| {
            let r = Arc::clone(&r);
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                sleep(3600 * SEC).await;
            }
        }),
    )
    .concurrency(2);
    let (worker, stop) = start(builder, Monitor::new().shutdown_timeout(Duration::ZERO));
    source.enqueue(Task::new(1));
    source.enqueue(Task::new(2));
    assert!(
        until(SEC, || get(&running) == 2).await,
        "both tasks started"
    );
    stop.cancel();
    worker.await.unwrap();
}

/// Every record has event and action from the vocabulary and a message.
#[tokio::test(start_paused = true)]
async fn every_log_record_has_event_action_and_message() {
    const EVENTS: [&str; 7] = [
        "task", "worker", "source", "shutdown", "recovery", "lease", "observer",
    ];
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Faulty = Arc::default();
    let service = task_fn(|n: u32| async move {
        if n == 0 {
            panic!("boom")
        }
    });
    let (worker, stop) = start(queue_of(&source, service), Monitor::new());
    source.inject_poison("bad");
    source.fail_next_polls(1);
    source.enqueue(Task::new(0));
    source.enqueue(Task::new(1));
    assert!(until(10 * SEC, || source.acks() == 3).await);
    stop.cancel();
    worker.await.unwrap();

    let records = logs.records();
    assert!(records.len() > 5);
    for record in records {
        let event = record.fields.get("event").expect("event field");
        assert!(EVENTS.contains(&event.as_str()), "{record:?}");
        assert!(record.fields.contains_key("action"), "{record:?}");
        let message = record.fields.get("message").expect("message");
        assert!(!message.is_empty() && message != "{}", "{record:?}");
        let extra: Vec<_> = record
            .fields
            .keys()
            .filter(|k| !["event", "action", "message"].contains(&k.as_str()))
            .collect();
        assert!(extra.is_empty(), "only three fields: {record:?}");
    }
}

// ------------------------------------------------------- harness scenarios

/// The real worker behind the harness interface.
struct WorkerRunner;

impl Runner for WorkerRunner {
    fn run(&self, setup: RunnerSetup) -> BoxFuture<'static, ()> {
        let handler = setup.handler.clone();
        let service = tower::service_fn(move |request: TaskRequest<u32>| {
            let call = handler.call(request);
            async move { Ok::<_, std::convert::Infallible>(call.await) }
        });
        let queue = Queue::builder("scenario", setup.source, setup.codec, service)
            .retry_policy(RetryPolicy {
                max_attempts: setup.max_attempts,
                base: setup.retry_pause,
                factor: 1.0,
                max: setup.retry_pause,
                jitter: 0.0,
                hold_slot: false,
            })
            .no_recovery()
            .build()
            .unwrap();
        let monitor = Monitor::new().register(queue).unwrap();
        Box::pin(async move {
            monitor.run(setup.stop).await.unwrap();
        })
    }
}

#[tokio::test(start_paused = true)]
async fn harness_scenarios_pass_on_the_worker() {
    scenarios::empty_never_ends_worker(&WorkerRunner)
        .await
        .unwrap();
    scenarios::boxed_abort_is_not_retried(&WorkerRunner)
        .await
        .unwrap();
    scenarios::cancel_during_retry_pause_stops_work(&WorkerRunner)
        .await
        .unwrap();
    scenarios::poison_does_not_stop_worker(&WorkerRunner)
        .await
        .unwrap();
}
