//! The first consumer's scenarios end to end, and the criteria of section 3
//! that had no end-to-end test yet (change `consumer-scenarios`).

// The `fsm` macro generates public types and needs `&mut self` handlers;
// test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(
    missing_docs,
    unreachable_pub,
    clippy::needless_pass_by_ref_mut,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]
#![cfg(feature = "test-util")]

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use statecraft_fsm::fsm;
use taskcraft::testing::{FaultyCodec, FaultySource};
use taskcraft::{
    AckPoint, AckPointSupport, Attempt, BoxError, CancelOutcome, CancellationToken, Capabilities,
    DeferError, IdentityCodec, InMemorySource, JsonCodec, MetadataRegistry, Monitor, Outcome,
    OutcomeSlot, PollStrategy, Polled, PushOutcome, Queue, RejectReason, RetryPolicy, Run,
    ShutdownReport, Source, SpawnedMachine, Task, TaskError, TaskId, TaskState, task_fn,
};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

mod common;
use common::Captured;

const SEC: Duration = Duration::from_secs(1);
const MIN: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(3600);

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
    let running = tokio::spawn({
        let stop = stop.clone();
        async move { monitor.run(stop).await.unwrap() }
    });
    (running, stop)
}

fn policy(max_attempts: u32, base: Duration) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        base,
        factor: 1.0,
        max: base,
        jitter: 0.0,
        hold_slot: false,
    }
}

/// Change criterion 1, "long task": a limit of 2 with "reject" answering the
/// sender, tasks of an hour, a 5-minute retry pause, then shutdown.
#[tokio::test(start_paused = true)]
async fn long_tasks_reject_overflow_and_stop_mid_pause() {
    let source = Arc::new(InMemorySource::<u32>::default());
    let replies = Arc::new(Mutex::new(Vec::new()));
    let answered = Arc::clone(&replies);
    let started = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&started);
    let handler = task_fn(move |n: u32, Attempt(attempt): Attempt| {
        seen.lock().unwrap().push((n, attempt));
        async move {
            if n == 9 {
                return Outcome::retry("partner not ready");
            }
            sleep(HOUR).await;
            Outcome::Success
        }
    });
    let queue = Queue::builder("notify", Arc::clone(&source), IdentityCodec::new(), handler)
        .concurrency(2)
        .reject_with(move |task: Task<u32>| {
            answered.lock().unwrap().push(*task.args());
        })
        .retry_policy(policy(3, 5 * MIN))
        .no_recovery()
        .build()
        .unwrap();
    let handle = queue.handle();
    let (running, stop) = spawn(Monitor::new().register(queue).unwrap());

    for n in 0..4 {
        let _ = handle.push(Task::new(n)).await.unwrap();
    }
    // Two run for an hour; the other two are refused at once, and intake
    // goes on meanwhile.
    assert!(until(SEC, || replies.lock().unwrap().len() == 2).await);
    assert_eq!(*replies.lock().unwrap(), [2, 3]);
    assert_eq!(handle.live_tasks(), 2);

    // After the hour a task asking to retry waits out its 5-minute pause...
    assert!(until(2 * HOUR, || handle.live_tasks() == 0).await);
    let _ = handle.push(Task::new(9)).await.unwrap();
    assert!(
        until(SEC, || handle.live_tasks() > 0
            && started.lock().unwrap().iter().any(|r| r.0 == 9))
        .await
    );
    sleep(SEC).await;
    // ...and shutdown ends it at once, without moving the clock.
    let at = Instant::now();
    stop.cancel();
    let report = running.await.unwrap();
    assert_eq!(at.elapsed(), Duration::ZERO);
    assert!(report.queues[0].cancelled >= 1, "{report:?}");
}

/// Change criterion 2, "resource-heavy task": pools of different sizes limit
/// parallelism; status and cancel by id; permits come back on cancel.
#[tokio::test(start_paused = true)]
async fn pools_limit_work_and_cancel_frees_permits() {
    let unpack_src = Arc::new(InMemorySource::<u32>::default());
    let pack_src = Arc::new(InMemorySource::<u32>::default());
    let busy = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let peak = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let handler = |slot: usize| {
        let (busy, peak) = (Arc::clone(&busy), Arc::clone(&peak));
        task_fn(move |_: u32, cancel: taskcraft::Cancel| {
            let (busy, peak) = (Arc::clone(&busy), Arc::clone(&peak));
            async move {
                let now = busy[slot].fetch_add(1, Ordering::SeqCst) + 1;
                peak[slot].fetch_max(now, Ordering::SeqCst);
                tokio::select! {
                    () = cancel.cancelled() => {}
                    () = sleep(HOUR) => {}
                }
                busy[slot].fetch_sub(1, Ordering::SeqCst);
                Outcome::Success
            }
        })
    };
    let unpack = Queue::builder(
        "unpack",
        Arc::clone(&unpack_src),
        IdentityCodec::new(),
        handler(0),
    )
    .concurrency(8)
    .pool("unpack", 1)
    .no_recovery()
    .build()
    .unwrap();
    let pack = Queue::builder(
        "pack",
        Arc::clone(&pack_src),
        IdentityCodec::new(),
        handler(1),
    )
    .concurrency(8)
    .pool("pack", 1)
    .no_recovery()
    .build()
    .unwrap();
    let (unpack_handle, pack_handle) = (unpack.handle(), pack.handle());
    let monitor = Monitor::new()
        .pool("unpack", 2)
        .unwrap()
        .pool("pack", 1)
        .unwrap()
        .register(unpack)
        .unwrap()
        .register(pack)
        .unwrap();
    let (running, stop) = spawn(monitor);
    for n in 0..4 {
        let id = format!("u{n}");
        let _ = unpack_handle
            .push(Task::new(n).with_id(id.as_str()))
            .await
            .unwrap();
        let id = format!("p{n}");
        let _ = pack_handle
            .push(Task::new(n).with_id(id.as_str()))
            .await
            .unwrap();
    }
    assert!(
        until(SEC, || unpack_handle.live_tasks() == 4
            && pack_handle.live_tasks() == 4)
        .await
    );
    sleep(SEC).await;
    assert_eq!(busy[0].load(Ordering::SeqCst), 2, "unpack pool of 2");
    assert_eq!(busy[1].load(Ordering::SeqCst), 1, "pack pool of 1");

    // Status follows 2.4.1: the running pack task and the waiting ones.
    let states: Vec<_> = (0..4)
        .map(|n| {
            pack_handle
                .status(&TaskId::new(format!("p{n}")))
                .unwrap()
                .state()
        })
        .collect();
    assert_eq!(
        states.iter().filter(|s| **s == TaskState::Running).count(),
        1
    );
    assert_eq!(
        states.iter().filter(|s| **s == TaskState::Accepted).count(),
        3
    );

    // Cancelling the running one gives its permit to the next.
    let runner = (0..4)
        .map(|n| TaskId::new(format!("p{n}")))
        .find(|id| {
            pack_handle
                .status(id)
                .is_some_and(|s| s.state() == TaskState::Running)
        })
        .unwrap();
    assert_eq!(
        pack_handle.cancel(&runner).await,
        CancelOutcome::CancelRequested
    );
    assert!(until(SEC, || pack_handle.status(&runner).is_none()).await);
    assert!(
        until(SEC, || pack_handle.live_tasks() == 3
            && busy[1].load(Ordering::SeqCst) == 1)
        .await
    );
    assert_eq!(peak[0].load(Ordering::SeqCst), 2);
    assert_eq!(peak[1].load(Ordering::SeqCst), 1);
    stop.cancel();
    running.await.unwrap();
}

/// The consumer's own records: job → last finished step.
type Records = Arc<Mutex<HashMap<String, u32>>>;

const STEPS: u32 = 3;

#[derive(Debug)]
pub struct ExportContext {
    job: String,
    step: u32,
    records: Records,
    slot: OutcomeSlot,
}

#[fsm(initial = Idle)]
impl Export {
    type Context = ExportContext;

    #[on(state = Idle, event = Start, next = Working)]
    async fn on_start(&mut self) {
        self.emit(ExportEvent::Step);
    }

    #[on(state = Working, event = Step, next = [Working, Done])]
    async fn on_step(&mut self) -> WorkingStepNext {
        sleep(10 * MIN).await;
        self.context.step += 1;
        let (job, step) = (self.context.job.clone(), self.context.step);
        self.context.records.lock().unwrap().insert(job, step);
        if step == STEPS {
            self.context.slot.set(Outcome::Success);
            WorkingStepNext::Done
        } else {
            self.emit(ExportEvent::Step);
            WorkingStepNext::Working
        }
    }
}

/// Runs the export machine from the step the consumer recorded.
fn export_handler(
    records: &Records,
    runs: &Arc<AtomicU32>,
) -> impl taskcraft::Handler<String, (TaskId,)> + use<> {
    let (records, runs) = (Arc::clone(records), Arc::clone(runs));
    move |job: String, _: TaskId| {
        let (records, runs) = (Arc::clone(&records), Arc::clone(&runs));
        async move {
            runs.fetch_add(1, Ordering::SeqCst);
            // Accepted first, recorded second (rule 2.3.18 p. 6).
            let step = *records.lock().unwrap().entry(job.clone()).or_insert(0);
            let slot = OutcomeSlot::new();
            let (handle, join) = Export::spawn(ExportContext {
                job,
                step,
                records,
                slot: slot.clone(),
            });
            handle.send(ExportEvent::Start).await?;
            Ok::<_, TaskError>(Run(SpawnedMachine::new(join, slot, || async {})))
        }
    }
}

/// Change criterion 3, "machine task": the process fails mid-export with ack
/// on accept; after restart the recovery hook brings the job back from the
/// consumer's records and the machine finishes it from where it was.
#[tokio::test(start_paused = true)]
async fn machine_task_survives_a_crash_through_the_recovery_hook() {
    let records: Records = Arc::default();
    let runs = Arc::new(AtomicU32::new(0));

    // First run: the job is accepted (and acked), two steps of three done.
    let source = Arc::new(InMemorySource::<String>::default());
    let queue = Queue::builder(
        "exports",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(export_handler(&records, &runs)),
    )
    .ack_point(AckPoint::OnAccept)
    .recover_with(|| async { Ok::<_, BoxError>(Vec::new()) })
    .build()
    .unwrap();
    let handle = queue.handle();
    let (crashed, _) = spawn(Monitor::new().register(queue).unwrap());
    let _ = handle
        .push(Task::new("job-7".to_owned()).with_id("job-7"))
        .await
        .unwrap();
    assert!(until(HOUR, || records.lock().unwrap().get("job-7") == Some(&2)).await);
    assert!(source.is_empty(), "acked on accept: gone from the source");

    // The process dies: its tasks, machines and source go with it.
    crashed.abort();
    let _ = crashed.await;
    drop(source);
    sleep(HOUR).await;
    assert_eq!(
        records.lock().unwrap().get("job-7"),
        Some(&2),
        "nothing ran meanwhile"
    );

    // Restart: the hook reads the consumer's records.
    let pending: Vec<String> = records
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, step)| **step < STEPS)
        .map(|(job, _)| job.clone())
        .collect();
    let queue = Queue::builder(
        "exports",
        Arc::new(InMemorySource::<String>::default()),
        IdentityCodec::new(),
        task_fn(export_handler(&records, &runs)),
    )
    .ack_point(AckPoint::OnAccept)
    .recover_with(move || async move {
        Ok::<_, BoxError>(
            pending
                .into_iter()
                .map(|job| Task::new(job.clone()).with_id(job))
                .collect(),
        )
    })
    .build()
    .unwrap();
    let handle = queue.handle();
    let (running, stop) = spawn(Monitor::new().register(queue).unwrap());
    assert!(
        until(HOUR, || records.lock().unwrap().get("job-7")
            == Some(&STEPS))
        .await
    );
    assert!(until(SEC, || handle.live_tasks() == 0).await);
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "one run before the crash, one after"
    );
    stop.cancel();
    let report = running.await.unwrap();
    assert_eq!(report.queues[0].completed, 1);
}

/// Criterion 6: a panic with attempts left runs once, ends "panicked" with an
/// ERROR record, and the other tasks and the intake go on.
#[tokio::test(start_paused = true)]
async fn panic_is_final_and_others_go_on() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source = Arc::new(InMemorySource::<u32>::default());
    let runs = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&runs);
    let handler = task_fn(move |n: u32| {
        seen.lock().unwrap().push(n);
        async move {
            assert!(n != 0, "handler bug");
            Outcome::Success
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), IdentityCodec::new(), handler)
        .retry_policy(policy(3, SEC))
        .ack_point(AckPoint::OnCompletion)
        .build()
        .unwrap();
    let handle = queue.handle();
    let (running, stop) = spawn(Monitor::new().register(queue).unwrap());
    for n in [0, 1, 2] {
        let _ = handle.push(Task::new(n)).await.unwrap();
    }
    assert!(until(10 * SEC, || source.is_empty()).await);
    sleep(10 * SEC).await;
    let mut ran = runs.lock().unwrap().clone();
    ran.sort_unstable();
    assert_eq!(
        ran,
        [0, 1, 2],
        "the panicking task ran once, the others ran"
    );
    let panicked: Vec<_> = logs
        .records()
        .into_iter()
        .filter(|r| r.is("task", "panicked"))
        .collect();
    assert_eq!(panicked.len(), 1);
    assert_eq!(panicked[0].level, tracing::Level::ERROR);
    stop.cancel();
    running.await.unwrap();
}

/// Criterion 20: a worker sleeping 30 s between polls drains at once on the
/// shutdown signal.
#[tokio::test(start_paused = true)]
async fn shutdown_ends_the_poll_sleep_at_once() {
    let source = Arc::new(FaultySource::<u32>::new());
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        FaultyCodec::new(),
        task_fn(|_: u32| async {}),
    )
    .poll_strategy(PollStrategy::Interval(30 * SEC))
    .no_recovery()
    .build()
    .unwrap();
    let (running, stop) = spawn(Monitor::new().register(queue).unwrap());
    assert!(until(SEC, || source.polls() >= 1).await);
    sleep(SEC).await;
    let at = Instant::now();
    stop.cancel();
    running.await.unwrap();
    assert_eq!(at.elapsed(), Duration::ZERO);
}

/// Criteria 24 (table part) and 47: a source with per-task ack takes a task
/// with its own ack point; a full in-memory source refuses the push.
#[tokio::test]
async fn own_ack_point_and_full_source() {
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
    let handle = queue.handle();
    let own = Task::new(1).with_ack_point(AckPoint::OnCompletion);
    assert!(matches!(
        handle.push(own).await.unwrap(),
        PushOutcome::Enqueued { .. }
    ));
    let refused = handle.push(Task::new(2)).await.unwrap();
    assert!(matches!(
        refused,
        PushOutcome::Rejected {
            reason: RejectReason::SourceFull,
            ..
        }
    ));
    assert_eq!(source.len(), 1, "the refused task was not stored");
}

/// A source of one JSON message that keeps what is deferred back to it.
struct DeferringSource {
    message: Mutex<Option<Vec<u8>>>,
    deferred: Mutex<Vec<Vec<u8>>>,
}

impl Source for DeferringSource {
    type Message = Vec<u8>;
    type Receipt = ();
    type Error = Infallible;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::PerTask).with_defer()
    }

    async fn poll(&self) -> Result<Polled<Vec<u8>, ()>, Infallible> {
        Ok(match self.message.lock().unwrap().take() {
            Some(message) => Polled::Task {
                message,
                receipt: (),
            },
            None => Polled::Empty,
        })
    }

    async fn ack(&self, (): ()) -> Result<(), Infallible> {
        Ok(())
    }

    async fn defer(
        &self,
        (): (),
        message: Vec<u8>,
        _: tokio::time::Instant,
    ) -> Result<(), DeferError<Infallible>> {
        self.deferred.lock().unwrap().push(message);
        Ok(())
    }
}

/// Criterion 29: a task carrying metadata under a name this worker does not
/// know is deferred, and the value is written back unchanged.
#[tokio::test(start_paused = true)]
async fn unknown_metadata_survives_a_defer() {
    let message = br#"{"id":"t","args":1,"metadata":{"newer.sender":{"tier":"gold","n":[1,2]}},"attempt":0,"retries":0}"#;
    let source = Arc::new(DeferringSource {
        message: Mutex::new(Some(message.to_vec())),
        deferred: Mutex::default(),
    });
    let handler = task_fn(|_: u32| async { Outcome::defer(MIN, "later") });
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        JsonCodec::new(MetadataRegistry::new()),
        handler,
    )
    .ack_point(AckPoint::OnCompletion)
    .build()
    .unwrap();
    let (running, stop) = spawn(Monitor::new().register(queue).unwrap());
    assert!(until(SEC, || !source.deferred.lock().unwrap().is_empty()).await);
    let deferred: serde_json::Value =
        serde_json::from_slice(&source.deferred.lock().unwrap()[0]).unwrap();
    assert_eq!(
        deferred["metadata"]["newer.sender"],
        serde_json::json!({"tier": "gold", "n": [1, 2]})
    );
    stop.cancel();
    running.await.unwrap();
}
