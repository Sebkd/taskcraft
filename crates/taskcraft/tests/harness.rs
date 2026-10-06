//! The harness scenarios against a reference worker (they must pass) and
//! against stubs that each carry one known defect (they must fail).

#![cfg(feature = "test-util")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::testing::{FaultySource, Runner, RunnerSetup, ScenarioFailure, scenarios};
use taskcraft::{
    AckPointSupport, BoxFuture, Capabilities, Codec, InMemorySource, Outcome, Polled, PushError,
    PushResult, Source, Task, TaskId, TaskParts, TaskRequest, WakeHandle, WakeSignal,
};
use tokio::time::sleep;

/// Defects a stub worker can carry. All off: the reference worker.
#[derive(Clone, Copy, Default)]
struct Defects {
    /// "Empty" is treated as the end of the stream (apalis A-1).
    empty_ends: bool,
    /// Anything but success is retried (apalis B-1).
    ignore_abort: bool,
    /// A decode error ends the loop (apalis B-4).
    poison_stops: bool,
    /// The retry pause is slept through, ignoring stop (apalis A-4).
    sleep_ignores_stop: bool,
}

struct Stub(Defects);

impl Runner for Stub {
    fn run(&self, setup: RunnerSetup) -> BoxFuture<'static, ()> {
        Box::pin(run_loop(self.0, setup))
    }
}

/// Waits for `d` or for stop; returns `true` when stopped.
async fn pause(setup: &RunnerSetup, d: Duration, ignore_stop: bool) -> bool {
    if ignore_stop {
        sleep(d).await;
        return setup.stop.is_cancelled();
    }
    tokio::select! {
        () = setup.stop.cancelled() => true,
        () = sleep(d) => false,
    }
}

async fn run_loop(defects: Defects, setup: RunnerSetup) {
    let source = Arc::clone(&setup.source);
    let mut wake = source.subscribe();
    loop {
        if let Some(signal) = wake.as_mut() {
            signal.mark_seen();
        }
        let polled = tokio::select! {
            () = setup.stop.cancelled() => return,
            polled = source.poll() => polled,
        };
        match polled {
            Err(_) => {
                if pause(&setup, Duration::from_millis(100), false).await {
                    return;
                }
            }
            Ok(Polled::Closed(_)) => return,
            Ok(Polled::Empty) => {
                if defects.empty_ends {
                    return;
                }
                tokio::select! {
                    () = setup.stop.cancelled() => return,
                    () = async {
                        match wake.as_mut() {
                            Some(signal) => { let _ = signal.changed().await; }
                            None => sleep(Duration::from_millis(100)).await,
                        }
                    } => {}
                }
            }
            Ok(Polled::Task { message, receipt }) => {
                let task = match setup.codec.decode(message) {
                    Ok(task) => task,
                    Err(_) if defects.poison_stops => return,
                    Err(_) => {
                        let _ = source.ack(receipt).await;
                        continue;
                    }
                };
                let mut parts: TaskParts<u32> = task.into_parts();
                loop {
                    parts.attempt += 1;
                    let request = TaskRequest::new(
                        Task::from_parts(parts.clone()),
                        Arc::clone(&setup.registry),
                        Arc::clone(&setup.shared),
                    );
                    let outcome = setup.handler.call(request).await;
                    let retry = match outcome {
                        Outcome::Retry { .. } => true,
                        Outcome::Success => false,
                        _ => defects.ignore_abort,
                    };
                    if !retry || parts.attempt >= setup.max_attempts {
                        break;
                    }
                    if pause(&setup, setup.retry_pause, defects.sleep_ignores_stop).await {
                        return;
                    }
                }
                let _ = source.ack(receipt).await;
            }
        }
    }
}

fn reference() -> Stub {
    Stub(Defects::default())
}

fn assert_fails(result: Result<(), ScenarioFailure>, defect: &str) {
    let failure = result.expect_err(defect);
    assert!(!failure.to_string().is_empty());
}

#[tokio::test(start_paused = true)]
async fn empty_never_ends_worker() {
    scenarios::empty_never_ends_worker(&reference())
        .await
        .unwrap();
    let stub = Stub(Defects {
        empty_ends: true,
        ..Defects::default()
    });
    assert_fails(scenarios::empty_never_ends_worker(&stub).await, "A-1 stub");
}

#[tokio::test(start_paused = true)]
async fn boxed_abort_is_not_retried() {
    scenarios::boxed_abort_is_not_retried(&reference())
        .await
        .unwrap();
    let stub = Stub(Defects {
        ignore_abort: true,
        ..Defects::default()
    });
    assert_fails(
        scenarios::boxed_abort_is_not_retried(&stub).await,
        "B-1 stub",
    );
}

#[tokio::test(start_paused = true)]
async fn poison_does_not_stop_worker() {
    scenarios::poison_does_not_stop_worker(&reference())
        .await
        .unwrap();
    let stub = Stub(Defects {
        poison_stops: true,
        ..Defects::default()
    });
    assert_fails(
        scenarios::poison_does_not_stop_worker(&stub).await,
        "B-4 stub",
    );
}

#[tokio::test(start_paused = true)]
async fn cancel_during_retry_pause_stops_work() {
    let real = std::time::Instant::now();
    scenarios::cancel_during_retry_pause_stops_work(&reference())
        .await
        .unwrap();
    assert!(
        real.elapsed() < Duration::from_secs(5),
        "virtual time, no real wait"
    );
    let stub = Stub(Defects {
        sleep_ignores_stop: true,
        ..Defects::default()
    });
    assert_fails(
        scenarios::cancel_during_retry_pause_stops_work(&stub).await,
        "A-4 stub",
    );
}

/// A source that keeps only one wake-up slot: the last subscriber wins
/// (apalis A-5).
#[derive(Default)]
struct SingleWakerSource {
    slot: Mutex<Option<WakeHandle>>,
}

impl Source for SingleWakerSource {
    type Message = Task<u32>;
    type Receipt = ();
    type Error = std::convert::Infallible;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::PerTask).with_push()
    }

    async fn poll(&self) -> Result<Polled<Task<u32>, ()>, Self::Error> {
        Ok(Polled::Empty)
    }

    async fn ack(&self, (): ()) -> Result<(), Self::Error> {
        Ok(())
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        let handle = WakeHandle::new();
        let signal = handle.subscribe();
        *self.slot.lock().unwrap() = Some(handle);
        Some(signal)
    }

    async fn push(&self, _: &TaskId, _: Task<u32>) -> Result<PushResult, PushError<Self::Error>> {
        if let Some(handle) = self.slot.lock().unwrap().as_ref() {
            handle.wake();
        }
        Ok(PushResult::Stored)
    }
}

#[tokio::test(start_paused = true)]
async fn wake_reaches_every_subscriber() {
    let id = TaskId::new("w");
    let memory = InMemorySource::new(10);
    scenarios::wake_reaches_every_subscriber(&memory, &id, Task::new(1).with_id(id.clone()))
        .await
        .unwrap();
    let faulty = FaultySource::new();
    let message = taskcraft::testing::Scripted::Task(Task::new(1_u32).with_id(id.clone()));
    scenarios::wake_reaches_every_subscriber(&faulty, &id, message)
        .await
        .unwrap();

    let single = SingleWakerSource::default();
    assert_fails(
        scenarios::wake_reaches_every_subscriber(&single, &id, Task::new(1)).await,
        "A-5 stub",
    );
}

#[tokio::test(start_paused = true)]
async fn ledger_catches_redelivery_after_a_failed_ack() {
    // A failed ack puts the task back; the reference worker runs it again.
    // The ledger reports it as a duplicate: at-least-once made visible.
    let ledger = Arc::new(taskcraft::testing::DeliveryLedger::new());
    let source = Arc::new(FaultySource::new());
    let stop = tokio_util_token();
    let setup = RunnerSetup {
        source: Arc::clone(&source),
        codec: taskcraft::testing::FaultyCodec::new(),
        handler: {
            let ledger = Arc::clone(&ledger);
            taskcraft::testing::ScenarioHandler::new(move |request: TaskRequest<u32>| {
                ledger.executed(request.task().id());
                async { Outcome::Success }
            })
        },
        max_attempts: 1,
        retry_pause: Duration::from_secs(1),
        stop: stop.clone(),
        registry: Arc::default(),
        shared: Arc::default(),
    };
    let id = TaskId::new("once");
    ledger.pushed(&id);
    ledger.pushed(&TaskId::new("never"));
    source.fail_next_acks(1);
    source.enqueue(Task::new(0).with_id(id.clone()));
    let worker = tokio::spawn(reference().run(setup));
    sleep(Duration::from_secs(5)).await;
    stop.cancel();
    worker.await.unwrap();

    let report = ledger.report();
    assert_eq!(report.duplicates, [(id, 2)]);
    assert_eq!(report.lost, [TaskId::new("never")]);
    assert_eq!(source.redeliveries(), 1);
}

fn tokio_util_token() -> taskcraft::testing::CancellationToken {
    taskcraft::testing::CancellationToken::new()
}
