//! Runnables through the worker: a statecraft-fsm machine and a plain type
//! (rule 2.3.21).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// The `fsm` macro generates public types and needs `&mut self` handlers.
#![allow(missing_docs, unreachable_pub, clippy::needless_pass_by_ref_mut)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use statecraft_fsm::fsm;
use taskcraft::codec::IdentityCodec;
use taskcraft::runnable::{OutcomeSlot, Run, Runnable, SpawnedMachine};
use taskcraft::{
    AckPoint, CancelOutcome, CancellationToken, InMemorySource, Monitor, Outcome, Queue, Task,
    TaskError, TaskId, task_fn,
};
use tokio::time::sleep;

mod common;
use common::{Captured, run};

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

/// What the machine does on start.
#[derive(Debug, Clone, Copy)]
pub enum Mode {
    Finish,
    Panic,
    Wait,
}

#[derive(Debug)]
pub struct ExportContext {
    mode: Mode,
    slot: OutcomeSlot,
    stopped: Arc<AtomicBool>,
}

#[fsm(initial = Idle)]
impl Export {
    type Context = ExportContext;

    #[on(state = Idle, event = Start, next = Working)]
    async fn on_start(&mut self) {
        match self.context.mode {
            Mode::Finish => self.emit(ExportEvent::Finish),
            Mode::Panic => panic!("export crashed"),
            Mode::Wait => {}
        }
    }

    #[on(state = Working, event = Finish, next = Done)]
    async fn on_finish(&mut self) {
        self.context.slot.set(Outcome::Success);
    }

    #[on(state = Working, event = Stop, next = Stopped)]
    async fn on_stop(&mut self) {
        self.context.stopped.store(true, Ordering::SeqCst);
        self.context.slot.set(Outcome::abort("stopped"));
    }
}

/// Starts the machine and hands it to the worker.
async fn export(mode: Mode, stopped: Arc<AtomicBool>) -> Result<Run<SpawnedMachine>, TaskError> {
    let slot = OutcomeSlot::new();
    let (handle, join) = Export::spawn(ExportContext {
        mode,
        slot: slot.clone(),
        stopped,
    });
    handle.send(ExportEvent::Start).await?;
    Ok(Run(SpawnedMachine::new(join, slot, move || async move {
        let _ = handle.send(ExportEvent::Stop).await;
    })))
}

struct Setup {
    logs: Captured,
    source: Arc<InMemorySource<Mode>>,
    handle: taskcraft::QueueHandle<Mode>,
    stopped: Arc<AtomicBool>,
    stop: CancellationToken,
    monitor: tokio::task::JoinHandle<taskcraft::ShutdownReport>,
}

fn start(logs: Captured) -> Setup {
    let source = Arc::new(InMemorySource::<Mode>::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stopped);
    let handler = task_fn(move |mode: Mode| export(mode, Arc::clone(&flag)));
    let queue = Queue::builder(
        "exports",
        Arc::clone(&source),
        IdentityCodec::new(),
        handler,
    )
    .ack_point(AckPoint::OnCompletion)
    .cancel_grace(10 * SEC)
    .build()
    .unwrap();
    let stop = CancellationToken::new();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let monitor = tokio::spawn(run(monitor, stop.clone()));
    Setup {
        logs,
        source,
        handle,
        stopped,
        stop,
        monitor,
    }
}

fn finished_with(logs: &Captured, outcome: &str) -> bool {
    logs.records().iter().any(|r| {
        r.is("task", "finished") && r.fields["message"].contains(&format!("outcome={outcome}"))
    })
}

/// Criterion 35 a: the machine's outcome is the task's.
#[tokio::test]
async fn machine_outcome_is_the_task_outcome() {
    let logs = Captured::default();
    let _guard = logs.install();
    let s = start(logs);
    let _ = s.handle.push(Task::new(Mode::Finish)).await.unwrap();
    assert!(until(5 * SEC, || finished_with(&s.logs, "succeeded")).await);
    assert!(until(SEC, || s.source.is_empty()).await);
    s.stop.cancel();
    s.monitor.await.unwrap();
}

/// Criterion 35 b: a panic inside the machine is the task's panic.
#[tokio::test]
async fn machine_panic_is_a_task_panic() {
    let logs = Captured::default();
    let _guard = logs.install();
    let s = start(logs);
    let _ = s.handle.push(Task::new(Mode::Panic)).await.unwrap();
    assert!(until(5 * SEC, || s.logs.count("task", "panicked") == 1).await);
    let panicked = s
        .logs
        .records()
        .into_iter()
        .find(|r| r.is("task", "panicked"))
        .unwrap();
    assert!(panicked.fields["message"].contains("export crashed"));
    s.stop.cancel();
    s.monitor.await.unwrap();
}

/// Criterion 35 c: cancelling the task stops the machine softly; the task
/// is cancelled.
#[tokio::test]
async fn cancel_stops_the_machine_softly() {
    let logs = Captured::default();
    let _guard = logs.install();
    let s = start(logs);
    let id = TaskId::new("waiting");
    let _ = s
        .handle
        .push(Task::new(Mode::Wait).with_id("waiting"))
        .await
        .unwrap();
    assert!(until(5 * SEC, || s.handle.live_tasks() == 1).await);
    sleep(Duration::from_millis(100)).await;

    assert_eq!(s.handle.cancel(&id).await, CancelOutcome::CancelRequested);
    assert!(until(5 * SEC, || finished_with(&s.logs, "cancelled")).await);
    assert!(
        s.stopped.load(Ordering::SeqCst),
        "the machine got its stop event"
    );
    assert_eq!(
        s.logs.count("task", "aborted"),
        0,
        "stopped within the grace"
    );
    s.stop.cancel();
    s.monitor.await.unwrap();
}

/// A consumer's own type, not a state machine.
struct Countdown {
    from: u32,
}

impl Runnable for Countdown {
    type Output = u32;

    async fn run(self, stop: CancellationToken) -> u32 {
        let mut left = self.from;
        while left > 0 && !stop.is_cancelled() {
            sleep(Duration::from_millis(1)).await;
            left -= 1;
        }
        left
    }

    fn into_outcome(left: u32) -> Outcome {
        if left == 0 {
            Outcome::Success
        } else {
            Outcome::retry("interrupted")
        }
    }
}

/// Change criterion 1: a consumer's own runnable runs like a machine.
#[tokio::test]
async fn own_runnable_runs_like_a_machine() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source = Arc::new(InMemorySource::<u32>::default());
    let handler = task_fn(|from: u32| async move { Run(Countdown { from }) });
    let queue = Queue::builder(
        "countdown",
        Arc::clone(&source),
        IdentityCodec::new(),
        handler,
    )
    .ack_point(AckPoint::OnCompletion)
    .build()
    .unwrap();
    let stop = CancellationToken::new();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let monitor = tokio::spawn(run(monitor, stop.clone()));
    let _ = handle.push(Task::new(20)).await.unwrap();
    assert!(until(5 * SEC, || finished_with(&logs, "succeeded")).await);
    assert!(until(SEC, || source.is_empty()).await);
    stop.cancel();
    monitor.await.unwrap();
}
