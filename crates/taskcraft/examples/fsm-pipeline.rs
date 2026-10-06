//! E-15 `fsm-pipeline` — the heavy case: a multi-step export run by a
//! statecraft-fsm state machine as a task (spec 2.3.21, 2.3.8, 2.3.15,
//! 2.3.18).
//!
//! Steps: prepare → copy parts → pack → notify the recipient → done.
//!
//! - Copying holds the "copy" pool's single permit for the whole export.
//! - Notifying waits for the recipient's answer for up to six hours.
//! - Cancelling a task stops its machine softly between steps and parts.
//! - A panic in a step is the task's panic; other exports go on.
//! - After a crash the recovery hook brings unfinished exports back from the
//!   consumer's records, and the machine goes on from the recorded step.
//!
//! This uses the adapter for statecraft-fsm as it is today: the machine
//! leaves its outcome in an `OutcomeSlot`. A generic implementation behind a
//! `statecraft` feature follows a change in statecraft-fsm.
//!
//! Runs on virtual time: hours pass at once.
//!
//! ```text
//! cargo run -p taskcraft --example fsm-pipeline
//! ```

// The `fsm` macro generates public types and needs `&mut self` handlers.
#![allow(missing_docs, unreachable_pub, clippy::needless_pass_by_ref_mut)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use statecraft_fsm::fsm;
use taskcraft::{
    BoxError, CancelOutcome, CancellationToken, Data, Event, IdentityCodec, InMemorySource,
    Monitor, Observer, Outcome, OutcomeSlot, Queue, Run, SharedData, SpawnedMachine, Task,
    TaskError, TaskId, TaskState, task_fn,
};
use tokio::time::sleep;

const MIN: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(3600);
const STEPS: [&str; 4] = ["prepare", "copy parts", "pack", "notify recipient"];

/// The consumer's own records: export → steps done, exports closed by an
/// outcome, and a journal of what ran.
#[derive(Debug, Default)]
pub struct Records {
    done: Mutex<BTreeMap<String, usize>>,
    closed: Mutex<BTreeSet<String>>,
    journal: Mutex<Vec<String>>,
}

impl Records {
    fn done(&self, job: &str) -> usize {
        let done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        done.get(job).copied().unwrap_or(0)
    }

    fn record(&self, job: &str, steps: usize) {
        let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        done.insert(job.to_owned(), steps);
    }

    fn log(&self, line: String) {
        println!("    {line}");
        self.journal
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line);
    }

    fn ran(&self, entry: &str) -> usize {
        let journal = self.journal.lock().unwrap_or_else(PoisonError::into_inner);
        journal.iter().filter(|l| l.as_str() == entry).count()
    }
}

#[derive(Debug)]
pub struct ExportContext {
    job: String,
    records: Arc<Records>,
    slot: OutcomeSlot,
    /// Set by the task's cancel flag: the machine's soft stop.
    stop: CancellationToken,
}

impl ExportContext {
    fn stopped(&self) -> bool {
        if self.stop.is_cancelled() {
            self.records.log(format!("{}: stopped softly", self.job));
            self.slot.set(Outcome::abort("stopped"));
            return true;
        }
        false
    }
}

#[fsm(initial = Idle)]
impl Export {
    type Context = ExportContext;

    #[on(state = Idle, event = Start, next = Working)]
    async fn on_start(&mut self) {
        self.emit(ExportEvent::Step);
    }

    #[on(state = Working, event = Step, next = [Working, Done, Stopped])]
    async fn on_step(&mut self) -> WorkingStepNext {
        let ctx = &mut self.context;
        let step = ctx.records.done(&ctx.job);
        let Some(name) = STEPS.get(step) else {
            return WorkingStepNext::Done;
        };
        ctx.records.log(format!("{}: {name}", ctx.job));
        match step {
            0 => sleep(5 * MIN).await,
            1 => {
                for part in 1..=3 {
                    if ctx.stopped() {
                        return WorkingStepNext::Stopped;
                    }
                    sleep(20 * MIN).await;
                    ctx.records.log(format!("{}: part {part} copied", ctx.job));
                }
            }
            2 => pack(&ctx.job).await,
            _ => {
                // The recipient answers in two hours; we wait up to six.
                tokio::select! {
                    () = ctx.stop.cancelled() => {}
                    () = sleep(2 * HOUR) => ctx.records.log(format!("{}: recipient confirmed", ctx.job)),
                    () = sleep(6 * HOUR) => {
                        ctx.slot.set(Outcome::retry("recipient silent"));
                        return WorkingStepNext::Done;
                    }
                }
            }
        }
        if ctx.stopped() {
            return WorkingStepNext::Stopped;
        }
        ctx.records.record(&ctx.job, step + 1);
        if step + 1 == STEPS.len() {
            ctx.slot.set(Outcome::Success);
            return WorkingStepNext::Done;
        }
        self.emit(ExportEvent::Step);
        WorkingStepNext::Working
    }
}

#[allow(clippy::panic)] // The example shows a panic in a step.
async fn pack(job: &str) {
    if job == "broken" {
        panic!("the packer crashed");
    }
    sleep(10 * MIN).await;
}

/// The task: start the machine, hand it to the worker.
/// The records are shared by both runs of the process, hence `Arc<Records>`.
async fn export(
    job: String,
    Data(records): Data<Arc<Records>>,
) -> Result<Run<SpawnedMachine>, TaskError> {
    let slot = OutcomeSlot::new();
    let stop = CancellationToken::new();
    let (machine, join) = Export::spawn(ExportContext {
        job,
        records: Arc::clone(&records),
        slot: slot.clone(),
        stop: stop.clone(),
    });
    machine.send(ExportEvent::Start).await?;
    Ok(Run(SpawnedMachine::new(join, slot, move || async move {
        stop.cancel();
    })))
}

/// Final states by export; a final state also closes the consumer's record,
/// so recovery brings back only exports still in progress.
struct Finals {
    states: Mutex<BTreeMap<String, TaskState>>,
    records: Arc<Records>,
}

impl Observer for Finals {
    fn on_event(&self, event: &Event<'_>) {
        if let Event::Finished { task_id, state, .. } = event {
            println!("  {task_id} -> {state}");
            let job = task_id.as_str().to_owned();
            let mut states = self.states.lock().unwrap_or_else(PoisonError::into_inner);
            states.insert(job.clone(), *state);
            if state.is_terminal() {
                let mut closed = self
                    .records
                    .closed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                closed.insert(job);
            }
        }
    }
}

impl Finals {
    fn of(&self, job: &str) -> Option<TaskState> {
        self.states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(job)
            .copied()
    }
}

type Handle = taskcraft::QueueHandle<InMemorySource<String>, IdentityCodec<String>, String>;

/// A process: the exports queue with the copy pool, recovering from the
/// consumer's records.
fn process(
    records: &Arc<Records>,
    finals: &Arc<Finals>,
) -> Result<(Monitor, Handle), Box<dyn std::error::Error>> {
    let mut shared = SharedData::new();
    shared.insert(Arc::clone(records));
    let unfinished: Vec<String> = {
        let done = records.done.lock().unwrap_or_else(PoisonError::into_inner);
        let closed = records
            .closed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        done.keys()
            .filter(|job| !closed.contains(*job))
            .cloned()
            .collect()
    };
    let queue = Queue::builder(
        "exports",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(export),
    )
    .concurrency(3)
    .pool("copy", 1)
    .cancel_grace(HOUR)
    .shared_data(shared)
    .recover_with(move || async move {
        if !unfinished.is_empty() {
            println!("  recovery hook: {unfinished:?}");
        }
        Ok::<_, BoxError>(
            unfinished
                .into_iter()
                .map(|job| Task::new(job.clone()).with_id(job))
                .collect(),
        )
    })
    .build()?;
    let handle = queue.handle();
    let monitor = Monitor::new()
        .pool("copy", 1)?
        .observer(Arc::clone(finals))
        .register(queue)?;
    Ok((monitor, handle))
}

async fn until(what: &str, cond: impl Fn() -> bool) -> Result<(), String> {
    for _ in 0..10_000 {
        if cond() {
            return Ok(());
        }
        sleep(MIN).await;
    }
    Err(format!("timed out waiting for {what}"))
}

async fn push(handle: &Handle, job: &str) -> Result<(), Box<dyn std::error::Error>> {
    let _ = handle.push(Task::new(job.to_owned()).with_id(job)).await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("virtual time: hours pass at once");
    let records = Arc::new(Records::default());
    let finals = Arc::new(Finals {
        states: Mutex::default(),
        records: Arc::clone(&records),
    });
    let (monitor, handle) = process(&records, &finals)?;
    let first = tokio::spawn(monitor.run(CancellationToken::new()));

    println!("1. an export from start to done");
    push(&handle, "report-1").await?;
    until("report-1", || finals.of("report-1").is_some()).await?;

    println!("2. cancel mid-copy; the copy permit passes to the next export");
    push(&handle, "report-2").await?;
    push(&handle, "report-3").await?;
    until("report-2 copying", || {
        records.ran("report-2: part 1 copied") == 1
    })
    .await?;
    if handle.cancel(&TaskId::new("report-2")).await != CancelOutcome::CancelRequested {
        return Err("report-2 was not running".into());
    }
    until("report-3", || finals.of("report-3").is_some()).await?;

    println!("3. a step panics; other exports go on");
    push(&handle, "broken").await?;
    push(&handle, "report-5").await?;
    until("report-5", || {
        finals.of("report-5").is_some() && finals.of("broken").is_some()
    })
    .await?;

    println!("4. the process crashes while the recipient is being notified");
    push(&handle, "report-6").await?;
    until("report-6 notifying", || records.done("report-6") == 3).await?;
    sleep(MIN).await;
    first.abort();
    let _ = first.await;
    println!("  crashed");
    let (monitor, _handle) = process(&records, &finals)?;
    let stop = CancellationToken::new();
    let second = tokio::spawn(monitor.run(stop.clone()));
    until("report-6", || finals.of("report-6").is_some()).await?;
    stop.cancel();
    second.await??;

    let expected = [
        ("report-1", TaskState::Succeeded),
        ("report-2", TaskState::Cancelled),
        ("report-3", TaskState::Succeeded),
        ("broken", TaskState::Panicked),
        ("report-5", TaskState::Succeeded),
        ("report-6", TaskState::Succeeded),
    ];
    for (job, state) in expected {
        if finals.of(job) != Some(state) {
            return Err(format!("{job}: expected {state}, got {:?}", finals.of(job)).into());
        }
    }
    let resumed = records.ran("report-6: copy parts") == 1
        && records.ran("report-6: notify recipient") == 2
        && records.ran("report-2: pack") == 0;
    if !resumed {
        return Err("report-6 did not resume from its step, or report-2 went on".into());
    }
    println!("every export ended as expected; report-6 resumed at its last step");
    Ok(())
}
