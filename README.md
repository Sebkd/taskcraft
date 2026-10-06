# taskcraft

[![CI](https://github.com/Sebkd/taskcraft/actions/workflows/ci.yml/badge.svg)](https://github.com/Sebkd/taskcraft/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/taskcraft.svg)](https://crates.io/crates/taskcraft)
[![docs.rs](https://img.shields.io/docsrs/taskcraft)](https://docs.rs/taskcraft)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/Sebkd/taskcraft/blob/master/LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.99-blue.svg)](https://github.com/rust-lang/rust/blob/master/RELEASES.md)

Background task queue for Rust on tokio, built for long, heavy and
stateful work: tasks that run for hours, hold scarce resources, survive a
restart and can be asked about — and stopped — by id.

## Why taskcraft

- **An empty queue never stops a worker.** A poll answers with a task,
  "empty for now" or "closed"; only "closed" or shutdown ends intake.
- **Outcomes are data.** A handler says success, retry, abort or defer, and a
  wrapped error keeps its class. A panic is a final outcome, never retried.
- **Every wait can be interrupted.** Retry pauses, sleeps between polls,
  waits for a slot or a pool end at once on shutdown or cancel.
- **Built for the long haul.** Overflow policies, named resource pools,
  status and cancel by id, idempotent intake, crash recovery, attempt
  timeouts, a task store with leases.
- **No surprises in your process.** The library never panics on its own,
  installs no log subscriber, and keeps task arguments out of logs and
  metrics.

## Quick start

```toml
[dependencies]
taskcraft = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use std::sync::Arc;
use taskcraft::{CancellationToken, IdentityCodec, InMemorySource, Monitor, Queue, Task, task_fn};

async fn send_report(month: String) {
    println!("report for {month} sent");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::builder("reports", Arc::new(InMemorySource::default()), IdentityCodec::new(), task_fn(send_report))
        .concurrency(4)
        .no_recovery() // in-memory tasks may be lost on a crash: said out loud
        .build()?;
    let reports = queue.handle();
    let stop = CancellationToken::new();
    let monitor = tokio::spawn(Monitor::new().register(queue)?.run(stop.clone()));

    let _ = reports.push(Task::new("2026-10".to_owned())).await?;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    stop.cancel(); // or on Ctrl-C: intake stops, running tasks finish
    let report = monitor.await??;
    println!("stopped: {:?}", report.queues);
    Ok(())
}
```

A handler is a plain `async fn` of the task's arguments plus any values it
wants to extract: `Attempt`, `TaskId`, `Meta<T>`, `Data<T>`, `Cancel`.

## A tour

Each section is a cut-down version of an example from the
[catalog](https://github.com/Sebkd/taskcraft/tree/master/examples); the full
examples run as they are.

### Outcomes as data, retries

```rust
use std::time::Duration;
use taskcraft::{Attempt, Outcome, ResultExt, RetryPolicy, TaskError};

async fn charge(order: u64, Attempt(attempt): Attempt) -> Result<Outcome, TaskError> {
    let gateway = call_gateway(order).await.or_abort()?; // bad input: never retry
    Ok(match gateway {
        Gateway::Busy => Outcome::retry(format!("busy on attempt {attempt}")),
        Gateway::NotYet => Outcome::defer(Duration::from_secs(30), "not settled yet"),
        Gateway::Done => Outcome::Success,
    })
}

// At most 5 attempts, pauses 1 s, 2 s, 4 s … up to 5 min, with 10% jitter.
fn policy() -> RetryPolicy {
    RetryPolicy { max_attempts: 5, ..RetryPolicy::default() }
}

enum Gateway { Busy, NotYet, Done }
async fn call_gateway(_order: u64) -> Result<Gateway, std::io::Error> { Ok(Gateway::Done) }
```

`?` on any error retries; `or_abort()` marks an error as final however it is
wrapped later. Full example: [`outcomes`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft/examples/outcomes.rs).

### Long tasks: reject when full, pause without holding a slot

```rust
use std::sync::Arc;
use std::time::Duration;
use taskcraft::{IdentityCodec, InMemorySource, Queue, RetryPolicy, Task, task_fn};

async fn notify_partner(order: u32) { /* waits up to six hours for an answer */ }

fn reply_busy(order: u32) { /* tell the sender to come back later */ }

fn build() -> Result<(), taskcraft::ConfigError> {
    let _notify = Queue::builder("notify", Arc::new(InMemorySource::default()), IdentityCodec::new(), task_fn(notify_partner))
        .concurrency(2)
        // Full? Refuse at once and answer the sender; intake never blocks.
        .reject_with(|task: Task<u32>| reply_busy(*task.args()))
        // A retry pause gives the slot back; shutdown ends the pause at once.
        .retry_policy(RetryPolicy { max_attempts: 3, base: Duration::from_secs(300), ..RetryPolicy::default() })
        .no_recovery()
        .build()?;
    Ok(())
}
```

Full example: [`long-tasks-reject`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft/examples/long-tasks-reject.rs).

### Resource pools, status and cancel by id

```rust
use std::sync::Arc;
use taskcraft::{Cancel, CancelOutcome, IdentityCodec, InMemorySource, Monitor, Queue, TaskId, task_fn};

async fn unpack(archive: String, cancel: Cancel) {
    tokio::select! {
        () = cancel.cancelled() => { /* clean up and stop */ }
        () = unpack_archive(&archive) => {}
    }
}
async fn unpack_archive(_archive: &str) {}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::builder("unpack", Arc::new(InMemorySource::default()), IdentityCodec::new(), task_fn(unpack))
        .concurrency(8)
        .pool("disk-io", 1) // every attempt takes one permit of the shared pool
        .no_recovery()
        .build()?;
    let unpacks = queue.handle();
    let _monitor = Monitor::new().pool("disk-io", 2)?.register(queue)?;

    let id = TaskId::new("archive-17");
    if let Some(status) = unpacks.status(&id) {
        println!("{id}: {} (attempt {})", status.state(), status.attempt());
    }
    if unpacks.cancel(&id).await == CancelOutcome::CancelRequested {
        println!("{id}: stops at its next check, or after the cancel grace");
    }
    Ok(())
}
```

Pools are shared by queues and taken in name order, so two queues never
deadlock on them. Permits come back on every outcome, panics included. Full
example: [`resource-pools`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft/examples/resource-pools.rs).

### Idempotent push

```rust
use taskcraft::{InMemorySource, IdentityCodec, PushOutcome, QueueHandle, Task};

async fn ask(reports: &QueueHandle<InMemorySource<String>, IdentityCodec<String>, String>) -> Result<(), Box<dyn std::error::Error>> {
    // The id says what the task is: one report per month.
    let task = Task::new("2026-10".to_owned()).with_id("report-2026-10");
    match reports.push(task).await? {
        PushOutcome::Enqueued { .. } => println!("on its way"),
        PushOutcome::AlreadyRunning { state, .. } => println!("already {state}"),
        PushOutcome::AlreadyFinished { state, .. } => println!("already {state} (task store)"),
        PushOutcome::Rejected { reason, .. } => println!("refused: {reason:?}"),
    }
    Ok(())
}
```

Two pushes racing with one id create one task. Full example:
[`idempotent-push`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft/examples/idempotent-push.rs).

### Graceful shutdown and its report

```rust
use std::time::Duration;
use taskcraft::{CancellationToken, Monitor};

async fn serve(monitor: Monitor) -> Result<(), Box<dyn std::error::Error>> {
    let stop = CancellationToken::new();
    let running = tokio::spawn(
        monitor
            .shutdown_timeout(Duration::from_secs(30)) // running tasks may finish
            .run(stop.clone()),
    );
    tokio::signal::ctrl_c().await?;
    stop.cancel(); // intake stops now; then timeout, cancel flag, cancel grace
    let report = running.await??;
    println!(
        "finished in time: {}, cancelled: {}, aborted: {}",
        report.completed(),
        report.cancelled(),
        report.aborted()
    );
    Ok(())
}
```

Full example: [`graceful-shutdown`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft/examples/graceful-shutdown.rs).

### Recovery after a crash

A queue that acks on accept loses accepted tasks when the process dies —
unless a recovery hook brings them back from your own records. Without a task
store the hook is required; `no_recovery()` says losing them is fine.

```rust
use std::sync::Arc;
use taskcraft::{BoxError, IdentityCodec, InMemorySource, Queue, Task, task_fn};

async fn export(job: String) {}

async fn unfinished_jobs() -> Result<Vec<String>, BoxError> { Ok(Vec::new()) }

fn build() -> Result<(), taskcraft::ConfigError> {
    let _exports = Queue::builder("exports", Arc::new(InMemorySource::default()), IdentityCodec::new(), task_fn(export))
        // Runs once at start, before the first poll; an error keeps the
        // monitor from starting rather than silently losing work.
        .recover_with(|| async {
            let jobs = unfinished_jobs().await?;
            Ok::<_, BoxError>(jobs.into_iter().map(|job| Task::new(job.clone()).with_id(job)).collect())
        })
        .build()?;
    Ok(())
}
```

Close your record when a task ends (an observer sees every outcome), and the
hook brings back only work still in progress.

### A state machine as a task

A handler can return a process instead of an outcome: `Run(runnable)`. The
runnable gets the task's cancel flag as a soft stop and its result becomes
the task's outcome. `SpawnedMachine` runs a
[statecraft-fsm](https://crates.io/crates/statecraft-fsm) machine this way.

```rust
use statecraft_fsm::fsm;
use taskcraft::{CancellationToken, Outcome, OutcomeSlot, Run, SpawnedMachine, TaskError};

#[derive(Debug)]
pub struct ExportContext {
    slot: OutcomeSlot,         // where the machine leaves its outcome
    stop: CancellationToken,   // the task's cancel flag, as a soft stop
}

#[fsm(initial = Idle)]
impl Export {
    type Context = ExportContext;

    #[on(state = Idle, event = Start, next = [Done, Stopped])]
    async fn on_start(&mut self) -> IdleStartNext {
        // prepare, copy parts, pack, notify — checking `stop` between steps
        if self.context.stop.is_cancelled() {
            return IdleStartNext::Stopped;
        }
        self.context.slot.set(Outcome::Success);
        IdleStartNext::Done
    }
}

async fn export(job: String) -> Result<Run<SpawnedMachine>, TaskError> {
    let (slot, stop) = (OutcomeSlot::new(), CancellationToken::new());
    let (machine, join) = Export::spawn(ExportContext { slot: slot.clone(), stop: stop.clone() });
    machine.send(ExportEvent::Start).await?;
    Ok(Run(SpawnedMachine::new(join, slot, move || async move { stop.cancel() })))
}

fn main() {}
```

Implement `Runnable` for any type of yours — a thread, a child process, a
remote job. The heavy case — a pool, a six-hour wait, a panicking step,
recovery from the step it stopped at — is the
[`fsm-pipeline`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft/examples/fsm-pipeline.rs)
example.

### Kafka source

```rust,no_run
use std::sync::Arc;
use taskcraft::{AckPoint, CancellationToken, MetadataRegistry, Monitor, Queue, task_fn};
use taskcraft_kafka::{KafkaJsonCodec, KafkaSource};

async fn export(table: u32) {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = KafkaSource::builder("localhost:9092", "exports", "exporters").build()?;
    let queue = Queue::builder("exports", Arc::new(source), KafkaJsonCodec::new(MetadataRegistry::new()), task_fn(export))
        .ack_point(AckPoint::OnCompletion) // an offset is committed only past finished tasks
        .concurrency(4)
        .build()?;
    Monitor::new().register(queue)?.run(CancellationToken::new()).await?;
    Ok(())
}
```

Message keys become task ids, headers named in the metadata registry become
metadata, and Kafka shares partitions between the processes of a group. See
[`taskcraft-kafka`](https://crates.io/crates/taskcraft-kafka).

### PostgreSQL task store with leases

```rust,no_run
use std::sync::Arc;
use taskcraft::{BoxError, CancellationToken, JsonCodec, MetadataRegistry, Monitor, Queue, TaskId, task_fn};
use taskcraft_postgres::{Lease, PgStore};

async fn transfer(id: String) {}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let store = PgStore::builder("billing-1") // stable across restarts, unique among live processes
        .lease(Lease::default())               // a crashed process's tasks go to another one
        .connect("postgres://app:secret@db:5432/app") // or .with_pool(pool), e.g. from sea-orm
        .await?;
    let queue = Queue::builder("transfers", Arc::new(store.queue("transfers")), JsonCodec::new(MetadataRegistry::new()), task_fn(transfer))
        .build()?;
    let transfers = queue.handle();
    let running = tokio::spawn(Monitor::new().register(queue)?.run(CancellationToken::new()));

    // Status of any task: other processes' and finished ones too.
    if let Some(status) = transfers.fetch_status(&TaskId::new("transfer-42")).await? {
        println!("{}: {}, owner {:?}", status.id(), status.state(), status.owner());
    }
    running.await??;
    Ok(())
}
```

Polls claim tasks with `FOR UPDATE SKIP LOCKED`, so no task reaches two
processes. See [`taskcraft-postgres`](https://crates.io/crates/taskcraft-postgres).

### Observability

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use taskcraft::{Event, MetricsObserver, Monitor, Observer, TaskState};

/// Your own registry gets the same events as the `metrics` adapter.
#[derive(Default)]
struct Panics(AtomicU64);

impl Observer for Panics {
    fn on_event(&self, event: &Event<'_>) {
        if let Event::Finished { state: TaskState::Panicked, .. } = event {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn monitor() -> Monitor {
    Monitor::new()
        .observer(MetricsObserver::new()) // feature `metrics`: taskcraft_tasks_finished_total{queue, outcome}, …
        .observer(Panics::default())
}
```

Logs go through `tracing` with three fields — `event`, `action` and a
`message` with `key=value` arguments:

```text
WARN taskcraft::worker: task will be retried: queue=payments, task_id=…, attempt=1, pause=1s, reason="gateway timeout" event="task" action="retry"
```

Attempts run in DEBUG spans `taskcraft.attempt` with the queue, task id,
attempt number and W3C `trace_parent`. Full example:
[`observability`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft/examples/observability.rs).

### Testing with the harness

Feature `test-util` brings `taskcraft::testing`: a source with scripted
failures, a delivery ledger, and scenarios any worker must pass.

```rust
use taskcraft::testing::{FaultyCodec, FaultySource};
use taskcraft::{Task, task_fn};

async fn handler(n: u32) {}

fn faults() {
    let source = FaultySource::<u32>::new();
    source.fail_next_polls(3);          // the worker must restart and resume
    source.inject_poison("not a task"); // and survive a message it cannot decode
    source.enqueue(Task::new(7));
    let (_codec, _service) = (FaultyCodec::<u32>::new(), task_fn(handler));
}
```

## Packages and features

| Package | What |
|---------|------|
| [`taskcraft`](https://crates.io/crates/taskcraft) | The core: queues, workers, monitor, in-memory source, retries, pools, recovery, runnables, observers |
| [`taskcraft-kafka`](https://crates.io/crates/taskcraft-kafka) | Kafka source (on `rdkafka`) |
| [`taskcraft-postgres`](https://crates.io/crates/taskcraft-postgres) | PostgreSQL task store with leases (on `sqlx`) |

| Feature of `taskcraft` | What |
|------------------------|------|
| `metrics` | `MetricsObserver`: the standard series through the `metrics` facade |
| `log` | Library events also as `log` records while no `tracing` subscriber is set |
| `test-util` | `taskcraft::testing`: fault-injecting source, delivery ledger, scenarios |

`taskcraft-postgres` has `tls-rustls` and `tls-native-tls` for the
connections it opens itself. The core depends on none of Kafka, a database
driver, `metrics` or statecraft-fsm unless you ask for them.

## Guarantees

- **At least once.** A task may run again after a crash or a failed ack:
  keep handlers idempotent, or use the task store.
- **One live task per id.** A push or a redelivery of a live id does not
  start a second task.
- **No panics from the library.** Library code has no `unwrap`, `expect`,
  `panic!` or `assert!`; broken invariants end in a stop or an error.
- **Private arguments.** Logs and metrics carry ids, queue names and type
  names, never task arguments or metadata values.

## Environment variables

The library reads no environment variables: your application passes its
settings to the builders. The log is filtered by your subscriber, usually
through `RUST_LOG`: `RUST_LOG=taskcraft=debug` for attempts and spans,
`taskcraft=off` to silence it.

The tests and examples use:

| Variable | Used by | Meaning |
|----------|---------|---------|
| `TASKCRAFT_KAFKA_BROKERS` | `taskcraft-kafka` tests and example | Broker address; tests without it skip, the example uses `localhost:9092` |
| `TASKCRAFT_POSTGRES_URL` | `taskcraft-postgres` tests and examples | Connection string; tests without it skip, examples use the docker compose database |
| `TASKCRAFT_REQUIRE_SERVICES` | CI | Fail, instead of skipping, a test whose service address is missing |

## Examples and specification

- [Example catalog](https://github.com/Sebkd/taskcraft/tree/master/examples):
  fifteen examples from a minimal queue to a state machine pipeline, each
  checking its own result.
- [Specification](https://github.com/Sebkd/taskcraft/blob/master/openspec/specs/taskcraft/taskcraft.md)
  (in Russian): the behaviour, invariants and acceptance criteria; changes
  are kept in [`openspec/changes/archive`](https://github.com/Sebkd/taskcraft/tree/master/openspec/changes/archive),
  one record per change — the project's history.

## Requirements

Rust 1.99 or newer, edition 2024, the tokio runtime.

## Acknowledgements

taskcraft was inspired by [apalis](https://github.com/geofmureithi/apalis)
(MIT-licensed): the small polling source interface, tasks as `tower`
services, and features as layers. taskcraft is an independent implementation
with a different design — a three-answer poll, outcomes as data,
interruptible waits, idempotent intake, a task store with leases — and does
not use apalis code; credit is due to that project for the ideas. Handlers
with extracted parameters follow the pattern made familiar by
[axum](https://github.com/tokio-rs/axum).

taskcraft is a sibling of [statecraft-fsm](https://github.com/Sebkd/statecraft):
the state machine runs a multi-step process, taskcraft delivers, limits,
retries and recovers the work.

## License

MIT, see [LICENSE](https://github.com/Sebkd/taskcraft/blob/master/LICENSE).
