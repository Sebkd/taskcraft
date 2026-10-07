//! E-13 `durable-store` — a task store in PostgreSQL without leases: push,
//! delayed push, status of finished tasks, deferred redelivery, a failed task
//! queued again, and a process restart that gives its unfinished task back
//! (spec 2.6 "Task store", 2.1.2.1, 2.3.26, 2.3.19 p. 1).
//!
//! Needs a database: `docker compose -f examples/docker-compose.yml up -d postgres`.
//!
//! ```text
//! cargo run -p taskcraft-postgres --example durable-store
//! ```
//!
//! `TASKCRAFT_POSTGRES_URL` overrides the connection string (default
//! `postgres://postgres:postgres@localhost:5432/postgres`).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use taskcraft::{
    Attempt, BoxError, CancellationToken, Monitor, Outcome, PollStrategy, PushOutcome, Queue,
    QueueHandle, Task, TaskId, TaskState, task_fn,
};
use taskcraft_postgres::PgStore;
use tokio::task::JoinHandle;
use tokio::time::sleep;

type Handle = QueueHandle<String>;

const ALIVE: Duration = Duration::from_millis(500);

/// Invoices: "late" is deferred once, "typo" fails its first attempt (the
/// data is fixed after), "hang" hangs on its first attempt.
async fn invoice(name: String, Attempt(attempt): Attempt) -> Outcome {
    println!("  {name}: attempt {attempt}");
    match name.as_str() {
        "late" if attempt == 1 => Outcome::defer(Duration::from_secs(2), "the ledger is closing"),
        "typo" if attempt == 1 => Outcome::abort("unknown customer"),
        "hang" if attempt == 1 => {
            sleep(Duration::from_secs(3600)).await;
            Outcome::Success
        }
        _ => Outcome::Success,
    }
}

/// A process with this id: its store, queue and running monitor.
async fn start(
    url: &str,
    process: &str,
    queue_name: &str,
) -> Result<
    (
        PgStore,
        Handle,
        JoinHandle<Result<taskcraft::ShutdownReport, taskcraft::error::RecoveryError>>,
        CancellationToken,
    ),
    BoxError,
> {
    let store = PgStore::builder(process)
        .alive_interval(ALIVE)
        .connect(url)
        .await?;
    let queue = Queue::on_store(
        queue_name,
        Arc::new(store.queue(queue_name)),
        task_fn(invoice),
    )
    .concurrency(4)
    // Other processes' work (and expired leases) shows up only on polls.
    .poll_strategy(PollStrategy::Interval(Duration::from_millis(200)))
    .build()?;
    let stop = CancellationToken::new();
    let (monitor, handle) = Monitor::new().register(queue)?;
    let running = tokio::spawn(monitor.run(stop.clone()));
    Ok((store, handle, running, stop))
}

async fn state_of(handle: &Handle, id: &str) -> Result<Option<TaskState>, BoxError> {
    Ok(handle
        .fetch_status(&TaskId::new(id))
        .await?
        .map(|s| s.state()))
}

async fn wait_for(handle: &Handle, id: &str, state: TaskState) -> Result<(), BoxError> {
    for _ in 0..300 {
        if state_of(handle, id).await? == Some(state) {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(format!("{id} did not reach {state}").into())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::var("TASKCRAFT_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let run = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let (queue_name, process) = (format!("invoices-{run}"), format!("billing-{run}"));

    println!("1. push, defer, status of finished tasks");
    let (store, handle, running, stop) = start(&url, &process, &queue_name).await?;
    for name in ["march", "late"] {
        let _ = handle
            .push(Task::new(name.to_owned()).with_id(name))
            .await?;
    }
    wait_for(&handle, "march", TaskState::Succeeded).await?;
    wait_for(&handle, "late", TaskState::Succeeded).await?;
    let again = handle
        .push(Task::new("march".to_owned()).with_id("march"))
        .await?;
    println!("  push of a finished id: {again:?}");
    if !matches!(again, PushOutcome::AlreadyFinished { .. }) {
        return Err("expected \"already finished\"".into());
    }
    // A reminder for two seconds from now waits in the store, queued.
    let _ = handle
        .push(
            Task::new("reminder".to_owned())
                .with_id("reminder")
                .with_delay(Duration::from_secs(2)),
        )
        .await?;
    let waiting = handle
        .fetch_status(&TaskId::new("reminder"))
        .await?
        .ok_or("no status for reminder")?;
    println!(
        "  reminder: {}, next delivery in {:?}",
        waiting.state(),
        waiting
            .next_attempt_at()
            .and_then(|at| at.duration_since(SystemTime::now()).ok())
    );
    if waiting.state() != TaskState::Queued || waiting.next_attempt_at().is_none() {
        return Err("expected a queued reminder with its next delivery".into());
    }
    wait_for(&handle, "reminder", TaskState::Succeeded).await?;
    // A failed task stays in the store: once its data is fixed, run it again.
    let _ = handle
        .push(Task::new("typo".to_owned()).with_id("typo"))
        .await?;
    wait_for(&handle, "typo", TaskState::Failed).await?;
    handle.requeue(&TaskId::new("typo")).await?;
    wait_for(&handle, "typo", TaskState::Succeeded).await?;
    println!("  typo: requeued and done");

    println!("2. the process crashes with a task in hand");
    let _ = handle
        .push(Task::new("hang".to_owned()).with_id("hang"))
        .await?;
    wait_for(&handle, "hang", TaskState::Running).await?;
    running.abort();
    let _ = running.await;
    drop((handle, store, stop));
    println!("  crashed; restarting with the same process id");
    sleep(3 * ALIVE).await;

    let (_store, handle, running, stop) = start(&url, &process, &queue_name).await?;
    wait_for(&handle, "hang", TaskState::Succeeded).await?;
    let status = handle
        .fetch_status(&TaskId::new("hang"))
        .await?
        .ok_or("no status for hang")?;
    println!("  hang finished on attempt {}", status.attempt());
    stop.cancel();
    running.await??;
    if status.attempt() != 2 {
        return Err("expected the second attempt after the restart".into());
    }
    Ok(())
}
