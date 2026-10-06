//! E-13 `durable-store` — a task store in PostgreSQL without leases: push,
//! status of finished tasks, deferred redelivery, and a process restart that
//! gives its unfinished task back (spec 2.6 "Task store", 2.3.19 p. 1).
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
    Attempt, BoxError, CancellationToken, JsonCodec, MetadataRegistry, Monitor, Outcome,
    PollStrategy, PushOutcome, Queue, QueueHandle, Task, TaskId, TaskState, task_fn,
};
use taskcraft_postgres::{PgSource, PgStore};
use tokio::task::JoinHandle;
use tokio::time::sleep;

type Handle = QueueHandle<PgSource, JsonCodec, String>;

const ALIVE: Duration = Duration::from_millis(500);

/// Invoices: "late" is deferred once, "hang" hangs on its first attempt.
async fn invoice(name: String, Attempt(attempt): Attempt) -> Outcome {
    println!("  {name}: attempt {attempt}");
    match name.as_str() {
        "late" if attempt == 1 => Outcome::defer(Duration::from_secs(2), "the ledger is closing"),
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
        JoinHandle<Result<taskcraft::ShutdownReport, taskcraft::RecoveryError>>,
        CancellationToken,
    ),
    BoxError,
> {
    let store = PgStore::builder(process)
        .alive_interval(ALIVE)
        .connect(url)
        .await?;
    let queue = Queue::builder(
        queue_name,
        Arc::new(store.queue(queue_name)),
        JsonCodec::new(MetadataRegistry::new()),
        task_fn(invoice),
    )
    .concurrency(4)
    // Other processes' work (and expired leases) shows up only on polls.
    .poll_strategy(PollStrategy::Interval(Duration::from_millis(200)))
    .build()?;
    let handle = queue.handle();
    let stop = CancellationToken::new();
    let running = tokio::spawn(Monitor::new().register(queue)?.run(stop.clone()));
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
