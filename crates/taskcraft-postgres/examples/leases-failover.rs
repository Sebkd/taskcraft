//! E-14 `leases-failover` — two processes on one task store with leases:
//! one crashes holding a task, the other takes it over once the lease
//! expires, with the attempt count kept (spec 2.3.20, 2.1.2.18, 2.2.9).
//!
//! Needs a database: `docker compose -f examples/docker-compose.yml up -d postgres`.
//!
//! ```text
//! cargo run -p taskcraft-postgres --example leases-failover
//! ```
//!
//! `TASKCRAFT_POSTGRES_URL` overrides the connection string (default
//! `postgres://postgres:postgres@localhost:5432/postgres`).

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use taskcraft::{
    Attempt, BoxError, CancellationToken, Monitor, Outcome, PollStrategy, Queue, Task, TaskId,
    TaskState, task_fn,
};
use taskcraft_postgres::{Lease, PgStore};
use tokio::time::sleep;

const LEASE: Lease = Lease {
    duration: Duration::from_secs(3),
    heartbeat: Duration::from_secs(1),
};

/// Process A hangs on the transfer; process B completes it.
async fn transfer_on_a(id: String, Attempt(attempt): Attempt) -> Outcome {
    println!("  process A: {id}, attempt {attempt} — hangs");
    sleep(Duration::from_secs(3600)).await;
    Outcome::Success
}

async fn transfer_on_b(id: String, Attempt(attempt): Attempt) -> Outcome {
    println!("  process B: {id}, attempt {attempt} — done");
    Outcome::Success
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::var("TASKCRAFT_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let run = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let queue_name = format!("transfers-{run}");

    // Process A starts first and takes the task.
    let store_a = PgStore::builder(format!("a-{run}"))
        .lease(LEASE)
        .connect(&url)
        .await?;
    let queue_a = Queue::on_store(
        &queue_name,
        Arc::new(store_a.queue(&queue_name)),
        task_fn(transfer_on_a),
    )
    .build()?;
    let (monitor, handle_a) = Monitor::new().register(queue_a)?;
    let running_a = tokio::spawn(monitor.run(CancellationToken::new()));
    let _ = handle_a
        .push(Task::new("transfer-42".to_owned()).with_id("transfer-42"))
        .await?;
    let id = TaskId::new("transfer-42");
    for _ in 0..100 {
        if handle_a
            .status(&id)
            .is_some_and(|s| s.state() == TaskState::Running)
        {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Process B joins; the task is A's while A renews its lease.
    let store_b = PgStore::builder(format!("b-{run}"))
        .lease(LEASE)
        .connect(&url)
        .await?;
    let queue_b = Queue::on_store(
        &queue_name,
        Arc::new(store_b.queue(&queue_name)),
        task_fn(transfer_on_b),
    )
    // Pushes wake the workers of every process (LISTEN/NOTIFY), but an
    // expired lease is noticed only by a poll: a short interval shows the
    // takeover quickly.
    .poll_strategy(PollStrategy::Interval(Duration::from_millis(200)))
    .build()?;
    let stop_b = CancellationToken::new();
    let (monitor, handle_b) = Monitor::new().register(queue_b)?;
    let running_b = tokio::spawn(monitor.run(stop_b.clone()));
    sleep(Duration::from_secs(2)).await;

    println!("process A crashes");
    let crashed = Instant::now();
    running_a.abort();
    let _ = running_a.await;
    drop((handle_a, store_a));

    let mut finished = None;
    for _ in 0..200 {
        if let Some(status) = handle_b.fetch_status(&id).await?
            && status.state() == TaskState::Succeeded
        {
            finished = Some(status);
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    stop_b.cancel();
    running_b.await??;
    let status = finished.ok_or("process B did not take the task over")?;
    println!(
        "taken over and finished {:?} after the crash, attempt {}",
        crashed.elapsed(),
        status.attempt()
    );
    if status.attempt() != 2 {
        return Err("expected the attempt count to carry over".into());
    }
    Ok(())
}
