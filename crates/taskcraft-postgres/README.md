# taskcraft-postgres

[![crates.io](https://img.shields.io/crates/v/taskcraft-postgres.svg)](https://crates.io/crates/taskcraft-postgres)
[![docs.rs](https://img.shields.io/docsrs/taskcraft-postgres)](https://docs.rs/taskcraft-postgres)

A task store in PostgreSQL for [taskcraft](https://crates.io/crates/taskcraft):
queues keep their tasks, statuses and history outside the process, survive
restarts, and with leases hand a crashed process's tasks to another one.

```toml
[dependencies]
taskcraft = "0.2"
taskcraft-postgres = "0.2"
```

```rust,no_run
use std::sync::Arc;
use std::time::Duration;
use taskcraft::{BoxError, CancelOutcome, CancellationToken, Monitor, PushOutcome, Queue, Task, TaskId, task_fn};
use taskcraft_postgres::{Lease, PgStore};

async fn transfer(amount: u64) {}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let store = PgStore::builder("billing-1")         // this process: stable and unique
        .lease(Lease { duration: Duration::from_secs(60), heartbeat: Duration::from_secs(15) })
        .retention(Duration::from_secs(7 * 24 * 3600)) // how long finished tasks are kept
        .connect("postgres://app:secret@db:5432/app")
        .await?;
    // The store keeps tasks as JSON: the codec is built in.
    let queue = Queue::on_store("transfers", Arc::new(store.queue("transfers")), task_fn(transfer))
        .concurrency(4)
        .build()?;
    let (monitor, transfers) = Monitor::new().register(queue)?;
    let running = tokio::spawn(monitor.run(CancellationToken::new()));

    match transfers.push(Task::new(100).with_id("transfer-42")).await? {
        PushOutcome::AlreadyFinished { state, .. } => println!("done before: {state}"),
        other => println!("{other:?}"),
    }
    // Any process can ask about any task, and cancel it where it runs.
    let id = TaskId::new("transfer-42");
    println!("{:?}", transfers.fetch_status(&id).await?.map(|s| s.state()));
    if transfers.cancel(&id).await == CancelOutcome::CancelRequested {
        println!("the owner stops it at its next renewal");
    }
    running.await??;
    Ok(())
}
```

## Sharing the application's pool

`with_pool` takes a `sqlx::PgPool` (sqlx 0.9). An application on sea-orm 2.x
passes its own:

```rust,no_run
use taskcraft_postgres::{PgStore, PgStoreError};

async fn store(pool: sqlx::PgPool) -> Result<PgStore, PgStoreError> {
    // with sea-orm: db.get_postgres_connection_pool().clone()
    PgStore::builder("billing-1").with_pool(pool).await
}
```

## How it works

- **Tables.** `taskcraft_tasks` and `taskcraft_processes`, created on start.
  Tasks are stored as the JSON envelope of `JsonCodec`, the built-in codec
  of `Queue::on_store`; it names metadata through the queue's registry.
- **Schema versions.** `taskcraft_schema` keeps the schema version. On start
  the store applies the missing migrations, one process at a time; a base
  created by 0.1 or 0.2 is migrated in place, tasks and history kept. The
  first start on an older base builds indexes, holding writes to the tasks
  table meanwhile. A base migrated by a newer version is refused
  (`PgStoreError::SchemaTooNew`): update every process.
- **Claim on poll.** A statement with `FOR UPDATE SKIP LOCKED` takes the
  next task for this process — first one whose owner's lease expired, then
  the earliest due one — each through its own index: no task reaches two
  processes, and a poll never scans the table. Every later write checks
  that this process still owns the task.
- **Final states are recorded** on completion — the ack point is not
  configurable — and kept for the retention period: a push with the id of a
  finished task answers "already finished" meanwhile.
- **Restart.** On start the store refuses a process id marked alive by
  another process, then gives this process's unfinished tasks back to their
  queues with their attempt counts.
- **Leases.** The owner renews its tasks every heartbeat. A task whose lease
  expired is taken over by any process, attempt count kept; the old owner
  cancels it and writes nothing more.
- **Cancel across processes.** Cancelling a task another process runs records
  a request; the owner cancels it at its next renewal.

Settings: `lease` (default off; 60 s / 15 s when on), `retention` (7 days),
`alive_interval` (10 s), `cleanup_interval` (60 s) and `cleanup_batch`
(1000): finished tasks past retention are removed by index, a batch at a
time. Features `tls-rustls` and `tls-native-tls` enable
TLS for the connections the store opens itself.

## Testing

The integration tests need a database: set `TASKCRAFT_POSTGRES_URL` (for
example `postgres://postgres:postgres@localhost:5432/postgres`), or they are
skipped. Examples
[`durable-store`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft-postgres/examples/durable-store.rs)
and [`leases-failover`](https://github.com/Sebkd/taskcraft/blob/master/crates/taskcraft-postgres/examples/leases-failover.rs)
run against the database from the
[docker compose file](https://github.com/Sebkd/taskcraft/blob/master/examples/docker-compose.yml).

## License

MIT.
