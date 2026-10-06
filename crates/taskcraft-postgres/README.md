# taskcraft-postgres

A task store in PostgreSQL for [taskcraft](https://github.com/Sebkd/taskcraft):
queues keep their tasks, statuses and history outside the process.

- Connect with a connection string, or pass the application's
  `sqlx::PgPool` — also the pool of a sea-orm 2.x connection
  (`DatabaseConnection::get_postgres_connection_pool`).
- A poll claims the next due task for this process in one statement
  (`FOR UPDATE SKIP LOCKED`): no two processes get one task.
- Pushes and status requests are checked against the store: "already
  finished" for a finished task until its retention ends.
- A process restarted with the same id gives its unfinished tasks back
  first; two live processes cannot share an id.
- With leases, any process takes over a task whose owner stopped renewing
  it.

The store creates its tables (`taskcraft_tasks`, `taskcraft_processes`) on
start. Pair its queues with `taskcraft::JsonCodec`.

The integration tests need a database: set `TASKCRAFT_POSTGRES_URL`
(for example `postgres://postgres:postgres@localhost:5432/postgres`), or
they are skipped.
