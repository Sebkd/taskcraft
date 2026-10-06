# Changelog

All notable changes to the taskcraft packages. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Each change is also recorded as an
OpenSpec change under
[`openspec/changes/archive`](https://github.com/Sebkd/taskcraft/tree/master/openspec/changes/archive).

## [0.1.0] — unreleased

The first working version of `taskcraft`, `taskcraft-kafka` and
`taskcraft-postgres`.

### `taskcraft`

- Queues, workers and a monitor on tokio: a poll answers with a task, "empty
  for now" or "closed"; workers restart after source errors with growing
  delays; a graceful shutdown with a timeout, a cancel grace and a report.
- Handlers as plain `async fn` with extracted parameters (`Attempt`,
  `TaskId`, `Meta<T>`, `Data<T>`, `Cancel`) on top of `tower`.
- Outcomes as data: success, retry, abort, defer; panics are final. Retry
  policy with exponential pauses and jitter; pauses give the slot back
  unless told to keep it.
- Overflow policies (wait with a limit, or reject with a hook) and named
  resource pools shared by queues, taken in name order.
- Push, status and cancel by id through `QueueHandle`; idempotent intake by
  task id; cancel flags with a cancel grace; attempt timeouts.
- Recovery hook on start; `no_recovery()` for queues that may lose accepted
  tasks.
- Runnables: a handler may return a process (`Run<R>`); `SpawnedMachine`
  adapts a statecraft-fsm machine.
- In-memory source; codecs (`IdentityCodec`, `JsonCodec`) with a metadata
  registry; poison messages go to a dead-letter hook.
- Observers of every event; `metrics` feature with the standard series;
  `log` feature; `tracing` log in the `event`/`action`/`message` format and
  DEBUG attempt spans with the W3C trace parent.
- `test-util` feature: a fault-injecting source, a delivery ledger and
  reusable worker scenarios.
- No panics in library code, enforced by lints and a source scan.

### `taskcraft-kafka`

- A Kafka source on `rdkafka`: auto commit off, offsets committed by the
  commit boundary, keys as task ids, metadata from headers, partitions shared
  by consumer groups.

### `taskcraft-postgres`

- A PostgreSQL task store on `sqlx` 0.9, connected by URL or an existing
  pool (sea-orm 2.x included): claim on poll with `FOR UPDATE SKIP LOCKED`,
  recorded outcomes with retention, "already finished" pushes, status of any
  task, cancel across processes, restart recovery by process id, leases with
  heartbeats and takeover.

[0.1.0]: https://github.com/Sebkd/taskcraft/releases/tag/v0.1.0
