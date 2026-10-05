# taskcraft

Background task queue for Rust on the tokio runtime. A sibling of
[statecraft-fsm](https://github.com/Sebkd/statecraft): the state machine runs
a multi-step process, taskcraft delivers, limits, retries and recovers the
work.

> **Status:** `0.0.0` only reserves the crate name. The first working version
> will be `0.1.0`.

## What it will do

- A source poll with three answers — a task, empty for now, closed — so an
  empty queue never stops a worker.
- Handler outcomes as data: succeed, retry, abort, defer. Panics are a final
  outcome and are never retried.
- Retries with an interruptible pause that releases the concurrency slot.
- Overflow policy per queue (wait or reject) and named resource pools.
- Status and cancellation by task id; pushing a duplicate id reports
  "already running" instead of creating a second task.
- Supervision on by default and a shutdown that wakes every sleeper at once.
- Optional: Kafka source, a durable task store with leases, statecraft-fsm
  integration, metrics through the `metrics` facade.

The full behaviour is specified in
[`openspec/specs/taskcraft/taskcraft.md`](openspec/specs/taskcraft/taskcraft.md)
(in Russian). Work is split into changes under
[`openspec/changes/`](openspec/changes/).

## Requirements

Rust 1.99 or newer, edition 2024.

## License

MIT, see [LICENSE](LICENSE).
