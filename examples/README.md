# taskcraft examples

From the smallest queue to a multi-step export run by a state machine. Every
example checks its own result: it prints what happens and exits with an error
if the result is not what it describes. Long examples run on **virtual time**
— hours of work pass in milliseconds — and say so in their output.

Sections refer to the specification,
[`openspec/specs/taskcraft/taskcraft.md`](../openspec/specs/taskcraft/taskcraft.md).

| # | Example | Shows | Spec | Needs | Run |
|---|---------|-------|------|-------|-----|
| E-1 | [`hello`](../crates/taskcraft/examples/hello.rs) | The minimum: in-memory queue, handler, push, shutdown; shared data | 2.1.1, 2.5 | — | `cargo run -p taskcraft --example hello` |
| E-2 | [`outcomes`](../crates/taskcraft/examples/outcomes.rs) | Success, retry with a pause, abort, defer, panic; attempt counts | 2.3.2–2.3.6 | — | `cargo run -p taskcraft --example outcomes` |
| E-3 | [`long-tasks-reject`](../crates/taskcraft/examples/long-tasks-reject.rs) | Hour-long tasks; "reject" answers the sender, intake goes on; shutdown ends a retry pause at once | 2.2.1, 2.3.7, 2.3.14 | — | `cargo run -p taskcraft --example long-tasks-reject` |
| E-4 | [`resource-pools`](../crates/taskcraft/examples/resource-pools.rs) | Pools of different sizes; status and cancel by id; permits back on cancel and panic | 2.3.8, 2.1.2.14, 2.1.2.15 | — | `cargo run -p taskcraft --example resource-pools` |
| E-5 | [`idempotent-push`](../crates/taskcraft/examples/idempotent-push.rs) | Pushing twice: "already running", a race of two pushes | 2.2.3, 2.3.11 | — | `cargo run -p taskcraft --example idempotent-push` |
| E-6 | [`graceful-shutdown`](../crates/taskcraft/examples/graceful-shutdown.rs) | Ctrl-C, shutdown timeout, cancel grace, shutdown report | 2.3.14, 2.7.6 | — | `cargo run -p taskcraft --example graceful-shutdown` |
| E-7 | [`metadata`](../crates/taskcraft/examples/metadata.rs) | Metadata types of two scenarios; required and optional; registry; unknown names kept | 2.3.17, 2.7.2 | — | `cargo run -p taskcraft --example metadata` |
| E-8 | [`custom-source`](../crates/taskcraft/examples/custom-source.rs) | A source of your own, consumed by a queue (`Queue::consumer`): task, empty, closed; wake-up; acks | 2.3.1, 2.5 | — | `cargo run -p taskcraft --example custom-source` |
| E-9 | [`custom-runnable`](../crates/taskcraft/examples/custom-runnable.rs) | A runnable that is not a state machine: a thread with a soft stop | 2.3.21 | — | `cargo run -p taskcraft --example custom-runnable` |
| E-10 | [`observability`](../crates/taskcraft/examples/observability.rs) | `tracing-subscriber` with `RUST_LOG`, Prometheus through the `metrics` adapter, an observer of your own | 2.9, 4.4 | feature `metrics` | `RUST_LOG=taskcraft=debug cargo run -p taskcraft --example observability --features metrics` |
| E-11 | [`axum-service`](axum-service/src/main.rs) | HTTP: push, status and cancel by id | 2.1.2.1, 2.1.2.14, 2.1.2.15 | standalone crate | `cargo run --manifest-path examples/axum-service/Cargo.toml` |
| E-12 | [`kafka-consumer`](../crates/taskcraft-kafka/examples/kafka-consumer.rs) | Kafka source consumed by a queue (`Queue::consumer`), ack on accept with a recovery hook, partitions shared by two processes | 2.6 "Kafka", 2.3.9, 2.3.18 | Kafka | `cargo run -p taskcraft-kafka --example kafka-consumer` |
| E-13 | [`durable-store`](../crates/taskcraft-postgres/examples/durable-store.rs) | Task store (`Queue::on_store`, JSON codec built in): push, status of finished tasks, defer, restart without leases | 2.6 "Task store", 2.3.19 | PostgreSQL | `cargo run -p taskcraft-postgres --example durable-store` |
| E-14 | [`leases-failover`](../crates/taskcraft-postgres/examples/leases-failover.rs) | Two processes with leases: one crashes, the other takes over | 2.3.20, 2.1.2.18, 2.2.9 | PostgreSQL | `cargo run -p taskcraft-postgres --example leases-failover` |
| E-15 | [`fsm-pipeline`](../crates/taskcraft/examples/fsm-pipeline.rs) | **The heavy case.** A multi-step export as a statecraft-fsm machine: a copy pool, a six-hour wait, cancel as a soft stop, a panicking step, recovery after a crash from the consumer's records | 2.3.21, 2.3.8, 2.3.15, 2.3.18 | statecraft-fsm (dev) | `cargo run -p taskcraft --example fsm-pipeline` |

## Examples with infrastructure

E-12…E-14 need Kafka or PostgreSQL. Start them with Docker:

```sh
docker compose -f examples/docker-compose.yml up -d kafka      # E-12
docker compose -f examples/docker-compose.yml up -d postgres   # E-13, E-14
docker compose -f examples/docker-compose.yml down              # afterwards
```

`TASKCRAFT_KAFKA_BROKERS` and `TASKCRAFT_POSTGRES_URL` point the examples at
other addresses.

## Notes

- E-15 uses the adapter for statecraft-fsm as it is today: the machine leaves
  its outcome in an `OutcomeSlot` and the task's cancel flag reaches it as a
  soft stop. A generic implementation behind a `statecraft` feature follows a
  change in statecraft-fsm.
- In E-15 the consumer closes its own record when a task ends (through an
  observer), so the recovery hook brings back only exports still in progress
  — not cancelled or panicked ones.
- CI builds and lints every example; it does not run them. A change that
  alters what an example shows updates that example and runs it by hand.
