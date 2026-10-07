# Migrating from taskcraft 0.1 to 0.2

0.2 changes the public API in one go; queues behave exactly as before. The
compiler points at every place to change. This guide shows what each error
means.

```toml
[dependencies]
taskcraft = "0.2"
taskcraft-kafka = "0.2"    # if used
taskcraft-postgres = "0.2" # if used
```

## At a glance

| 0.1 | 0.2 |
|-----|-----|
| `let h = queue.handle(); Monitor::new().register(queue)?` | `let (monitor, h) = Monitor::new().register(queue)?;` |
| `QueueHandle<InMemorySource<String>, IdentityCodec<String>, String>` | `QueueHandle<String>` |
| Kafka: `Queue::builder(name, source, codec, handler)` | `Queue::consumer(name, source, codec, handler)`; its handle is a `ConsumerHandle` (no `push`) |
| PostgreSQL: `Queue::builder(name, store.queue(..), JsonCodec::new(registry), handler)` | `Queue::on_store(name, store.queue(..), handler)` with `.metadata_registry(registry)` |
| Your source: `impl Source` with `push`, `remove`, `defer` | `impl Source` (poll, ack) + `impl PushSource` (push, remove, defer) |
| `observer()` had to come before `register()` | Any order: observers reach every queue when the monitor runs |
| `use taskcraft::{IdentityCodec, Event, Source, …}` | `use taskcraft::{codec::IdentityCodec, observe::Event, source::Source, …}` or `use taskcraft::prelude::*` |

## The handle comes from `register`

`Queue::handle` is gone. `Monitor::register` returns the monitor together
with the queue's handle, so the handle cannot be taken from a queue that was
never registered.

```rust
// 0.1
let reports = queue.handle();
let running = tokio::spawn(Monitor::new().register(queue)?.run(stop.clone()));

// 0.2
let (monitor, reports) = Monitor::new().register(queue)?;
let running = tokio::spawn(monitor.run(stop.clone()));
```

Several queues are registered one after another:

```rust
let monitor = Monitor::new().pool("disk-io", 2)?;
let (monitor, unpacks) = monitor.register(unpack)?;
let (monitor, packs) = monitor.register(pack)?;
```

A queue whose handle you do not need: `let (monitor, _) = …`.

## Handles are typed by the arguments only

```rust
// 0.1
type Exports = QueueHandle<InMemorySource<String>, IdentityCodec<String>, String>;
// 0.2
type Exports = QueueHandle<String>;
```

The source and codec are hidden behind the handle; a push costs one more
allocation than before, the worker loop none.

## Three kinds of source

The single `Source` trait of 0.1 is three traits now. What a source can do
decides how its queue is built and what its handle offers.

| Source | Trait | Queue | Handle |
|--------|-------|-------|--------|
| Tasks pushed from code: `InMemorySource`, your own | `source::PushSource` (and `Source`) | `Queue::builder` | `QueueHandle`: push, status, cancel |
| A stream to consume: `KafkaSource`, an outbox table | `source::Source` | `Queue::consumer` | `ConsumerHandle`: status, cancel |
| A durable task store: `PgSource` | `source::TaskStore` | `Queue::on_store` | `QueueHandle` |

Pushing into a Kafka queue was a runtime error (`PushTaskError::Unsupported`);
now `ConsumerHandle` has no `push` and the code does not compile. A task
store cannot be built with `Queue::builder` or `Queue::consumer`, so it can
no longer be run without recording task outcomes.

### Kafka

```rust
// 0.1
let queue = Queue::builder("exports", Arc::new(source), codec, task_fn(export))
// 0.2
let queue = Queue::consumer("exports", Arc::new(source), codec, task_fn(export))
```

### PostgreSQL task store

The store keeps tasks as JSON. Its codec is built in and names metadata
through the queue's own registry: in 0.1 the codec and the handlers each had
a registry, and a mismatch showed only in the database.

```rust
// 0.1
let queue = Queue::builder("transfers", Arc::new(store.queue("transfers")), JsonCodec::new(registry.clone()), task_fn(transfer))
    .metadata_registry(registry)
// 0.2
let queue = Queue::on_store("transfers", Arc::new(store.queue("transfers")), task_fn(transfer))
    .metadata_registry(registry)
```

The table layout and the stored JSON are unchanged: a 0.2 process reads tasks
written by 0.1.

### Your own source

Split the implementation by what the source does:

```rust
use taskcraft::source::{Capabilities, Polled, PushError, PushResult, PushSource, Source, Withdrawal};

impl Source for Outbox {
    type Message = Task<String>;
    type Receipt = u64;
    type Error = Infallible;

    fn capabilities(&self) -> Capabilities { /* no `with_push()` any more */ }
    async fn poll(&self) -> Result<Polled<Task<String>, u64>, Infallible> { /* … */ }
    async fn ack(&self, receipt: u64) -> Result<(), Infallible> { /* … */ }
    // `subscribe` and `notices` stay here, optional as before.
}

// Only if tasks are pushed into it through the handle:
impl PushSource for Outbox {
    async fn push(&self, id: &TaskId, message: Task<String>) -> Result<PushResult, PushError<Infallible>> { /* … */ }
    async fn remove(&self, id: &TaskId) -> Result<Withdrawal, Infallible> { /* … */ }
    // `defer` stays optional; declare it with `Capabilities::with_defer`.
}
```

A source without `PushSource` is built with `Queue::consumer`. Removed with
the old trait: `Capabilities::with_push` and `accepts_push`,
`PushError::Unsupported`, `PushTaskError::Unsupported`. A durable store
implements `TaskStore` (poll, push, remove, defer, complete, progress,
status) on `StoreMessage`.

## Observers reach every queue

In 0.1 a queue got the observers added before its `register`. In 0.2 the
monitor hands its observers to every queue when it runs, whatever the order
of `observer` and `register`. Pushes made before `run` are not observed, as
before registration in 0.1.

## Modules

The crate root kept the names nearly every program uses; the rest moved into
modules. `taskcraft::prelude::*` brings the everyday set, codecs included.

| Module | Moved there |
|--------|-------------|
| `source` | `Source`, `PushSource`, `TaskStore`, `StoreMessage`, `Polled`, `CloseReason`, `Capabilities`, `AckPointSupport`, `PushResult`, `Withdrawal`, `Completion`, `Progress`, `Notice`, `Notices`, `PushError`, `DeferError`, `WakeHandle`, `WakeSignal`, `Delivery`, `OffsetTracker`, `Poller`, `Wakeup` |
| `codec` | `Codec`, `CodecError`, `IdentityCodec`, `JsonCodec` |
| `handler` | `Handler`, `TaskFn`, `TaskRequest`, `FromTask`, `Rejection`, `BoxFuture`, `HandlerOutput`, `IntoOutcome`, `CatchPanic`, `catch_panic`, `run_attempt`, `outcome_of` |
| `observe` | `Observer`, `Event`, `AttemptEnd`, `MetricsObserver` |
| `runnable` | `Runnable`, `Run`, `SpawnedMachine`, `OutcomeSlot`, `MachineEnd` |
| `error` | `ConfigError`, `InvalidTransition`, `MetadataError`, `RecoveryError`, `PushTaskError` |

`Attempt`, `Cancel`, `Data`, `Meta`, `SharedData` and `task_fn` stay at the
root (and are in `handler` too).
