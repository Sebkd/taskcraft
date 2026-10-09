//! Worker benchmarks (spec 4.2): per-task time and throughput by concurrency,
//! the cost of pools and observers, latency from push to start.
//!
//! Real time on a multi-thread runtime, in-memory source, an empty handler.
//! Run: `cargo bench -p taskcraft-benches`.

// A benchmark harness may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use taskcraft::codec::IdentityCodec;
use taskcraft::observe::{Event, MetricsObserver, Observer};
use taskcraft::{
    CancellationToken, Data, InMemorySource, Monitor, PushOutcome, Queue, QueueHandle, SharedData,
    Task, task_fn,
};
use tokio::runtime::Runtime;
use tokio::sync::Notify;

const CAPACITY: usize = 1_000_000;

/// Counts finished handlers and wakes the bench once `target` is reached.
#[derive(Default)]
struct Counter {
    done: AtomicU64,
    target: AtomicU64,
    reached: Notify,
}

async fn work(n: u64, Data(counter): Data<Counter>) {
    black_box(n);
    let done = counter.done.fetch_add(1, Ordering::Relaxed) + 1;
    if done == counter.target.load(Ordering::Relaxed) {
        counter.reached.notify_one();
    }
}

/// An observer that counts finished tasks: the cheapest real observer.
#[derive(Default)]
struct Finished(AtomicU64);

impl Observer for Finished {
    fn on_event(&self, event: &Event<'_>) {
        if matches!(event, Event::Finished { .. }) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Copy)]
enum Extra {
    None,
    Pool,
    Metrics,
    /// The `metrics` adapter with a Prometheus recorder installed.
    Prometheus,
    Observer,
}

impl Extra {
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pool => "pool",
            Self::Metrics => "metrics",
            Self::Prometheus => "metrics_prometheus",
            Self::Observer => "observer",
        }
    }
}

/// How the tasks of one measurement are pushed.
#[derive(Clone, Copy)]
enum Feed {
    /// All at once; the time runs from the first push to the last outcome.
    Stream,
    /// One at a time; each push waits for its handler to run.
    OneByOne,
}

async fn measure(iters: u64, concurrency: usize, extra: Extra, feed: Feed) -> Duration {
    let mut shared = SharedData::new();
    shared.insert(Counter::default());
    let counter = shared.get::<Counter>().unwrap();
    let mut builder = Queue::builder(
        "bench",
        Arc::new(InMemorySource::new(CAPACITY).unwrap()),
        IdentityCodec::new(),
        task_fn(work),
    )
    .concurrency(concurrency)
    .shared_data(shared)
    .no_recovery();
    let mut monitor = Monitor::new();
    match extra {
        Extra::None => {}
        Extra::Pool => {
            builder = builder.pool("bench", 1);
            monitor = monitor
                .pool("bench", u32::try_from(concurrency).unwrap())
                .unwrap();
        }
        Extra::Metrics => monitor = monitor.observer(MetricsObserver::new()),
        Extra::Prometheus => {
            install_prometheus();
            monitor = monitor.observer(MetricsObserver::new());
        }
        Extra::Observer => monitor = monitor.observer(Finished::default()),
    }
    let queue = builder.build().unwrap();
    let stop = CancellationToken::new();
    let (monitor, handle) = monitor.register(queue).unwrap();
    let running = tokio::spawn(monitor.run(stop.clone()));

    let start = Instant::now();
    match feed {
        Feed::Stream => {
            counter.target.store(iters, Ordering::Relaxed);
            for n in 0..iters {
                push(&handle, n).await;
            }
            counter.reached.notified().await;
        }
        Feed::OneByOne => {
            for n in 0..iters {
                counter.target.store(n + 1, Ordering::Relaxed);
                push(&handle, n).await;
                counter.reached.notified().await;
            }
        }
    }
    let elapsed = start.elapsed();

    stop.cancel();
    running.await.unwrap().unwrap();
    elapsed
}

type Handle = QueueHandle<u64>;

async fn push(handle: &Handle, n: u64) {
    loop {
        match handle.push(Task::new(n)).await.unwrap() {
            PushOutcome::Enqueued { .. } => return,
            // A full source: let the worker catch up.
            _ => tokio::task::yield_now().await,
        }
    }
}

/// Installs a Prometheus recorder as the global one, once.
fn install_prometheus() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        metrics::set_global_recorder(recorder).unwrap();
    });
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn throughput(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(1));
    for concurrency in [1, 8, 64] {
        group.bench_with_input(
            BenchmarkId::new("concurrency", concurrency),
            &concurrency,
            |b, &concurrency| {
                b.to_async(&rt)
                    .iter_custom(|iters| measure(iters, concurrency, Extra::None, Feed::Stream));
            },
        );
    }
    group.finish();
}

fn overhead(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("overhead");
    group.throughput(Throughput::Elements(1));
    // The recorder is global: the Prometheus case runs after the others.
    for extra in [
        Extra::None,
        Extra::Pool,
        Extra::Metrics,
        Extra::Observer,
        Extra::Prometheus,
    ] {
        group.bench_function(extra.name(), |b| {
            b.to_async(&rt)
                .iter_custom(|iters| measure(iters, 8, extra, Feed::Stream));
        });
    }
    group.finish();
}

fn latency(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("latency");
    group.bench_function("push_to_start", |b| {
        b.to_async(&rt)
            .iter_custom(|iters| measure(iters, 1, Extra::None, Feed::OneByOne));
    });
    group.finish();
}

criterion_group!(benches, throughput, overhead, latency);
criterion_main!(benches);
