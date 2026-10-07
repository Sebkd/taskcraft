//! E-10 `observability` — the log through `tracing-subscriber` with
//! `RUST_LOG`, the standard series through the `metrics` adapter and a
//! Prometheus exporter, and an observer of your own (spec 2.9, 4.4).
//!
//! ```text
//! RUST_LOG=taskcraft=debug cargo run -p taskcraft --example observability --features metrics
//! ```
//!
//! Without `RUST_LOG` the log shows `taskcraft=info`: worker start and stop,
//! failures, retries.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_exporter_prometheus::PrometheusBuilder;
use taskcraft::codec::IdentityCodec;
use taskcraft::observe::{Event, MetricsObserver, Observer};
use taskcraft::{
    AckPoint, Attempt, CancellationToken, InMemorySource, Monitor, Outcome, Queue, RetryPolicy,
    Task, task_fn,
};
use tracing_subscriber::EnvFilter;

/// A consumer's own registry: counts what it cares about.
#[derive(Default)]
struct OwnCounters {
    finished: AtomicU32,
    retries: AtomicU32,
}

impl Observer for OwnCounters {
    fn on_event(&self, event: &Event<'_>) {
        match event {
            Event::Finished { .. } => {
                self.finished.fetch_add(1, Ordering::Relaxed);
            }
            Event::Retry { .. } => {
                self.retries.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

/// Arguments are private: the log and the metrics never show them.
async fn charge(card_number: String, Attempt(attempt): Attempt) -> Outcome {
    let _ = card_number;
    if attempt == 1 {
        Outcome::retry("payment gateway timeout")
    } else {
        Outcome::Success
    }
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The application chooses output and levels; the library only emits.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("taskcraft=info")),
        )
        .init();
    let recorder = PrometheusBuilder::new().build_recorder();
    let prometheus = recorder.handle();
    metrics::set_global_recorder(recorder)?;

    let own = Arc::new(OwnCounters::default());
    let queue = Queue::builder(
        "payments",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(charge),
    )
    .ack_point(AckPoint::OnCompletion)
    .retry_policy(RetryPolicy {
        max_attempts: 2,
        base: Duration::from_secs(1),
        ..RetryPolicy::default()
    })
    .build()?;
    let (monitor, handle) = Monitor::new()
        .observer(MetricsObserver::new())
        .observer(Arc::clone(&own))
        .register(queue)?;
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));
    for n in 0..3 {
        let _ = handle
            .push(Task::new(format!("4111-1111-1111-{n:04}")))
            .await?;
    }
    while own.finished.load(Ordering::Relaxed) < 3 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.cancel();
    running.await??;

    let exposition = prometheus.render();
    println!("--- Prometheus exposition ---");
    for line in exposition
        .lines()
        .filter(|l| l.starts_with("taskcraft_tasks"))
    {
        println!("{line}");
    }
    println!(
        "--- own observer: finished={}, retries={} ---",
        own.finished.load(Ordering::Relaxed),
        own.retries.load(Ordering::Relaxed)
    );
    let agree = exposition
        .contains(r#"taskcraft_tasks_finished_total{queue="payments",outcome="succeeded"} 3"#)
        && own.retries.load(Ordering::Relaxed) == 3;
    if !agree || exposition.contains("4111") {
        return Err("metrics disagree or leak arguments".into());
    }
    Ok(())
}
