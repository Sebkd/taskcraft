//! The `metrics` adapter and a consumer's own observer (rule 2.3.22, spec
//! 4.4.2).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![cfg(feature = "metrics")]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use taskcraft::codec::IdentityCodec;
use taskcraft::observe::{Event, MetricsObserver, Observer};
use taskcraft::{
    AckPoint, Attempt, CancellationToken, InMemorySource, Meta, MetadataRegistry, Monitor, Outcome,
    Queue, RetryPolicy, Task, TaskState, task_fn,
};
use tokio::time::sleep;

mod common;
use common::{Captured, run};

const SEC: Duration = Duration::from_secs(1);

fn snapshotter() -> &'static Snapshotter {
    static RECORDER: OnceLock<Snapshotter> = OnceLock::new();
    RECORDER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder
            .install()
            .expect("no other recorder in this test binary");
        snapshotter
    })
}

/// The recorder is global and a snapshot drains its counters, so the
/// scenarios run one after another.
#[tokio::test(start_paused = true)]
async fn standard_series() {
    adapter_and_own_observer_agree().await;
    arguments_and_metadata_stay_out_of_logs_and_labels().await;
}

/// Series of one queue: (name, labels other than `queue`) → value.
type Series = BTreeMap<(String, Vec<(String, String)>), DebugValue>;

fn series(queue: &str) -> Series {
    snapshotter()
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key()
                .labels()
                .any(|l| l.key() == "queue" && l.value() == queue)
        })
        .map(|(key, _, _, value)| {
            let labels = key
                .key()
                .labels()
                .filter(|l| l.key() != "queue")
                .map(|l| (l.key().to_owned(), l.value().to_owned()))
                .collect();
            ((key.key().name().to_owned(), labels), value)
        })
        .collect()
}

fn counter(series: &Series, name: &str, labels: &[(&str, &str)]) -> u64 {
    let labels = labels
        .iter()
        .map(|&(k, v)| (String::from(k), String::from(v)))
        .collect();
    match series.get(&(name.to_owned(), labels)) {
        Some(DebugValue::Counter(n)) => *n,
        _ => 0,
    }
}

/// A consumer's own registry: counts the same events.
#[derive(Default)]
struct OwnCounters {
    accepted: AtomicU32,
    retries: AtomicU32,
    finished: Mutex<BTreeMap<&'static str, u32>>,
}

impl Observer for OwnCounters {
    fn on_event(&self, event: &Event<'_>) {
        match event {
            Event::Accepted { .. } => {
                self.accepted.fetch_add(1, Ordering::SeqCst);
            }
            Event::Retry { .. } => {
                self.retries.fetch_add(1, Ordering::SeqCst);
            }
            Event::Finished { state, .. } => {
                *self
                    .finished
                    .lock()
                    .unwrap()
                    .entry(state.as_str())
                    .or_default() += 1;
            }
            _ => {}
        }
    }
}

/// Criterion 32 and change criterion 1: ten tasks with every outcome — the
/// adapter's series and the own observer agree, and the series are named and
/// labelled as in spec 4.4.2.
async fn adapter_and_own_observer_agree() {
    snapshotter();
    let source = Arc::new(InMemorySource::<u32>::default());
    let handler = task_fn(|n: u32, Attempt(attempt): Attempt| async move {
        match (n % 5, attempt) {
            (1, _) => Outcome::abort("no"),
            (2, 1) => Outcome::retry("again"),
            (3, _) => panic!("handler bug"),
            (4, 1) => Outcome::defer(SEC, "later"),
            _ => Outcome::Success,
        }
    });
    let queue = Queue::builder("agree", Arc::clone(&source), IdentityCodec::new(), handler)
        .ack_point(AckPoint::OnCompletion)
        .concurrency(4)
        .retry_policy(RetryPolicy {
            max_attempts: 3,
            base: SEC,
            factor: 1.0,
            max: SEC,
            jitter: 0.0,
            hold_slot: false,
        })
        .build()
        .unwrap();
    let own = Arc::new(OwnCounters::default());
    let (monitor, handle) = Monitor::new()
        .observer(MetricsObserver::new())
        .observer(Arc::clone(&own))
        .register(queue)
        .unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(run(monitor, stop.clone()));
    for n in 0..10 {
        let _ = handle.push(Task::new(n)).await.unwrap();
    }
    // Every task ends once; deferred ones also end their first delivery.
    let finished = || own.finished.lock().unwrap().values().sum::<u32>();
    assert!(
        tokio::time::timeout(60 * SEC, async {
            while finished() < 12 || !source.is_empty() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    );
    stop.cancel();
    running.await.unwrap();

    let s = series("agree");
    let own_finished = own.finished.lock().unwrap().clone();
    assert_eq!(
        own_finished,
        BTreeMap::from([
            ("deferred", 2),
            ("failed", 2),
            ("panicked", 2),
            ("succeeded", 6)
        ])
    );
    for (outcome, n) in &own_finished {
        let value = counter(
            &s,
            "taskcraft_tasks_finished_total",
            &[("outcome", outcome)],
        );
        assert_eq!(value, u64::from(*n), "outcome {outcome}");
    }
    let accepted = u64::from(own.accepted.load(Ordering::SeqCst));
    assert_eq!(accepted, 12);
    assert_eq!(counter(&s, "taskcraft_tasks_accepted_total", &[]), accepted);
    let retries = u64::from(own.retries.load(Ordering::SeqCst));
    assert_eq!(retries, 2);
    assert_eq!(counter(&s, "taskcraft_task_retries_total", &[]), retries);
    assert_eq!(counter(&s, "taskcraft_task_panics_total", &[]), 2);

    // Names and labels of spec 4.4.2.
    let allowed: BTreeMap<&str, &[&str]> = BTreeMap::from([
        ("taskcraft_tasks_accepted_total", &[][..]),
        ("taskcraft_tasks_rejected_total", &["reason"][..]),
        ("taskcraft_tasks_duplicate_total", &[][..]),
        ("taskcraft_tasks_finished_total", &["outcome"][..]),
        ("taskcraft_task_retries_total", &[][..]),
        ("taskcraft_task_panics_total", &[][..]),
        ("taskcraft_tasks_running", &[][..]),
        ("taskcraft_tasks_waiting", &["state"][..]),
        ("taskcraft_attempt_duration_seconds", &["outcome"][..]),
        ("taskcraft_decode_errors_total", &[][..]),
        ("taskcraft_source_errors_total", &[][..]),
        ("taskcraft_worker_restarts_total", &[][..]),
    ]);
    for (name, labels) in s.keys() {
        let keys: Vec<&str> = labels.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            Some(&&keys[..]),
            allowed.get(name.as_str()),
            "{name} {labels:?}"
        );
    }
    let running = s
        .iter()
        .find(|((name, _), _)| name == "taskcraft_tasks_running")
        .map(|(_, v)| v);
    assert_eq!(
        running,
        Some(&DebugValue::Gauge(0.0.into())),
        "nothing runs at the end"
    );
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Recipient(String);

const MARKER: &str = "MARKER-7f3a9c";

/// Criterion 34: a marker in the arguments and metadata never reaches the
/// log or metric labels, whatever the outcome.
async fn arguments_and_metadata_stay_out_of_logs_and_labels() {
    snapshotter();
    let logs = Captured::default();
    let _guard = logs.install();
    let source = Arc::new(InMemorySource::<String>::default());
    let handler = task_fn(
        |args: String, Meta(Recipient(who)): Meta<Recipient>, Attempt(attempt): Attempt| async move {
            assert!(args.contains(MARKER) && who.contains(MARKER));
            match (args.len() % 4, attempt) {
                (1, 1) => Outcome::retry("transient"),
                (2, _) => Outcome::abort("rejected input"),
                (3, _) => panic!("handler bug"),
                _ => Outcome::Success,
            }
        },
    );
    let registry = MetadataRegistry::new()
        .register::<Recipient>("mail.recipient")
        .unwrap();
    let queue = Queue::builder("marker", Arc::clone(&source), IdentityCodec::new(), handler)
        .ack_point(AckPoint::OnCompletion)
        .metadata_registry(registry)
        .retry_policy(RetryPolicy {
            max_attempts: 2,
            base: SEC,
            factor: 1.0,
            max: SEC,
            jitter: 0.0,
            hold_slot: false,
        })
        .build()
        .unwrap();
    let (monitor, handle) = Monitor::new()
        .observer(MetricsObserver::new())
        .register(queue)
        .unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(run(monitor, stop.clone()));
    for pad in 0..4 {
        let args = format!("{MARKER}{}", "x".repeat(pad));
        let task = Task::new(args).with_meta(Recipient(MARKER.to_owned()));
        let _ = handle.push(task).await.unwrap();
    }
    assert!(
        tokio::time::timeout(60 * SEC, async {
            while !source.is_empty() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    );
    stop.cancel();
    running.await.unwrap();

    let records = logs.records();
    assert!(records.len() > 8);
    for record in &records {
        for value in record.fields.values() {
            assert!(!value.contains(MARKER), "in the log: {value}");
        }
    }
    let labels: BTreeSet<String> = series("marker")
        .keys()
        .flat_map(|(_, labels)| labels.iter().map(|(_, v)| v.clone()))
        .collect();
    assert!(labels.iter().all(|l| !l.contains(MARKER)), "{labels:?}");
    assert!(labels.contains("panicked") && labels.contains("failed"));
    let _ = TaskState::Succeeded;
}
