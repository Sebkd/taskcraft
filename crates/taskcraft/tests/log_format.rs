//! What an application sees in the log (spec 2.9, 4.4.1).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![cfg(feature = "test-util")]

use std::collections::BTreeSet;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::testing::{FaultyCodec, FaultySource};
use taskcraft::{
    AckPoint, Attempt, CancellationToken, Monitor, Outcome, Queue, RetryPolicy, Task, TraceParent,
    task_fn,
};
use tokio::time::sleep;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Level, Subscriber};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, fmt};

const SEC: Duration = Duration::from_secs(1);
const DOMAINS: [&str; 7] = [
    "task", "worker", "source", "shutdown", "recovery", "lease", "observer",
];

/// Every outcome, a source error and a shutdown.
async fn scenario(trace_parent: Option<&str>) {
    let source = Arc::new(FaultySource::<u32>::new());
    source.fail_next_polls(1);
    let handler = task_fn(|n: u32, Attempt(attempt): Attempt| async move {
        match (n, attempt) {
            (1, 1) => Outcome::retry("transient"),
            (2, _) => Outcome::abort("rejected input"),
            (3, _) => panic!("handler bug"),
            _ => Outcome::Success,
        }
    });
    let queue = Queue::builder("logs", Arc::clone(&source), FaultyCodec::new(), handler)
        .ack_point(AckPoint::OnCompletion)
        .concurrency(4)
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
    let monitor = Monitor::new()
        .restart_delays(SEC, SEC)
        .unwrap()
        .register(queue)
        .unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));
    for n in 0..4 {
        let mut task = Task::new(n);
        if let Some(parent) = trace_parent {
            task = task.with_meta(TraceParent(parent.to_owned()));
        }
        source.enqueue(task);
    }
    tokio::time::timeout(60 * SEC, async {
        while source.acks() < 4 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    running.await.unwrap().unwrap();
}

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for Buffer {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A target or span name with its fields.
type Entry = (String, Vec<(String, String)>);

/// Events and spans of a debug-level run.
#[derive(Clone, Default)]
struct Seen {
    events: Arc<Mutex<Vec<Entry>>>,
    spans: Arc<Mutex<Vec<Entry>>>,
}

struct Fields<'a>(&'a mut Vec<(String, String)>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_owned(), value.to_owned()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Seen {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Vec::new();
        event.record(&mut Fields(&mut fields));
        let target = event.metadata().target().to_owned();
        self.events.lock().unwrap().push((target, fields));
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, _: &Id, _: Context<'_, S>) {
        let mut fields = Vec::new();
        attrs.record(&mut Fields(&mut fields));
        let name = attrs.metadata().name().to_owned();
        self.spans.lock().unwrap().push((name, fields));
    }

    fn on_record(&self, _: &Id, values: &Record<'_>, _: Context<'_, S>) {
        let mut fields = Vec::new();
        values.record(&mut Fields(&mut fields));
        if let Some(last) = self.spans.lock().unwrap().last_mut() {
            last.1.extend(fields);
        }
    }
}

/// Criterion 48 with change criteria 2 and 4. One test: the subscribers are
/// per thread and set one after another.
#[tokio::test(start_paused = true)]
async fn the_log_as_an_application_sees_it() {
    // Criterion 48: a JSON subscriber at INFO, with its default span fields.
    let buffer = Buffer::default();
    let json = {
        let buffer = buffer.clone();
        fmt()
            .json()
            .with_max_level(Level::INFO)
            .with_writer(move || buffer.clone())
            .finish()
    };
    let guard = tracing::subscriber::set_default(json);
    scenario(None).await;
    drop(guard);

    let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    let mut actions = BTreeSet::new();
    for line in output.lines() {
        let record: serde_json::Value = serde_json::from_str(line).unwrap();
        let object = record.as_object().unwrap();
        assert!(
            !object.contains_key("span") && !object.contains_key("spans"),
            "no span fields at INFO: {line}"
        );
        assert!(object["target"].as_str().unwrap().starts_with("taskcraft"));
        let fields = object["fields"].as_object().unwrap();
        let keys: BTreeSet<&str> = fields.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            BTreeSet::from(["event", "action", "message"]),
            "{line}"
        );
        let event = fields["event"].as_str().unwrap();
        let action = fields["action"].as_str().unwrap();
        let message = fields["message"].as_str().unwrap();
        assert!(DOMAINS.contains(&event), "{line}");
        assert!(
            action.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "{line}"
        );
        assert!(!message.is_empty() && message != "{}", "{line}");
        assert!(message.contains("queue=") || event == "shutdown", "{line}");
        actions.insert(format!("{event}/{action}"));
    }
    for expected in [
        "source/failed",
        "worker/resumed",
        "task/retry",
        "task/failed",
        "task/panicked",
        "worker/stopped",
        "shutdown/finished",
    ] {
        assert!(
            actions.contains(expected),
            "{expected} missing: {actions:?}"
        );
    }

    // Change criteria 4 and 2: `taskcraft=debug` shows accept, start and
    // outcome of attempts; the attempt span carries the trace context.
    let seen = Seen::default();
    let debug = tracing_subscriber::registry().with(
        seen.clone()
            .with_filter(Targets::new().with_target("taskcraft", Level::DEBUG)),
    );
    let guard = tracing::subscriber::set_default(debug);
    scenario(Some(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    ))
    .await;
    drop(guard);

    let events = seen.events.lock().unwrap().clone();
    assert!(
        events
            .iter()
            .all(|(target, _)| target.starts_with("taskcraft"))
    );
    let actions: BTreeSet<String> = events
        .iter()
        .filter_map(|(_, fields)| {
            fields
                .iter()
                .find(|(k, _)| k == "action")
                .map(|(_, v)| v.clone())
        })
        .collect();
    for expected in ["accepted", "started", "finished"] {
        assert!(
            actions.contains(expected),
            "{expected} missing: {actions:?}"
        );
    }
    let spans = seen.spans.lock().unwrap().clone();
    let attempts: Vec<_> = spans
        .iter()
        .filter(|(name, _)| name == "taskcraft.attempt")
        .collect();
    assert!(!attempts.is_empty());
    for (_, fields) in attempts {
        let parent = fields.iter().find(|(k, _)| k == "trace_parent");
        assert_eq!(
            parent.map(|(_, v)| v.as_str()),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
    }
}
