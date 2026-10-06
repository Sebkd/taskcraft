//! Helpers shared by the integration tests.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![allow(dead_code, unreachable_pub)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Once};

use taskcraft::{CancellationToken, Monitor, ShutdownReport};
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};

#[derive(Debug, Clone)]
pub struct Record {
    pub level: tracing::Level,
    pub fields: BTreeMap<String, String>,
}

impl Record {
    pub fn is(&self, event: &str, action: &str) -> bool {
        self.fields.get("event").map(String::as_str) == Some(event)
            && self.fields.get("action").map(String::as_str) == Some(action)
    }
}

#[derive(Clone, Default)]
pub struct Captured(Arc<Mutex<Vec<Record>>>);

impl Captured {
    pub fn records(&self) -> Vec<Record> {
        self.0.lock().unwrap().clone()
    }

    pub fn count(&self, event: &str, action: &str) -> usize {
        self.records()
            .iter()
            .filter(|r| r.is(event, action))
            .count()
    }

    /// Captures events of the current thread until the guard drops; tokio
    /// tests run on one.
    ///
    /// One global subscriber routes events to the capture of their thread.
    /// A per-thread default subscriber would race with tests running in
    /// parallel without one: a callsite first hit there may cache "no
    /// interest" and the capturing test would miss its events.
    pub fn install(&self) -> Installed {
        static GLOBAL: Once = Once::new();
        GLOBAL.call_once(|| {
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Router))
                .expect("no other global subscriber in tests");
            tracing::callsite::rebuild_interest_cache();
        });
        Installed(CURRENT.with(|current| current.replace(Some(self.clone()))))
    }

    fn push(&self, record: Record) {
        self.0.lock().unwrap().push(record);
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Captured>> = const { RefCell::new(None) };
}

/// Restores the earlier capture of the thread on drop.
pub struct Installed(Option<Captured>);

impl Drop for Installed {
    fn drop(&mut self) {
        let earlier = self.0.take();
        CURRENT.with(|current| *current.borrow_mut() = earlier);
    }
}

/// Sends every event to the capture of its thread, if any.
struct Router;

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: tracing::Subscriber> Layer<S> for Router {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let Some(capture) = CURRENT.with(|current| current.borrow().clone()) else {
            return;
        };
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        capture.push(Record {
            level: *event.metadata().level(),
            fields,
        });
    }
}

/// Runs the monitor; the tests' recovery hooks never fail.
pub async fn run(monitor: Monitor, stop: CancellationToken) -> ShutdownReport {
    monitor.run(stop).await.expect("recovery hooks succeed")
}
