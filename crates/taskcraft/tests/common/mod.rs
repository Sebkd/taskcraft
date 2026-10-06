//! Helpers shared by the integration tests.

#![allow(dead_code, unreachable_pub)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

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

    /// Installs the capture for the current thread; tokio tests run on one.
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::registry().with(self.clone());
        tracing::subscriber::set_default(subscriber)
    }
}

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: tracing::Subscriber> Layer<S> for Captured {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().push(Record {
            level: *event.metadata().level(),
            fields,
        });
    }
}
