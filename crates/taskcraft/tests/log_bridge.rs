//! An application on `log`: with the `log` feature, library events arrive as
//! `log` records while no tracing subscriber is set (change criterion 5).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![cfg(feature = "log")]

use std::sync::{Arc, Mutex};

use taskcraft::codec::IdentityCodec;
use taskcraft::{CancellationToken, InMemorySource, Monitor, Queue, task_fn};

#[derive(Clone, Default)]
struct Records(Arc<Mutex<Vec<(String, String)>>>);

impl log::Log for Records {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        let entry = (record.target().to_owned(), record.args().to_string());
        self.0.lock().unwrap().push(entry);
    }

    fn flush(&self) {}
}

#[tokio::test]
async fn events_arrive_as_log_records() {
    let records = Records::default();
    log::set_logger(Box::leak(Box::new(records.clone()))).unwrap();
    log::set_max_level(log::LevelFilter::Trace);

    let source = Arc::new(InMemorySource::<u32>::default());
    let queue = Queue::builder(
        "bridged",
        source,
        IdentityCodec::new(),
        task_fn(|_: u32| async {}),
    )
    .no_recovery()
    .build()
    .unwrap();
    let stop = CancellationToken::new();
    let monitor = Monitor::new().register(queue).unwrap().0;
    let running = tokio::spawn(monitor.run(stop.clone()));
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    stop.cancel();
    running.await.unwrap().unwrap();

    assert!(!tracing::dispatcher::has_been_set());
    let records = records.0.lock().unwrap().clone();
    let started = records
        .iter()
        .find(|(_, text)| text.contains("worker started: queue=bridged"))
        .unwrap_or_else(|| panic!("no record: {records:?}"));
    assert!(started.0.starts_with("taskcraft"), "{started:?}");
}
