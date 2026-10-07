//! Observers reach every queue of the monitor whatever the order of
//! `observer` and `register` (change api-0.2, criterion 2).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use taskcraft::codec::IdentityCodec;
use taskcraft::observe::{Event, Observer};
use taskcraft::{CancellationToken, InMemorySource, Monitor, Queue, Task, task_fn};

#[derive(Default)]
struct Finished(AtomicU32);

impl Observer for Finished {
    fn on_event(&self, event: &Event<'_>) {
        if matches!(event, Event::Finished { .. }) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[tokio::test]
async fn observer_added_after_register_gets_events() {
    let queue = Queue::builder(
        "q",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(|_: u32| async {}),
    )
    .no_recovery()
    .build()
    .unwrap();
    let before = Arc::new(Finished::default());
    let after = Arc::new(Finished::default());
    let (monitor, handle) = Monitor::new()
        .observer(Arc::clone(&before))
        .register(queue)
        .unwrap();
    let monitor = monitor.observer(Arc::clone(&after));
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));

    let _ = handle.push(Task::new(1)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while after.0.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the observer added after register saw the task finish");
    assert_eq!(before.0.load(Ordering::SeqCst), 1);
    stop.cancel();
    running.await.unwrap().unwrap();
}
