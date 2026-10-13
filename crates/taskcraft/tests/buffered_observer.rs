//! An observer behind a buffer (change async-observer): a slow one does not
//! slow the tasks, events keep their order, losses are counted.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use taskcraft::codec::IdentityCodec;
use taskcraft::error::ConfigError;
use taskcraft::observe::{Buffered, Event, Observer};
use taskcraft::{CancellationToken, InMemorySource, Monitor, Queue, Task, TaskId, task_fn};

mod common;
use common::Captured;

/// Records the task of every `Pushed` and `Finished` — with a `sleep`, of
/// every `Finished` only, sleeping on each; panics on a task named "boom",
/// and waits while `hold` is set.
#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<String>>,
    sleep: Option<Duration>,
    hold: AtomicBool,
    held: AtomicBool,
}

impl Observer for Recorder {
    fn on_event(&self, event: &Event<'_>) {
        let task_id = match event {
            Event::Finished { task_id, .. } => *task_id,
            Event::Pushed { task_id, .. } if self.sleep.is_none() => *task_id,
            _ => return,
        };
        while self.hold.load(Ordering::SeqCst) {
            self.held.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_ne!(task_id.as_str(), "boom", "observer bug");
        if let (Some(sleep), Event::Finished { .. }) = (self.sleep, event) {
            std::thread::sleep(sleep);
        }
        self.seen.lock().unwrap().push(task_id.as_str().to_owned());
    }
}

fn pushed(id: &TaskId) -> Event<'_> {
    Event::Pushed {
        queue: "q",
        task_id: id,
    }
}

async fn until(limit: Duration, cond: impl Fn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

/// Change criterion 1: an observer sleeping on every event does not slow
/// the tasks; it gets their events later.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_observer_does_not_slow_the_tasks() {
    let slow = Arc::new(Recorder {
        sleep: Some(Duration::from_millis(100)),
        ..Recorder::default()
    });
    let done = Arc::new(Mutex::new(0_u32));
    let count = Arc::clone(&done);
    let queue = Queue::builder(
        "q",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(move |_: u32| {
            *count.lock().unwrap() += 1;
            async {}
        }),
    )
    .concurrency(4)
    .no_recovery()
    .build()
    .unwrap();
    let (monitor, handle) = Monitor::new()
        .observer(Buffered::new(Arc::clone(&slow), 1000).unwrap())
        .register(queue)
        .unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));

    let started = Instant::now();
    for n in 0..20 {
        let _ = handle
            .push(Task::new(n).with_id(format!("t{n}")))
            .await
            .unwrap();
    }
    assert!(until(Duration::from_secs(5), || *done.lock().unwrap() == 20).await);
    let took = started.elapsed();
    assert!(took < Duration::from_secs(1), "tasks took {took:?}");
    // 20 finishes at 100 ms each: the observer is still behind. (Pushes
    // made before the monitor started reach no observer, rule 2.3.22 p. 2:
    // only the finishes are counted.)
    assert!(slow.seen.lock().unwrap().len() < 20);
    let all = until(Duration::from_secs(10), || {
        slow.seen.lock().unwrap().len() == 20
    })
    .await;
    assert!(
        all,
        "every event arrives: {:?}",
        slow.seen.lock().unwrap().len()
    );
    stop.cancel();
    running.await.unwrap().unwrap();
}

/// Events come to the inner observer all and in their order.
#[tokio::test(flavor = "multi_thread")]
async fn events_keep_their_order() {
    let inner = Arc::new(Recorder::default());
    let buffered = Buffered::new(Arc::clone(&inner), 1000).unwrap();
    let ids: Vec<TaskId> = (0..200).map(|n| TaskId::new(format!("t{n}"))).collect();
    for id in &ids {
        buffered.on_event(&pushed(id));
    }
    assert!(
        until(Duration::from_secs(5), || inner.seen.lock().unwrap().len()
            == 200)
        .await
    );
    let expected: Vec<String> = ids.iter().map(|id| id.as_str().to_owned()).collect();
    assert_eq!(*inner.seen.lock().unwrap(), expected);
    assert_eq!(buffered.dropped(), 0);
}

/// Change criterion 2: a full buffer loses events, counts them and says so
/// once per interval.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_buffer_drops_and_warns() {
    let logs = Captured::default();
    let _guard = logs.install();
    let inner = Arc::new(Recorder::default());
    inner.hold.store(true, Ordering::SeqCst);
    let buffered = Buffered::new(Arc::clone(&inner), 2).unwrap();

    // The first event is taken by the delivery thread, which then waits.
    let first = TaskId::new("first");
    buffered.on_event(&pushed(&first));
    assert!(until(Duration::from_secs(5), || inner.held.load(Ordering::SeqCst)).await);
    let ids: Vec<TaskId> = (0..10).map(|n| TaskId::new(format!("t{n}"))).collect();
    for id in &ids {
        buffered.on_event(&pushed(id));
    }
    assert_eq!(buffered.dropped(), 8, "two fit in the buffer");
    let warnings: Vec<_> = logs
        .records()
        .into_iter()
        .filter(|r| r.is("observer", "dropped"))
        .collect();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].fields["message"].contains("count=1"),
        "{warnings:?}"
    );

    inner.hold.store(false, Ordering::SeqCst);
    assert!(
        until(Duration::from_secs(5), || inner.seen.lock().unwrap().len()
            == 3)
        .await
    );
    assert_eq!(*inner.seen.lock().unwrap(), ["first", "t0", "t1"]);
}

/// A panic of the inner observer loses that event only.
#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_observer_keeps_getting_events() {
    let inner = Arc::new(Recorder::default());
    let buffered = Buffered::new(Arc::clone(&inner), 10).unwrap();
    let (boom, after) = (TaskId::new("boom"), TaskId::new("after"));
    buffered.on_event(&pushed(&boom));
    buffered.on_event(&pushed(&after));
    assert!(
        until(Duration::from_secs(5), || inner.seen.lock().unwrap().len()
            == 1)
        .await
    );
    assert_eq!(*inner.seen.lock().unwrap(), ["after"]);
}

#[test]
fn zero_capacity_is_a_config_error() {
    let error = Buffered::new(Recorder::default(), 0).unwrap_err();
    assert!(matches!(error, ConfigError::InvalidCapacity), "{error:?}");
}
