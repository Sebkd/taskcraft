//! Pools are taken in name order whatever order a queue declares them in:
//! two queues sharing two pools never deadlock (rule 2.3.8; change
//! concurrency-and-fuzz-tests). `loom` cannot model tokio's semaphores, so
//! this runs many tasks on a multi-thread runtime.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use taskcraft::codec::IdentityCodec;
use taskcraft::{CancellationToken, InMemorySource, Monitor, Queue, Task, task_fn};

const TASKS: usize = 200;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_pools_declared_in_opposite_orders_never_deadlock() {
    let done = Arc::new(AtomicUsize::new(0));
    let queue = |name: &str, first: &str, second: &str| {
        let done = Arc::clone(&done);
        Queue::builder(
            name,
            Arc::new(InMemorySource::new(TASKS).unwrap()),
            IdentityCodec::new(),
            task_fn(move |_: usize| {
                let done = Arc::clone(&done);
                async move {
                    tokio::task::yield_now().await;
                    done.fetch_add(1, Ordering::SeqCst);
                }
            }),
        )
        .concurrency(8)
        .pool(first, 1)
        .pool(second, 1)
        .no_recovery()
        .build()
        .unwrap()
    };
    let monitor = Monitor::new().pool("a", 1).unwrap().pool("b", 1).unwrap();
    let (monitor, forward) = monitor.register(queue("forward", "a", "b")).unwrap();
    let (monitor, backward) = monitor.register(queue("backward", "b", "a")).unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));

    for n in 0..TASKS {
        let _ = forward.push(Task::new(n)).await.unwrap();
        let _ = backward.push(Task::new(n)).await.unwrap();
    }
    let finished = tokio::time::timeout(Duration::from_secs(30), async {
        while done.load(Ordering::SeqCst) < 2 * TASKS {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        finished.is_ok(),
        "deadlock: {} of {} tasks done",
        done.load(Ordering::SeqCst),
        2 * TASKS
    );
    stop.cancel();
    running.await.unwrap().unwrap();
}

/// Pool usage as reported, in order.
#[derive(Default)]
struct Usage(std::sync::Mutex<Vec<u32>>);

impl taskcraft::observe::Observer for Usage {
    fn on_event(&self, event: &taskcraft::observe::Event<'_>) {
        if let taskcraft::observe::Event::PoolUsage { in_use, .. } = event {
            self.0.lock().unwrap().push(*in_use);
        }
    }
}

/// Criterion 83: a permit released while a waiter is being cancelled passes
/// to the waiter and comes back unseen by the semaphore count; the reported
/// usage still ends at zero (rule 2.3.22 p. 1).
#[tokio::test(start_paused = true)]
async fn pool_usage_ends_at_zero_after_a_cancelled_waiter() {
    // On virtual time a stuck run times out at once instead of hanging.
    tokio::time::timeout(Duration::from_secs(60), cancelled_waiter_case())
        .await
        .expect("the case finishes");
}

async fn cancelled_waiter_case() {
    let queue = Queue::builder(
        "q",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(|_: u32, cancel: taskcraft::Cancel| async move { cancel.cancelled().await }),
    )
    .concurrency(2)
    .pool("p", 1)
    .no_recovery()
    .build()
    .unwrap();
    let usage = Arc::new(Usage::default());
    let (monitor, handle) = Monitor::new()
        .pool("p", 1)
        .unwrap()
        .observer(Arc::clone(&usage))
        .register(queue)
        .unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));
    let _ = handle.push(Task::new(1).with_id("holder")).await.unwrap();
    let _ = handle.push(Task::new(2).with_id("waiter")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(handle.live_tasks(), 2);

    // Both flags in one go: the holder runs first and hands its permit to
    // the waiter, which then sees its own flag and gives it back.
    let _ = handle.cancel(&taskcraft::TaskId::new("holder")).await;
    let _ = handle.cancel(&taskcraft::TaskId::new("waiter")).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(handle.live_tasks(), 0);
    assert_eq!(
        usage.0.lock().unwrap().last(),
        Some(&0),
        "{:?}",
        usage.0.lock().unwrap()
    );
    stop.cancel();
    running.await.unwrap().unwrap();
}
