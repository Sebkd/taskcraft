//! Overflow policy and resource pools, on virtual time.

#![cfg(feature = "test-util")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::testing::{FaultyCodec, FaultySource};
use taskcraft::{
    CancellationToken, ConfigError, Monitor, Outcome, Queue, ShutdownReport, StopReason, Task,
    TaskError, TaskId, task_fn,
};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

mod common;
use common::Captured;

const SEC: Duration = Duration::from_secs(1);
const HOUR: Duration = Duration::from_secs(3600);

type Faulty = Arc<FaultySource<u32>>;

async fn until(limit: Duration, cond: impl Fn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

fn counter() -> Arc<AtomicU32> {
    Arc::new(AtomicU32::new(0))
}

fn get(c: &AtomicU32) -> u32 {
    c.load(Ordering::SeqCst)
}

/// A handler that counts its starts and then sleeps for an hour.
fn long(
    started: &Arc<AtomicU32>,
) -> impl Fn(u32) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Clone + Send + Sync + 'static
{
    let started = Arc::clone(started);
    move |_: u32| {
        let started = Arc::clone(&started);
        Box::pin(async move {
            started.fetch_add(1, Ordering::SeqCst);
            sleep(HOUR).await;
        })
    }
}

fn spawn(monitor: Monitor) -> (JoinHandle<ShutdownReport>, CancellationToken) {
    let stop = CancellationToken::new();
    (tokio::spawn(monitor.run(stop.clone())), stop)
}

/// Criterion 13: two run, two wait, and the intake keeps polling.
#[tokio::test(start_paused = true)]
async fn waiting_tasks_do_not_block_intake() {
    let source: Faulty = Arc::default();
    let started = counter();
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        FaultyCodec::new(),
        task_fn(long(&started)),
    )
    .concurrency(2)
    .wait_limit(10)
    .cancel_grace(SEC)
    .build()
    .unwrap();
    let (monitor, stop) = spawn(
        Monitor::new()
            .shutdown_timeout(Duration::ZERO)
            .register(queue)
            .unwrap(),
    );
    for n in 0..4 {
        source.enqueue(Task::new(n));
    }
    // Acked on accept: all four were polled while two still wait for a slot.
    assert!(until(SEC, || source.acks() == 4).await);
    assert!(until(SEC, || get(&started) == 2).await);
    sleep(10 * SEC).await;
    assert_eq!(get(&started), 2);

    stop.cancel();
    let report = monitor.await.unwrap();
    let q = &report.queues[0];
    assert_eq!((q.completed, q.cancelled, q.aborted), (0, 4, 2), "{q:?}");
}

/// Criterion 40: a full waiting room stops polling; shutdown is seen at once.
#[tokio::test(start_paused = true)]
async fn full_waiting_room_stops_polling() {
    let source: Faulty = Arc::default();
    let started = counter();
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        FaultyCodec::new(),
        task_fn(long(&started)),
    )
    .wait_limit(1)
    .cancel_grace(SEC)
    .build()
    .unwrap();
    let (monitor, stop) = spawn(
        Monitor::new()
            .shutdown_timeout(Duration::ZERO)
            .register(queue)
            .unwrap(),
    );
    for n in 0..3 {
        source.enqueue(Task::new(n));
    }
    assert!(until(SEC, || source.acks() == 2).await);
    sleep(60 * SEC).await;
    assert_eq!(source.acks(), 2, "the third task is not polled");
    assert_eq!(get(&started), 1);

    let at = Instant::now();
    stop.cancel();
    let report = monitor.await.unwrap();
    assert_eq!(
        at.elapsed(),
        SEC,
        "only the cancel grace of the running task"
    );
    let q = &report.queues[0];
    assert_eq!((q.cancelled, q.aborted), (2, 1), "{q:?}");
}

/// Criterion 14: reject hands the task to the hook, acks it, and goes on.
#[tokio::test(start_paused = true)]
async fn reject_hands_the_task_to_the_hook() {
    let logs = Captured::default();
    let _guard = logs.install();
    let source: Faulty = Arc::default();
    let started = counter();
    let rejected = Arc::new(Mutex::new(Vec::new()));
    let r = Arc::clone(&rejected);
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        FaultyCodec::new(),
        task_fn(long(&started)),
    )
    .reject_with(move |task: Task<u32>| r.lock().unwrap().push(task.id().clone()))
    .build()
    .unwrap();
    let (monitor, stop) = spawn(
        Monitor::new()
            .shutdown_timeout(Duration::ZERO)
            .register(queue)
            .unwrap(),
    );
    source.enqueue(Task::new(0).with_id("runs"));
    assert!(until(SEC, || get(&started) == 1).await);
    source.enqueue(Task::new(1).with_id("second"));
    source.enqueue(Task::new(2).with_id("third"));

    assert!(until(SEC, || rejected.lock().unwrap().len() == 2).await);
    assert_eq!(
        *rejected.lock().unwrap(),
        [TaskId::new("second"), TaskId::new("third")]
    );
    assert_eq!(source.acks(), 3);
    assert_eq!(logs.count("task", "rejected"), 2);
    stop.cancel();
    monitor.await.unwrap();
}

/// Criterion 41: a 60 s restart delay and a task waiting for a slot both end
/// at once on shutdown.
#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_restart_and_slot_wait() {
    let source: Faulty = Arc::default();
    let started = counter();
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        FaultyCodec::new(),
        task_fn(long(&started)),
    )
    .wait_limit(2)
    .cancel_grace(SEC)
    .build()
    .unwrap();
    let monitor = Monitor::new()
        .shutdown_timeout(Duration::ZERO)
        .restart_delays(60 * SEC, 60 * SEC)
        .unwrap()
        .register(queue)
        .unwrap();
    let (monitor, stop) = spawn(monitor);
    source.enqueue(Task::new(0));
    source.enqueue(Task::new(1));
    assert!(until(SEC, || source.acks() == 2).await);
    // One runs, one waits for the slot; the next poll fails: 60 s restart.
    source.fail_next_polls(1);
    sleep(5 * SEC).await;

    let at = Instant::now();
    stop.cancel();
    let report = monitor.await.unwrap();
    assert_eq!(
        at.elapsed(),
        SEC,
        "neither the restart delay nor the slot wait held shutdown"
    );
    let q = &report.queues[0];
    assert_eq!(q.reason, StopReason::Shutdown);
    assert_eq!((q.cancelled, q.aborted), (2, 1), "{q:?}");
}

/// Criterion 15: with a pool of one permit and one slot, every outcome frees
/// both — otherwise the next task would never start.
#[tokio::test(start_paused = true)]
async fn every_outcome_frees_the_slot_and_the_pool() {
    let source: Faulty = Arc::default();
    let service = task_fn(|n: u32| async move {
        match n {
            1 => Err(TaskError::abort(std::io::Error::other("bad"))),
            2 => panic!("boom"),
            3 => Ok(Outcome::retry("later")),
            _ => Ok(Outcome::Success),
        }
    });
    let queue = Queue::builder("q", Arc::clone(&source), FaultyCodec::new(), service)
        .pool("unzip", 1)
        .build()
        .unwrap();
    let (monitor, stop) = spawn(
        Monitor::new()
            .pool("unzip", 1)
            .unwrap()
            .register(queue)
            .unwrap(),
    );
    for n in 0..6 {
        source.enqueue(Task::new(n));
    }
    assert!(until(10 * SEC, || source.acks() == 6).await);
    // All six accepted and each ran after the previous released its permits.
    sleep(SEC).await;
    stop.cancel();
    let q = &monitor.await.unwrap().queues[0];
    assert_eq!((q.cancelled, q.aborted), (0, 0), "{q:?}");
}

/// Shutdown while a task waits for a pool cancels it at once and frees its
/// slot.
#[tokio::test(start_paused = true)]
async fn shutdown_cancels_a_task_waiting_for_a_pool() {
    let source: Faulty = Arc::default();
    let started = counter();
    let queue = Queue::builder(
        "q",
        Arc::clone(&source),
        FaultyCodec::new(),
        task_fn(long(&started)),
    )
    .concurrency(2)
    .pool("pack", 1)
    .cancel_grace(SEC)
    .build()
    .unwrap();
    let monitor = Monitor::new()
        .shutdown_timeout(Duration::ZERO)
        .pool("pack", 1)
        .unwrap()
        .register(queue)
        .unwrap();
    let (monitor, stop) = spawn(monitor);
    source.enqueue(Task::new(0));
    source.enqueue(Task::new(1));
    assert!(until(SEC, || source.acks() == 2).await);
    sleep(5 * SEC).await;
    assert_eq!(get(&started), 1, "the second task waits for the pool");

    stop.cancel();
    let q = &monitor.await.unwrap().queues[0];
    assert_eq!((q.cancelled, q.aborted), (2, 1), "{q:?}");
}

/// Criterion 16: pools {a, b} and {b, a} cannot deadlock: both queues take
/// them in name order.
#[tokio::test(start_paused = true)]
async fn crossed_pool_sets_do_not_deadlock() {
    let first: Faulty = Arc::default();
    let second: Faulty = Arc::default();
    let runs = counter();
    let handler = {
        let runs = Arc::clone(&runs);
        move |_: u32| {
            let runs = Arc::clone(&runs);
            async move {
                sleep(SEC).await;
                runs.fetch_add(1, Ordering::SeqCst);
            }
        }
    };
    let queue_ab = Queue::builder(
        "ab",
        Arc::clone(&first),
        FaultyCodec::new(),
        task_fn(handler.clone()),
    )
    .concurrency(3)
    .pool("a", 1)
    .pool("b", 1)
    .build()
    .unwrap();
    let queue_ba = Queue::builder(
        "ba",
        Arc::clone(&second),
        FaultyCodec::new(),
        task_fn(handler),
    )
    .concurrency(3)
    .pool("b", 1)
    .pool("a", 1)
    .build()
    .unwrap();
    let monitor = Monitor::new()
        .pool("a", 1)
        .unwrap()
        .pool("b", 1)
        .unwrap()
        .register(queue_ab)
        .unwrap()
        .register(queue_ba)
        .unwrap();
    let (monitor, stop) = spawn(monitor);
    for n in 0..3 {
        first.enqueue(Task::new(n));
        second.enqueue(Task::new(n));
    }
    assert!(
        until(60 * SEC, || get(&runs) == 6).await,
        "all six ran, one at a time"
    );
    stop.cancel();
    monitor.await.unwrap();
}

/// Change criterion 1 and the pool checks of 2.10.
#[test]
fn pools_are_checked_on_registration() {
    let queue = |pool: &str, permits: u32| {
        Queue::builder(
            "q",
            Arc::new(FaultySource::<u32>::new()),
            FaultyCodec::new(),
            task_fn(|_: u32| async {}),
        )
        .pool(pool, permits)
        .build()
        .unwrap()
    };
    let monitor = || Monitor::new().pool("unzip", 2).unwrap();

    let err = monitor().register(queue("pack", 1)).unwrap_err();
    assert_eq!(err.to_string(), "unknown pool: pack");
    let err = monitor().register(queue("unzip", 3)).unwrap_err();
    assert_eq!(
        err,
        ConfigError::PermitsExceedPool {
            name: "unzip".into()
        }
    );
    assert_eq!(err.to_string(), "permits exceed pool size: unzip");
    assert!(monitor().register(queue("unzip", 0)).is_err());
    assert!(monitor().register(queue("unzip", 2)).is_ok());

    assert!(Monitor::new().pool("p", 0).is_err());
    assert!(Monitor::new().pool("p", 1).unwrap().pool("p", 1).is_err());
}
