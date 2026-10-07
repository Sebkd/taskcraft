//! The store against a real PostgreSQL. Set `TASKCRAFT_POSTGRES_URL` (for
//! example `postgres://postgres:postgres@localhost:5432/postgres`), or the
//! tests are skipped.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use taskcraft::error::RequeueError;
use taskcraft::source::{Notice, Polled, StoreMessage, TaskStore};
use taskcraft::{
    Attempt, Cancel, CancelOutcome, CancellationToken, FinishReason, Monitor, Outcome,
    PollStrategy, PushOutcome, Queue, QueueHandle, ShutdownReport, Task, TaskId, TaskState,
    task_fn,
};
use taskcraft_postgres::{Lease, PgSource, PgStore, PgStoreError};
use tokio::task::JoinHandle;
use tokio::time::sleep;

const SEC: Duration = Duration::from_secs(1);

fn url() -> Option<String> {
    let url = std::env::var("TASKCRAFT_POSTGRES_URL").ok();
    if url.is_none() {
        // CI sets this so that a lost database address fails, not skips.
        assert!(
            std::env::var_os("TASKCRAFT_REQUIRE_SERVICES").is_none(),
            "TASKCRAFT_POSTGRES_URL is required"
        );
        eprintln!("TASKCRAFT_POSTGRES_URL is not set: skipped");
    }
    url
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

const ALIVE: Duration = Duration::from_millis(200);
const LEASE: Lease = Lease {
    duration: Duration::from_millis(1500),
    heartbeat: Duration::from_millis(300),
};

async fn store(url: &str, process: &str, lease: bool) -> PgStore {
    let mut builder = PgStore::builder(process).alive_interval(ALIVE);
    if lease {
        builder = builder.lease(LEASE);
    }
    builder.connect(url).await.unwrap()
}

async fn until(limit: Duration, cond: impl AsyncFn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond().await {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok()
}

type Handle = QueueHandle<u32>;

/// A queue on `source` running `n`: 0 succeeds, 1 aborts, 2 panics, 3 waits
/// for its cancel flag, 4 runs for an hour.
fn queue(name: &str, source: PgSource, seen: &Arc<Mutex<Vec<(u32, u32)>>>) -> (Monitor, Handle) {
    let record = Arc::clone(seen);
    let handler = task_fn(move |n: u32, Attempt(attempt): Attempt, cancel: Cancel| {
        record.lock().unwrap().push((n, attempt));
        async move {
            match n {
                1 => Outcome::abort("rejected input"),
                2 => panic!("handler bug"),
                3 => {
                    cancel.cancelled().await;
                    Outcome::Success
                }
                4 => {
                    sleep(3600 * SEC).await;
                    Outcome::Success
                }
                _ => Outcome::Success,
            }
        }
    });
    let queue = Queue::on_store(name, Arc::new(source), handler)
        .cancel_grace(SEC)
        .build()
        .unwrap();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    (monitor, handle)
}

fn run(monitor: Monitor) -> (JoinHandle<ShutdownReport>, CancellationToken) {
    let stop = CancellationToken::new();
    let running = tokio::spawn({
        let stop = stop.clone();
        async move { monitor.run(stop).await.unwrap() }
    });
    (running, stop)
}

async fn state(handle: &Handle, id: &str) -> Option<TaskState> {
    handle
        .fetch_status(&TaskId::new(id))
        .await
        .unwrap()
        .map(|s| s.state())
}

/// Outcomes are recorded; a finished id answers "already finished"
/// (change criterion 2).
#[tokio::test(flavor = "multi_thread")]
async fn outcomes_are_recorded_and_remembered() {
    let Some(url) = url() else { return };
    let store = store(&url, &unique("p"), false).await;
    let name = unique("q");
    let seen = Arc::default();
    let (monitor, handle) = queue(&name, store.queue(&name), &seen);
    let (running, stop) = run(monitor);
    for (id, n) in [("ok", 0), ("bad", 1), ("boom", 2)] {
        let pushed = handle.push(Task::new(n).with_id(id)).await.unwrap();
        assert!(matches!(pushed, PushOutcome::Enqueued { .. }));
    }
    assert!(
        until(30 * SEC, async || state(&handle, "boom").await
            == Some(TaskState::Panicked))
        .await
    );
    assert!(
        until(30 * SEC, async || state(&handle, "bad").await
            == Some(TaskState::Failed))
        .await
    );
    assert!(
        until(30 * SEC, async || state(&handle, "ok").await
            == Some(TaskState::Succeeded))
        .await
    );
    let failed = handle
        .fetch_status(&TaskId::new("bad"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        failed.reason(),
        Some(&FinishReason::Handler("rejected input".into()))
    );

    let again = handle.push(Task::new(0).with_id("ok")).await.unwrap();
    assert_eq!(
        again,
        PushOutcome::AlreadyFinished {
            id: "ok".into(),
            state: TaskState::Succeeded
        }
    );
    stop.cancel();
    running.await.unwrap();
}

/// Change criterion 1: two processes poll one task — exactly one gets it.
#[tokio::test(flavor = "multi_thread")]
async fn one_task_goes_to_one_process() {
    let Some(url) = url() else { return };
    let name = unique("q");
    let (a, b) = (
        store(&url, &unique("a"), false).await,
        store(&url, &unique("b"), false).await,
    );
    let (a, b) = (a.queue(&name), b.queue(&name));
    let message = br#"{"id":"only","args":1,"metadata":{},"attempt":0,"retries":0}"#;
    let _ = a
        .push(
            &TaskId::new("only"),
            StoreMessage::from_bytes(message.to_vec()),
        )
        .await
        .unwrap();
    for _ in 0..5 {
        let (x, y) = tokio::join!(a.poll(), b.poll());
        let got = [x.unwrap(), y.unwrap()]
            .iter()
            .filter(|p| matches!(p, Polled::Task { .. }))
            .count();
        if got > 0 {
            assert_eq!(got, 1);
            return;
        }
    }
    panic!("nobody got the task");
}

/// Criterion 44: a live process id cannot be used twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_process_id_is_taken() {
    let Some(url) = url() else { return };
    let id = unique("p");
    let _first = store(&url, &id, false).await;
    let second = PgStore::builder(&id)
        .alive_interval(ALIVE)
        .connect(&url)
        .await;
    assert!(
        matches!(second, Err(PgStoreError::ProcessIdTaken(_))),
        "{second:?}"
    );
}

/// Claims two tasks and records a first attempt, then "crashes": the
/// store goes away without finishing them.
async fn claim_and_crash(url: &str, process: &str, name: &str, lease: bool) {
    let store = store(url, process, lease).await;
    let source = store.queue(name);
    for id in ["t1", "t2"] {
        let message =
            format!(r#"{{"id":"{id}","args":0,"metadata":{{}},"attempt":0,"retries":0}}"#);
        let _ = source
            .push(
                &TaskId::new(id),
                StoreMessage::from_bytes(message.into_bytes()),
            )
            .await
            .unwrap();
    }
    for _ in 0..2 {
        let Polled::Task { receipt, .. } = source.poll().await.unwrap() else {
            panic!("expected a task");
        };
        let progress = taskcraft::source::Progress {
            state: TaskState::Running,
            attempt: 1,
            retries: 0,
        };
        source.progress(receipt, progress).await.unwrap();
    }
}

/// Criteria 30 b and 43: without leases another process does not take the
/// tasks of a crashed one; the crashed one, restarted with its id, gives
/// them back and they run again with their attempt counts.
#[tokio::test(flavor = "multi_thread")]
async fn restarted_process_gives_its_tasks_back() {
    let Some(url) = url() else { return };
    let (process, name) = (unique("p"), unique("q"));
    claim_and_crash(&url, &process, &name, false).await;

    let other = store(&url, &unique("o"), false).await.queue(&name);
    sleep(2 * SEC).await;
    assert!(
        matches!(other.poll().await.unwrap(), Polled::Empty),
        "not taken by another"
    );

    let restarted = store(&url, &process, false).await;
    let seen = Arc::default();
    let (monitor, _) = queue(&name, restarted.queue(&name), &seen);
    let (running, stop) = run(monitor);
    assert!(until(30 * SEC, async || seen.lock().unwrap().len() == 2).await);
    assert_eq!(*seen.lock().unwrap(), [(0, 2), (0, 2)], "second attempts");
    stop.cancel();
    running.await.unwrap();
}

/// Criterion 31: with leases another process takes over the task of a
/// crashed one once the lease expires, with the attempt count kept.
#[tokio::test(flavor = "multi_thread")]
async fn expired_lease_is_taken_over() {
    let Some(url) = url() else { return };
    let (crashed, name) = (unique("a"), unique("q"));
    claim_and_crash(&url, &crashed, &name, true).await;

    let b = store(&url, &unique("b"), true).await.queue(&name);
    let mut notices = b.notices().unwrap();
    let mut taken = Vec::new();
    assert!(
        tokio::time::timeout(30 * SEC, async {
            while taken.len() < 2 {
                if let Polled::Task { message, .. } = b.poll().await.unwrap() {
                    taken.push(String::from_utf8(message.into_bytes()).unwrap());
                } else {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        })
        .await
        .is_ok()
    );
    assert!(
        taken
            .iter()
            .all(|m| m.contains(r#""attempt": 1"#) || m.contains(r#""attempt":1"#)),
        "{taken:?}"
    );
    let notice = notices.recv().await.unwrap();
    assert!(
        matches!(notice, Notice::TakenOver { previous_owner, .. } if previous_owner == crashed)
    );
}

/// Criterion 45: a task losing its lease while still accepted is cancelled
/// with "lease lost" and its row is left to the new owner.
#[tokio::test(flavor = "multi_thread")]
async fn lost_lease_stops_the_task_without_writing() {
    let Some(url) = url() else { return };
    let store = store(&url, &unique("a"), true).await;
    let pool = store.pool().clone();
    let name = unique("q");
    let seen = Arc::default();
    let (monitor, handle) = queue(&name, store.queue(&name), &seen);
    let (running, stop) = run(monitor);
    let _ = handle.push(Task::new(4).with_id("busy")).await.unwrap();
    assert!(
        until(10 * SEC, async || handle
            .status(&TaskId::new("busy"))
            .is_some())
        .await
    );
    let _ = handle.push(Task::new(0).with_id("victim")).await.unwrap();
    assert!(
        until(10 * SEC, async || handle
            .status(&TaskId::new("victim"))
            .is_some())
        .await
    );

    sqlx::query("UPDATE taskcraft_tasks SET owner = 'thief', lease_until = now() + interval '1 hour' WHERE queue = $1 AND id = 'victim'")
        .bind(&name)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        until(10 * SEC, async || handle
            .status(&TaskId::new("victim"))
            .is_none())
        .await
    );
    let row: (String, Option<String>) = sqlx::query_as(
        "SELECT state, owner FROM taskcraft_tasks WHERE queue = $1 AND id = 'victim'",
    )
    .bind(&name)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        row,
        ("accepted".to_owned(), Some("thief".to_owned())),
        "nothing written"
    );
    assert!(
        seen.lock().unwrap().iter().all(|(n, _)| *n != 0),
        "never ran"
    );
    stop.cancel();
    running.await.unwrap();
}

/// Change criterion 3: past retention a finished task is removed, and its id
/// can be pushed again.
#[tokio::test(flavor = "multi_thread")]
async fn finished_tasks_expire() {
    let Some(url) = url() else { return };
    let store = PgStore::builder(unique("p"))
        .alive_interval(ALIVE)
        .retention(SEC)
        .cleanup_interval(ALIVE)
        .connect(&url)
        .await
        .unwrap();
    let name = unique("q");
    let seen = Arc::default();
    let (monitor, handle) = queue(&name, store.queue(&name), &seen);
    let (running, stop) = run(monitor);
    let _ = handle.push(Task::new(0).with_id("once")).await.unwrap();
    assert!(
        until(30 * SEC, async || state(&handle, "once").await
            == Some(TaskState::Succeeded))
        .await
    );
    assert!(until(30 * SEC, async || state(&handle, "once").await.is_none()).await);
    let again = handle.push(Task::new(0).with_id("once")).await.unwrap();
    assert!(matches!(again, PushOutcome::Enqueued { .. }));
    stop.cancel();
    running.await.unwrap();
}

/// Change criterion 4: cancelling a task another process runs records a
/// request; the owner cancels it at its next renewal.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_reaches_the_owning_process() {
    let Some(url) = url() else { return };
    let name = unique("q");
    let owner = store(&url, &unique("a"), true).await;
    let seen = Arc::default();
    let (monitor, owner_handle) = queue(&name, owner.queue(&name), &seen);
    let (running, stop) = run(monitor);
    let _ = owner_handle
        .push(Task::new(3).with_id("long"))
        .await
        .unwrap();
    assert!(
        until(10 * SEC, async || owner_handle
            .status(&TaskId::new("long"))
            .is_some_and(|s| s.state() == TaskState::Running))
        .await
    );

    let other = store(&url, &unique("b"), true).await;
    let (_, other_handle) = queue(&name, other.queue(&name), &seen);
    assert_eq!(
        other_handle.cancel(&TaskId::new("long")).await,
        CancelOutcome::CancelRequested
    );
    assert!(
        until(10 * SEC, async || state(&other_handle, "long").await
            == Some(TaskState::Cancelled))
        .await
    );
    let status = other_handle
        .fetch_status(&TaskId::new("long"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.reason(), Some(&FinishReason::CancelledByUser));
    stop.cancel();
    running.await.unwrap();
}

/// Change delayed-push, criteria 2 and 4: a delayed task is queued with its
/// next delivery, any process takes it only after the moment.
#[tokio::test(flavor = "multi_thread")]
async fn delayed_push_waits_for_its_moment() {
    let Some(url) = url() else { return };
    let name = unique("q");
    let (a, b) = (
        store(&url, &unique("a"), false).await,
        store(&url, &unique("b"), false).await,
    );
    // Process A only pushes; process B polls.
    let (_monitor, handle) = queue(&name, a.queue(&name), &Arc::default());
    let other = b.queue(&name);
    let at = SystemTime::now() + 2 * SEC;
    let pushed = handle
        .push(Task::new(0).with_id("later").with_deliver_at(at))
        .await
        .unwrap();
    assert!(matches!(pushed, PushOutcome::Enqueued { .. }));

    let status = handle
        .fetch_status(&TaskId::new("later"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state(), TaskState::Queued);
    let next = status.next_attempt_at().unwrap();
    let skew = next.duration_since(at).unwrap_or_else(|e| e.duration());
    assert!(skew < SEC, "next delivery {next:?}, asked {at:?}");

    assert!(matches!(other.poll().await.unwrap(), Polled::Empty));
    assert!(
        until(10 * SEC, async || matches!(
            other.poll().await.unwrap(),
            Polled::Task { .. }
        ))
        .await
    );
    assert!(
        SystemTime::now() + Duration::from_millis(500) >= at,
        "taken before its moment"
    );
}

/// Change failed-tasks, criteria 3 and 4: a failed task is queued again and
/// runs with the next attempt number; a task that did not fail is refused.
#[tokio::test(flavor = "multi_thread")]
async fn failed_task_is_requeued() {
    let Some(url) = url() else { return };
    let name = unique("q");
    let store = store(&url, &unique("p"), false).await;
    let seen: Arc<Mutex<Vec<(u32, u32)>>> = Arc::default();
    let (monitor, handle) = queue(&name, store.queue(&name), &seen);
    let (running, stop) = run(monitor);

    for (id, n) in [("failed", 1), ("done", 0)] {
        let _ = handle.push(Task::new(n).with_id(id)).await.unwrap();
    }
    assert!(
        until(10 * SEC, async || state(&handle, "failed").await
            == Some(TaskState::Failed))
        .await
    );
    assert!(
        until(10 * SEC, async || state(&handle, "done").await
            == Some(TaskState::Succeeded))
        .await
    );
    handle.requeue(&TaskId::new("failed")).await.unwrap();
    assert!(until(10 * SEC, async || seen.lock().unwrap().contains(&(1, 2))).await);
    assert!(
        until(10 * SEC, async || state(&handle, "failed").await
            == Some(TaskState::Failed))
        .await
    );
    let status = handle
        .fetch_status(&TaskId::new("failed"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((status.attempt(), status.retries()), (2, 0));

    let done = handle.requeue(&TaskId::new("done")).await;
    assert!(
        matches!(done, Err(RequeueError::NotFailed(TaskState::Succeeded))),
        "{done:?}"
    );
    let _ = handle.push(Task::new(4).with_id("long")).await.unwrap();
    assert!(
        until(10 * SEC, async || state(&handle, "long").await
            == Some(TaskState::Running))
        .await
    );

    let long = handle.requeue(&TaskId::new("long")).await;
    assert!(
        matches!(long, Err(RequeueError::NotFailed(TaskState::Running))),
        "{long:?}"
    );
    let unknown = handle.requeue(&TaskId::new("never-pushed")).await;
    assert!(matches!(unknown, Err(RequeueError::Unknown)), "{unknown:?}");
    stop.cancel();
    running.await.unwrap();
}

/// The records of a thread while installed, as `field=value` lines.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<String>>>);

thread_local! {
    static CURRENT: std::cell::RefCell<Option<Logs>> = const { std::cell::RefCell::new(None) };
}

/// One global subscriber sends each record to the capture of its thread: a
/// per-thread default would race with tests running in parallel without one
/// (a callsite first hit there caches "no interest").
struct Router;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Router {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!("{}={:?} ", field.name(), value));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.push_str(&format!("{}={} ", field.name(), value));
            }
        }
        CURRENT.with(|current| {
            if let Some(logs) = &*current.borrow() {
                let mut fields = Fields(String::new());
                event.record(&mut fields);
                logs.0.lock().unwrap().push(fields.0);
            }
        });
    }
}

/// Clears the capture of the thread on drop.
struct Installed;

impl Drop for Installed {
    fn drop(&mut self) {
        CURRENT.with(|current| *current.borrow_mut() = None);
    }
}

impl Logs {
    fn install(&self) -> Installed {
        static GLOBAL: std::sync::Once = std::sync::Once::new();
        GLOBAL.call_once(|| {
            use tracing_subscriber::layer::SubscriberExt;
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Router))
                .unwrap();
            tracing::callsite::rebuild_interest_cache();
        });
        CURRENT.with(|current| *current.borrow_mut() = Some(self.clone()));
        Installed
    }

    fn find(&self, action: &str, owner: &str) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|r| {
                r.contains(&format!("action={action} ")) && r.contains(&format!("owner={owner},"))
            })
            .cloned()
            .collect()
    }
}

async fn states(url: &str, queue: &str) -> Vec<(String, i32)> {
    let pool = sqlx::PgPool::connect(url).await.unwrap();
    sqlx::query_as("SELECT state, attempt FROM taskcraft_tasks WHERE queue = $1 ORDER BY id")
        .bind(queue)
        .fetch_all(&pool)
        .await
        .unwrap()
}

/// Change store-orphan-release, criterion 1: the tasks of a retired process
/// go back to their queues, attempt counts kept.
#[tokio::test]
async fn retired_process_tasks_are_released() {
    let Some(url) = url() else { return };
    let (retired, name) = (unique("retired"), unique("q"));
    claim_and_crash(&url, &retired, &name, false).await;
    sleep(3 * ALIVE).await;
    let other = store(&url, &unique("admin"), false).await;

    let logs = Logs::default();
    let released = {
        let _guard = logs.install();
        other.release_process(&retired).await.unwrap()
    };
    assert_eq!(released, 2);
    assert_eq!(
        states(&url, &name).await,
        [("queued".to_owned(), 1), ("queued".to_owned(), 1)]
    );
    let records = logs.find("released", &retired);
    assert_eq!(records.len(), 1, "{records:?}");
    assert!(records[0].contains("count=2"));
    assert_eq!(other.release_process(&retired).await.unwrap(), 0);
}

/// Criterion 2: a live process keeps its tasks.
#[tokio::test]
async fn live_process_tasks_are_not_released() {
    let Some(url) = url() else { return };
    let (live, name) = (unique("live"), unique("q"));
    let owner = store(&url, &live, false).await;
    let source = owner.queue(&name);
    let message = br#"{"id":"t","args":0,"metadata":{},"attempt":0,"retries":0}"#;
    let _ = source
        .push(
            &TaskId::new("t"),
            StoreMessage::from_bytes(message.to_vec()),
        )
        .await
        .unwrap();
    assert!(matches!(source.poll().await.unwrap(), Polled::Task { .. }));

    let other = store(&url, &unique("admin"), false).await;
    let refused = other.release_process(&live).await;
    assert!(
        matches!(&refused, Err(PgStoreError::ProcessIdTaken(id)) if *id == live),
        "{refused:?}"
    );
    let own = other.release_process(other.process_id()).await;
    assert!(
        matches!(own, Err(PgStoreError::ProcessIdTaken(_))),
        "{own:?}"
    );
    assert_eq!(states(&url, &name).await, [("accepted".to_owned(), 0)]);
    drop(owner);
}

/// Criterion 3: a starting process without leases warns about tasks of a
/// silent owner; with leases it does not — they are taken over anyway.
#[tokio::test]
async fn silent_owner_is_reported_on_start() {
    let Some(url) = url() else { return };
    let (silent, name) = (unique("silent"), unique("q"));
    claim_and_crash(&url, &silent, &name, false).await;
    sleep(3 * ALIVE).await;

    let logs = Logs::default();
    {
        let _guard = logs.install();
        let _plain = PgStore::builder(unique("plain"))
            .alive_interval(ALIVE)
            .stale_owner_warning(Duration::from_millis(300))
            .connect(&url)
            .await
            .unwrap();
    }
    let records = logs.find("stale_owner", &silent);
    assert_eq!(records.len(), 1, "{records:?}");
    assert!(records[0].contains("count=2"), "{records:?}");

    let leased = Logs::default();
    {
        let _guard = leased.install();
        let _with_leases = PgStore::builder(unique("leased"))
            .alive_interval(ALIVE)
            .lease(LEASE)
            .stale_owner_warning(Duration::from_millis(300))
            .connect(&url)
            .await
            .unwrap();
    }
    assert!(leased.find("stale_owner", &silent).is_empty());
}

/// A process that takes tasks of `name` and polls once a minute unless
/// woken: it sees the work of another process only through a notification.
async fn sleepy(
    pool: sqlx::PgPool,
    name: &str,
    notifications: bool,
) -> (
    PgStore,
    Arc<Mutex<Vec<u32>>>,
    JoinHandle<ShutdownReport>,
    CancellationToken,
) {
    let store = PgStore::builder(unique("b"))
        .alive_interval(ALIVE)
        .notifications(notifications)
        .with_pool(pool)
        .await
        .unwrap();
    let ran: Arc<Mutex<Vec<u32>>> = Arc::default();
    let record = Arc::clone(&ran);
    let queue = Queue::on_store(
        name,
        Arc::new(store.queue(name)),
        task_fn(move |n: u32| {
            record.lock().unwrap().push(n);
            async {}
        }),
    )
    .poll_strategy(PollStrategy::FirstOf(vec![
        PollStrategy::Wake,
        PollStrategy::Interval(60 * SEC),
    ]))
    .build()
    .unwrap();
    let (monitor, _handle) = Monitor::new().register(queue).unwrap();
    let (running, stop) = run(monitor);
    // The first poll finds nothing; the worker goes to sleep.
    sleep(SEC).await;
    (store, ran, running, stop)
}

/// A process that only pushes into `name`.
async fn pusher(url: &str, name: &str) -> (PgStore, Handle) {
    let store = store(url, &unique("a"), false).await;
    let (_monitor, handle) = queue(name, store.queue(name), &Arc::default());
    (store, handle)
}

/// Change store-notify-wakeup, criterion 1: a push from another process
/// wakes the worker at once.
#[tokio::test(flavor = "multi_thread")]
async fn push_from_another_process_wakes_the_worker() {
    let Some(url) = url() else { return };
    let name = unique("q");
    let (_b, ran, running, stop) =
        sleepy(sqlx::PgPool::connect(&url).await.unwrap(), &name, true).await;
    let (_a, handle) = pusher(&url, &name).await;
    let _ = handle.push(Task::new(7)).await.unwrap();
    assert!(until(5 * SEC, async || *ran.lock().unwrap() == [7]).await);
    stop.cancel();
    running.await.unwrap();
}

/// Criterion 2: without notifications the worker waits for its next poll.
#[tokio::test(flavor = "multi_thread")]
async fn without_notifications_the_worker_polls() {
    let Some(url) = url() else { return };
    let name = unique("q");
    let (_b, ran, running, stop) =
        sleepy(sqlx::PgPool::connect(&url).await.unwrap(), &name, false).await;
    let (_a, handle) = pusher(&url, &name).await;
    let _ = handle.push(Task::new(7)).await.unwrap();
    sleep(3 * SEC).await;
    assert!(ran.lock().unwrap().is_empty());
    stop.cancel();
    running.await.unwrap();
}

/// Criterion 3: a lost `LISTEN` connection is logged and restored; until
/// then polling covers.
#[tokio::test]
async fn lost_listen_connection_is_restored() {
    let Some(url) = url() else { return };
    let logs = Logs::default();
    let _guard = logs.install();
    let name = unique("q");
    let app = unique("listener");
    let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_with(options.application_name(&app))
        .await
        .unwrap();
    let (b, ran, running, stop) = sleepy(pool, &name, true).await;

    let admin = sqlx::PgPool::connect(&url).await.unwrap();
    let ended: Vec<bool> = sqlx::query_scalar(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
          WHERE application_name = $1 AND query ILIKE 'LISTEN%'",
    )
    .bind(&app)
    .fetch_all(&admin)
    .await
    .unwrap();
    assert_eq!(ended, [true], "one LISTEN connection");
    assert!(
        until(10 * SEC, async || logs
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.contains("action=notify_lost ")
                && r.contains(&format!("process_id={},", b.process_id()))))
        .await
    );

    // Reconnected: a push from another process wakes the worker again.
    sleep(3 * SEC).await;
    let (_a, handle) = pusher(&url, &name).await;
    let _ = handle.push(Task::new(8)).await.unwrap();
    assert!(until(5 * SEC, async || *ran.lock().unwrap() == [8]).await);
    stop.cancel();
    running.await.unwrap();
}
