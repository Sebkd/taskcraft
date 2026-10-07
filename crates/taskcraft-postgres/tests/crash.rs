//! A real process crash (scenario 2.2.9; change concurrency-and-fuzz-tests,
//! criterion 4): a child process — this test binary run again — takes a task
//! under a lease and is killed with SIGKILL; another process takes the task
//! over when the lease runs out, attempt count kept. Set
//! `TASKCRAFT_POSTGRES_URL`, or the test is skipped.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::{
    Attempt, CancellationToken, Monitor, PollStrategy, Queue, Task, TaskId, TaskState, task_fn,
};
use taskcraft_postgres::{Lease, PgStore};
use tokio::time::sleep;

const SEC: Duration = Duration::from_secs(1);
const LEASE: Lease = Lease {
    duration: Duration::from_millis(1500),
    heartbeat: Duration::from_millis(300),
};
/// The child: its test name and the variable that makes it one.
const CHILD: &str = "child_holds_a_task";
const CHILD_ENV: &str = "TASKCRAFT_CRASH_CHILD";

fn url() -> Option<String> {
    let url = std::env::var("TASKCRAFT_POSTGRES_URL").ok();
    if url.is_none() {
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

/// As the child (with `TASKCRAFT_CRASH_CHILD=process|queue`): pushes one
/// task and runs it forever, until killed. Otherwise does nothing.
#[tokio::test(flavor = "multi_thread")]
async fn child_holds_a_task() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let (process, queue) = spec.split_once('|').unwrap();
    let url = std::env::var("TASKCRAFT_POSTGRES_URL").unwrap();
    let store = PgStore::builder(process)
        .lease(LEASE)
        .connect(&url)
        .await
        .unwrap();
    let queue = Queue::on_store(
        queue,
        Arc::new(store.queue(queue)),
        task_fn(|_: u32| std::future::pending::<()>()),
    )
    .build()
    .unwrap();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let _ = handle.push(Task::new(1).with_id("job")).await.unwrap();
    monitor.run(CancellationToken::new()).await.unwrap();
}

/// Kills the child however the test ends.
struct Killed(Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn until(limit: Duration, cond: impl AsyncFn() -> bool) -> bool {
    tokio::time::timeout(limit, async {
        while !cond().await {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn killed_process_task_is_taken_over() {
    let Some(url) = url() else { return };
    let (process, name) = (unique("child"), unique("q"));
    let child = Command::new(std::env::current_exe().unwrap())
        .args([CHILD, "--exact", "--nocapture"])
        .env(CHILD_ENV, format!("{process}|{name}"))
        .env("TASKCRAFT_POSTGRES_URL", &url)
        .spawn()
        .unwrap();
    let mut child = Killed(child);

    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let owned = async || {
        let row: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT state, owner FROM taskcraft_tasks WHERE queue = $1 AND id = 'job'",
        )
        .bind(&name)
        .fetch_optional(&pool)
        .await
        .unwrap();
        row == Some(("running".to_owned(), Some(process.clone())))
    };
    assert!(until(60 * SEC, owned).await, "the child took the task");

    // kill -9: no shutdown, no last write.
    child.0.kill().unwrap();
    child.0.wait().unwrap();

    let attempts: Arc<Mutex<Vec<u32>>> = Arc::default();
    let record = Arc::clone(&attempts);
    let store = PgStore::builder(unique("b"))
        .lease(LEASE)
        .connect(&url)
        .await
        .unwrap();
    let queue = Queue::on_store(
        &name,
        Arc::new(store.queue(&name)),
        task_fn(move |_: u32, Attempt(attempt): Attempt| {
            record.lock().unwrap().push(attempt);
            async {}
        }),
    )
    // An expired lease is found by a poll.
    .poll_strategy(PollStrategy::Interval(Duration::from_millis(200)))
    .build()
    .unwrap();
    let (monitor, handle) = Monitor::new().register(queue).unwrap();
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));

    let done = async || {
        handle
            .fetch_status(&TaskId::new("job"))
            .await
            .unwrap()
            .is_some_and(|s| s.state() == TaskState::Succeeded)
    };
    assert!(until(30 * SEC, done).await, "taken over after the lease");
    assert_eq!(*attempts.lock().unwrap(), [2], "attempt count kept");
    stop.cancel();
    running.await.unwrap().unwrap();
}
