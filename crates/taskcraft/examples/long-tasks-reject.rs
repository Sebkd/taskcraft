//! E-3 `long-tasks-reject` — tasks of hours: with the "reject" policy a full
//! queue answers the sender at once and keeps taking work; shutdown ends a
//! retry pause at once (spec 2.2.1, 2.3.7, 2.3.14).
//!
//! Runs on virtual time: an hour passes at once.
//!
//! ```text
//! cargo run -p taskcraft --example long-tasks-reject
//! ```

use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::{
    Attempt, CancellationToken, IdentityCodec, InMemorySource, Monitor, Outcome, Queue,
    RetryPolicy, Task, task_fn,
};
use tokio::time::{Instant, sleep};

const HOUR: Duration = Duration::from_secs(3600);

/// A notification that waits for the partner's answer for an hour; the
/// partner of order 9 is not ready yet.
async fn notify(order: u32, Attempt(attempt): Attempt) -> Outcome {
    println!("  order {order}: attempt {attempt} starts");
    if order == 9 {
        return Outcome::retry("partner not ready");
    }
    sleep(HOUR).await;
    println!("  order {order}: partner answered");
    Outcome::Success
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("virtual time: hours pass at once");
    let start = Instant::now();
    let source = Arc::new(InMemorySource::default());
    let replies = Arc::new(Mutex::new(Vec::new()));
    let answered = Arc::clone(&replies);
    let queue = Queue::builder(
        "notify",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(notify),
    )
    .concurrency(2)
    .reject_with(move |task: Task<u32>| {
        // Answer the sender from here: "busy, try later".
        println!(
            "  order {}: rejected, the sender is told to retry",
            task.args()
        );
        if let Ok(mut replies) = answered.lock() {
            replies.push(*task.args());
        }
    })
    .retry_policy(RetryPolicy {
        max_attempts: 3,
        base: Duration::from_secs(300),
        ..RetryPolicy::default()
    })
    .no_recovery()
    .build()?;
    let handle = queue.handle();
    let stop = CancellationToken::new();
    let running = tokio::spawn(Monitor::new().register(queue)?.run(stop.clone()));

    for order in 1..=4 {
        let _ = handle.push(Task::new(order)).await?;
    }
    // The worker takes 1 and 2 and refuses 3 and 4 at once.
    sleep(Duration::from_secs(1)).await;
    while handle.live_tasks() > 0 {
        sleep(Duration::from_secs(1)).await;
    }
    println!(
        "after {:?}: orders 1 and 2 done, 3 and 4 refused",
        start.elapsed()
    );

    let _ = handle.push(Task::new(9)).await?;
    sleep(Duration::from_secs(1)).await;
    println!("order 9 waits 5 minutes to retry; shutdown now");
    let at = Instant::now();
    stop.cancel();
    let report = running.await??;
    println!(
        "stopped after {:?} of waiting: cancelled={}",
        at.elapsed(),
        report.cancelled()
    );

    let refused = replies.lock().map_err(|_| "poisoned")?.clone();
    if refused != [3, 4] || !at.elapsed().is_zero() || report.cancelled() != 1 {
        return Err("unexpected result".into());
    }
    Ok(())
}
