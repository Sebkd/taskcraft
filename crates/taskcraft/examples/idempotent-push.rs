//! E-5 `idempotent-push` — pushing a task twice: "already running" while it
//! lives, a race of two pushes creates one task, a finished id is free
//! again without a task store (spec 2.2.3, 2.3.11).
//!
//! ```text
//! cargo run -p taskcraft --example idempotent-push
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use taskcraft::{
    CancellationToken, Data, IdentityCodec, InMemorySource, Monitor, PushOutcome, Queue,
    SharedData, Task, task_fn,
};
use tokio::time::sleep;

/// A report that takes a while; counts how often it really ran.
async fn build_report(_month: String, Data(runs): Data<AtomicU32>) {
    runs.fetch_add(1, Ordering::SeqCst);
    sleep(Duration::from_millis(200)).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut shared = SharedData::new();
    shared.insert(AtomicU32::new(0));
    let runs = shared.get::<AtomicU32>().ok_or("no counter")?;
    let queue = Queue::builder(
        "reports",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(build_report),
    )
    .shared_data(shared)
    .no_recovery()
    .build()?;
    let handle = queue.handle();
    let stop = CancellationToken::new();
    let running = tokio::spawn(Monitor::new().register(queue)?.run(stop.clone()));

    // The id says what the task is: one report per month.
    let report = || Task::new("2026-10".to_owned()).with_id("report-2026-10");

    // Two pushes racing: one creates the task, the other is told it exists.
    let (a, b) = tokio::join!(handle.push(report()), handle.push(report()));
    let (a, b) = (a?, b?);
    println!("race: {a:?} / {b:?}");
    sleep(Duration::from_millis(50)).await;

    // While it runs, a repeated push changes nothing.
    let again = handle.push(report()).await?;
    println!("while running: {again:?}");

    sleep(Duration::from_millis(400)).await;
    // Without a task store a finished id is free again.
    let later = handle.push(report()).await?;
    println!("after it finished: {later:?}");
    sleep(Duration::from_millis(400)).await;
    stop.cancel();
    running.await??;

    let created = [&a, &b]
        .iter()
        .filter(|o| matches!(o, PushOutcome::Enqueued { .. }))
        .count();
    let ok = created == 1
        && matches!(again, PushOutcome::AlreadyRunning { .. })
        && matches!(later, PushOutcome::Enqueued { .. })
        && runs.load(Ordering::SeqCst) == 2;
    println!("the report ran {} times", runs.load(Ordering::SeqCst));
    if !ok {
        return Err("unexpected push answers".into());
    }
    Ok(())
}
