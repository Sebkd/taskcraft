//! E-2 `outcomes` — every outcome a handler can give: success, retry with a
//! pause, abort, defer and panic; the attempt count (spec 2.3.2–2.3.6).
//!
//! Runs on virtual time: pauses of seconds pass at once.
//!
//! ```text
//! cargo run -p taskcraft --example outcomes
//! ```

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::{
    AckPoint, Attempt, CancellationToken, Event, IdentityCodec, InMemorySource, Monitor, Observer,
    Outcome, Queue, RetryPolicy, Task, TaskError, TaskState, task_fn,
};

/// What each task does, by name.
async fn work(name: String, Attempt(attempt): Attempt) -> Result<Outcome, TaskError> {
    println!("  {name}: attempt {attempt}");
    Ok(match name.as_str() {
        "flaky" if attempt < 3 => Outcome::retry("the upstream is busy"),
        // A classified error: `?` on it would also abort.
        "bad-input" => return Err(TaskError::abort("the month is not a month")),
        "later" if attempt == 1 => {
            Outcome::defer(Duration::from_secs(30), "the data is not there yet")
        }
        "boom" => panic_now(),
        _ => Outcome::Success,
    })
}

#[allow(clippy::panic)] // The example shows what a panic turns into.
fn panic_now() -> Outcome {
    panic!("a bug in the handler")
}

/// Records the final state of each task.
#[derive(Default)]
struct Finals(Mutex<BTreeMap<String, TaskState>>);

impl Observer for Finals {
    fn on_event(&self, event: &Event<'_>) {
        if let Event::Finished {
            task_id,
            state,
            reason,
            ..
        } = event
        {
            let why = reason.map(|r| format!(" ({r})")).unwrap_or_default();
            println!("  {task_id} -> {state}{why}");
            if let Ok(mut finals) = self.0.lock() {
                finals.insert(task_id.as_str().to_owned(), *state);
            }
        }
    }
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("virtual time: retry and defer pauses pass at once");
    println!("(the panic message printed below is the point of task \"boom\")");
    let source = Arc::new(InMemorySource::default());
    let finals = Arc::new(Finals::default());
    let queue = Queue::builder(
        "outcomes",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(work),
    )
    .concurrency(5)
    .ack_point(AckPoint::OnCompletion)
    .retry_policy(RetryPolicy {
        max_attempts: 3,
        base: Duration::from_secs(1),
        ..RetryPolicy::default()
    })
    .build()?;
    let handle = queue.handle();
    let monitor = Monitor::new()
        .observer(Arc::clone(&finals))
        .register(queue)?;
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));

    for name in ["ok", "flaky", "bad-input", "later", "boom"] {
        let _ = handle
            .push(Task::new(name.to_owned()).with_id(name))
            .await?;
    }
    // "later" is deferred to the source once and comes back.
    while !source.is_empty() || handle.live_tasks() > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.cancel();
    running.await??;

    let finals = finals.0.lock().map_err(|_| "observer poisoned")?.clone();
    let expected = [
        ("bad-input", TaskState::Failed),
        ("boom", TaskState::Panicked),
        ("flaky", TaskState::Succeeded),
        ("later", TaskState::Succeeded),
        ("ok", TaskState::Succeeded),
    ];
    for (name, state) in expected {
        if finals.get(name) != Some(&state) {
            return Err(format!("{name}: expected {state}, got {:?}", finals.get(name)).into());
        }
    }
    println!("every outcome as expected");
    Ok(())
}
