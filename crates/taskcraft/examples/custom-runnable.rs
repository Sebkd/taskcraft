//! E-9 `custom-runnable` — a runnable of your own that is not a state
//! machine: a computation on an OS thread that stops softly when the task is
//! cancelled (spec 2.3.21).
//!
//! ```text
//! cargo run -p taskcraft --example custom-runnable
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use taskcraft::codec::IdentityCodec;
use taskcraft::observe::{Event, Observer};
use taskcraft::runnable::{Run, Runnable};
use taskcraft::{
    CancelOutcome, CancellationToken, InMemorySource, Monitor, Outcome, Queue, Task, TaskId,
    TaskState, task_fn,
};
use tokio::sync::oneshot;

/// Checks numbers for primes on its own thread, a step at a time.
struct PrimeSearch {
    up_to: u64,
}

/// How the search ended.
enum Searched {
    Done(usize),
    Stopped,
}

impl Runnable for PrimeSearch {
    type Output = Searched;

    async fn run(self, stop: CancellationToken) -> Searched {
        let soft_stop = Arc::new(AtomicBool::new(false));
        let (done, result) = oneshot::channel();
        let flag = Arc::clone(&soft_stop);
        std::thread::spawn(move || {
            let mut found = 0;
            for n in 2..=self.up_to {
                if flag.load(Ordering::Relaxed) {
                    let _ = done.send(Searched::Stopped);
                    return;
                }
                if (2..n).take_while(|d| d * d <= n).all(|d| n % d != 0) {
                    found += 1;
                }
                std::thread::sleep(Duration::from_micros(200));
            }
            let _ = done.send(Searched::Done(found));
        });
        // The task's cancel flag becomes the thread's soft stop.
        tokio::select! {
            result = result => return result.unwrap_or(Searched::Stopped),
            () = stop.cancelled() => soft_stop.store(true, Ordering::Relaxed),
        }
        Searched::Stopped
    }

    fn into_outcome(searched: Searched) -> Outcome {
        match searched {
            Searched::Done(found) => {
                println!("  found {found} primes");
                Outcome::Success
            }
            Searched::Stopped => Outcome::abort("stopped"),
        }
    }
}

async fn search(up_to: u64) -> Run<PrimeSearch> {
    Run(PrimeSearch { up_to })
}

#[derive(Default)]
struct Finals(std::sync::Mutex<Vec<(String, TaskState)>>);

impl Observer for Finals {
    fn on_event(&self, event: &Event<'_>) {
        if let Event::Finished { task_id, state, .. } = event
            && let Ok(mut finals) = self.0.lock()
        {
            finals.push((task_id.as_str().to_owned(), *state));
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let finals = Arc::new(Finals::default());
    let queue = Queue::builder(
        "primes",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(search),
    )
    .concurrency(2)
    .no_recovery()
    .build()?;
    let stop = CancellationToken::new();
    let (monitor, handle) = Monitor::new()
        .observer(Arc::clone(&finals))
        .register(queue)?;
    let running = tokio::spawn(monitor.run(stop.clone()));

    let _ = handle.push(Task::new(1_000).with_id("small")).await?;
    let _ = handle.push(Task::new(1_000_000).with_id("huge")).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("cancel the huge search");
    if handle.cancel(&TaskId::new("huge")).await != CancelOutcome::CancelRequested {
        return Err("the huge search was not running".into());
    }
    while handle.live_tasks() > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.cancel();
    running.await??;

    let mut finals = finals.0.lock().map_err(|_| "poisoned")?.clone();
    finals.sort_by(|a, b| a.0.cmp(&b.0));
    println!("final states: {finals:?}");
    let expected = [
        ("huge".to_owned(), TaskState::Cancelled),
        ("small".to_owned(), TaskState::Succeeded),
    ];
    if finals != expected {
        return Err("unexpected final states".into());
    }
    Ok(())
}
