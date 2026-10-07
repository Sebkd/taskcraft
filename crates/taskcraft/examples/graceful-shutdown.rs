//! E-6 `graceful-shutdown` — the OS signal, the shutdown timeout, the cancel
//! grace and the shutdown report (spec 2.3.14, 2.7.6).
//!
//! Press Ctrl-C, or wait: the example stops itself after a few (virtual)
//! seconds.
//!
//! ```text
//! cargo run -p taskcraft --example graceful-shutdown
//! ```

use std::sync::Arc;
use std::time::Duration;

use taskcraft::codec::IdentityCodec;
use taskcraft::{Cancel, CancellationToken, InMemorySource, Monitor, Queue, Task, task_fn};
use tokio::time::{Instant, sleep};

/// Three kinds of task: one finishes within the shutdown timeout, one
/// watches its cancel flag, one ignores it.
async fn job(kind: String, cancel: Cancel) {
    match kind.as_str() {
        "quick" => sleep(Duration::from_secs(6)).await,
        "polite" => {
            tokio::select! {
                () = cancel.cancelled() => println!("  polite: cancel flag seen, cleaning up"),
                () = sleep(Duration::from_secs(3600)) => {}
            }
        }
        _ => sleep(Duration::from_secs(3600)).await,
    }
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("virtual time; press Ctrl-C or wait");
    let queue = Queue::builder(
        "jobs",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(job),
    )
    .concurrency(3)
    .cancel_grace(Duration::from_secs(2))
    .no_recovery()
    .build()?;
    let (monitor, handle) = Monitor::new()
        .shutdown_timeout(Duration::from_secs(5))
        .register(queue)?;
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));
    for kind in ["quick", "polite", "stubborn"] {
        let _ = handle.push(Task::new(kind.to_owned())).await?;
    }
    sleep(Duration::from_secs(1)).await;

    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal?;
            println!("Ctrl-C");
        }
        () = sleep(Duration::from_secs(3)) => println!("no Ctrl-C: stopping by itself"),
    }
    let at = Instant::now();
    // The signal stops intake at once; running tasks get the shutdown
    // timeout, then their cancel flag, then the cancel grace.
    stop.cancel();
    let report = running.await??;
    println!(
        "stopped in {:?}: completed={}, cancelled={}, aborted={}",
        at.elapsed(),
        report.completed(),
        report.cancelled(),
        report.aborted()
    );
    if (report.completed(), report.cancelled(), report.aborted()) != (1, 2, 1) {
        return Err("unexpected shutdown report".into());
    }
    Ok(())
}
