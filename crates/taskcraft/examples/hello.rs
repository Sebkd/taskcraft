//! E-1 `hello` — the smallest queue: an in-memory source, a handler, a push
//! and a shutdown (spec 2.1.1, 2.5).
//!
//! ```text
//! cargo run -p taskcraft --example hello
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use taskcraft::codec::IdentityCodec;
use taskcraft::{
    CancellationToken, Data, InMemorySource, Monitor, Queue, SharedData, Task, task_fn,
};

/// The handler: a plain async function of the task's arguments, plus any
/// extractable values — here a counter shared by the queue.
async fn greet(name: String, Data(greeted): Data<AtomicU32>) {
    println!("hello, {name}");
    greeted.fetch_add(1, Ordering::SeqCst);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut shared = SharedData::new();
    shared.insert(AtomicU32::new(0));
    let greeted = shared.get::<AtomicU32>().ok_or("no counter")?;

    let source = Arc::new(InMemorySource::default());
    let queue = Queue::builder(
        "greetings",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(greet),
    )
    .shared_data(shared)
    // An in-memory queue acks on accept: tasks accepted before a crash
    // are lost, and saying so is a decision (rule 2.3.9 p. 4).
    .no_recovery()
    .build()?;
    let stop = CancellationToken::new();
    let (monitor, handle) = Monitor::new().register(queue)?;
    let monitor = tokio::spawn(monitor.run(stop.clone()));

    for name in ["Ada", "Grace", "Linus"] {
        let _ = handle.push(Task::new(name.to_owned())).await?;
    }
    while greeted.load(Ordering::SeqCst) < 3 {
        tokio::task::yield_now().await;
    }

    stop.cancel();
    let report = monitor.await??;
    println!(
        "stopped: queue={}, reason={}",
        report.queues[0].queue, report.queues[0].reason
    );
    Ok(())
}
