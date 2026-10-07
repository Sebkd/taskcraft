//! E-4 `resource-pools` — two pools of different sizes, one per queue; status
//! and cancel by id; permits come back on cancel and on panic (spec 2.3.8,
//! 2.1.2.14, 2.1.2.15).
//!
//! Runs on virtual time.
//!
//! ```text
//! cargo run -p taskcraft --example resource-pools
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use taskcraft::codec::IdentityCodec;
use taskcraft::{
    Cancel, CancelOutcome, CancellationToken, Data, InMemorySource, Monitor, Outcome, Queue,
    QueueHandle, SharedData, Task, TaskId, TaskState, task_fn,
};
use tokio::time::sleep;

const HOUR: Duration = Duration::from_secs(3600);

type Handle = QueueHandle<u32>;

/// How many tasks of a queue hold its pool now, and the most at once.
#[derive(Default)]
struct Usage {
    now: AtomicUsize,
    peak: AtomicUsize,
}

/// An hour of work holding a permit; archive 13 is corrupt and panics.
#[allow(clippy::panic)] // The example shows a panic giving its permit back.
async fn heavy(archive: u32, cancel: Cancel, Data(usage): Data<Usage>) -> Outcome {
    let now = usage.now.fetch_add(1, Ordering::SeqCst) + 1;
    usage.peak.fetch_max(now, Ordering::SeqCst);
    if archive == 13 {
        usage.now.fetch_sub(1, Ordering::SeqCst);
        panic!("corrupt archive");
    }
    tokio::select! {
        () = cancel.cancelled() => {}
        () = sleep(HOUR) => {}
    }
    usage.now.fetch_sub(1, Ordering::SeqCst);
    Outcome::Success
}

/// The states of tasks `<prefix>0..4` this process holds.
fn states(prefix: &str, handle: &Handle) -> Vec<(String, TaskState)> {
    (0..4)
        .filter_map(|n| {
            let id = TaskId::new(format!("{prefix}{n}"));
            handle
                .status(&id)
                .map(|s| (id.as_str().to_owned(), s.state()))
        })
        .collect()
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("virtual time: hour-long tasks");
    let mut queues = Vec::new();
    let mut usages = Vec::new();
    for (name, pool) in [("unpack", "unpack"), ("pack", "pack")] {
        let mut shared = SharedData::new();
        shared.insert(Usage::default());
        usages.push(shared.get::<Usage>().ok_or("no usage")?);
        let queue = Queue::builder(
            name,
            Arc::new(InMemorySource::default()),
            IdentityCodec::new(),
            task_fn(heavy),
        )
        .concurrency(8)
        .pool(pool, 1)
        .shared_data(shared)
        .no_recovery()
        .build()?;
        queues.push(queue);
    }
    let pack = queues.pop().ok_or("no pack queue")?;
    let unpack = queues.pop().ok_or("no unpack queue")?;
    let monitor = Monitor::new().pool("unpack", 2)?.pool("pack", 1)?;
    let (monitor, unpack_handle) = monitor.register(unpack)?;
    let (monitor, pack_handle) = monitor.register(pack)?;
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));

    for n in 0..4 {
        let _ = unpack_handle
            .push(Task::new(n).with_id(format!("u{n}")))
            .await?;
        let _ = pack_handle
            .push(Task::new(n).with_id(format!("p{n}")))
            .await?;
    }
    sleep(Duration::from_secs(1)).await;
    println!("unpack (pool of 2): {:?}", states("u", &unpack_handle));
    println!("pack (pool of 1):   {:?}", states("p", &pack_handle));

    let running_pack = states("p", &pack_handle)
        .into_iter()
        .find(|(_, s)| *s == TaskState::Running)
        .map(|(id, _)| TaskId::new(id))
        .ok_or("no pack task running")?;
    println!("cancel {running_pack}");
    if pack_handle.cancel(&running_pack).await != CancelOutcome::CancelRequested {
        return Err("cancel was not requested".into());
    }
    sleep(Duration::from_secs(1)).await;
    let after = states("p", &pack_handle);
    println!("pack after cancel:  {after:?}");
    if after.len() != 3
        || after
            .iter()
            .filter(|(_, s)| *s == TaskState::Running)
            .count()
            != 1
    {
        return Err("the pack permit did not pass on".into());
    }

    println!("a panicking unpack task gives its permit back too");
    let _ = unpack_handle.push(Task::new(13).with_id("u13")).await?;
    sleep(3 * HOUR).await;
    let peaks: Vec<usize> = usages
        .iter()
        .map(|u| u.peak.load(Ordering::SeqCst))
        .collect();
    let all_done = unpack_handle.live_tasks() == 0;
    stop.cancel();
    running.await??;
    println!(
        "most at once: unpack {}, pack {}; unpack done: {all_done}",
        peaks[0], peaks[1]
    );
    if peaks != [2, 1] || !all_done {
        return Err("pools did not hold".into());
    }
    Ok(())
}
