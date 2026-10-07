//! E-8 `custom-source` — a source of your own on the contract: "task",
//! "empty for now" and "closed" answers, a wake-up on new work, acks
//! (spec 2.3.1, 2.5 "Source").
//!
//! The source is an outbox table kept in memory: rows are written by the
//! application, handed out by polls and marked done by acks. Tasks are not
//! pushed through the queue, so the queue consumes the source
//! (`Queue::consumer`); a source that takes pushes implements `PushSource`
//! as well and is built with `Queue::builder`.
//!
//! ```text
//! cargo run -p taskcraft --example custom-source
//! ```

use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};

use taskcraft::codec::IdentityCodec;
use taskcraft::source::{
    AckPointSupport, Capabilities, CloseReason, Polled, Source, WakeHandle, WakeSignal,
};
use taskcraft::{AckPoint, CancellationToken, Monitor, Queue, StopReason, Task, task_fn};

/// Rows waiting, rows handed out, and whether the outbox is closed.
#[derive(Default)]
struct Rows {
    waiting: VecDeque<(u64, String)>,
    handed_out: BTreeMap<u64, String>,
    done: Vec<String>,
    closed: bool,
    next: u64,
}

/// An outbox: the application writes rows, the queue works them off.
#[derive(Default)]
struct Outbox {
    rows: Mutex<Rows>,
    wake: WakeHandle,
}

impl Outbox {
    fn write(&self, text: &str) {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let id = rows.next;
        rows.next += 1;
        rows.waiting.push_back((id, text.to_owned()));
        drop(rows);
        // Tell a sleeping worker there is work.
        self.wake.wake();
    }

    fn close(&self) {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
        self.wake.wake();
    }
}

impl Source for Outbox {
    type Message = Task<String>;
    /// The row id: what an ack marks done.
    type Receipt = u64;
    type Error = Infallible;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::PerTask)
    }

    async fn poll(&self) -> Result<Polled<Task<String>, u64>, Infallible> {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((id, text)) = rows.waiting.pop_front() {
            rows.handed_out.insert(id, text.clone());
            return Ok(Polled::Task {
                message: Task::new(text).with_id(format!("row-{id}")),
                receipt: id,
            });
        }
        // "Empty" keeps the worker polling; only "closed" ends it.
        if rows.closed && rows.handed_out.is_empty() {
            return Ok(Polled::Closed(CloseReason::new("outbox closed")));
        }
        Ok(Polled::Empty)
    }

    async fn ack(&self, id: u64) -> Result<(), Infallible> {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(text) = rows.handed_out.remove(&id) {
            rows.done.push(text);
        }
        Ok(())
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        Some(self.wake.subscribe())
    }
}

async fn deliver(text: String) {
    println!("  delivered {text:?}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let outbox = Arc::new(Outbox::default());
    let queue = Queue::consumer(
        "outbox",
        Arc::clone(&outbox),
        IdentityCodec::new(),
        task_fn(deliver),
    )
    .ack_point(AckPoint::OnCompletion)
    .build()?;
    // A consumed source takes no pushes: its handle only asks and cancels.
    let (monitor, _outbox_handle) = Monitor::new().register(queue)?;
    let running = tokio::spawn(monitor.run(CancellationToken::new()));

    for text in ["order 1 paid", "order 2 shipped"] {
        outbox.write(text);
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // The worker sleeps on "empty"; a write wakes it at once.
    outbox.write("order 3 delivered");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    outbox.close();

    // No stop signal: the worker ends because the source closed.
    let report = running.await??;
    let reason = report.queues.first().map(|q| q.reason.clone());
    println!("worker stopped: {reason:?}");
    let done = outbox
        .rows
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .done
        .len();
    if done != 3 || !matches!(reason, Some(StopReason::SourceClosed(_))) {
        return Err("unexpected outbox state".into());
    }
    Ok(())
}
