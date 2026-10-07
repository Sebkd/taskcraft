//! Notices from a source reach the worker (change kafka-commit-errors).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::convert::Infallible;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use taskcraft::codec::IdentityCodec;
use taskcraft::observe::{Event, Observer};
use taskcraft::source::{AckPointSupport, Capabilities, Notice, Notices, Polled, Source};
use taskcraft::{CancellationToken, Monitor, Queue, Task, task_fn};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::sleep;

/// A source that never has work and hands its notices to the worker.
struct Quiet {
    notices: Mutex<Option<Notices>>,
    polls: AtomicU32,
}

impl Source for Quiet {
    type Message = Task<u32>;
    type Receipt = ();
    type Error = Infallible;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::PerTask)
    }

    async fn poll(&self) -> Result<Polled<Task<u32>, ()>, Infallible> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Ok(Polled::Empty)
    }

    async fn ack(&self, (): ()) -> Result<(), Infallible> {
        Ok(())
    }

    fn notices(&self) -> Option<Notices> {
        self.notices.lock().unwrap().take()
    }
}

#[derive(Default)]
struct Counts {
    source_failed: AtomicU32,
    restarted: AtomicU32,
}

impl Observer for Counts {
    fn on_event(&self, event: &Event<'_>) {
        match event {
            Event::SourceFailed { .. } => {
                self.source_failed.fetch_add(1, Ordering::SeqCst);
            }
            Event::WorkerRestarted { .. } => {
                self.restarted.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

/// A background source error is counted as a source error; intake goes on
/// without a restart.
#[tokio::test(start_paused = true)]
async fn source_error_notice_is_counted_without_a_restart() {
    let (notify, notices): (UnboundedSender<Notice>, Notices) = Notices::channel();
    let source = Arc::new(Quiet {
        notices: Mutex::new(Some(notices)),
        polls: AtomicU32::new(0),
    });
    let queue = Queue::consumer(
        "q",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(|_: u32| async {}),
    )
    .no_recovery()
    .build()
    .unwrap();
    let counts = Arc::new(Counts::default());
    let monitor = Monitor::new()
        .observer(Arc::clone(&counts))
        .register(queue)
        .unwrap()
        .0;
    let stop = CancellationToken::new();
    let running = tokio::spawn(monitor.run(stop.clone()));
    sleep(Duration::from_secs(1)).await;

    notify
        .send(Notice::SourceError("offset commit failed".to_owned()))
        .unwrap();
    sleep(Duration::from_secs(1)).await;
    assert_eq!(counts.source_failed.load(Ordering::SeqCst), 1);
    assert_eq!(counts.restarted.load(Ordering::SeqCst), 0, "no restart");
    let polls = source.polls.load(Ordering::SeqCst);
    sleep(Duration::from_secs(60)).await;
    assert!(
        source.polls.load(Ordering::SeqCst) > polls,
        "intake goes on"
    );
    stop.cancel();
    running.await.unwrap().unwrap();
}
