//! Random sequences of pushes, cancels and pauses, ended by a stop, against
//! invariants 1.3.6, 1.3.8 and 1.3.9 (change concurrency-and-fuzz-tests,
//! criterion 2). Cases: `PROPTEST_CASES` (256 by default; CI runs 10 000).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use proptest::prelude::*;
use taskcraft::codec::IdentityCodec;
use taskcraft::observe::{Event, Observer};
use taskcraft::{
    Cancel, CancellationToken, InMemorySource, Monitor, Outcome, PushOutcome, Queue, RetryPolicy,
    Task, TaskId, task_fn,
};

const IDS: [&str; 5] = ["a", "b", "c", "d", "e"];

/// What a task does: its argument.
#[derive(Debug, Clone, Copy)]
enum Kind {
    Success,
    Abort,
    Retry,
    Panic,
    Long,
}

#[derive(Debug, Clone)]
enum Op {
    Push(usize, Kind),
    Cancel(usize),
    Pause(u64),
}

fn kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        Just(Kind::Success),
        Just(Kind::Abort),
        Just(Kind::Retry),
        Just(Kind::Panic),
        Just(Kind::Long),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..IDS.len(), kind()).prop_map(|(id, kind)| Op::Push(id, kind)),
        1 => (0..IDS.len()).prop_map(Op::Cancel),
        2 => (0_u64..200).prop_map(Op::Pause),
    ]
}

/// Running attempts per id, and the most ever seen at once.
#[derive(Default)]
struct Running(Mutex<(HashMap<String, usize>, usize)>);

/// Leaves the count when the attempt ends, panics included.
struct Attempt(Arc<Running>, String);

impl Drop for Attempt {
    fn drop(&mut self) {
        let mut running = self.0.0.lock().unwrap();
        if let Some(n) = running.0.get_mut(&self.1) {
            *n -= 1;
        }
    }
}

/// Pool usage as reported: never above its size; the last report.
#[derive(Default)]
struct Pools(Mutex<(u32, Option<u32>)>);

impl Observer for Pools {
    fn on_event(&self, event: &Event<'_>) {
        if let Event::PoolUsage { in_use, total, .. } = event {
            let mut pools = self.0.lock().unwrap();
            pools.0 = pools.0.max(*in_use);
            pools.1 = Some(*in_use);
            assert!(in_use <= total, "{in_use} of {total} permits in use");
        }
    }
}

fn run_case(ops: &[Op]) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async {
        let running = Arc::new(Running::default());
        let record = Arc::clone(&running);
        let handler = task_fn(move |kind: u8, id: TaskId, cancel: Cancel| {
            let record = Arc::clone(&record);
            async move {
                let _attempt = {
                    let mut counts = record.0.lock().unwrap();
                    let n = counts.0.entry(id.as_str().to_owned()).or_default();
                    *n += 1;
                    let now = *n;
                    counts.1 = counts.1.max(now);
                    Attempt(Arc::clone(&record), id.as_str().to_owned())
                };
                match kind {
                    0 => Outcome::Success,
                    1 => Outcome::abort("no"),
                    2 => Outcome::retry("again"),
                    3 => panic!("boom"),
                    _ => {
                        tokio::select! {
                            () = cancel.cancelled() => {}
                            () = tokio::time::sleep(Duration::from_secs(10)) => {}
                        }
                        Outcome::Success
                    }
                }
            }
        });
        let queue = Queue::builder(
            "q",
            Arc::new(InMemorySource::<u8>::default()),
            IdentityCodec::new(),
            handler,
        )
        .concurrency(2)
        .pool("p", 1)
        .cancel_grace(Duration::from_millis(500))
        .retry_policy(RetryPolicy {
            max_attempts: 2,
            base: Duration::from_millis(20),
            jitter: 0.0,
            ..RetryPolicy::default()
        })
        .no_recovery()
        .build()
        .unwrap();
        let pools = Arc::new(Pools::default());
        let (monitor, handle) = Monitor::new()
            .shutdown_timeout(Duration::from_secs(1))
            .pool("p", 1)
            .unwrap()
            .observer(Arc::clone(&pools))
            .register(queue)
            .unwrap();
        let stop = CancellationToken::new();
        let monitor = tokio::spawn(monitor.run(stop.clone()));

        for op in ops {
            match op {
                Op::Push(id, kind) => {
                    let task = Task::new(*kind as u8).with_id(IDS[*id]);
                    let pushed = handle.push(task).await.unwrap();
                    assert!(
                        matches!(
                            pushed,
                            PushOutcome::Enqueued { .. } | PushOutcome::AlreadyRunning { .. }
                        ),
                        "{pushed:?}"
                    );
                }
                Op::Cancel(id) => {
                    let _ = handle.cancel(&TaskId::new(IDS[*id])).await;
                }
                Op::Pause(ms) => tokio::time::sleep(Duration::from_millis(*ms)).await,
            }
        }
        stop.cancel();
        monitor.await.unwrap().unwrap();

        // 1.3.8: one id never runs twice at once.
        let most = running.0.lock().unwrap().1;
        assert!(most <= 1, "an id ran {most} times at once");
        // 1.3.9: nothing stays in the registry.
        assert_eq!(handle.live_tasks(), 0);
        // 1.3.6: every permit came back, and never more than the pool held.
        let (peak, last) = *pools.0.lock().unwrap();
        assert!(peak <= 1);
        assert!(
            matches!(last, None | Some(0)),
            "permits left in use: {last:?}"
        );
    });
}

proptest! {
    #[test]
    fn invariants_hold_for_any_sequence(ops in prop::collection::vec(op(), 1..40)) {
        // The panicking task kind would print a message per case; every
        // other panic, a failed check included, is printed.
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if info.payload().downcast_ref::<&str>() != Some(&"boom") {
                default(info);
            }
        }));
        run_case(&ops);
    }
}
