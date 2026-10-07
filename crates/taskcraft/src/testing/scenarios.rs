//! Reusable scenarios for the defects this design is known to invite.
//!
//! Every scenario expects virtual time: run it from a test with
//! `#[tokio::test(start_paused = true)]`. Pauses inside are minutes long and
//! take no real time there.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::handler::SharedData;
use crate::metadata::MetadataRegistry;
use crate::outcome::{BoxError, Outcome, TaskError};
use crate::source::PushSource;
use crate::task::{Task, TaskId};
use crate::testing::ledger::DeliveryLedger;
use crate::testing::runner::{Runner, RunnerSetup, ScenarioHandler};
use crate::testing::source::{FaultyCodec, FaultySource};

/// A scenario found the defect it looks for.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ScenarioFailure(String);

impl ScenarioFailure {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

struct Harness {
    source: Arc<FaultySource<u32>>,
    ledger: Arc<DeliveryLedger>,
    stop: CancellationToken,
    worker: JoinHandle<()>,
}

/// A handler that records each execution and then answers `outcome`.
fn recording(ledger: &Arc<DeliveryLedger>, outcome: fn() -> Outcome) -> ScenarioHandler {
    let ledger = Arc::clone(ledger);
    ScenarioHandler::new(move |request| {
        ledger.executed(request.task().id());
        let result = outcome();
        async move { result }
    })
}

fn start(
    runner: &impl Runner,
    outcome: fn() -> Outcome,
    max_attempts: u32,
    retry_pause: Duration,
) -> Harness {
    let source = Arc::new(FaultySource::new());
    let ledger = Arc::new(DeliveryLedger::new());
    let stop = CancellationToken::new();
    let setup = RunnerSetup {
        source: Arc::clone(&source),
        codec: FaultyCodec::new(),
        handler: recording(&ledger, outcome),
        max_attempts,
        retry_pause,
        stop: stop.clone(),
        registry: Arc::new(MetadataRegistry::new()),
        shared: Arc::new(SharedData::new()),
    };
    let worker = tokio::spawn(runner.run(setup));
    Harness {
        source,
        ledger,
        stop,
        worker,
    }
}

impl Harness {
    fn push(&self, id: &str) -> TaskId {
        let id = TaskId::new(id);
        self.ledger.pushed(&id);
        self.source.enqueue(Task::new(0).with_id(id.clone()));
        id
    }

    /// Waits up to `limit` of virtual time for the task to run `n` times.
    async fn executed(&self, id: &TaskId, n: u32, limit: Duration) -> bool {
        timeout(limit, async {
            while self.ledger.executions(id) < n {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    }

    /// Stops the worker and checks it stops within a second.
    async fn stop(self) -> Result<Arc<DeliveryLedger>, ScenarioFailure> {
        self.stop.cancel();
        match timeout(Duration::from_secs(1), self.worker).await {
            Ok(_) => Ok(self.ledger),
            Err(_) => Err(ScenarioFailure::new(
                "the worker did not stop within 1 s of the stop signal",
            )),
        }
    }
}

/// An empty source must never end the worker (apalis A-1).
///
/// The source stays empty for ten minutes; the worker must still be running
/// and pick up a task pushed afterwards.
///
/// # Errors
///
/// When the worker ends on the empty source or never runs the late task.
pub async fn empty_never_ends_worker(runner: &impl Runner) -> Result<(), ScenarioFailure> {
    let h = start(runner, || Outcome::Success, 1, Duration::from_secs(1));
    sleep(Duration::from_secs(600)).await;
    if h.worker.is_finished() {
        return Err(ScenarioFailure::new(
            "the worker ended while the source was only empty",
        ));
    }
    let id = h.push("late");
    if !h.executed(&id, 1, Duration::from_secs(60)).await {
        return Err(ScenarioFailure::new(
            "a task pushed after a long empty spell never ran",
        ));
    }
    h.stop().await.map(drop)
}

fn boxed_abort() -> Outcome {
    let wrapped: BoxError = Box::new(std::io::Error::other("bad input"));
    Outcome::from(TaskError::abort(wrapped))
}

/// An abort must not be retried, even when it wraps a boxed error
/// (apalis B-1).
///
/// # Errors
///
/// When the task runs more than once.
pub async fn boxed_abort_is_not_retried(runner: &impl Runner) -> Result<(), ScenarioFailure> {
    let h = start(runner, boxed_abort, 3, Duration::from_secs(1));
    let id = h.push("abort");
    if !h.executed(&id, 1, Duration::from_secs(60)).await {
        return Err(ScenarioFailure::new("the task never ran"));
    }
    // Leave room for the retries a defective worker would make.
    sleep(Duration::from_secs(30)).await;
    let ledger = h.stop().await?;
    match ledger.executions(&id) {
        1 => Ok(()),
        n => Err(ScenarioFailure::new(format!(
            "an aborted task ran {n} times; abort must not be retried"
        ))),
    }
}

/// One poison message must not stop the worker (apalis B-4).
///
/// # Errors
///
/// When the task queued after the poison message never runs, or the worker
/// ends.
pub async fn poison_does_not_stop_worker(runner: &impl Runner) -> Result<(), ScenarioFailure> {
    let h = start(runner, || Outcome::Success, 1, Duration::from_secs(1));
    h.source.inject_poison("not a task");
    let id = h.push("after-poison");
    if !h.executed(&id, 1, Duration::from_secs(60)).await {
        return Err(ScenarioFailure::new(
            "a task after a poison message never ran",
        ));
    }
    if h.worker.is_finished() {
        return Err(ScenarioFailure::new(
            "the worker ended after a poison message",
        ));
    }
    h.stop().await.map(drop)
}

/// A stop signal during a retry pause stops the worker at once, without
/// waiting for the pause to end (apalis A-4; the pattern of the first
/// consumer's cancellation-during-retry-pause test).
///
/// # Errors
///
/// When stopping waits for the pause, or the task runs again.
pub async fn cancel_during_retry_pause_stops_work(
    runner: &impl Runner,
) -> Result<(), ScenarioFailure> {
    let pause = Duration::from_secs(300);
    let h = start(runner, || Outcome::retry("not yet"), 3, pause);
    let id = h.push("lookup");
    if !h.executed(&id, 1, Duration::from_secs(60)).await {
        return Err(ScenarioFailure::new("the task never ran"));
    }
    let started = Instant::now();
    let ledger = h.stop().await?;
    if started.elapsed() >= pause {
        return Err(ScenarioFailure::new(
            "stopping waited for the retry pause to end",
        ));
    }
    match ledger.executions(&id) {
        1 => Ok(()),
        n => Err(ScenarioFailure::new(format!(
            "the task ran {n} times after the stop signal"
        ))),
    }
}

/// A push must wake every subscriber of the source, not only one
/// (apalis A-5).
///
/// # Errors
///
/// When the source has no wake-up signal, or a subscriber is not woken
/// within a second.
pub async fn wake_reaches_every_subscriber<S: PushSource>(
    source: &S,
    id: &TaskId,
    message: S::Message,
) -> Result<(), ScenarioFailure> {
    let (Some(mut first), Some(mut second)) = (source.subscribe(), source.subscribe()) else {
        return Err(ScenarioFailure::new("the source has no wake-up signal"));
    };
    first.mark_seen();
    second.mark_seen();
    if source.push(id, message).await.is_err() {
        return Err(ScenarioFailure::new("the source refused the push"));
    }
    for (name, signal) in [("first", &mut first), ("second", &mut second)] {
        if !matches!(
            timeout(Duration::from_secs(1), signal.changed()).await,
            Ok(true)
        ) {
            return Err(ScenarioFailure::new(format!(
                "the {name} subscriber was not woken by a push"
            )));
        }
    }
    Ok(())
}
