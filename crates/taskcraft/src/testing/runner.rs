//! What a worker under test receives from a scenario.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::handler::{BoxFuture, SharedData, TaskRequest};
use crate::metadata::MetadataRegistry;
use crate::outcome::Outcome;
use crate::testing::source::{FaultyCodec, FaultySource};

/// The handler a scenario runs: a cloneable function from request to outcome.
#[derive(Clone)]
pub struct ScenarioHandler(
    Arc<dyn Fn(TaskRequest<u32>) -> BoxFuture<'static, Outcome> + Send + Sync>,
);

impl ScenarioHandler {
    /// A handler from an async function.
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: Fn(TaskRequest<u32>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Outcome> + Send + 'static,
    {
        Self(Arc::new(move |request| Box::pin(f(request))))
    }

    /// Runs the handler.
    pub fn call(&self, request: TaskRequest<u32>) -> BoxFuture<'static, Outcome> {
        (self.0)(request)
    }
}

impl fmt::Debug for ScenarioHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScenarioHandler")
    }
}

/// Everything a worker under test needs to run one scenario.
///
/// The contract is deliberately small: poll until "closed" or `stop`; on
/// "empty" wait for a wake-up or poll again; decode with `codec`; run
/// `handler`; repeat a "retry" after `retry_pause`, up to `max_attempts`
/// attempts; ack after the outcome.
#[derive(Debug, Clone)]
pub struct RunnerSetup {
    /// The source to work from.
    pub source: Arc<FaultySource<u32>>,
    /// Its codec.
    pub codec: FaultyCodec<u32>,
    /// The handler to run.
    pub handler: ScenarioHandler,
    /// Attempts allowed per task.
    pub max_attempts: u32,
    /// Pause before a retry.
    pub retry_pause: Duration,
    /// Stops the worker.
    pub stop: CancellationToken,
    /// Metadata registry for requests.
    pub registry: Arc<MetadataRegistry>,
    /// Shared data for requests.
    pub shared: Arc<SharedData>,
}

/// A worker that scenarios can run: a stub in tests today, the real worker
/// once it exists.
pub trait Runner: Send + Sync + 'static {
    /// Runs the worker loop until the source closes or `setup.stop` fires.
    fn run(&self, setup: RunnerSetup) -> BoxFuture<'static, ()>;
}
