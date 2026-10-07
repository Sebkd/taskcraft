//! The worker of one queue: intake loop, supervision, execution and drain
//! (spec 2.4.2, 2.3.12–2.3.14).

mod drain;
mod execute;
mod intake;
mod notices;
mod pools;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;

use self::drain::stop_worker;
use self::intake::Intake;
use self::notices::spawn_listener;
pub(crate) use self::pools::PoolClaim;
use crate::backend::Backend;
use crate::codec::Codec;
use crate::handler::{SharedData, TaskRequest};
use crate::metadata::MetadataRegistry;
use crate::monitor::{QueueReport, WorkerContext};
use crate::observe::{Observers, Occupancy, observers_of};
use crate::outcome::{BoxError, Outcome};
use crate::poll::Poller;
use crate::queue::{OverflowPolicy, Queue, TimeoutOutcome};
use crate::registry::TaskRegistry;
use crate::retry::RetryPolicy;
use crate::source::AckPointSupport;
use crate::task::Task;

/// How a task left the worker, for the shutdown report.
enum TaskEnd {
    /// Reached its outcome after intake ended: the shutdown or the closed
    /// source found it at work.
    Finished,
    /// Reached its outcome before intake ended; not in the report (spec
    /// 2.7.6).
    Before,
    /// Cancelled by shutdown while waiting for a slot or a pool.
    CancelledWaiting,
}

/// The slot an accepted task runs with.
enum Slot {
    /// Taken already.
    Held(OwnedSemaphorePermit),
    /// To be waited for, holding a place in the waiting room meanwhile.
    Wait {
        place: Option<OwnedSemaphorePermit>,
        slots: Arc<Semaphore>,
    },
}

/// What every execution of a queue shares.
struct ExecCtx<S: Backend, C> {
    queue: Arc<str>,
    source: Arc<S>,
    codec: Arc<C>,
    registry: Arc<MetadataRegistry>,
    shared: Arc<SharedData>,
    supports_defer: bool,
    ack_errors: mpsc::UnboundedSender<String>,
    stop: CancellationToken,
    pools: Vec<PoolClaim>,
    slots: Arc<Semaphore>,
    retry: RetryPolicy,
    tasks: Arc<TaskRegistry>,
    cancel_grace: Duration,
    attempt_timeout: Option<Duration>,
    timeout_outcome: TimeoutOutcome,
    occupancy: Arc<Occupancy>,
    observers: Observers,
    /// The source keeps task history: progress and final states are
    /// recorded (rule 2.3.9 p. 6).
    records: bool,
    /// Set when intake ends, by the shutdown signal or a closed source:
    /// outcomes from then on count in the shutdown report.
    draining: CancellationToken,
}

pub(crate) async fn run_worker<S, C, Svc, Args>(
    queue: Queue<S, C, Svc, Args>,
    recovered: Vec<Task<Args>>,
    pools: Vec<PoolClaim>,
    ctx: WorkerContext,
) -> QueueReport
where
    S: Backend,
    C: Codec<Args, S::Message>,
    Svc: tower::Service<TaskRequest<Args>, Response = Outcome> + Clone + Send + 'static,
    Svc::Error: Into<BoxError>,
    Svc::Future: Send,
    Args: Clone + Send + 'static,
{
    let Queue {
        config,
        source,
        codec,
        service,
        registry,
        shared,
        dead_letter,
        overflow,
        tasks,
        observers: observer_cell,
        ..
    } = queue;
    let name: Arc<str> = config.name.into();
    let observers = observers_of(&observer_cell);
    let store = source.capabilities().ack_point_support() == AckPointSupport::Fixed;
    info!(
        event = "worker",
        action = "started",
        "worker started: queue={}",
        name
    );

    let (ack_errors, ack_failures) = mpsc::unbounded_channel();
    let slots = Arc::new(Semaphore::new(config.concurrency));
    let exec = Arc::new(ExecCtx {
        queue: Arc::clone(&name),
        source: Arc::clone(&source),
        codec: Arc::clone(&codec),
        registry,
        shared,
        supports_defer: source.capabilities().supports_defer(),
        records: store,
        draining: CancellationToken::new(),
        ack_errors: ack_errors.clone(),
        stop: ctx.stop.clone(),
        pools,
        slots: Arc::clone(&slots),
        retry: config.retry,
        tasks: Arc::clone(&tasks),
        cancel_grace: config.cancel_grace,
        attempt_timeout: config.attempt_timeout,
        timeout_outcome: config.timeout_outcome,
        occupancy: Occupancy::new(Arc::clone(&name), observers.clone()),
        observers: observers.clone(),
    });
    let (reject, wait_limit) = match overflow {
        OverflowPolicy::Wait { limit } => (None, limit.unwrap_or(config.concurrency)),
        OverflowPolicy::Reject(hook) => (Some(hook), 0),
    };
    let poller = Poller::new(&config.poll);
    let wake = source.subscribe();
    let listener = spawn_listener(&*source, &tasks, &name, &observers);
    let mut intake = Intake {
        poller,
        wake,
        ctx,
        name: Arc::clone(&name),
        observers: observers.clone(),
        source,
        codec,
        service,
        dead_letter,
        tasks: Arc::clone(&tasks),
        exec,
        store,
        ack_point: config.ack_point,
        slots,
        waiting_room: Arc::new(Semaphore::new(wait_limit)),
        reject,
        running: JoinSet::new(),
        ids: HashMap::new(),
        drain_cancel: CancellationToken::new(),
        failures: 0,
        ack_errors,
        ack_failures,
    };
    intake.accept_recovered(recovered);
    let reason = intake.run().await;
    stop_worker(intake, reason, listener, config.cancel_grace).await
}
