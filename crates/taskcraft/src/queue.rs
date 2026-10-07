//! A queue: a source, a codec and a handler with their settings (spec 2.5,
//! 2.8).

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::Backend;
use crate::codec::{Codec, CodecError, JsonCodec};
use crate::error::ConfigError;
use crate::handler::{BoxFuture, SharedData};
use crate::metadata::MetadataRegistry;
use crate::observe::ObserverCell;
use crate::outcome::BoxError;
use crate::poll::PollStrategy;
use crate::registry::TaskRegistry;
use crate::retry::RetryPolicy;
use crate::source::{
    AckPointSupport, Consumed, PushSource, Pushed, Source, StoreMessage, Stored, TaskStore,
};
use crate::state::TaskState;
use crate::status::FinishReason;
use crate::task::{AckPoint, Task};

/// What the dead-letter hook receives: a message that could not be decoded.
#[derive(Debug)]
pub struct DeadLetter<M> {
    /// The queue it came from.
    pub queue: String,
    /// The raw message.
    pub message: M,
    /// Why it could not be decoded.
    pub error: CodecError,
}

type DeadLetterFn<M> = Arc<dyn Fn(DeadLetter<M>) + Send + Sync>;

pub(crate) struct DeadLetterHook<M> {
    pub(crate) hook: DeadLetterFn<M>,
    pub(crate) copy: fn(&M) -> M,
}

impl<M> Clone for DeadLetterHook<M> {
    fn clone(&self) -> Self {
        Self {
            hook: Arc::clone(&self.hook),
            copy: self.copy,
        }
    }
}

/// The recovery hook, called once when the monitor starts.
pub(crate) type RecoveryFn<Args> =
    Box<dyn FnOnce() -> BoxFuture<'static, Result<Vec<Task<Args>>, BoxError>> + Send>;

/// What an attempt that ran past the task timeout ends with (rule 2.3.16
/// p. 4), whatever the handler returned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TimeoutOutcome {
    /// Failed with "attempt timed out". The default.
    #[default]
    Abort,
    /// Retried within the retry policy, like a "retry" outcome.
    Retry,
}

pub(crate) type RejectFn<Args> = Arc<dyn Fn(Task<Args>) + Send + Sync>;

/// What the failed-task hook receives: a task that ended failed or panicked
/// (rule 2.3.26).
#[derive(Debug)]
#[non_exhaustive]
pub struct FailedTask<Args> {
    /// The queue it ran in.
    pub queue: String,
    /// The task: arguments, metadata and the number of its last attempt.
    pub task: Task<Args>,
    /// Failed or panicked.
    pub state: TaskState,
    /// Why: the abort reason, "attempts exhausted", "attempt timed out", the
    /// panic message.
    pub reason: FinishReason,
}

pub(crate) type FailedFn<Args> = Arc<dyn Fn(FailedTask<Args>) + Send + Sync>;

/// What happens to an accepted task when no slot is free (rule 2.3.7).
pub(crate) enum OverflowPolicy<Args> {
    /// Wait for a slot; at most `limit` tasks wait (default: the concurrency
    /// limit).
    Wait { limit: Option<usize> },
    /// Refuse at once and hand the task to the hook.
    Reject(RejectFn<Args>),
}

impl<Args> fmt::Debug for OverflowPolicy<Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wait { limit } => f.debug_struct("Wait").field("limit", limit).finish(),
            Self::Reject(_) => f.write_str("Reject"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueueConfig {
    pub(crate) name: String,
    pub(crate) concurrency: usize,
    /// Pool name → permits taken per attempt; ordered by name (rule 2.3.8).
    pub(crate) pools: BTreeMap<String, u32>,
    pub(crate) ack_point: AckPoint,
    pub(crate) poll: PollStrategy,
    pub(crate) cancel_grace: Duration,
    pub(crate) retry: RetryPolicy,
    pub(crate) attempt_timeout: Option<Duration>,
    pub(crate) timeout_outcome: TimeoutOutcome,
}

/// A queue: where tasks come from, how they are decoded, what runs them, and
/// how. Build it with [`Queue::builder`], [`Queue::consumer`] or
/// [`Queue::on_store`] and register it with a [`Monitor`](crate::Monitor),
/// which returns its handle.
///
/// `B` is the kind of source the queue was built on: [`Pushed`],
/// [`Consumed`] or [`Stored`].
pub struct Queue<B: Backend, C, Svc, Args> {
    pub(crate) config: QueueConfig,
    pub(crate) source: Arc<B>,
    pub(crate) codec: Arc<C>,
    pub(crate) service: Svc,
    pub(crate) registry: Arc<MetadataRegistry>,
    pub(crate) shared: Arc<SharedData>,
    pub(crate) dead_letter: Option<DeadLetterHook<B::Message>>,
    pub(crate) overflow: OverflowPolicy<Args>,
    pub(crate) tasks: Arc<TaskRegistry>,
    pub(crate) recovery: Option<RecoveryFn<Args>>,
    pub(crate) failed: Option<FailedFn<Args>>,
    pub(crate) observers: ObserverCell,
    pub(crate) _args: PhantomData<fn() -> Args>,
}

impl<S: PushSource, C: Codec<Args, S::Message>, Svc, Args> Queue<Pushed<S>, C, Svc, Args> {
    /// Starts building a queue named `name` on a source that tasks are
    /// pushed into, such as the [`InMemorySource`](crate::InMemorySource):
    /// it decodes them with `codec` and runs them with `service` (usually
    /// built with [`task_fn`](crate::task_fn)). Its handle pushes.
    pub fn builder(
        name: impl Into<String>,
        source: Arc<S>,
        codec: C,
        service: Svc,
    ) -> QueueBuilder<Pushed<S>, C, Svc, Args> {
        QueueBuilder::new(name.into(), Pushed(source), codec, service, None)
    }
}

impl<S: Source, C: Codec<Args, S::Message>, Svc, Args> Queue<Consumed<S>, C, Svc, Args> {
    /// Starts building a queue named `name` that consumes a stream, such as
    /// a Kafka topic. Its handle asks about tasks and cancels them, but does
    /// not push: tasks come from the stream.
    pub fn consumer(
        name: impl Into<String>,
        source: Arc<S>,
        codec: C,
        service: Svc,
    ) -> QueueBuilder<Consumed<S>, C, Svc, Args> {
        QueueBuilder::new(name.into(), Consumed(source), codec, service, None)
    }
}

impl<S: TaskStore, Svc, Args> Queue<Stored<S>, JsonCodec, Svc, Args>
where
    JsonCodec: Codec<Args, StoreMessage>,
{
    /// Starts building a queue named `name` on a durable task store, such as
    /// the PostgreSQL store. Tasks are stored as JSON; the codec is built in
    /// and names metadata through the queue's
    /// [metadata registry](QueueBuilder::metadata_registry). Its handle
    /// pushes.
    ///
    /// A store is neither a stream nor a plain push source, so it cannot be
    /// given another codec:
    ///
    /// ```compile_fail,E0277
    /// # use std::sync::Arc;
    /// # fn f<S: taskcraft::source::TaskStore>(store: Arc<S>) {
    /// let _ = taskcraft::Queue::builder("q", store, taskcraft::codec::JsonCodec::new(Default::default()), taskcraft::task_fn(|_: u32| async {}));
    /// # }
    /// ```
    ///
    /// ```compile_fail,E0277
    /// # use std::sync::Arc;
    /// # fn f<S: taskcraft::source::TaskStore>(store: Arc<S>) {
    /// let _ = taskcraft::Queue::consumer("q", store, taskcraft::codec::JsonCodec::new(Default::default()), taskcraft::task_fn(|_: u32| async {}));
    /// # }
    /// ```
    pub fn on_store(
        name: impl Into<String>,
        store: Arc<S>,
        service: Svc,
    ) -> QueueBuilder<Stored<S>, JsonCodec, Svc, Args> {
        let codec = JsonCodec::new(MetadataRegistry::new());
        let bind: fn(&MetadataRegistry) -> JsonCodec = |registry| JsonCodec::new(registry.clone());
        QueueBuilder::new(name.into(), Stored(store), codec, service, Some(bind))
    }
}

impl<B: Backend, C, Svc, Args> Queue<B, C, Svc, Args> {
    /// The queue name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.config.name
    }
}

impl<B: Backend, C, Svc, Args> fmt::Debug for Queue<B, C, Svc, Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Queue")
            .field("config", &self.config)
            .field("dead_letter", &self.dead_letter.is_some())
            .field("overflow", &self.overflow)
            .field("recovery", &self.recovery.is_some())
            .field("failed_task", &self.failed.is_some())
            .finish_non_exhaustive()
    }
}

/// Builds a [`Queue`]; settings are checked by [`build`](Self::build).
#[must_use]
pub struct QueueBuilder<B: Backend, C, Svc, Args> {
    queue: Queue<B, C, Svc, Args>,
    /// Builds the codec from the queue's metadata registry: the built-in
    /// codec of a task store.
    bind_codec: Option<fn(&MetadataRegistry) -> C>,
}

impl<B: Backend, C, Svc, Args> QueueBuilder<B, C, Svc, Args> {
    fn new(
        name: String,
        source: B,
        codec: C,
        service: Svc,
        bind_codec: Option<fn(&MetadataRegistry) -> C>,
    ) -> Self {
        Self {
            queue: Queue {
                config: QueueConfig {
                    name,
                    concurrency: 1,
                    pools: BTreeMap::new(),
                    ack_point: AckPoint::OnAccept,
                    poll: PollStrategy::default(),
                    cancel_grace: Duration::from_secs(30),
                    retry: RetryPolicy::default(),
                    attempt_timeout: None,
                    timeout_outcome: TimeoutOutcome::Abort,
                },
                source: Arc::new(source),
                codec: Arc::new(codec),
                service,
                registry: Arc::new(MetadataRegistry::new()),
                shared: Arc::new(SharedData::new()),
                dead_letter: None,
                overflow: OverflowPolicy::Wait { limit: None },
                tasks: Arc::new(TaskRegistry::new()),
                recovery: None,
                failed: None,
                observers: ObserverCell::default(),
                _args: PhantomData,
            },
            bind_codec,
        }
    }

    /// How many tasks run at once. Default 1.
    pub fn concurrency(mut self, limit: usize) -> Self {
        self.queue.config.concurrency = limit;
        self
    }

    /// When no slot is free, wait for one; at most `limit` tasks wait, and
    /// the worker stops polling while the waiting room is full. This is the
    /// default, with the limit equal to the concurrency limit (spec 2.8).
    pub fn wait_limit(mut self, limit: usize) -> Self {
        self.queue.overflow = OverflowPolicy::Wait { limit: Some(limit) };
        self
    }

    /// When no slot is free, refuse the task at once: `hook` gets it to
    /// answer the sender, and the message is acknowledged (scenario 2.2.1).
    ///
    /// The hook runs in the intake loop: build the answer and send it from a
    /// spawned task if sending takes time.
    pub fn reject_with<F>(mut self, hook: F) -> Self
    where
        F: Fn(Task<Args>) + Send + Sync + 'static,
    {
        self.queue.overflow = OverflowPolicy::Reject(Arc::new(hook));
        self
    }

    /// Takes `permits` permits of the monitor's pool `name` for every attempt
    /// (rule 2.3.8). The pool must exist in the monitor the queue is
    /// registered with.
    pub fn pool(mut self, name: impl Into<String>, permits: u32) -> Self {
        self.queue.config.pools.insert(name.into(), permits);
        self
    }

    /// When tasks are acknowledged to the source. Default: on accept.
    pub fn ack_point(mut self, point: AckPoint) -> Self {
        self.queue.config.ack_point = point;
        self
    }

    /// How long to sleep after an empty poll. Default: a wake-up from the
    /// source or growing pauses of 100 ms to 30 s.
    pub fn poll_strategy(mut self, strategy: PollStrategy) -> Self {
        self.queue.config.poll = strategy;
        self
    }

    /// How tasks answering "retry" are retried. Default: no retries.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.queue.config.retry = policy;
        self
    }

    /// How long a cancelled task may take to finish before it is aborted.
    /// Default 30 s.
    pub fn cancel_grace(mut self, grace: Duration) -> Self {
        self.queue.config.cancel_grace = grace;
        self
    }

    /// Limits every attempt to `timeout`, counted from its start (rule
    /// 2.3.16). On expiry the task's cancel flag is set, and after the cancel
    /// grace the attempt is aborted. Default: no limit.
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.queue.config.attempt_timeout = Some(timeout);
        self
    }

    /// What a timed-out attempt ends with. Default: abort.
    pub fn timeout_outcome(mut self, outcome: TimeoutOutcome) -> Self {
        self.queue.config.timeout_outcome = outcome;
        self
    }

    /// Called once when the monitor starts, before the first poll (rule
    /// 2.3.18). The tasks it returns are accepted — never rejected, waiting
    /// for a slot if needed — and are not acknowledged to the source. An error
    /// keeps the monitor from starting.
    ///
    /// Required when the queue acks on accept and the source is not a task
    /// store: those tasks are acknowledged before they finish, so after a
    /// crash only the consumer's own records can bring them back.
    pub fn recover_with<F, Fut, E>(mut self, hook: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Vec<Task<Args>>, E>> + Send + 'static,
        E: Into<BoxError>,
    {
        self.queue.recovery = Some(Box::new(move || {
            Box::pin(async move { hook().await.map_err(Into::into) })
        }));
        self
    }

    /// Declares that tasks accepted and acknowledged before a crash may be
    /// lost: a recovery hook that returns nothing. For queues whose tasks
    /// are cheap to lose or are sent again by their producer.
    pub fn no_recovery(self) -> Self
    where
        Args: 'static,
    {
        self.recover_with(|| async { Ok::<_, BoxError>(Vec::new()) })
    }

    /// The registry that names metadata types for this queue's codec and
    /// handlers.
    pub fn metadata_registry(mut self, registry: MetadataRegistry) -> Self {
        self.queue.registry = Arc::new(registry);
        self
    }

    /// Values shared by every task of the queue, handed out as
    /// [`Data<T>`](crate::Data).
    pub fn shared_data(mut self, shared: SharedData) -> Self {
        self.queue.shared = Arc::new(shared);
        self
    }

    /// Called with every message that cannot be decoded, before it is
    /// acknowledged. Without a hook such messages are logged and acknowledged.
    pub fn dead_letter<F>(mut self, hook: F) -> Self
    where
        B::Message: Clone,
        F: Fn(DeadLetter<B::Message>) + Send + Sync + 'static,
    {
        self.queue.dead_letter = Some(DeadLetterHook {
            hook: Arc::new(hook),
            copy: Clone::clone,
        });
        self
    }

    /// Called with every task that ends failed or panicked — aborted,
    /// out of attempts, timed out, panicked — before its outcome is logged
    /// and acknowledged (rule 2.3.26): send it to an error topic or a table
    /// to look into. A cancelled task is not a failure.
    ///
    /// The hook runs in the task's place, holding its slot: hand slow work
    /// to a spawned task. A panic of the hook is logged and changes nothing.
    pub fn failed_task<F>(mut self, hook: F) -> Self
    where
        F: Fn(FailedTask<Args>) + Send + Sync + 'static,
    {
        self.queue.failed = Some(Arc::new(hook));
        self
    }

    /// Checks the settings (spec 2.10) and returns the queue.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for an empty name, a concurrency limit below 1, an
    /// invalid poll strategy or retry policy, a zero cancel grace or attempt
    /// timeout, or ack on accept without a recovery hook on a source that is
    /// not a task store.
    pub fn build(mut self) -> Result<Queue<B, C, Svc, Args>, ConfigError> {
        if let Some(bind) = self.bind_codec {
            self.queue.codec = Arc::new(bind(&self.queue.registry));
        }
        let config = &self.queue.config;
        if config.name.is_empty() {
            return Err(ConfigError::EmptyQueueName);
        }
        if config.concurrency == 0 {
            return Err(ConfigError::InvalidConcurrency);
        }
        config.poll.validate()?;
        config.retry.validate()?;
        if config.cancel_grace.is_zero() || config.attempt_timeout.is_some_and(|t| t.is_zero()) {
            return Err(ConfigError::InvalidDuration {
                reason: "duration must be positive",
            });
        }
        let store = self.queue.source.capabilities().ack_point_support() == AckPointSupport::Fixed;
        if config.ack_point == AckPoint::OnAccept && !store && self.queue.recovery.is_none() {
            return Err(ConfigError::RecoveryRequired);
        }
        Ok(self.queue)
    }
}

impl<B: Backend, C, Svc, Args> fmt::Debug for QueueBuilder<B, C, Svc, Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueBuilder")
            .field("queue", &self.queue)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::IdentityCodec;
    use crate::handler::task_fn;
    use crate::memory::InMemorySource;

    async fn noop(_: u32) {}

    macro_rules! queue {
        ($name:expr) => {
            Queue::builder(
                $name,
                Arc::new(InMemorySource::<u32>::new(1).unwrap()),
                IdentityCodec::new(),
                task_fn(noop),
            )
            .no_recovery()
        };
    }

    fn error<T>(result: Result<T, ConfigError>) -> String {
        result.err().map(|e| e.to_string()).unwrap_or_default()
    }

    #[test]
    fn settings_are_checked() {
        assert_eq!(error(queue!("").build()), "queue name must not be empty");
        assert_eq!(
            error(queue!("q").concurrency(0).build()),
            "concurrency must be at least 1"
        );
        assert_eq!(
            error(queue!("q").cancel_grace(Duration::ZERO).build()),
            "invalid duration: duration must be positive"
        );
        assert_eq!(
            error(
                queue!("q")
                    .poll_strategy(PollStrategy::FirstOf(vec![]))
                    .build()
            ),
            "invalid poll strategy: composition must not be empty"
        );
        assert_eq!(
            error(queue!("q").attempt_timeout(Duration::ZERO).build()),
            "invalid duration: duration must be positive"
        );
        let unrecoverable = Queue::builder(
            "q",
            Arc::new(InMemorySource::<u32>::new(1).unwrap()),
            IdentityCodec::new(),
            task_fn(noop),
        );
        assert_eq!(
            error(unrecoverable.build()),
            "ack-on-accept without a store requires a recovery hook"
        );
        let on_completion = Queue::builder(
            "q",
            Arc::new(InMemorySource::<u32>::new(1).unwrap()),
            IdentityCodec::new(),
            task_fn(noop),
        )
        .ack_point(AckPoint::OnCompletion);
        assert!(on_completion.build().is_ok());
        let queue = queue!("reports").concurrency(4).build().unwrap();
        assert_eq!((queue.name(), queue.config.concurrency), ("reports", 4));
    }
}
