//! The monitor: owns the workers of a process, starts and stops them, and
//! reports how shutdown went (spec 2.5, 2.7.6).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::codec::Codec;
use crate::error::ConfigError;
use crate::handler::{BoxFuture, TaskRequest};
use crate::outcome::{BoxError, Outcome};
use crate::queue::Queue;
use crate::source::{CloseReason, Source};
use crate::worker::{PoolClaim, run_worker};

/// Why a queue's worker stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StopReason {
    /// The shutdown signal.
    Shutdown,
    /// The source closed.
    SourceClosed(CloseReason),
    /// The worker itself failed; a bug in the library.
    Failed(String),
}

impl fmt::Display for StopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => f.write_str("shutdown signal"),
            Self::SourceClosed(reason) => write!(f, "source closed: {reason}"),
            Self::Failed(reason) => write!(f, "worker failed: {reason}"),
        }
    }
}

/// How one queue stopped (spec 2.7.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueReport {
    /// The queue name.
    pub queue: String,
    /// Why its worker stopped.
    pub reason: StopReason,
    /// Running tasks that reached a final state before the shutdown timeout.
    pub completed: u32,
    /// Tasks cancelled by shutdown.
    pub cancelled: u32,
    /// The part of `cancelled` aborted after the cancel grace ran out.
    pub aborted: u32,
}

/// How the monitor stopped: one report per queue.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Per-queue reports, in registration order.
    pub queues: Vec<QueueReport>,
}

impl ShutdownReport {
    /// Completed tasks over all queues.
    #[must_use]
    pub fn completed(&self) -> u32 {
        self.queues.iter().map(|q| q.completed).sum()
    }

    /// Cancelled tasks over all queues.
    #[must_use]
    pub fn cancelled(&self) -> u32 {
        self.queues.iter().map(|q| q.cancelled).sum()
    }

    /// Aborted tasks over all queues.
    #[must_use]
    pub fn aborted(&self) -> u32 {
        self.queues.iter().map(|q| q.aborted).sum()
    }
}

/// Delays between restarts of an intake loop after source errors.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RestartDelays {
    initial: Duration,
    max: Duration,
}

impl RestartDelays {
    /// The delay before the restart after `failures` errors in a row (≥ 1).
    pub(crate) fn delay(self, failures: u32) -> Duration {
        let exponent = failures.saturating_sub(1).min(31);
        self.initial.saturating_mul(1 << exponent).min(self.max)
    }
}

/// What the monitor hands every worker.
#[derive(Debug, Clone)]
pub(crate) struct WorkerContext {
    pub(crate) stop: CancellationToken,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) restart: RestartDelays,
}

type WorkerFn = Box<dyn FnOnce(WorkerContext) -> BoxFuture<'static, QueueReport> + Send>;

/// Owns the workers of a process: one per registered queue.
///
/// ```no_run
/// use std::sync::Arc;
/// use taskcraft::{CancellationToken, IdentityCodec, InMemorySource, Monitor, Queue, task_fn};
/// # async fn example() -> Result<(), taskcraft::ConfigError> {
/// async fn send_report(month: String) {}
///
/// let source = Arc::new(InMemorySource::default());
/// let queue = Queue::builder("reports", source, IdentityCodec::new(), task_fn(send_report))
///     .concurrency(4)
///     .build()?;
/// let stop = CancellationToken::new();
/// let report = Monitor::new().register(queue)?.run(stop).await;
/// # let _ = report;
/// # Ok(()) }
/// ```
pub struct Monitor {
    shutdown_timeout: Duration,
    restart: RestartDelays,
    names: HashSet<String>,
    pools: HashMap<String, (u32, Arc<Semaphore>)>,
    workers: Vec<WorkerFn>,
}

impl Monitor {
    /// A monitor with the default settings: shutdown timeout 30 s, restart
    /// delays from 1 s to 60 s (spec 2.8).
    #[must_use]
    pub fn new() -> Self {
        Self {
            shutdown_timeout: Duration::from_secs(30),
            restart: RestartDelays {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(60),
            },
            names: HashSet::new(),
            pools: HashMap::new(),
            workers: Vec::new(),
        }
    }

    /// How long running tasks get to finish after the shutdown signal before
    /// they are cancelled. Zero cancels them at once.
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// The delay before the first restart of an intake loop after a source
    /// error, and the longest delay; it doubles in between.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidDuration`] when `initial` is zero or above `max`.
    pub fn restart_delays(mut self, initial: Duration, max: Duration) -> Result<Self, ConfigError> {
        if initial.is_zero() {
            return Err(ConfigError::InvalidDuration {
                reason: "duration must be positive",
            });
        }
        if initial > max {
            return Err(ConfigError::InvalidDuration {
                reason: "restart delay exceeds max",
            });
        }
        self.restart = RestartDelays { initial, max };
        Ok(self)
    }

    /// Declares a resource pool shared by every queue of this monitor
    /// (rule 2.3.8). Declare pools before registering queues that use them.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidPool`] for size 0 or a name declared twice.
    pub fn pool(mut self, name: impl Into<String>, size: u32) -> Result<Self, ConfigError> {
        let name = name.into();
        if size == 0 {
            return Err(ConfigError::InvalidPool {
                name,
                reason: "pool size must be at least 1",
            });
        }
        if self.pools.contains_key(&name) {
            return Err(ConfigError::InvalidPool {
                name,
                reason: "duplicate pool name",
            });
        }
        let semaphore = Arc::new(Semaphore::new(size as usize));
        self.pools.insert(name, (size, semaphore));
        Ok(self)
    }

    /// Registers a queue.
    ///
    /// # Errors
    ///
    /// [`ConfigError::DuplicateQueueName`] when the monitor already has a
    /// queue with this name; [`ConfigError::UnknownPool`],
    /// [`ConfigError::PermitsExceedPool`] or [`ConfigError::InvalidPool`]
    /// when the queue requires a pool the monitor does not have, more permits
    /// than the pool holds, or zero permits.
    pub fn register<S, C, Svc, Args>(
        mut self,
        queue: Queue<S, C, Svc, Args>,
    ) -> Result<Self, ConfigError>
    where
        S: Source,
        C: Codec<Args, S::Message>,
        Svc: tower::Service<TaskRequest<Args>, Response = Outcome> + Clone + Send + 'static,
        Svc::Error: Into<BoxError>,
        Svc::Future: Send,
        Args: Clone + Send + 'static,
    {
        let name = queue.name().to_owned();
        if self.names.contains(&name) {
            return Err(ConfigError::DuplicateQueueName { name });
        }
        let mut claims = Vec::new();
        for (pool, &permits) in &queue.config.pools {
            let Some((size, semaphore)) = self.pools.get(pool) else {
                return Err(ConfigError::UnknownPool { name: pool.clone() });
            };
            if permits == 0 {
                return Err(ConfigError::InvalidPool {
                    name: pool.clone(),
                    reason: "permits must be at least 1",
                });
            }
            if permits > *size {
                return Err(ConfigError::PermitsExceedPool { name: pool.clone() });
            }
            claims.push(PoolClaim {
                semaphore: Arc::clone(semaphore),
                permits,
            });
        }
        self.names.insert(name);
        self.workers.push(Box::new(move |ctx| {
            Box::pin(run_worker(queue, claims, ctx))
        }));
        Ok(self)
    }

    /// Runs every registered queue until `stop` fires or every source
    /// closes, and reports how each one stopped.
    pub async fn run(self, stop: CancellationToken) -> ShutdownReport {
        let ctx = WorkerContext {
            stop: stop.clone(),
            shutdown_timeout: self.shutdown_timeout,
            restart: self.restart,
        };
        let mut workers = JoinSet::new();
        for (index, worker) in self.workers.into_iter().enumerate() {
            let ctx = ctx.clone();
            workers.spawn(async move { (index, worker(ctx).await) });
        }
        let mut reports = Vec::new();
        let mut shutting_down = false;
        loop {
            tokio::select! {
                joined = workers.join_next() => match joined {
                    Some(Ok(report)) => reports.push(report),
                    Some(Err(e)) => error!(
                        event = "worker",
                        action = "failed",
                        "worker failed: error={:?}",
                        e.to_string()
                    ),
                    None => break,
                },
                () = stop.cancelled(), if !shutting_down => {
                    shutting_down = true;
                    info!(event = "shutdown", action = "started", "shutdown started");
                }
            }
        }
        reports.sort_by_key(|(index, _)| *index);
        let report = ShutdownReport {
            queues: reports.into_iter().map(|(_, r)| r).collect(),
        };
        if shutting_down {
            info!(
                event = "shutdown",
                action = "finished",
                "shutdown finished: completed={}, cancelled={}, aborted={}",
                report.completed(),
                report.cancelled(),
                report.aborted()
            );
        }
        report
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Monitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Monitor")
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("restart", &self.restart)
            .field("queues", &self.names)
            .field("pools", &self.pools.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::codec::IdentityCodec;
    use crate::handler::task_fn;
    use crate::memory::InMemorySource;

    async fn noop(_: u32) {}

    #[test]
    fn restart_delay_doubles_up_to_max() {
        let d = RestartDelays {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
        };
        let delays: Vec<_> = (1..=8).map(|n| d.delay(n).as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60]);
        assert_eq!(d.delay(u32::MAX), Duration::from_secs(60));
    }

    #[test]
    fn restart_delays_are_checked() {
        let err = Monitor::new()
            .restart_delays(Duration::from_secs(5), Duration::from_secs(1))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid duration: restart delay exceeds max"
        );
        assert!(
            Monitor::new()
                .restart_delays(Duration::ZERO, Duration::from_secs(1))
                .is_err()
        );
    }

    #[test]
    fn queue_names_are_unique() {
        let queue = || {
            Queue::builder(
                "reports",
                Arc::new(InMemorySource::<u32>::new(1)),
                IdentityCodec::new(),
                task_fn(noop),
            )
            .build()
            .unwrap()
        };
        let err = Monitor::new()
            .register(queue())
            .unwrap()
            .register(queue())
            .unwrap_err();
        assert_eq!(err.to_string(), "duplicate queue name: reports");
    }
}
