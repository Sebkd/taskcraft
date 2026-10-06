//! The store: connection, schema, process mark, lease renewal and cleanup.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use taskcraft::{ConfigError, Notice, TaskId};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::source::PgSource;

// In the queries below, a process holds tasks in 'accepted', 'running' and
// 'retry_waiting' until it finishes them; 'succeeded', 'failed', 'panicked'
// and 'cancelled' are final and kept for the retention period.

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS taskcraft_tasks (
    queue            text        NOT NULL,
    id               text        NOT NULL,
    task             jsonb       NOT NULL,
    state            text        NOT NULL,
    attempt          integer     NOT NULL DEFAULT 0,
    retries          integer     NOT NULL DEFAULT 0,
    cancel_requested boolean     NOT NULL DEFAULT false,
    next_delivery    timestamptz,
    owner            text,
    lease_until      timestamptz,
    reason           jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue, id)
);
CREATE INDEX IF NOT EXISTS taskcraft_tasks_ready
    ON taskcraft_tasks (queue, state, next_delivery, created_at);
CREATE INDEX IF NOT EXISTS taskcraft_tasks_owner
    ON taskcraft_tasks (owner) WHERE owner IS NOT NULL;
CREATE TABLE IF NOT EXISTS taskcraft_processes (
    id      text        PRIMARY KEY,
    seen_at timestamptz NOT NULL
);
";

/// Serialises schema creation between processes starting together.
const SCHEMA_LOCK: i64 = 0x7461_736b_6372_6166;

/// What the store can fail with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PgStoreError {
    /// A setting is out of range (spec 2.10).
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The database failed or is unreachable.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// The process id is empty.
    #[error("process id must not be empty")]
    EmptyProcessId,
    /// A live process already uses this process id (rule 2.3.19 p. 5).
    #[error("process id is taken: {0}")]
    ProcessIdTaken(String),
    /// A stored task or status could not be read.
    #[error("stored data could not be read: {0}")]
    Data(String),
}

/// Leases: any process may take a task whose owner stopped renewing it
/// (rule 2.3.20).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    /// How long a claim lasts without renewal. Default 60 s.
    pub duration: Duration,
    /// How often the owner renews it; strictly below half the duration.
    /// Default 15 s.
    pub heartbeat: Duration,
}

impl Default for Lease {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(60),
            heartbeat: Duration::from_secs(15),
        }
    }
}

/// Builds a [`PgStore`].
#[derive(Debug, Clone)]
#[must_use]
pub struct PgStoreBuilder {
    process_id: String,
    lease: Option<Lease>,
    retention: Duration,
    alive_interval: Duration,
}

impl PgStoreBuilder {
    /// Takes tasks over from processes that stopped renewing their leases.
    /// Without leases a task stays with the process that took it until that
    /// process, restarted with the same id, gives it back.
    pub fn lease(mut self, lease: Lease) -> Self {
        self.lease = Some(lease);
        self
    }

    /// How long finished tasks are kept: a push with their id answers
    /// "already finished" meanwhile. Default 7 days.
    pub fn retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    /// How often the process marks itself alive. Default 10 s.
    pub fn alive_interval(mut self, interval: Duration) -> Self {
        self.alive_interval = interval;
        self
    }

    fn validate(&self) -> Result<(), PgStoreError> {
        let positive = [self.retention, self.alive_interval]
            .into_iter()
            .chain(self.lease.iter().flat_map(|l| [l.duration, l.heartbeat]));
        for duration in positive {
            if duration.is_zero() {
                return Err(ConfigError::InvalidDuration {
                    reason: "duration must be positive",
                }
                .into());
            }
        }
        if self.process_id.is_empty() {
            return Err(PgStoreError::EmptyProcessId);
        }
        if let Some(lease) = self.lease
            && lease.heartbeat * 2 >= lease.duration
        {
            return Err(ConfigError::InvalidDuration {
                reason: "heartbeat interval must be below half the lease duration",
            }
            .into());
        }
        Ok(())
    }

    /// Connects with a connection string such as
    /// `postgres://user:password@host:5432/db`.
    ///
    /// # Errors
    ///
    /// As [`with_pool`](Self::with_pool), and the connection failing.
    pub async fn connect(self, url: &str) -> Result<PgStore, PgStoreError> {
        self.validate()?;
        let pool = PgPoolOptions::new().connect(url).await?;
        self.with_pool(pool).await
    }

    /// Uses the application's pool, for example the one of a sea-orm
    /// connection (`DatabaseConnection::get_postgres_connection_pool`).
    ///
    /// Creates the tables if needed, checks that no live process uses this
    /// process id, gives back to the queues the tasks a previous run of this
    /// process left unfinished (rule 2.3.19 p. 1), and starts marking the
    /// process alive, renewing leases and removing expired finished tasks.
    ///
    /// # Errors
    ///
    /// [`PgStoreError::Config`] for invalid settings,
    /// [`PgStoreError::ProcessIdTaken`] when a live process uses the id, and
    /// [`PgStoreError::Database`] when the database fails.
    pub async fn with_pool(self, pool: PgPool) -> Result<PgStore, PgStoreError> {
        self.validate()?;
        create_schema(&pool).await?;
        claim_process_id(&pool, &self.process_id, self.alive_interval).await?;
        give_back(&pool, &self.process_id).await?;
        let shared = Arc::new(Shared {
            pool,
            process_id: self.process_id,
            lease: self.lease,
            retention: self.retention,
            queues: Mutex::default(),
            upkeep: Mutex::default(),
        });
        let tick = self.lease.map_or(self.alive_interval, |l| {
            l.heartbeat.min(self.alive_interval)
        });
        let handle = tokio::spawn(upkeep(Arc::downgrade(&shared), tick));
        *lock(&shared.upkeep) = Some(handle);
        Ok(PgStore { shared })
    }
}

/// A task store in PostgreSQL (spec 2.6, 2.7.5): the source of queues that
/// keep their tasks outside the process.
///
/// One store per process; [`queue`](Self::queue) gives the source of one
/// queue. Pair it with [`JsonCodec`](taskcraft::JsonCodec).
#[derive(Clone)]
pub struct PgStore {
    shared: Arc<Shared>,
}

impl PgStore {
    /// Starts building a store for the process `process_id`: stable across
    /// restarts and unique among live processes (rule 2.3.19).
    pub fn builder(process_id: impl Into<String>) -> PgStoreBuilder {
        PgStoreBuilder {
            process_id: process_id.into(),
            lease: None,
            retention: Duration::from_secs(7 * 24 * 3600),
            alive_interval: Duration::from_secs(10),
        }
    }

    /// The source of queue `name`; give the queue the same name.
    #[must_use]
    pub fn queue(&self, name: impl Into<String>) -> PgSource {
        let name: Arc<str> = name.into().into();
        let (notify, notices) = taskcraft::Notices::channel();
        let held = Arc::new(Held {
            tasks: Mutex::default(),
            notify,
        });
        lock(&self.shared.queues).insert(Arc::clone(&name), Arc::clone(&held));
        PgSource::new(Arc::clone(&self.shared), name, held, notices)
    }

    /// The process id.
    #[must_use]
    pub fn process_id(&self) -> &str {
        &self.shared.process_id
    }

    /// The pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.shared.pool
    }
}

impl fmt::Debug for PgStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgStore")
            .field("process_id", &self.shared.process_id)
            .field("lease", &self.shared.lease)
            .field("retention", &self.shared.retention)
            .finish_non_exhaustive()
    }
}

/// Tasks a queue of this process holds, and where to report about them.
#[derive(Debug)]
pub(crate) struct Held {
    /// Task id → a cancel request was already reported.
    pub(crate) tasks: Mutex<HashMap<TaskId, bool>>,
    pub(crate) notify: mpsc::UnboundedSender<Notice>,
}

#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) pool: PgPool,
    pub(crate) process_id: String,
    pub(crate) lease: Option<Lease>,
    retention: Duration,
    queues: Mutex<HashMap<Arc<str>, Arc<Held>>>,
    upkeep: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        if let Some(handle) = lock(&self.upkeep).take() {
            handle.abort();
        }
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section is a few infallible map calls.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn create_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SCHEMA_LOCK)
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(SCHEMA).execute(&mut *tx).await?;
    tx.commit().await
}

/// Refuses an id marked alive within two intervals, then marks it (rule
/// 2.3.19 p. 5).
async fn claim_process_id(
    pool: &PgPool,
    process_id: &str,
    alive_interval: Duration,
) -> Result<(), PgStoreError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind(process_id)
        .execute(&mut *tx)
        .await?;
    let alive: Option<bool> = sqlx::query_scalar(
        "SELECT seen_at > now() - make_interval(secs => $2) FROM taskcraft_processes WHERE id = $1",
    )
    .bind(process_id)
    .bind(2.0 * alive_interval.as_secs_f64())
    .fetch_optional(&mut *tx)
    .await?;
    if alive == Some(true) {
        return Err(PgStoreError::ProcessIdTaken(process_id.to_owned()));
    }
    sqlx::query(
        "INSERT INTO taskcraft_processes (id, seen_at) VALUES ($1, now())
         ON CONFLICT (id) DO UPDATE SET seen_at = now()",
    )
    .bind(process_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Tasks this process held when it stopped go back to their queues with
/// their attempt counts (rule 2.3.19 p. 1).
async fn give_back(pool: &PgPool, process_id: &str) -> Result<(), sqlx::Error> {
    let queues: Vec<(String, i64)> = sqlx::query_as(
        "WITH back AS (
             UPDATE taskcraft_tasks
                SET state = 'queued', owner = NULL, lease_until = NULL, updated_at = now()
              WHERE owner = $1 AND state IN ('accepted', 'running', 'retry_waiting')
          RETURNING queue)
         SELECT queue, count(*) FROM back GROUP BY queue",
    )
    .bind(process_id)
    .fetch_all(pool)
    .await?;
    for (queue, count) in queues {
        info!(
            event = "recovery",
            action = "recovered",
            "tasks recovered: queue={}, count={}",
            queue,
            count
        );
    }
    Ok(())
}

/// Marks the process alive, renews leases, reports cancel requests and lost
/// leases, and removes finished tasks past retention — every `tick`.
async fn upkeep(shared: Weak<Shared>, tick: Duration) {
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let Some(shared) = shared.upgrade() else {
            return;
        };
        if let Err(error) = upkeep_once(&shared).await {
            warn!(
                event = "source",
                action = "store_error",
                "task store upkeep failed: process_id={}, error={:?}",
                shared.process_id,
                error.to_string()
            );
        }
    }
}

async fn upkeep_once(shared: &Shared) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE taskcraft_processes SET seen_at = now() WHERE id = $1")
        .bind(&shared.process_id)
        .execute(&shared.pool)
        .await?;
    let queues: Vec<(Arc<str>, Arc<Held>)> = lock(&shared.queues)
        .iter()
        .map(|(q, h)| (Arc::clone(q), Arc::clone(h)))
        .collect();
    for (queue, held) in queues {
        let ids: Vec<String> = lock(&held.tasks)
            .keys()
            .map(|id| id.as_str().to_owned())
            .collect();
        if ids.is_empty() {
            continue;
        }
        let rows: Vec<(String, bool)> = match shared.lease {
            Some(lease) => {
                sqlx::query_as(
                    "UPDATE taskcraft_tasks
                        SET lease_until = now() + make_interval(secs => $4)
                      WHERE queue = $1 AND id = ANY($2) AND owner = $3
                  RETURNING id, cancel_requested",
                )
                .bind(&*queue)
                .bind(&ids)
                .bind(&shared.process_id)
                .bind(lease.duration.as_secs_f64())
                .fetch_all(&shared.pool)
                .await?
            }
            None => {
                sqlx::query_as(
                    "SELECT id, cancel_requested FROM taskcraft_tasks
                      WHERE queue = $1 AND id = ANY($2) AND owner = $3",
                )
                .bind(&*queue)
                .bind(&ids)
                .bind(&shared.process_id)
                .fetch_all(&shared.pool)
                .await?
            }
        };
        report(&held, &ids, &rows, shared.lease.is_some());
    }
    sqlx::query("DELETE FROM taskcraft_tasks
          WHERE state IN ('succeeded', 'failed', 'panicked', 'cancelled') AND updated_at < now() - make_interval(secs => $1)")
    .bind(shared.retention.as_secs_f64())
    .execute(&shared.pool)
    .await?;
    Ok(())
}

/// Turns one renewal into notices: tasks no longer ours lost their lease,
/// new cancel requests are reported once.
fn report(held: &Held, ids: &[String], rows: &[(String, bool)], leases: bool) {
    let ours: HashMap<&str, bool> = rows.iter().map(|(id, c)| (id.as_str(), *c)).collect();
    let mut tasks = lock(&held.tasks);
    for id in ids {
        let task_id = TaskId::new(id.as_str());
        match ours.get(id.as_str()) {
            None if leases => {
                tasks.remove(&task_id);
                let _ = held.notify.send(Notice::LeaseLost(task_id));
            }
            Some(true) => {
                if let Some(reported) = tasks.get_mut(&task_id)
                    && !*reported
                {
                    *reported = true;
                    let _ = held.notify.send(Notice::CancelRequested(task_id));
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn builder() -> PgStoreBuilder {
        PgStore::builder("p")
    }

    #[test]
    fn settings_are_checked() {
        assert!(builder().validate().is_ok());
        assert!(builder().lease(Lease::default()).validate().is_ok());
        let slow = Lease {
            duration: Duration::from_secs(60),
            heartbeat: Duration::from_secs(30),
        };
        let error = builder().lease(slow).validate().unwrap_err();
        assert!(error.to_string().contains("below half the lease duration"));
        assert!(builder().retention(Duration::ZERO).validate().is_err());
        assert!(matches!(
            PgStore::builder("").validate(),
            Err(PgStoreError::EmptyProcessId)
        ));
    }

    fn held(ids: &[&str]) -> (Held, mpsc::UnboundedReceiver<Notice>) {
        let (notify, receiver) = mpsc::unbounded_channel();
        let tasks = ids.iter().map(|id| (TaskId::new(*id), false)).collect();
        let held = Held {
            tasks: Mutex::new(tasks),
            notify,
        };
        (held, receiver)
    }

    fn drain(receiver: &mut mpsc::UnboundedReceiver<Notice>) -> Vec<Notice> {
        std::iter::from_fn(|| receiver.try_recv().ok()).collect()
    }

    #[test]
    fn renewal_reports_lost_leases_and_cancel_requests_once() {
        let (held, mut notices) = held(&["kept", "lost", "cancel"]);
        let ids = ["kept", "lost", "cancel"].map(str::to_owned);
        let rows = [("kept".to_owned(), false), ("cancel".to_owned(), true)];
        report(&held, &ids, &rows, true);
        let mut seen = drain(&mut notices);
        seen.sort_by_key(|n| format!("{n:?}"));
        assert_eq!(
            seen,
            [
                Notice::CancelRequested(TaskId::new("cancel")),
                Notice::LeaseLost(TaskId::new("lost")),
            ]
        );
        assert!(!lock(&held.tasks).contains_key(&TaskId::new("lost")));

        report(&held, &ids[..1], &rows, true);
        report(&held, &["cancel".to_owned()], &rows[1..], true);
        assert!(drain(&mut notices).is_empty(), "reported once");
    }

    #[test]
    fn without_leases_nothing_is_lost() {
        let (held, mut notices) = held(&["mine"]);
        report(&held, &["mine".to_owned()], &[], false);
        assert!(drain(&mut notices).is_empty());
    }
}
