//! The store: connection, schema, process mark, lease renewal and cleanup.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use sqlx::postgres::{PgListener, PgPoolOptions};
use sqlx::{PgConnection, PgPool};
use taskcraft::TaskId;
use taskcraft::error::ConfigError;
use taskcraft::source::{Notice, WakeHandle};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::source::PgSource;
use crate::sql::{CLEANUP_BATCH, MIGRATIONS, SCHEMA_LOCK};

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
    /// The cleanup batch is zero.
    #[error("cleanup batch must be at least 1")]
    InvalidCleanupBatch,
    /// The process id is empty.
    #[error("task store requires a process id")]
    EmptyProcessId,
    /// A live process already uses this process id (rule 2.3.19 p. 5).
    #[error("process id is taken: {0}")]
    ProcessIdTaken(String),
    /// A stored task or status could not be read.
    #[error("stored data could not be read: {0}")]
    Data(String),
    /// The database has a newer schema than this library supports: a
    /// process of a newer version migrated it. Update every process.
    #[error("task store schema is newer than this library: found={found}, supported={supported}")]
    SchemaTooNew {
        /// The schema version in the database.
        found: i32,
        /// The newest version this library knows.
        supported: i32,
    },
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
    cleanup_interval: Duration,
    cleanup_batch: u32,
    stale_owner_warning: Duration,
    notifications: bool,
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

    /// How often finished tasks past retention are removed. Default 60 s.
    pub fn cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// How many finished tasks one cleanup statement removes; batches repeat
    /// while full, each in a short transaction. Default 1000.
    pub fn cleanup_batch(mut self, batch: u32) -> Self {
        self.cleanup_batch = batch;
        self
    }

    /// Wakes the workers of every process at once when work appears in
    /// their queue — a push, a requeue, tasks given back — through
    /// `LISTEN`/`NOTIFY` on one more connection; the poll strategy stays a
    /// safety net. Default on. Turn it off behind a connection pooler in
    /// transaction mode, where `LISTEN` does not work: workers then see the
    /// work of other processes on their next poll.
    pub fn notifications(mut self, on: bool) -> Self {
        self.notifications = on;
        self
    }

    /// Without leases, how long a process may stay silent — no "alive" mark
    /// — while holding tasks before a starting process warns about it
    /// (`recovery/stale_owner`): its tasks wait for it to restart, or for
    /// [`PgStore::release_process`]. Default 1 h.
    pub fn stale_owner_warning(mut self, threshold: Duration) -> Self {
        self.stale_owner_warning = threshold;
        self
    }

    fn validate(&self) -> Result<(), PgStoreError> {
        let positive = [
            self.retention,
            self.alive_interval,
            self.cleanup_interval,
            self.stale_owner_warning,
        ]
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
        if self.cleanup_batch == 0 {
            return Err(PgStoreError::InvalidCleanupBatch);
        }
        if self.process_id.is_empty() {
            return Err(PgStoreError::EmptyProcessId);
        }
        if let Some(lease) = self.lease
            && lease.heartbeat * 2 >= lease.duration
        {
            return Err(ConfigError::InvalidDuration {
                reason: "heartbeat interval must be less than half the lease",
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
    /// Creates or migrates the schema (one process at a time; the first
    /// start on an older base builds indexes, which holds writes to the
    /// tasks table meanwhile), checks that no live process uses this
    /// process id, gives back to the queues the tasks a previous run of this
    /// process left unfinished (rule 2.3.19 p. 1), and starts marking the
    /// process alive, renewing leases and removing expired finished tasks.
    ///
    /// # Errors
    ///
    /// [`PgStoreError::Config`] for invalid settings,
    /// [`PgStoreError::SchemaTooNew`] when a newer version migrated the
    /// database, [`PgStoreError::ProcessIdTaken`] when a live process uses
    /// the id, and
    /// [`PgStoreError::Database`] when the database fails.
    pub async fn with_pool(self, pool: PgPool) -> Result<PgStore, PgStoreError> {
        self.validate()?;
        migrate(&pool).await?;
        claim_process_id(&pool, &self.process_id, self.alive_interval).await?;
        give_back(&pool, &self.process_id, self.notifications).await?;
        if self.lease.is_none() {
            warn_stale_owners(&pool, &self.process_id, self.stale_owner_warning).await?;
        }
        let shared = Arc::new(Shared {
            pool,
            process_id: self.process_id,
            lease: self.lease,
            retention: self.retention,
            alive_interval: self.alive_interval,
            cleanup_interval: self.cleanup_interval,
            cleanup_batch: self.cleanup_batch,
            notifications: self.notifications,
            queues: Mutex::default(),
            upkeep: Mutex::default(),
        });
        let tick = self.lease.map_or(self.alive_interval, |l| {
            l.heartbeat.min(self.alive_interval)
        });
        let mut tasks = vec![tokio::spawn(upkeep(Arc::downgrade(&shared), tick))];
        if self.notifications {
            let mut listener = PgListener::connect_with(&shared.pool).await?;
            listener.listen(CHANNEL).await?;
            tasks.push(tokio::spawn(listen(Arc::downgrade(&shared), listener)));
        }
        *lock(&shared.upkeep) = tasks;
        Ok(PgStore { shared })
    }
}

/// A task store in PostgreSQL (spec 2.6, 2.7.5): the source of queues that
/// keep their tasks outside the process.
///
/// One store per process; [`queue`](Self::queue) gives the source of one
/// queue; build the queue on it with [`Queue::on_store`](taskcraft::Queue::on_store).
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
            cleanup_interval: Duration::from_secs(60),
            cleanup_batch: 1000,
            stale_owner_warning: Duration::from_secs(3600),
            notifications: true,
        }
    }

    /// The source of queue `name`; give the queue the same name.
    #[must_use]
    pub fn queue(&self, name: impl Into<String>) -> PgSource {
        let name: Arc<str> = name.into().into();
        let (notify, notices) = taskcraft::source::Notices::channel();
        let held = Arc::new(Held {
            tasks: Mutex::default(),
            notify,
            wake: WakeHandle::new(),
        });
        let mut queues = lock(&self.shared.queues);
        let sources = queues.entry(Arc::clone(&name)).or_default();
        sources.retain(|held| held.strong_count() > 0);
        sources.push(Arc::downgrade(&held));
        drop(queues);
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

    /// Gives the tasks of a retired process back to their queues, attempt
    /// counts kept (rule 2.3.19 p. 6): without leases nothing else takes
    /// them, and the process will not restart to take them back itself.
    /// Returns how many tasks went back.
    ///
    /// A process counts as retired when its "alive" mark is older than two
    /// of this store's alive intervals — the processes of one deployment are
    /// expected to share the interval. Turning leases on makes this call
    /// unnecessary.
    ///
    /// # Errors
    ///
    /// [`PgStoreError::ProcessIdTaken`] when the process is alive — this
    /// process included — and [`PgStoreError::Database`] when the database
    /// fails.
    pub async fn release_process(&self, process_id: &str) -> Result<u64, PgStoreError> {
        let shared = &self.shared;
        if process_id == shared.process_id {
            return Err(PgStoreError::ProcessIdTaken(process_id.to_owned()));
        }
        let mut tx = shared.pool.begin().await?;
        if alive(&mut tx, process_id, shared.alive_interval).await? {
            return Err(PgStoreError::ProcessIdTaken(process_id.to_owned()));
        }
        release(shared, tx, process_id).await
    }

    /// Closes the store when the process stops (rule 2.3.19 p. 7): stops
    /// the upkeep, gives the tasks this process left unfinished back to
    /// their queues, attempt counts kept, and removes its "alive" mark — a
    /// process with the same id starts at once, and other processes take
    /// the tasks without waiting for their leases. Returns how many tasks
    /// went back. Closing again does nothing and returns 0. The pool stays
    /// open: it may be the application's.
    ///
    /// Call it after [`Monitor::run`](taskcraft::Monitor::run) returned:
    /// tasks still running would go back to their queues and run twice.
    /// A crashed process does not close; its tasks wait for its restart or
    /// for their leases, as before.
    ///
    /// # Errors
    ///
    /// [`PgStoreError::Database`] when the database fails; the upkeep is
    /// stopped anyway, and the next start with this id waits for the
    /// "alive" mark to age.
    pub async fn close(&self) -> Result<u64, PgStoreError> {
        let shared = &self.shared;
        for handle in lock(&shared.upkeep).drain(..) {
            handle.abort();
        }
        let mut tx = shared.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(&shared.process_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM taskcraft_processes WHERE id = $1")
            .bind(&shared.process_id)
            .execute(&mut *tx)
            .await?;
        release(shared, tx, &shared.process_id).await
    }
}

/// Gives the unfinished tasks of `owner` back in `tx` and commits, then
/// wakes their queues and logs `recovery/released`; returns how many.
async fn release(
    shared: &Shared,
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
    owner: &str,
) -> Result<u64, PgStoreError> {
    let queues = return_tasks(&mut tx, owner).await?;
    tx.commit().await?;
    for (queue, _) in &queues {
        announce(shared, queue).await;
    }
    let count: i64 = queues.iter().map(|(_, n)| n).sum();
    info!(
        event = "recovery",
        action = "released",
        "tasks released: owner={}, count={}",
        owner,
        count
    );
    Ok(u64::try_from(count).unwrap_or(0))
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

/// Tasks one source of a queue holds, and where to report about them. A
/// queue may have several sources in a process (one running, another for
/// status and pushes): each keeps its own.
#[derive(Debug)]
pub(crate) struct Held {
    /// Task id → a cancel request was already reported.
    pub(crate) tasks: Mutex<HashMap<TaskId, bool>>,
    pub(crate) notify: mpsc::UnboundedSender<Notice>,
    /// Wakes the queue's workers: on a push from this process, and on a
    /// notification from any.
    pub(crate) wake: WakeHandle,
}

#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) pool: PgPool,
    pub(crate) process_id: String,
    pub(crate) lease: Option<Lease>,
    retention: Duration,
    alive_interval: Duration,
    cleanup_interval: Duration,
    cleanup_batch: u32,
    notifications: bool,
    /// Queue name → the bookkeeping of its live sources in this process.
    queues: Mutex<HashMap<Arc<str>, Vec<Weak<Held>>>>,
    /// Upkeep and the notification listener, stopped with the store.
    upkeep: Mutex<Vec<JoinHandle<()>>>,
}

impl Shared {
    /// The live sources of `queue` in this process.
    fn sources(&self, queue: &str) -> Vec<Arc<Held>> {
        lock(&self.queues)
            .get(queue)
            .map(|sources| sources.iter().filter_map(Weak::upgrade).collect())
            .unwrap_or_default()
    }

    /// Every live source in this process, with its queue.
    fn all_sources(&self) -> Vec<(Arc<str>, Arc<Held>)> {
        lock(&self.queues)
            .iter()
            .flat_map(|(queue, sources)| {
                sources
                    .iter()
                    .filter_map(Weak::upgrade)
                    .map(|held| (Arc::clone(queue), held))
            })
            .collect()
    }

    /// Wakes the workers of every source of `queue` in this process.
    pub(crate) fn wake(&self, queue: &str) {
        for held in self.sources(queue) {
            held.wake.wake();
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        for handle in lock(&self.upkeep).drain(..) {
            handle.abort();
        }
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section is a few infallible map calls.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The schema version this library creates and reads.
fn supported_version() -> i32 {
    i32::try_from(MIGRATIONS.len()).unwrap_or(i32::MAX)
}

/// Brings the schema to the supported version in one transaction under the
/// schema lock: a process starting meanwhile waits and finds it done. A base
/// of 0.1 or 0.2 has no version table and counts as version 0, its tables as
/// version 1.
async fn migrate(pool: &PgPool) -> Result<(), PgStoreError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SCHEMA_LOCK)
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql("CREATE TABLE IF NOT EXISTS taskcraft_schema (version integer NOT NULL)")
        .execute(&mut *tx)
        .await?;
    let found: Option<i32> = sqlx::query_scalar("SELECT max(version) FROM taskcraft_schema")
        .fetch_one(&mut *tx)
        .await?;
    let found = found.unwrap_or(0);
    let supported = supported_version();
    if found > supported {
        return Err(PgStoreError::SchemaTooNew { found, supported });
    }
    let done = usize::try_from(found).unwrap_or(0);
    for migration in MIGRATIONS.iter().skip(done) {
        sqlx::raw_sql(*migration).execute(&mut *tx).await?;
    }
    if found < supported {
        sqlx::query("DELETE FROM taskcraft_schema")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO taskcraft_schema (version) VALUES ($1)")
            .bind(supported)
            .execute(&mut *tx)
            .await?;
        info!(
            event = "store",
            action = "migrated",
            "task store schema migrated: from={}, to={}",
            found,
            supported
        );
    }
    tx.commit().await?;
    Ok(())
}

/// Refuses an id marked alive within two intervals, then marks it (rule
/// 2.3.19 p. 5).
async fn claim_process_id(
    pool: &PgPool,
    process_id: &str,
    alive_interval: Duration,
) -> Result<(), PgStoreError> {
    let mut tx = pool.begin().await?;
    if alive(&mut tx, process_id, alive_interval).await? {
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

/// Takes the lock of a process id for the transaction and tells whether the
/// id was marked alive within two intervals (rule 2.3.19 p. 5).
async fn alive(
    tx: &mut PgConnection,
    process_id: &str,
    alive_interval: Duration,
) -> Result<bool, sqlx::Error> {
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
    Ok(alive == Some(true))
}

/// Puts the unfinished tasks of `owner` back into their queues, attempt
/// counts kept; returns how many per queue.
async fn return_tasks(
    conn: &mut PgConnection,
    owner: &str,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as(
        "WITH back AS (
             UPDATE taskcraft_tasks
                SET state = 'queued', owner = NULL, lease_until = NULL, updated_at = now()
              WHERE owner = $1 AND state IN ('accepted', 'running', 'retry_waiting')
          RETURNING queue)
         SELECT queue, count(*) FROM back GROUP BY queue",
    )
    .bind(owner)
    .fetch_all(conn)
    .await
}

/// The channel of the "work appeared in queue <payload>" notifications.
const CHANNEL: &str = "taskcraft_tasks";

/// Tells every process listening that queue `queue` has work now (rule
/// 2.3.25 p. 6). Best effort: polling picks the work up anyway.
pub(crate) async fn announce(shared: &Shared, queue: &str) {
    if shared.notifications {
        let _ = notify(&shared.pool, queue).await;
    }
}

async fn notify(pool: &PgPool, queue: &str) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(CHANNEL)
        .bind(queue)
        .execute(pool)
        .await?;
    Ok(())
}

/// Wakes the workers of the queue named in each notification. A lost
/// connection loses notifications: every queue is woken, and the listener
/// reconnects on the next receive.
async fn listen(shared: Weak<Shared>, mut listener: PgListener) {
    loop {
        let received = listener.try_recv().await;
        let Some(shared) = shared.upgrade() else {
            return;
        };
        let error = match received {
            Ok(Some(notification)) => {
                shared.wake(notification.payload());
                continue;
            }
            Ok(None) => "connection lost".to_owned(),
            Err(sqlx::Error::PoolClosed) => return,
            Err(error) => error.to_string(),
        };
        warn!(
            event = "source",
            action = "notify_lost",
            "task store notifications lost, reconnecting: process_id={}, error={:?}",
            shared.process_id,
            error
        );
        for (_, held) in shared.all_sources() {
            held.wake.wake();
        }
        drop(shared);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Tasks this process held when it stopped go back to their queues with
/// their attempt counts (rule 2.3.19 p. 1).
async fn give_back(
    pool: &PgPool,
    process_id: &str,
    notifications: bool,
) -> Result<(), sqlx::Error> {
    let mut conn = pool.acquire().await?;
    let queues = return_tasks(&mut conn, process_id).await?;
    drop(conn);
    for (queue, count) in queues {
        if notifications {
            let _ = notify(pool, &queue).await;
        }
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

/// Warns about tasks held by other processes silent for longer than
/// `threshold` (rule 2.3.19 p. 6): without leases they wait for their owner.
async fn warn_stale_owners(
    pool: &PgPool,
    process_id: &str,
    threshold: Duration,
) -> Result<(), sqlx::Error> {
    let owners: Vec<(String, i64, Option<f64>)> = sqlx::query_as(
        "SELECT t.owner, count(*), extract(epoch FROM now() - max(p.seen_at))::float8
           FROM taskcraft_tasks t
           LEFT JOIN taskcraft_processes p ON p.id = t.owner
          WHERE t.owner IS NOT NULL AND t.owner <> $1
            AND t.state IN ('accepted', 'running', 'retry_waiting')
            AND (p.seen_at IS NULL OR p.seen_at < now() - make_interval(secs => $2))
          GROUP BY t.owner",
    )
    .bind(process_id)
    .bind(threshold.as_secs_f64())
    .fetch_all(pool)
    .await?;
    for (owner, count, silent) in owners {
        let silent_for = silent
            .and_then(|s| Duration::try_from_secs_f64(s).ok())
            .map_or_else(|| "unknown".to_owned(), |d| format!("{d:?}"));
        warn!(
            event = "recovery",
            action = "stale_owner",
            "tasks held by a silent process: owner={}, count={}, silent_for={}; release them with PgStore::release_process or turn leases on",
            owner,
            count,
            silent_for
        );
    }
    Ok(())
}

/// Marks the process alive, renews leases and reports cancel requests and
/// lost leases every `tick`; removes finished tasks past retention every
/// cleanup interval.
async fn upkeep(shared: Weak<Shared>, tick: Duration) {
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut cleaned: Option<tokio::time::Instant> = None;
    loop {
        interval.tick().await;
        let Some(shared) = shared.upgrade() else {
            return;
        };
        let due = cleaned.is_none_or(|at| at.elapsed() >= shared.cleanup_interval);
        let mut result = upkeep_once(&shared).await;
        if result.is_ok() && due {
            cleaned = Some(tokio::time::Instant::now());
            result = cleanup(&shared).await;
        }
        if let Err(error) = result {
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
    for (queue, held) in shared.all_sources() {
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
    Ok(())
}

/// Removes finished tasks past retention in batches, oldest first, while
/// batches come back full.
async fn cleanup(shared: &Shared) -> Result<(), sqlx::Error> {
    loop {
        let removed = sqlx::query(CLEANUP_BATCH)
            .bind(shared.retention.as_secs_f64())
            .bind(i64::from(shared.cleanup_batch))
            .execute(&shared.pool)
            .await?
            .rows_affected();
        if removed < u64::from(shared.cleanup_batch) {
            return Ok(());
        }
    }
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
        assert!(
            error
                .to_string()
                .contains("heartbeat interval must be less than half the lease")
        );
        assert!(builder().retention(Duration::ZERO).validate().is_err());
        assert!(
            builder()
                .cleanup_interval(Duration::ZERO)
                .validate()
                .is_err()
        );
        assert!(
            builder()
                .stale_owner_warning(Duration::ZERO)
                .validate()
                .is_err()
        );
        let batch = builder().cleanup_batch(0).validate().unwrap_err();
        assert!(
            batch
                .to_string()
                .contains("cleanup batch must be at least 1")
        );
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
            wake: WakeHandle::new(),
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
