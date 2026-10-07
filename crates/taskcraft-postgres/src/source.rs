//! The source of one queue on the store.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use taskcraft::source::{
    Completion, DeferError, Notice, Notices, Polled, Progress, PushError, PushResult, StoreMessage,
    TaskStore, WakeHandle, WakeSignal, Withdrawal,
};
use taskcraft::{FinishReason, TaskId, TaskState, TaskStatus};
use tokio::time::Instant;

use crate::store::{Held, PgStoreError, Shared, lock};

/// Identifies a task this process took from the store.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PgReceipt {
    id: TaskId,
}

impl PgReceipt {
    /// The task.
    #[must_use]
    pub fn id(&self) -> &TaskId {
        &self.id
    }
}

/// The source of one queue in a [`PgStore`](crate::PgStore).
///
/// - A poll takes the next due task and claims it for this process in one
///   statement (`FOR UPDATE SKIP LOCKED`): no two processes get one task
///   (rule 2.3.25).
/// - Pushes are checked against the store: a live task answers "already
///   running", a finished one "already finished" until its retention ends.
/// - Final states are recorded by the worker on completion; the ack point is
///   not configurable (rule 2.3.9 p. 6).
pub struct PgSource {
    shared: Arc<Shared>,
    queue: Arc<str>,
    held: Arc<Held>,
    notices: Mutex<Option<Notices>>,
    wake: WakeHandle,
}

impl PgSource {
    pub(crate) fn new(
        shared: Arc<Shared>,
        queue: Arc<str>,
        held: Arc<Held>,
        notices: Notices,
    ) -> Self {
        Self {
            shared,
            queue,
            held,
            notices: Mutex::new(Some(notices)),
            wake: WakeHandle::new(),
        }
    }

    /// The queue name.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }

    fn release(&self, id: &TaskId) {
        lock(&self.held.tasks).remove(id);
    }
}

impl fmt::Debug for PgSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgSource")
            .field("queue", &self.queue)
            .field("process_id", &self.shared.process_id)
            .finish_non_exhaustive()
    }
}

fn parse_state(state: &str) -> Result<TaskState, PgStoreError> {
    serde_json::from_value(serde_json::Value::String(state.to_owned()))
        .map_err(|e| PgStoreError::Data(format!("state {state}: {e}")))
}

fn time(epoch: Option<f64>) -> Option<SystemTime> {
    epoch.and_then(|secs| {
        SystemTime::UNIX_EPOCH.checked_add(Duration::try_from_secs_f64(secs).ok()?)
    })
}

fn count(n: u32) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

type StatusRow = (
    String,
    i32,
    i32,
    Option<String>,
    Option<String>,
    Option<f64>,
    Option<f64>,
);

impl TaskStore for PgSource {
    type Receipt = PgReceipt;
    type Error = PgStoreError;

    async fn poll(&self) -> Result<Polled<StoreMessage, PgReceipt>, PgStoreError> {
        let lease = self.shared.lease;
        let row: Option<(String, String, Option<String>)> = sqlx::query_as("WITH next AS (
                 SELECT queue, id, owner AS previous
                   FROM taskcraft_tasks
                  WHERE queue = $1
                    AND ((state IN ('queued', 'deferred')
                          AND (next_delivery IS NULL OR next_delivery <= now()))
                      OR ($2 AND state IN ('accepted', 'running', 'retry_waiting') AND lease_until < now()))
                  ORDER BY COALESCE(next_delivery, created_at), created_at
                  LIMIT 1
                    FOR UPDATE SKIP LOCKED)
             UPDATE taskcraft_tasks t
                SET state = 'accepted', owner = $3, next_delivery = NULL, updated_at = now(),
                    lease_until = CASE WHEN $2 THEN now() + make_interval(secs => $4) END
               FROM next
              WHERE t.queue = next.queue AND t.id = next.id
          RETURNING t.id,
                    (t.task || jsonb_build_object('attempt', t.attempt, 'retries', t.retries))::text,
                    next.previous")
        .bind(&*self.queue)
        .bind(lease.is_some())
        .bind(&self.shared.process_id)
        .bind(lease.map_or(0.0, |l| l.duration.as_secs_f64()))
        .fetch_optional(&self.shared.pool)
        .await?;
        let Some((id, task, previous)) = row else {
            return Ok(Polled::Empty);
        };
        let id = TaskId::new(id);
        lock(&self.held.tasks).insert(id.clone(), false);
        if let Some(previous) = previous
            && previous != self.shared.process_id
        {
            let _ = self.held.notify.send(Notice::TakenOver {
                task_id: id.clone(),
                previous_owner: previous,
            });
        }
        Ok(Polled::Task {
            message: StoreMessage::from_bytes(task.into_bytes()),
            receipt: PgReceipt { id },
        })
    }

    async fn complete(
        &self,
        receipt: PgReceipt,
        completion: Completion<'_>,
    ) -> Result<(), PgStoreError> {
        let reason = completion
            .reason
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| PgStoreError::Data(e.to_string()))?;
        sqlx::query(
            "UPDATE taskcraft_tasks
                SET state = $4, reason = $5::jsonb, lease_until = NULL, updated_at = now()
              WHERE queue = $1 AND id = $2 AND owner = $3",
        )
        .bind(&*self.queue)
        .bind(receipt.id.as_str())
        .bind(&self.shared.process_id)
        .bind(completion.state.as_str())
        .bind(reason.map(|r| r.to_string()))
        .execute(&self.shared.pool)
        .await?;
        self.release(&receipt.id);
        Ok(())
    }

    async fn progress(&self, receipt: PgReceipt, progress: Progress) -> Result<(), PgStoreError> {
        sqlx::query(
            "UPDATE taskcraft_tasks
                SET state = $4, attempt = $5, retries = $6, updated_at = now()
              WHERE queue = $1 AND id = $2 AND owner = $3",
        )
        .bind(&*self.queue)
        .bind(receipt.id.as_str())
        .bind(&self.shared.process_id)
        .bind(progress.state.as_str())
        .bind(count(progress.attempt))
        .bind(count(progress.retries))
        .execute(&self.shared.pool)
        .await?;
        Ok(())
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        Some(self.wake.subscribe())
    }

    fn notices(&self) -> Option<Notices> {
        lock(&self.notices).take()
    }

    async fn push(
        &self,
        id: &TaskId,
        message: StoreMessage,
    ) -> Result<PushResult, PushError<PgStoreError>> {
        let task = String::from_utf8(message.into_bytes())
            .map_err(|e| PushError::Source(PgStoreError::Data(e.to_string())))?;
        let inserted = sqlx::query(
            "INSERT INTO taskcraft_tasks (queue, id, task, state)
             VALUES ($1, $2, $3::jsonb, 'queued')
             ON CONFLICT (queue, id) DO NOTHING",
        )
        .bind(&*self.queue)
        .bind(id.as_str())
        .bind(&task)
        .execute(&self.shared.pool)
        .await
        .map_err(|e| PushError::Source(e.into()))?
        .rows_affected();
        if inserted == 1 {
            self.wake.wake();
            return Ok(PushResult::Stored);
        }
        let state: Option<String> =
            sqlx::query_scalar("SELECT state FROM taskcraft_tasks WHERE queue = $1 AND id = $2")
                .bind(&*self.queue)
                .bind(id.as_str())
                .fetch_optional(&self.shared.pool)
                .await
                .map_err(|e| PushError::Source(e.into()))?;
        match state
            .as_deref()
            .map(parse_state)
            .transpose()
            .map_err(PushError::Source)?
        {
            Some(state) if state.is_terminal() => Ok(PushResult::Finished(state)),
            _ => Ok(PushResult::Duplicate),
        }
    }

    async fn defer(
        &self,
        receipt: PgReceipt,
        message: StoreMessage,
        at: Instant,
    ) -> Result<(), DeferError<PgStoreError>> {
        let task = String::from_utf8(message.into_bytes())
            .map_err(|e| DeferError::Source(PgStoreError::Data(e.to_string())))?;
        let delay = at.saturating_duration_since(Instant::now());
        sqlx::query(
            "UPDATE taskcraft_tasks
                SET task = $4::jsonb, state = 'deferred',
                    attempt = ($4::jsonb ->> 'attempt')::integer,
                    retries = ($4::jsonb ->> 'retries')::integer,
                    next_delivery = now() + make_interval(secs => $5),
                    owner = NULL, lease_until = NULL, updated_at = now()
              WHERE queue = $1 AND id = $2 AND owner = $3",
        )
        .bind(&*self.queue)
        .bind(receipt.id.as_str())
        .bind(&self.shared.process_id)
        .bind(&task)
        .bind(delay.as_secs_f64())
        .execute(&self.shared.pool)
        .await
        .map_err(|e| DeferError::Source(e.into()))?;
        self.release(&receipt.id);
        Ok(())
    }

    /// Cancels a task waiting in the store, or records a cancel request for
    /// the process that holds it (spec 2.1.2.15 pp. 4–6).
    async fn remove(&self, id: &TaskId) -> Result<Withdrawal, PgStoreError> {
        let reason = serde_json::to_value(FinishReason::CancelledByUser)
            .map_err(|e| PgStoreError::Data(e.to_string()))?;
        let cancelled = sqlx::query(
            "UPDATE taskcraft_tasks
                SET state = 'cancelled', reason = $3::jsonb, updated_at = now()
              WHERE queue = $1 AND id = $2 AND state IN ('queued', 'deferred')",
        )
        .bind(&*self.queue)
        .bind(id.as_str())
        .bind(reason.to_string())
        .execute(&self.shared.pool)
        .await?
        .rows_affected();
        if cancelled == 1 {
            return Ok(Withdrawal::Removed);
        }
        let requested = sqlx::query(
            "UPDATE taskcraft_tasks SET cancel_requested = true, updated_at = now()
              WHERE queue = $1 AND id = $2 AND state IN ('accepted', 'running', 'retry_waiting')",
        )
        .bind(&*self.queue)
        .bind(id.as_str())
        .execute(&self.shared.pool)
        .await?
        .rows_affected();
        if requested == 1 {
            return Ok(Withdrawal::CancelRequested);
        }
        let finished: Option<bool> = sqlx::query_scalar("SELECT state IN ('succeeded', 'failed', 'panicked', 'cancelled') FROM taskcraft_tasks WHERE queue = $1 AND id = $2")
        .bind(&*self.queue)
        .bind(id.as_str())
        .fetch_optional(&self.shared.pool)
        .await?;
        Ok(if finished == Some(true) {
            Withdrawal::Finished
        } else {
            Withdrawal::NotFound
        })
    }

    async fn status(&self, id: &TaskId) -> Result<Option<TaskStatus>, PgStoreError> {
        let row: Option<StatusRow> = sqlx::query_as(
            "SELECT state, attempt, retries, reason::text, owner,
                    extract(epoch FROM created_at)::float8,
                    extract(epoch FROM next_delivery)::float8
               FROM taskcraft_tasks WHERE queue = $1 AND id = $2",
        )
        .bind(&*self.queue)
        .bind(id.as_str())
        .fetch_optional(&self.shared.pool)
        .await?;
        let Some((state, attempt, retries, reason, owner, created, next)) = row else {
            return Ok(None);
        };
        let state = parse_state(&state)?;
        let attempt = u32::try_from(attempt).unwrap_or(0);
        let retries = u32::try_from(retries).unwrap_or(0);
        let mut status = TaskStatus::new(id.clone(), state, attempt, retries).with_times(
            time(created),
            None,
            time(next),
        );
        if let Some(reason) = reason {
            let reason: FinishReason =
                serde_json::from_str(&reason).map_err(|e| PgStoreError::Data(e.to_string()))?;
            status = status.with_reason(reason);
        }
        if let Some(owner) = owner
            && !state.is_terminal()
        {
            status = status.with_owner(owner);
        }
        Ok(Some(status))
    }
}
