//! The in-memory source (spec 2.6).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use tokio::time::Instant;

use crate::error::ConfigError;
use crate::source::{
    AckPointSupport, Capabilities, CloseReason, DeferError, Polled, PushError, PushResult,
    PushSource, Source, WakeHandle, WakeSignal, Withdrawal,
};
use crate::state::TaskState;
use crate::status::TaskStatus;
use crate::task::{Task, TaskId};

/// Identifies one delivery of an [`InMemorySource`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Delivery(u64);

struct State<Args> {
    ready: VecDeque<Task<Args>>,
    deferred: BTreeMap<(Instant, u64), Task<Args>>,
    in_flight: HashMap<u64, TaskId>,
    held: HashSet<TaskId>,
    closed: bool,
    next: u64,
}

/// A bounded source in process memory: accepts pushes, acks per task and
/// supports deferred redelivery. Tasks do not survive the process.
///
/// Capacity counts every task the source holds: ready, deferred and handed
/// out but not yet acknowledged.
pub struct InMemorySource<Args> {
    state: Mutex<State<Args>>,
    capacity: usize,
    wake: WakeHandle,
}

impl<Args> InMemorySource<Args> {
    /// The default capacity (spec 2.8).
    pub const DEFAULT_CAPACITY: usize = 10_000;

    /// A source holding at most `capacity` tasks.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidCapacity`] when `capacity` is zero (spec 2.10).
    pub fn new(capacity: usize) -> Result<Self, ConfigError> {
        if capacity == 0 {
            return Err(ConfigError::InvalidCapacity);
        }
        Ok(Self::with_capacity(capacity))
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            state: Mutex::new(State {
                ready: VecDeque::new(),
                deferred: BTreeMap::new(),
                in_flight: HashMap::new(),
                held: HashSet::new(),
                closed: false,
                next: 0,
            }),
            capacity,
            wake: WakeHandle::new(),
        }
    }

    /// Closes the source: pushes are refused, and once the remaining ready
    /// and deferred tasks are handed out, polls answer "closed".
    pub fn close(&self) {
        self.lock().closed = true;
        self.wake.wake();
    }

    /// Number of tasks held: ready, deferred and handed out but unacknowledged.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().held.len()
    }

    /// Whether the source holds no task.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> MutexGuard<'_, State<Args>> {
        // A panic while holding the lock cannot leave the state half-updated:
        // every critical section below is a few infallible collection calls.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<Args> Default for InMemorySource<Args> {
    fn default() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }
}

impl<Args> fmt::Debug for InMemorySource<Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        f.debug_struct("InMemorySource")
            .field("capacity", &self.capacity)
            .field("ready", &state.ready.len())
            .field("deferred", &state.deferred.len())
            .field("in_flight", &state.in_flight.len())
            .field("closed", &state.closed)
            .finish()
    }
}

impl<Args: Send + 'static> Source for InMemorySource<Args> {
    type Message = Task<Args>;
    type Receipt = Delivery;
    type Error = Infallible;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::PerTask).with_defer()
    }

    async fn poll(&self) -> Result<Polled<Task<Args>, Delivery>, Infallible> {
        let mut state = self.lock();
        let now = Instant::now();
        while let Some(entry) = state.deferred.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let task = entry.remove();
            state.ready.push_back(task);
        }
        if let Some(task) = state.ready.pop_front() {
            let n = state.next;
            state.next = n.wrapping_add(1);
            state.in_flight.insert(n, task.id().clone());
            return Ok(Polled::Task {
                message: task,
                receipt: Delivery(n),
            });
        }
        if state.closed && state.deferred.is_empty() {
            return Ok(Polled::Closed(CloseReason::new("in-memory source closed")));
        }
        Ok(Polled::Empty)
    }

    async fn ack(&self, receipt: Delivery) -> Result<(), Infallible> {
        let mut state = self.lock();
        if let Some(id) = state.in_flight.remove(&receipt.0) {
            state.held.remove(&id);
        }
        Ok(())
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        Some(self.wake.subscribe())
    }
}

impl<Args> InMemorySource<Args> {
    /// Stores a pushed task, ready or due at `due`, checking its id and the
    /// capacity.
    fn store(
        &self,
        id: &TaskId,
        message: Task<Args>,
        due: Option<Instant>,
    ) -> Result<PushResult, PushError<Infallible>> {
        let mut state = self.lock();
        if state.closed {
            return Err(PushError::Closed);
        }
        if state.held.contains(id) {
            return Ok(PushResult::Duplicate);
        }
        if state.held.len() >= self.capacity {
            return Ok(PushResult::Full);
        }
        state.held.insert(id.clone());
        match due {
            None => state.ready.push_back(message),
            Some(due) => {
                let n = state.next;
                state.next = n.wrapping_add(1);
                state.deferred.insert((due, n), message);
            }
        }
        Ok(PushResult::Stored)
    }
}

impl<Args: Send + 'static> PushSource for InMemorySource<Args> {
    async fn push(
        &self,
        id: &TaskId,
        message: Task<Args>,
    ) -> Result<PushResult, PushError<Infallible>> {
        let stored = self.store(id, message, None)?;
        if stored == PushResult::Stored {
            self.wake.wake();
        }
        Ok(stored)
    }

    /// Held back with the tasks deferred by a handler; nothing to wake for
    /// until the moment comes.
    async fn push_at(
        &self,
        id: &TaskId,
        message: Task<Args>,
        at: SystemTime,
    ) -> Result<PushResult, PushError<Infallible>> {
        // The wall-clock moment on the runtime's clock, paused time included.
        let delay = at.duration_since(SystemTime::now()).unwrap_or_default();
        self.store(id, message, Some(Instant::now() + delay))
    }

    async fn remove(&self, id: &TaskId) -> Result<Withdrawal, Infallible> {
        let mut state = self.lock();
        let before = state.ready.len() + state.deferred.len();
        state.ready.retain(|task| task.id() != id);
        state.deferred.retain(|_, task| task.id() != id);
        let removed = state.ready.len() + state.deferred.len() < before;
        if !removed {
            return Ok(Withdrawal::NotFound);
        }
        state.held.remove(id);
        Ok(Withdrawal::Removed)
    }

    async fn defer(
        &self,
        receipt: Delivery,
        message: Task<Args>,
        at: Instant,
    ) -> Result<(), DeferError<Infallible>> {
        let mut state = self.lock();
        state.in_flight.remove(&receipt.0);
        let n = state.next;
        state.next = n.wrapping_add(1);
        state.deferred.insert((at, n), message);
        Ok(())
    }

    /// Ready: queued. Held back: queued with its moment when pushed for
    /// later, deferred when a handler deferred it after an attempt. Handed
    /// out: unknown here — the registry knows it.
    async fn status(&self, id: &TaskId) -> Result<Option<TaskStatus>, Infallible> {
        let state = self.lock();
        if let Some(task) = state.ready.iter().find(|task| task.id() == id) {
            let status = TaskStatus::new(
                id.clone(),
                TaskState::Queued,
                task.attempt(),
                task.retries(),
            )
            .with_times(task.accepted_at(), None, None);
            return Ok(Some(status));
        }
        let held = state
            .deferred
            .iter()
            .find(|(_, task)| task.id() == id)
            .map(|(&(due, _), task)| (due, task));
        Ok(held.map(|(due, task)| {
            let kind = if task.attempt() == 0 {
                TaskState::Queued
            } else {
                TaskState::Deferred
            };
            // The runtime's moment on the wall clock, paused time included.
            let at = SystemTime::now() + due.saturating_duration_since(Instant::now());
            TaskStatus::new(id.clone(), kind, task.attempt(), task.retries()).with_times(
                task.accepted_at(),
                None,
                Some(at),
            )
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn task(id: &str) -> Task<u32> {
        Task::new(0).with_id(id)
    }

    async fn push(source: &InMemorySource<u32>, id: &str) -> PushResult {
        source.push(&TaskId::new(id), task(id)).await.unwrap()
    }

    async fn take(source: &InMemorySource<u32>) -> (Task<u32>, Delivery) {
        match source.poll().await.unwrap() {
            Polled::Task { message, receipt } => (message, receipt),
            other => panic!("expected a task, got {other:?}"),
        }
    }

    /// Change memory-queued-status, criteria 1 and 2: the source answers
    /// for the tasks it holds and has not handed out.
    #[tokio::test(start_paused = true)]
    async fn status_of_held_tasks() {
        let source = InMemorySource::<u32>::new(8).unwrap();
        let status = |id: &'static str| {
            let source = &source;
            async move { source.status(&TaskId::new(id)).await.unwrap() }
        };
        assert_eq!(push(&source, "ready").await, PushResult::Stored);
        let ready = status("ready").await.unwrap();
        assert_eq!((ready.state(), ready.attempt()), (TaskState::Queued, 0));
        assert_eq!(ready.next_attempt_at(), None);

        let hour = Duration::from_secs(3600);
        let later = SystemTime::now() + hour;
        let pushed = source
            .push_at(&TaskId::new("later"), task("later"), later)
            .await;
        assert_eq!(pushed.unwrap(), PushResult::Stored);
        let held = status("later").await.unwrap();
        assert_eq!(held.state(), TaskState::Queued);
        let at = held.next_attempt_at().unwrap();
        let off = at.duration_since(later).unwrap_or_else(|e| e.duration());
        assert!(off < Duration::from_secs(1), "{off:?}");

        // Handed out: the registry answers, not the source.
        let (mut taken, receipt) = take(&source).await;
        assert_eq!(status("ready").await, None);

        // Deferred by its handler after an attempt.
        taken.begin_attempt();
        source
            .defer(receipt, taken, Instant::now() + hour)
            .await
            .unwrap();
        let deferred = status("ready").await.unwrap();
        assert_eq!(
            (deferred.state(), deferred.attempt()),
            (TaskState::Deferred, 1)
        );
        assert!(deferred.next_attempt_at().is_some());

        assert_eq!(status("never-pushed").await, None);
    }

    #[test]
    fn zero_capacity_is_a_config_error() {
        let error = InMemorySource::<u32>::new(0).unwrap_err();
        assert_eq!(error.to_string(), "capacity must be at least 1");
    }

    #[tokio::test]
    async fn capacity_counts_every_held_task() {
        let source = InMemorySource::new(2).unwrap();
        assert_eq!(push(&source, "a").await, PushResult::Stored);
        assert_eq!(push(&source, "b").await, PushResult::Stored);
        assert_eq!(push(&source, "c").await, PushResult::Full);
        assert_eq!(source.len(), 2);

        // Handed out but unacknowledged still counts.
        let (_, receipt) = take(&source).await;
        assert_eq!(push(&source, "c").await, PushResult::Full);
        source.ack(receipt).await.unwrap();
        assert_eq!(push(&source, "c").await, PushResult::Stored);
    }

    #[tokio::test]
    async fn duplicate_ids_are_refused_until_acked() {
        let source = InMemorySource::new(10).unwrap();
        assert_eq!(push(&source, "a").await, PushResult::Stored);
        assert_eq!(push(&source, "a").await, PushResult::Duplicate);
        let (_, receipt) = take(&source).await;
        assert_eq!(push(&source, "a").await, PushResult::Duplicate);
        source.ack(receipt).await.unwrap();
        source.ack(receipt).await.unwrap(); // idempotent
        assert_eq!(push(&source, "a").await, PushResult::Stored);
    }

    #[tokio::test]
    async fn empty_is_not_closed() {
        let source = InMemorySource::<u32>::new(10).unwrap();
        for _ in 0..1000 {
            assert!(matches!(source.poll().await.unwrap(), Polled::Empty));
        }
        assert_eq!(push(&source, "late").await, PushResult::Stored);
        take(&source).await;
        source.close();
        assert!(matches!(source.poll().await.unwrap(), Polled::Closed(_)));
        assert!(matches!(
            source.push(&TaskId::new("x"), task("x")).await,
            Err(PushError::Closed)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_task_returns_when_due() {
        let source = InMemorySource::new(10).unwrap();
        assert_eq!(push(&source, "a").await, PushResult::Stored);
        let (delivered, receipt) = take(&source).await;
        let at = Instant::now() + Duration::from_secs(60);
        source.defer(receipt, delivered, at).await.unwrap();
        source.close();

        assert!(matches!(source.poll().await.unwrap(), Polled::Empty));
        assert!(matches!(
            source.push(&TaskId::new("b"), task("b")).await,
            Err(PushError::Closed)
        ));
        tokio::time::advance(Duration::from_secs(60)).await;
        let (again, receipt) = take(&source).await;
        assert_eq!(again.id().as_str(), "a");
        source.ack(receipt).await.unwrap();
        assert!(matches!(source.poll().await.unwrap(), Polled::Closed(_)));
        assert!(source.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn remove_takes_out_ready_and_deferred_tasks_only() {
        let source = InMemorySource::new(10).unwrap();
        let (a, b, c) = (TaskId::new("a"), TaskId::new("b"), TaskId::new("c"));
        for id in ["a", "b", "c"] {
            assert_eq!(push(&source, id).await, PushResult::Stored);
        }
        let (delivered, receipt) = take(&source).await;
        assert_eq!(delivered.id(), &a);
        assert_eq!(
            source.remove(&a).await.unwrap(),
            Withdrawal::NotFound,
            "handed out"
        );

        let (later, receipt_b) = take(&source).await;
        let at = Instant::now() + Duration::from_secs(60);
        source.defer(receipt_b, later, at).await.unwrap();
        assert_eq!(
            source.remove(&b).await.unwrap(),
            Withdrawal::Removed,
            "deferred"
        );
        assert_eq!(
            source.remove(&c).await.unwrap(),
            Withdrawal::Removed,
            "ready"
        );
        assert_eq!(source.remove(&c).await.unwrap(), Withdrawal::NotFound);
        assert_eq!(push(&source, "c").await, PushResult::Stored, "id is free");

        source.ack(receipt).await.unwrap();
        assert_eq!(source.len(), 1);
    }

    #[tokio::test]
    async fn push_wakes_subscribers() {
        let source = InMemorySource::new(10).unwrap();
        let mut signal = source.subscribe().unwrap();
        signal.mark_seen();
        assert_eq!(push(&source, "a").await, PushResult::Stored);
        assert!(signal.changed().await);
    }
}
