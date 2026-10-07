//! A source whose failures are scripted by the test.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::codec::{Codec, CodecError};
use crate::source::{
    AckPointSupport, Capabilities, CloseReason, Polled, PushError, PushResult, PushSource, Source,
    WakeHandle, WakeSignal, Withdrawal,
};
use crate::task::{Task, TaskId};

/// A message of a [`FaultySource`]: a task, or a poison message that cannot
/// be decoded.
#[derive(Debug, Clone)]
pub enum Scripted<Args> {
    /// A regular task.
    Task(Task<Args>),
    /// A message that fails to decode, with the decode error text.
    Poison(String),
}

/// A failure injected by the test script.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("injected source failure")]
pub struct InjectedError;

struct State<Args> {
    queue: VecDeque<Scripted<Args>>,
    in_flight: HashMap<u64, Scripted<Args>>,
    poll_failures: u32,
    ack_failures: u32,
    closed: bool,
    next: u64,
    polls: u64,
    acks: u64,
    redeliveries: u64,
}

/// A source driven by the test: pushed tasks, poison messages, a number of
/// failing polls and a number of failing acks. Every failure happens exactly
/// where the script puts it.
///
/// A failing ack models a crash between running a task and acknowledging it:
/// the ack returns an error and the task goes back to the front of the queue,
/// to be delivered again.
pub struct FaultySource<Args> {
    state: Mutex<State<Args>>,
    wake: WakeHandle,
}

impl<Args> FaultySource<Args> {
    /// An empty, open source with no failures scripted.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                in_flight: HashMap::new(),
                poll_failures: 0,
                ack_failures: 0,
                closed: false,
                next: 0,
                polls: 0,
                acks: 0,
                redeliveries: 0,
            }),
            wake: WakeHandle::new(),
        }
    }

    /// Queues a task without going through `push`.
    pub fn enqueue(&self, task: Task<Args>) {
        self.lock().queue.push_back(Scripted::Task(task));
        self.wake.wake();
    }

    /// Queues a message that fails to decode with this error text.
    pub fn inject_poison(&self, error: impl Into<String>) {
        self.lock().queue.push_back(Scripted::Poison(error.into()));
        self.wake.wake();
    }

    /// Makes the next `n` polls fail with [`InjectedError`].
    pub fn fail_next_polls(&self, n: u32) {
        self.lock().poll_failures = n;
    }

    /// Makes the next `n` acks fail with [`InjectedError`]; each failed ack
    /// puts its task back at the front of the queue.
    pub fn fail_next_acks(&self, n: u32) {
        self.lock().ack_failures = n;
    }

    /// Closes the source: once the queue is empty, polls answer "closed".
    pub fn close(&self) {
        self.lock().closed = true;
        self.wake.wake();
    }

    /// Number of polls so far, failed ones included.
    #[must_use]
    pub fn polls(&self) -> u64 {
        self.lock().polls
    }

    /// Number of acks so far, failed ones included.
    #[must_use]
    pub fn acks(&self) -> u64 {
        self.lock().acks
    }

    /// Number of tasks put back by a failed ack.
    #[must_use]
    pub fn redeliveries(&self) -> u64 {
        self.lock().redeliveries
    }

    /// Number of messages handed out and not yet acknowledged.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.lock().in_flight.len()
    }

    fn lock(&self) -> MutexGuard<'_, State<Args>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<Args> Default for FaultySource<Args> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Args> fmt::Debug for FaultySource<Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        f.debug_struct("FaultySource")
            .field("queued", &state.queue.len())
            .field("in_flight", &state.in_flight.len())
            .field("poll_failures", &state.poll_failures)
            .field("ack_failures", &state.ack_failures)
            .field("closed", &state.closed)
            .finish()
    }
}

impl<Args: Clone + Send + 'static> Source for FaultySource<Args> {
    type Message = Scripted<Args>;
    type Receipt = u64;
    type Error = InjectedError;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::PerTask)
    }

    async fn poll(&self) -> Result<Polled<Scripted<Args>, u64>, InjectedError> {
        let mut state = self.lock();
        state.polls += 1;
        if state.poll_failures > 0 {
            state.poll_failures -= 1;
            return Err(InjectedError);
        }
        if let Some(message) = state.queue.pop_front() {
            let n = state.next;
            state.next += 1;
            state.in_flight.insert(n, message.clone());
            return Ok(Polled::Task {
                message,
                receipt: n,
            });
        }
        if state.closed {
            return Ok(Polled::Closed(CloseReason::new("faulty source closed")));
        }
        Ok(Polled::Empty)
    }

    async fn ack(&self, receipt: u64) -> Result<(), InjectedError> {
        let failed = {
            let mut state = self.lock();
            state.acks += 1;
            let message = state.in_flight.remove(&receipt);
            if state.ack_failures > 0 {
                state.ack_failures -= 1;
                if let Some(message) = message {
                    state.queue.push_front(message);
                    state.redeliveries += 1;
                }
                true
            } else {
                false
            }
        };
        if failed {
            self.wake.wake();
            Err(InjectedError)
        } else {
            Ok(())
        }
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        Some(self.wake.subscribe())
    }
}

impl<Args: Clone + Send + 'static> PushSource for FaultySource<Args> {
    async fn push(
        &self,
        _id: &TaskId,
        message: Scripted<Args>,
    ) -> Result<PushResult, PushError<InjectedError>> {
        {
            let mut state = self.lock();
            if state.closed {
                return Err(PushError::Closed);
            }
            state.queue.push_back(message);
        }
        self.wake.wake();
        Ok(PushResult::Stored)
    }

    /// Scripted messages are not taken back.
    async fn remove(&self, _id: &TaskId) -> Result<Withdrawal, InjectedError> {
        Ok(Withdrawal::NotFound)
    }
}

/// The codec of a [`FaultySource`]: tasks pass through, poison messages fail
/// to decode.
pub struct FaultyCodec<Args>(PhantomData<fn(Args) -> Args>);

impl<Args> FaultyCodec<Args> {
    /// The codec.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Args> Default for FaultyCodec<Args> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Args> Clone for FaultyCodec<Args> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<Args> fmt::Debug for FaultyCodec<Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FaultyCodec")
    }
}

impl<Args: Send + 'static> Codec<Args, Scripted<Args>> for FaultyCodec<Args> {
    fn encode(&self, task: Task<Args>) -> Result<Scripted<Args>, CodecError> {
        Ok(Scripted::Task(task))
    }

    fn decode(&self, message: Scripted<Args>) -> Result<Task<Args>, CodecError> {
        match message {
            Scripted::Task(task) => Ok(task),
            Scripted::Poison(error) => Err(CodecError::Decode(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn take(source: &FaultySource<u32>) -> (Scripted<u32>, u64) {
        match source.poll().await.unwrap() {
            Polled::Task { message, receipt } => (message, receipt),
            other => panic!("expected a message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn poison_fails_to_decode() {
        let source = FaultySource::<u32>::new();
        source.inject_poison("bad json");
        let (message, _) = take(&source).await;
        let err = FaultyCodec::new().decode(message).unwrap_err();
        assert!(matches!(err, CodecError::Decode(ref e) if e == "bad json"));
    }

    #[tokio::test]
    async fn scripted_poll_failures_are_exact() {
        let source = FaultySource::new();
        source.enqueue(Task::new(1_u32));
        source.fail_next_polls(2);
        assert_eq!(source.poll().await.unwrap_err(), InjectedError);
        assert_eq!(source.poll().await.unwrap_err(), InjectedError);
        let _ = take(&source).await;
        assert!(matches!(source.poll().await.unwrap(), Polled::Empty));
        assert_eq!(source.polls(), 4);
    }

    #[tokio::test]
    async fn failed_ack_redelivers_the_same_task() {
        let source = FaultySource::new();
        source.enqueue(Task::new(1_u32).with_id("a"));
        source.enqueue(Task::new(2_u32).with_id("b"));
        source.fail_next_acks(1);

        let (_, receipt) = take(&source).await;
        assert_eq!(source.ack(receipt).await, Err(InjectedError));
        assert_eq!(source.redeliveries(), 1);

        let (again, receipt) = take(&source).await;
        let Scripted::Task(again) = again else {
            panic!("expected a task")
        };
        assert_eq!(again.id().as_str(), "a");
        assert_eq!(source.ack(receipt).await, Ok(()));
        assert_eq!((source.acks(), source.in_flight()), (2, 0));
    }

    #[tokio::test]
    async fn closed_after_the_queue_drains() {
        let source = FaultySource::new();
        source.enqueue(Task::new(1_u32));
        source.close();
        let _ = take(&source).await;
        assert!(matches!(source.poll().await.unwrap(), Polled::Closed(_)));
    }
}
