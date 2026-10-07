//! One internal view of a source of any kind, for the worker and the queue
//! handle: [`Consumed`] streams, [`Pushed`] sources and [`Stored`] task
//! stores answer the same operations. Sealed: the module is private.

use std::future::Future;
use std::time::SystemTime;

use tokio::time::Instant;

use crate::handle::{ConsumerHandle, HandleCore, QueueHandle};
use crate::source::{
    AckPointSupport, Capabilities, Completion, Consumed, DeferError, Notices, Polled, Progress,
    PushError, PushResult, PushSource, Pushed, Requeue, Source, StoreMessage, Stored, TaskStore,
    WakeSignal, Withdrawal,
};
use crate::status::TaskStatus;
use crate::task::TaskId;

/// Every operation the worker and the handle may ask of a source; what a
/// kind of source cannot do has a fixed answer.
pub trait Backend: Send + Sync + 'static {
    type Message: Send + 'static;
    type Receipt: Clone + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn capabilities(&self) -> Capabilities;

    fn poll(
        &self,
    ) -> impl Future<Output = Result<Polled<Self::Message, Self::Receipt>, Self::Error>> + Send;

    fn ack(&self, receipt: Self::Receipt) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn subscribe(&self) -> Option<WakeSignal>;

    fn notices(&self) -> Option<Notices>;

    /// Records how a task ended; a plain ack for sources without history.
    fn complete(
        &self,
        receipt: Self::Receipt,
        completion: Completion<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Records where a task stands; nothing for sources without history.
    fn progress(
        &self,
        receipt: Self::Receipt,
        progress: Progress,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn defer(
        &self,
        receipt: Self::Receipt,
        message: Self::Message,
        at: Instant,
    ) -> impl Future<Output = Result<(), DeferError<Self::Error>>> + Send;

    fn status(
        &self,
        id: &TaskId,
    ) -> impl Future<Output = Result<Option<TaskStatus>, Self::Error>> + Send;

    fn remove(&self, id: &TaskId) -> impl Future<Output = Result<Withdrawal, Self::Error>> + Send;

    /// Queues a failed task again; only a task store keeps one.
    fn requeue(&self, id: &TaskId) -> impl Future<Output = Result<Requeue, Self::Error>> + Send;

    /// Only reached through a [`QueueHandle`], which consumed streams do not
    /// have.
    fn push(
        &self,
        id: &TaskId,
        message: Self::Message,
    ) -> impl Future<Output = Result<PushResult, PushError<Self::Error>>> + Send;

    /// A push to be handed out not before `at`; only through a
    /// [`QueueHandle`].
    fn push_at(
        &self,
        id: &TaskId,
        message: Self::Message,
        at: SystemTime,
    ) -> impl Future<Output = Result<PushResult, PushError<Self::Error>>> + Send;
}

/// Which handle `register` returns for a queue of this kind.
pub trait HandleKind<Args>: Backend {
    type Handle;

    fn handle(core: HandleCore<Args>) -> Self::Handle;
}

impl<S: Source> Backend for Consumed<S> {
    type Message = S::Message;
    type Receipt = S::Receipt;
    type Error = S::Error;

    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }

    fn poll(
        &self,
    ) -> impl Future<Output = Result<Polled<S::Message, S::Receipt>, S::Error>> + Send {
        self.0.poll()
    }

    fn ack(&self, receipt: S::Receipt) -> impl Future<Output = Result<(), S::Error>> + Send {
        self.0.ack(receipt)
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        self.0.subscribe()
    }

    fn notices(&self) -> Option<Notices> {
        self.0.notices()
    }

    fn complete(
        &self,
        receipt: S::Receipt,
        _: Completion<'_>,
    ) -> impl Future<Output = Result<(), S::Error>> + Send {
        self.0.ack(receipt)
    }

    async fn progress(&self, _: S::Receipt, _: Progress) -> Result<(), S::Error> {
        Ok(())
    }

    async fn defer(
        &self,
        _: S::Receipt,
        _: S::Message,
        _: Instant,
    ) -> Result<(), DeferError<S::Error>> {
        Err(DeferError::Unsupported)
    }

    async fn status(&self, _: &TaskId) -> Result<Option<TaskStatus>, S::Error> {
        Ok(None)
    }

    async fn remove(&self, _: &TaskId) -> Result<Withdrawal, S::Error> {
        Ok(Withdrawal::NotFound)
    }

    async fn requeue(&self, _: &TaskId) -> Result<Requeue, S::Error> {
        Ok(Requeue::NotFound)
    }

    async fn push(&self, _: &TaskId, _: S::Message) -> Result<PushResult, PushError<S::Error>> {
        Err(PushError::Closed)
    }

    async fn push_at(
        &self,
        _: &TaskId,
        _: S::Message,
        _: SystemTime,
    ) -> Result<PushResult, PushError<S::Error>> {
        Err(PushError::Closed)
    }
}

impl<S: Source, Args> HandleKind<Args> for Consumed<S> {
    type Handle = ConsumerHandle<Args>;

    fn handle(core: HandleCore<Args>) -> ConsumerHandle<Args> {
        ConsumerHandle::new(core)
    }
}

impl<S: PushSource> Backend for Pushed<S> {
    type Message = S::Message;
    type Receipt = S::Receipt;
    type Error = S::Error;

    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }

    fn poll(
        &self,
    ) -> impl Future<Output = Result<Polled<S::Message, S::Receipt>, S::Error>> + Send {
        self.0.poll()
    }

    fn ack(&self, receipt: S::Receipt) -> impl Future<Output = Result<(), S::Error>> + Send {
        self.0.ack(receipt)
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        self.0.subscribe()
    }

    fn notices(&self) -> Option<Notices> {
        self.0.notices()
    }

    fn complete(
        &self,
        receipt: S::Receipt,
        _: Completion<'_>,
    ) -> impl Future<Output = Result<(), S::Error>> + Send {
        self.0.ack(receipt)
    }

    async fn progress(&self, _: S::Receipt, _: Progress) -> Result<(), S::Error> {
        Ok(())
    }

    fn defer(
        &self,
        receipt: S::Receipt,
        message: S::Message,
        at: Instant,
    ) -> impl Future<Output = Result<(), DeferError<S::Error>>> + Send {
        self.0.defer(receipt, message, at)
    }

    async fn status(&self, _: &TaskId) -> Result<Option<TaskStatus>, S::Error> {
        Ok(None)
    }

    fn remove(&self, id: &TaskId) -> impl Future<Output = Result<Withdrawal, S::Error>> + Send {
        self.0.remove(id)
    }

    async fn requeue(&self, _: &TaskId) -> Result<Requeue, S::Error> {
        Ok(Requeue::NotFound)
    }

    fn push(
        &self,
        id: &TaskId,
        message: S::Message,
    ) -> impl Future<Output = Result<PushResult, PushError<S::Error>>> + Send {
        self.0.push(id, message)
    }

    fn push_at(
        &self,
        id: &TaskId,
        message: S::Message,
        at: SystemTime,
    ) -> impl Future<Output = Result<PushResult, PushError<S::Error>>> + Send {
        self.0.push_at(id, message, at)
    }
}

impl<S: PushSource, Args> HandleKind<Args> for Pushed<S> {
    type Handle = QueueHandle<Args>;

    fn handle(core: HandleCore<Args>) -> QueueHandle<Args> {
        QueueHandle::new(core)
    }
}

impl<S: TaskStore> Backend for Stored<S> {
    type Message = StoreMessage;
    type Receipt = S::Receipt;
    type Error = S::Error;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::Fixed).with_defer()
    }

    fn poll(
        &self,
    ) -> impl Future<Output = Result<Polled<StoreMessage, S::Receipt>, S::Error>> + Send {
        self.0.poll()
    }

    /// A store's outcome is recorded by `complete`; a duplicate delivery
    /// needs nothing recorded.
    async fn ack(&self, _: S::Receipt) -> Result<(), S::Error> {
        Ok(())
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        self.0.subscribe()
    }

    fn notices(&self) -> Option<Notices> {
        self.0.notices()
    }

    fn complete(
        &self,
        receipt: S::Receipt,
        completion: Completion<'_>,
    ) -> impl Future<Output = Result<(), S::Error>> + Send {
        self.0.complete(receipt, completion)
    }

    fn progress(
        &self,
        receipt: S::Receipt,
        progress: Progress,
    ) -> impl Future<Output = Result<(), S::Error>> + Send {
        self.0.progress(receipt, progress)
    }

    fn defer(
        &self,
        receipt: S::Receipt,
        message: StoreMessage,
        at: Instant,
    ) -> impl Future<Output = Result<(), DeferError<S::Error>>> + Send {
        self.0.defer(receipt, message, at)
    }

    fn status(
        &self,
        id: &TaskId,
    ) -> impl Future<Output = Result<Option<TaskStatus>, S::Error>> + Send {
        self.0.status(id)
    }

    fn remove(&self, id: &TaskId) -> impl Future<Output = Result<Withdrawal, S::Error>> + Send {
        self.0.remove(id)
    }

    fn requeue(&self, id: &TaskId) -> impl Future<Output = Result<Requeue, S::Error>> + Send {
        self.0.requeue(id)
    }

    fn push(
        &self,
        id: &TaskId,
        message: StoreMessage,
    ) -> impl Future<Output = Result<PushResult, PushError<S::Error>>> + Send {
        self.0.push(id, message)
    }

    fn push_at(
        &self,
        id: &TaskId,
        message: StoreMessage,
        at: SystemTime,
    ) -> impl Future<Output = Result<PushResult, PushError<S::Error>>> + Send {
        self.0.push_at(id, message, at)
    }
}

impl<S: TaskStore, Args> HandleKind<Args> for Stored<S> {
    type Handle = QueueHandle<Args>;

    fn handle(core: HandleCore<Args>) -> QueueHandle<Args> {
        QueueHandle::new(core)
    }
}
