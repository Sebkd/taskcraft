//! Handlers: plain async functions taking the task's arguments and any
//! extractable values, turned into tower services (spec 2.5).

use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

pub use crate::attempt::{CatchPanic, catch_panic, outcome_of, run_attempt};
pub use crate::outcome::IntoOutcome;
pub use crate::runnable::HandlerOutput;
use std::task::{Context, Poll};

use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::error::MetadataError;
use crate::metadata::MetadataRegistry;
use crate::outcome::Outcome;
use crate::status::FinishReason;
use crate::task::{Task, TaskId};

/// A boxed, sendable future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Values shared by every task of a queue — clients, connection pools — keyed
/// by type and handed to handlers as [`Data<T>`].
#[derive(Default, Clone)]
pub struct SharedData {
    values: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl SharedData {
    /// No shared values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a value, replacing any earlier value of the same type.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) {
        self.values.insert(TypeId::of::<T>(), Arc::new(value));
    }

    /// The value of type `T`.
    #[must_use]
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.values
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|v| v.downcast::<T>().ok())
    }
}

impl fmt::Debug for SharedData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedData")
            .field("values", &self.values.len())
            .finish()
    }
}

/// What a handler is called with: the task, the metadata registry to parse
/// its metadata, the queue's shared data and the task's cancel flag.
#[derive(Debug)]
pub struct TaskRequest<Args> {
    task: Task<Args>,
    registry: Arc<MetadataRegistry>,
    shared: Arc<SharedData>,
    cancel: CancellationToken,
}

impl<Args> TaskRequest<Args> {
    /// A request for `task`, with a cancel flag that is never set.
    pub fn new(task: Task<Args>, registry: Arc<MetadataRegistry>, shared: Arc<SharedData>) -> Self {
        Self {
            task,
            registry,
            shared,
            cancel: CancellationToken::new(),
        }
    }

    /// The same request with the task's cancel flag.
    #[must_use]
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// The task's cancel flag.
    #[must_use]
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// The task.
    #[must_use]
    pub fn task(&self) -> &Task<Args> {
        &self.task
    }

    /// The metadata registry.
    #[must_use]
    pub fn registry(&self) -> &MetadataRegistry {
        &self.registry
    }

    /// The shared data.
    #[must_use]
    pub fn shared(&self) -> &SharedData {
        &self.shared
    }

    /// Consumes the request and returns the task.
    pub fn into_task(self) -> Task<Args> {
        self.task
    }
}

/// Why a value could not be extracted. The handler is not called and the
/// attempt ends in "abort" with this reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection(FinishReason);

impl Rejection {
    /// A rejection with this reason.
    #[must_use]
    pub fn new(reason: FinishReason) -> Self {
        Self(reason)
    }

    /// The reason.
    #[must_use]
    pub fn reason(&self) -> &FinishReason {
        &self.0
    }
}

impl From<Rejection> for Outcome {
    fn from(rejection: Rejection) -> Self {
        Self::Abort {
            reason: rejection.0,
        }
    }
}

/// A value a handler can declare as a parameter and get from the request.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a handler parameter",
    label = "not extractable from a task",
    note = "handler parameters after the arguments must implement `FromTask`: Meta<T>, Option<Meta<T>>, Attempt, TaskId, Data<T>, Cancel"
)]
pub trait FromTask<Args>: Sized {
    /// Extracts the value.
    ///
    /// # Errors
    ///
    /// A [`Rejection`] when the value is not available; the handler is then
    /// not called.
    fn from_task(request: &TaskRequest<Args>) -> Result<Self, Rejection>;
}

/// Required metadata of type `T`: missing or unparsable metadata aborts the
/// attempt before the handler runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meta<T>(pub T);

impl<T> Meta<T> {
    /// Unwraps the value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for Meta<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

fn unparsable(error: MetadataError) -> Rejection {
    match error {
        MetadataError::Unparsable {
            name, type_name, ..
        } => Rejection::new(FinishReason::UnparsableMetadata {
            name,
            type_name: type_name.to_owned(),
        }),
        other => Rejection::new(FinishReason::Handler(other.to_string())),
    }
}

impl<Args, T> FromTask<Args> for Meta<T>
where
    T: Clone + DeserializeOwned + Send + Sync + 'static,
{
    fn from_task(request: &TaskRequest<Args>) -> Result<Self, Rejection> {
        match request.task.metadata().resolve::<T>(&request.registry) {
            Ok(Some(value)) => Ok(Self(value)),
            Ok(None) => Err(Rejection::new(FinishReason::MissingMetadata {
                type_name: type_name::<T>().to_owned(),
            })),
            Err(e) => Err(unparsable(e)),
        }
    }
}

/// Optional metadata: absence is fine, an unparsable value still aborts.
impl<Args, T> FromTask<Args> for Option<Meta<T>>
where
    T: Clone + DeserializeOwned + Send + Sync + 'static,
{
    fn from_task(request: &TaskRequest<Args>) -> Result<Self, Rejection> {
        request
            .task
            .metadata()
            .resolve::<T>(&request.registry)
            .map(|v| v.map(Meta))
            .map_err(unparsable)
    }
}

/// The number of the current attempt, starting at 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attempt(pub u32);

impl<Args> FromTask<Args> for Attempt {
    fn from_task(request: &TaskRequest<Args>) -> Result<Self, Rejection> {
        Ok(Self(request.task.attempt()))
    }
}

impl<Args> FromTask<Args> for TaskId {
    fn from_task(request: &TaskRequest<Args>) -> Result<Self, Rejection> {
        Ok(request.task.id().clone())
    }
}

/// The task's cancel flag, set by a cancel request or by shutdown (rule
/// 2.3.15). A handler that checks it can stop early; one that does not is
/// aborted when the queue's cancel grace runs out.
#[derive(Debug, Clone)]
pub struct Cancel(pub CancellationToken);

impl Cancel {
    /// Whether cancellation was requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Completes once cancellation is requested.
    pub async fn cancelled(&self) {
        self.0.cancelled().await;
    }
}

impl<Args> FromTask<Args> for Cancel {
    fn from_task(request: &TaskRequest<Args>) -> Result<Self, Rejection> {
        Ok(Self(request.cancel.clone()))
    }
}

/// A shared value of type `T` registered with the queue.
#[derive(Debug)]
pub struct Data<T>(pub Arc<T>);

impl<T> Clone for Data<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Deref for Data<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<Args, T: Send + Sync + 'static> FromTask<Args> for Data<T> {
    fn from_task(request: &TaskRequest<Args>) -> Result<Self, Rejection> {
        request.shared.get::<T>().map(Self).ok_or_else(|| {
            Rejection::new(FinishReason::Handler(format!(
                "shared data {} is not registered",
                type_name::<T>()
            )))
        })
    }
}

/// A function usable as a handler: `async fn(Args, X1, …, Xn) -> R` with up
/// to eight [`FromTask`] parameters and `R:` [`HandlerOutput`] — an outcome
/// or a [`Run`](crate::runnable::Run) of a process.
///
/// ```compile_fail
/// // A handler must return (), Outcome or Result<_, TaskError>.
/// async fn wrong(_args: u32) -> String { String::new() }
/// let _ = taskcraft::task_fn(wrong);
/// ```
///
/// ```compile_fail
/// // Every extra parameter must be extractable.
/// async fn wrong(_args: u32, _not_extractable: std::fs::File) {}
/// let _ = taskcraft::task_fn(wrong);
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a taskcraft handler",
    label = "not a handler function",
    note = "a handler is `async fn(Args, X1, …, Xn) -> R` with at most 8 extra parameters",
    note = "every extra parameter must implement `FromTask`: Meta<T>, Option<Meta<T>>, Attempt, TaskId, Data<T>, Cancel",
    note = "`R` must be (), Outcome, Result<(), TaskError>, Result<Outcome, TaskError>, Run<R> or Result<Run<R>, TaskError>",
    note = "the function must be Clone + Send + Sync + 'static and its future Send"
)]
pub trait Handler<Args, X>: Clone + Send + Sync + 'static {
    /// Extracts the parameters and runs the function. A failed extraction
    /// aborts without calling it.
    fn call(self, request: TaskRequest<Args>) -> BoxFuture<'static, Outcome>;
}

macro_rules! impl_handler {
    ($($x:ident),*) => {
        impl<F, Fut, R, Args, $($x,)*> Handler<Args, ($($x,)*)> for F
        where
            F: Fn(Args, $($x,)*) -> Fut + Clone + Send + Sync + 'static,
            Fut: Future<Output = R> + Send + 'static,
            R: HandlerOutput,
            Args: Send + 'static,
            $($x: FromTask<Args> + Send + 'static,)*
        {
            #[allow(non_snake_case)]
            fn call(self, request: TaskRequest<Args>) -> BoxFuture<'static, Outcome> {
                $(
                    let $x = match $x::from_task(&request) {
                        Ok(value) => value,
                        Err(rejection) => return Box::pin(ready(Outcome::from(rejection))),
                    };
                )*
                let stop = request.cancel_token().clone();
                let args = request.into_task().into_args();
                let future = self(args, $($x,)*);
                Box::pin(async move { future.await.finish(stop).await })
            }
        }
    };
}

impl_handler!();
impl_handler!(X1);
impl_handler!(X1, X2);
impl_handler!(X1, X2, X3);
impl_handler!(X1, X2, X3, X4);
impl_handler!(X1, X2, X3, X4, X5);
impl_handler!(X1, X2, X3, X4, X5, X6);
impl_handler!(X1, X2, X3, X4, X5, X6, X7);
impl_handler!(X1, X2, X3, X4, X5, X6, X7, X8);

/// A handler function as a tower service. Build it with [`task_fn`].
pub struct TaskFn<F, Args, X> {
    f: F,
    _types: PhantomData<fn(Args, X)>,
}

/// Turns a handler function into a tower service.
///
/// ```
/// use taskcraft::{Attempt, Outcome, TaskError, task_fn};
///
/// async fn send_report(month: String, Attempt(n): Attempt) -> Result<(), TaskError> {
///     if month.is_empty() {
///         return Ok(());
///     }
///     let _ = n;
///     Ok(())
/// }
///
/// let service = task_fn(send_report);
/// # let _ = service;
/// ```
pub fn task_fn<F, Args, X>(f: F) -> TaskFn<F, Args, X>
where
    F: Handler<Args, X>,
{
    TaskFn {
        f,
        _types: PhantomData,
    }
}

impl<F: Clone, Args, X> Clone for TaskFn<F, Args, X> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            _types: PhantomData,
        }
    }
}

impl<F, Args, X> fmt::Debug for TaskFn<F, Args, X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskFn")
            .field("handler", &type_name::<F>())
            .finish()
    }
}

impl<F, Args, X> tower::Service<TaskRequest<Args>> for TaskFn<F, Args, X>
where
    F: Handler<Args, X>,
{
    type Response = Outcome;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<'static, Result<Outcome, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: TaskRequest<Args>) -> Self::Future {
        let future = self.f.clone().call(request);
        Box::pin(async move { Ok(future.await) })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use serde::{Deserialize, Serialize};
    use serde_json::{Map, json};
    use tower::{Service, ServiceExt};

    use super::*;
    use crate::outcome::TaskError;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Priority(u8);

    fn registry() -> Arc<MetadataRegistry> {
        Arc::new(
            MetadataRegistry::new()
                .register::<Priority>("report.priority")
                .unwrap(),
        )
    }

    fn request(task: Task<u32>) -> TaskRequest<u32> {
        TaskRequest::new(task, registry(), Arc::new(SharedData::new()))
    }

    async fn run<X, H: Handler<u32, X>>(h: H, req: TaskRequest<u32>) -> Outcome {
        task_fn(h).oneshot(req).await.unwrap()
    }

    #[tokio::test]
    async fn missing_required_metadata_aborts_without_calling() {
        static CALLED: AtomicBool = AtomicBool::new(false);
        async fn handler(_: u32, _: Meta<Priority>) {
            CALLED.store(true, Ordering::SeqCst);
        }
        let outcome = run(handler, request(Task::new(1))).await;
        assert!(matches!(
            outcome,
            Outcome::Abort { reason: FinishReason::MissingMetadata { ref type_name } }
                if type_name.ends_with("Priority")
        ));
        assert!(!CALLED.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unparsable_metadata_aborts_without_calling() {
        static CALLED: AtomicBool = AtomicBool::new(false);
        async fn handler(_: u32, _: Option<Meta<Priority>>) {
            CALLED.store(true, Ordering::SeqCst);
        }
        let mut stored = Map::new();
        stored.insert("report.priority".into(), json!("high"));
        let mut task = Task::new(1);
        *task.metadata_mut() = registry().decode(stored);

        let outcome = run(handler, request(task)).await;
        assert_eq!(
            outcome,
            Outcome::Abort {
                reason: FinishReason::UnparsableMetadata {
                    name: "report.priority".into(),
                    type_name: type_name::<Priority>().into(),
                }
            }
        );
        assert!(!CALLED.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn optional_metadata_may_be_absent() {
        async fn handler(_: u32, p: Option<Meta<Priority>>) -> Outcome {
            if p.is_none() {
                Outcome::Success
            } else {
                Outcome::abort("unexpected")
            }
        }
        assert_eq!(run(handler, request(Task::new(1))).await, Outcome::Success);
    }

    #[tokio::test]
    async fn extractors_see_the_task() {
        async fn handler(
            args: u32,
            Meta(p): Meta<Priority>,
            Attempt(n): Attempt,
            id: TaskId,
            data: Data<String>,
        ) -> Result<(), TaskError> {
            assert_eq!((args, p, n, id.as_str()), (7, Priority(3), 0, "t-1"));
            assert_eq!(data.as_str(), "shared");
            Ok(())
        }
        let mut shared = SharedData::new();
        shared.insert(String::from("shared"));
        let req = TaskRequest::new(
            Task::new(7).with_id("t-1").with_meta(Priority(3)),
            registry(),
            Arc::new(shared),
        );
        assert_eq!(run(handler, req).await, Outcome::Success);
    }

    #[tokio::test]
    async fn unregistered_shared_data_aborts() {
        async fn handler(_: u32, _: Data<u64>) {}
        let outcome = run(handler, request(Task::new(1))).await;
        assert!(
            matches!(outcome, Outcome::Abort { reason: FinishReason::Handler(ref r) } if r.contains("u64"))
        );
    }

    #[tokio::test]
    async fn service_is_always_ready() {
        async fn handler(_: u32) {}
        let mut service = task_fn(handler);
        let ready = service.ready().await.unwrap();
        assert_eq!(
            ready.call(request(Task::new(1))).await.unwrap(),
            Outcome::Success
        );
    }
}
