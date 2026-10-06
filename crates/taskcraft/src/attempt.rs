//! Running one attempt: panics become an outcome, service errors become a
//! retry (spec 2.3.2 pp. 2-3).

use std::any::Any;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::handler::TaskRequest;
use crate::outcome::{BoxError, Outcome};
use crate::status::FinishReason;

/// The text of a panic payload.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic with a non-string payload".to_owned()
    }
}

/// A future that turns a panic of the wrapped future into `Err(message)`.
/// Build it with [`catch_panic`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct CatchPanic<F> {
    inner: Option<Pin<Box<F>>>,
}

/// Wraps a future so that a panic while polling it becomes `Err(message)`.
pub fn catch_panic<F: Future>(future: F) -> CatchPanic<F> {
    CatchPanic {
        inner: Some(Box::pin(future)),
    }
}

impl<F: Future> Future for CatchPanic<F> {
    type Output = Result<F::Output, String>;

    // Polling a finished future breaks the `Future` contract on the caller's
    // side; panicking is the standard answer (`std::future::Ready` does the
    // same). The one allowed panic of invariant 1.3.19.
    #[allow(clippy::panic)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            panic!("CatchPanic polled after completion");
        };
        let polled = catch_unwind(AssertUnwindSafe(|| inner.as_mut().poll(cx)));
        match polled {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => {
                this.inner = None;
                Poll::Ready(Ok(value))
            }
            Err(payload) => {
                this.inner = None;
                Poll::Ready(Err(panic_message(&*payload)))
            }
        }
    }
}

/// The outcome of a service call: the response as is, an error as "retry".
///
/// An error from the service is an error without explicit classification
/// (spec 2.3.2 p. 2) — for example a tower timeout.
pub fn outcome_of<E: Into<BoxError>>(result: Result<Outcome, E>) -> Outcome {
    result.unwrap_or_else(|e| Outcome::Retry {
        reason: FinishReason::Handler(e.into().to_string()),
        delay: None,
    })
}

/// Runs one attempt on a handler service, catching panics in `poll_ready`,
/// in the synchronous part of `call` and in the returned future.
pub async fn run_attempt<S, Args>(service: &mut S, request: TaskRequest<Args>) -> Outcome
where
    S: tower::Service<TaskRequest<Args>, Response = Outcome>,
    S::Error: Into<BoxError>,
{
    match catch_panic(poll_fn(|cx| service.poll_ready(cx))).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return outcome_of(Err(e)),
        Err(message) => return Outcome::Panic { message },
    }
    let future = match catch_unwind(AssertUnwindSafe(|| service.call(request))) {
        Ok(future) => future,
        Err(payload) => {
            return Outcome::Panic {
                message: panic_message(&*payload),
            };
        }
    };
    match catch_panic(future).await {
        Ok(result) => outcome_of(result),
        Err(message) => Outcome::Panic { message },
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;

    use super::*;
    use crate::handler::{BoxFuture, SharedData, task_fn};
    use crate::metadata::MetadataRegistry;
    use crate::task::Task;

    fn request() -> TaskRequest<u32> {
        TaskRequest::new(
            Task::new(1),
            Arc::new(MetadataRegistry::new()),
            Arc::new(SharedData::new()),
        )
    }

    #[tokio::test]
    async fn panic_in_poll_becomes_err() {
        let r = catch_panic(async { panic!("boom") }).await;
        assert_eq!(r, Err::<(), _>("boom".to_owned()));
        let r = catch_panic(async { std::panic::panic_any(format!("n={}", 3)) }).await;
        assert_eq!(r, Err::<(), _>("n=3".to_owned()));
        let r = catch_panic(async { std::panic::panic_any(42_u8) }).await;
        assert_eq!(
            r,
            Err::<(), _>("panic with a non-string payload".to_owned())
        );
        assert_eq!(catch_panic(async { 5 }).await, Ok(5));
    }

    #[tokio::test]
    async fn handler_panic_is_a_panic_outcome_not_a_retry() {
        async fn handler(_: u32) {
            panic!("handler exploded");
        }
        let outcome = run_attempt(&mut task_fn(handler), request()).await;
        assert_eq!(
            outcome,
            Outcome::Panic {
                message: "handler exploded".into()
            }
        );
        assert!(outcome.is_final());
    }

    struct PanicsInCall;

    impl tower::Service<TaskRequest<u32>> for PanicsInCall {
        type Response = Outcome;
        type Error = Infallible;
        type Future = BoxFuture<'static, Result<Outcome, Infallible>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: TaskRequest<u32>) -> Self::Future {
            panic!("panicked before returning a future")
        }
    }

    #[tokio::test]
    async fn panic_in_synchronous_call() {
        let outcome = run_attempt(&mut PanicsInCall, request()).await;
        assert!(matches!(outcome, Outcome::Panic { .. }));
    }

    #[test]
    fn service_error_means_retry() {
        let e: Result<Outcome, std::io::Error> = Err(std::io::Error::other("elapsed"));
        assert_eq!(
            outcome_of(e),
            Outcome::Retry {
                reason: FinishReason::Handler("elapsed".into()),
                delay: None
            }
        );
        assert_eq!(
            outcome_of(Ok::<_, Infallible>(Outcome::Success)),
            Outcome::Success
        );
    }
}
