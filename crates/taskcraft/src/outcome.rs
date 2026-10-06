//! The outcome of an attempt, and handler errors that carry their
//! classification as data (spec 2.3.2).

use std::error::Error;
use std::fmt;
use std::time::Duration;

use crate::status::FinishReason;

/// A boxed error, as produced by `?` on any error type.
pub type BoxError = Box<dyn Error + Send + Sync>;

/// How one attempt of a task ended.
///
/// The classification is the value itself: wrapping an error in another
/// error type cannot turn an abort into a retry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// The task is done.
    Success,
    /// Try again later, within the retry limit.
    Retry {
        /// Why.
        reason: FinishReason,
        /// The pause the handler asks for; the retry policy decides when absent.
        delay: Option<Duration>,
    },
    /// Give up: no further attempts, whatever the retry limit.
    Abort {
        /// Why.
        reason: FinishReason,
    },
    /// Not now: hand the task back to run after `delay`. Does not use up the
    /// retry limit.
    Defer {
        /// When to run it again.
        delay: Duration,
        /// Why.
        reason: FinishReason,
    },
    /// The handler panicked. Never retried.
    Panic {
        /// The panic message.
        message: String,
    },
}

impl Outcome {
    /// "Retry" with this reason and no requested pause.
    pub fn retry(reason: impl fmt::Display) -> Self {
        Self::Retry {
            reason: FinishReason::Handler(reason.to_string()),
            delay: None,
        }
    }

    /// "Abort" with this reason.
    pub fn abort(reason: impl fmt::Display) -> Self {
        Self::Abort {
            reason: FinishReason::Handler(reason.to_string()),
        }
    }

    /// "Defer" by `delay` with this reason.
    pub fn defer(delay: Duration, reason: impl fmt::Display) -> Self {
        Self::Defer {
            delay,
            reason: FinishReason::Handler(reason.to_string()),
        }
    }

    /// Whether no further attempt follows, whatever the retry limit:
    /// success, abort or panic.
    #[must_use]
    pub fn is_final(&self) -> bool {
        matches!(
            self,
            Self::Success | Self::Abort { .. } | Self::Panic { .. }
        )
    }
}

/// How a [`TaskError`] is classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Retry, optionally after a requested pause. What `?` gives.
    Retry {
        /// The requested pause.
        delay: Option<Duration>,
    },
    /// Do not retry.
    Abort,
    /// Run again after `delay`.
    Defer {
        /// When to run it again.
        delay: Duration,
    },
}

/// An error returned by a handler, with its classification stored next to
/// it.
///
/// `?` on any error gives a retry. Say "do not retry" explicitly:
///
/// ```
/// use taskcraft::{ErrorKind, ResultExt, TaskError};
///
/// fn parse(input: &str) -> Result<u32, TaskError> {
///     // A malformed input will not get better on retry.
///     let n: u32 = input.parse().or_abort()?;
///     Ok(n)
/// }
///
/// assert_eq!(parse("x").unwrap_err().kind(), ErrorKind::Abort);
/// ```
///
/// Like `anyhow::Error`, `TaskError` does not implement
/// [`std::error::Error`] itself; that is what lets every error convert into
/// it with `?`. The wrapped error is available through
/// [`source`](Self::source).
pub struct TaskError {
    kind: ErrorKind,
    source: BoxError,
}

impl TaskError {
    /// An error that must not be retried.
    pub fn abort(error: impl Into<BoxError>) -> Self {
        Self::with_kind(ErrorKind::Abort, error)
    }

    /// An error to retry after `delay`.
    pub fn retry_after(error: impl Into<BoxError>, delay: Duration) -> Self {
        Self::with_kind(ErrorKind::Retry { delay: Some(delay) }, error)
    }

    /// An error that defers the task by `delay`.
    pub fn defer(error: impl Into<BoxError>, delay: Duration) -> Self {
        Self::with_kind(ErrorKind::Defer { delay }, error)
    }

    /// An error with an explicit classification.
    pub fn with_kind(kind: ErrorKind, error: impl Into<BoxError>) -> Self {
        Self {
            kind,
            source: error.into(),
        }
    }

    /// The classification.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The wrapped error.
    #[must_use]
    pub fn source(&self) -> &(dyn Error + Send + Sync + 'static) {
        &*self.source
    }

    /// Unwraps the error.
    #[must_use]
    pub fn into_source(self) -> BoxError {
        self.source
    }
}

impl<E: Into<BoxError>> From<E> for TaskError {
    fn from(error: E) -> Self {
        Self::with_kind(ErrorKind::Retry { delay: None }, error)
    }
}

impl fmt::Debug for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskError")
            .field("kind", &self.kind)
            .field("source", &self.source)
            .finish()
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.source, f)
    }
}

impl From<TaskError> for Outcome {
    fn from(error: TaskError) -> Self {
        let reason = FinishReason::Handler(error.source.to_string());
        match error.kind {
            ErrorKind::Retry { delay } => Self::Retry { reason, delay },
            ErrorKind::Abort => Self::Abort { reason },
            ErrorKind::Defer { delay } => Self::Defer { delay, reason },
        }
    }
}

/// Classifying errors on a `Result` before `?`.
pub trait ResultExt<T> {
    /// On error: do not retry.
    ///
    /// # Errors
    ///
    /// The original error, classified as abort.
    fn or_abort(self) -> Result<T, TaskError>;

    /// On error: retry after `delay`.
    ///
    /// # Errors
    ///
    /// The original error, classified as retry with a pause.
    fn or_retry_after(self, delay: Duration) -> Result<T, TaskError>;

    /// On error: defer the task by `delay`.
    ///
    /// # Errors
    ///
    /// The original error, classified as defer.
    fn or_defer(self, delay: Duration) -> Result<T, TaskError>;
}

impl<T, E: Into<BoxError>> ResultExt<T> for Result<T, E> {
    fn or_abort(self) -> Result<T, TaskError> {
        self.map_err(TaskError::abort)
    }

    fn or_retry_after(self, delay: Duration) -> Result<T, TaskError> {
        self.map_err(|e| TaskError::retry_after(e, delay))
    }

    fn or_defer(self, delay: Duration) -> Result<T, TaskError> {
        self.map_err(|e| TaskError::defer(e, delay))
    }
}

/// What a handler may return.
///
/// Implemented for `()`, [`Outcome`], `Result<(), TaskError>` and
/// `Result<Outcome, TaskError>`; any other return type does not compile.
#[diagnostic::on_unimplemented(
    message = "a handler cannot return `{Self}`",
    label = "not a handler outcome",
    note = "return (), Outcome, Result<(), TaskError> or Result<Outcome, TaskError>; `?` turns any error into TaskError"
)]
pub trait IntoOutcome {
    /// The outcome this value stands for.
    fn into_outcome(self) -> Outcome;
}

impl IntoOutcome for () {
    fn into_outcome(self) -> Outcome {
        Outcome::Success
    }
}

impl IntoOutcome for Outcome {
    fn into_outcome(self) -> Outcome {
        self
    }
}

impl IntoOutcome for Result<(), TaskError> {
    fn into_outcome(self) -> Outcome {
        self.map_or_else(Outcome::from, |()| Outcome::Success)
    }
}

impl IntoOutcome for Result<Outcome, TaskError> {
    fn into_outcome(self) -> Outcome {
        self.unwrap_or_else(Outcome::from)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn io_error() -> io::Error {
        io::Error::other("disk is gone")
    }

    fn question_mark() -> Result<(), TaskError> {
        Err(io_error())?;
        Ok(())
    }

    #[test]
    fn question_mark_means_retry() {
        assert_eq!(
            question_mark().into_outcome(),
            Outcome::Retry {
                reason: FinishReason::Handler("disk is gone".into()),
                delay: None
            }
        );
    }

    #[test]
    fn explicit_classifications() {
        let d = Duration::from_secs(5);
        let r: Result<(), io::Error> = Err(io_error());
        assert!(matches!(r.or_abort().into_outcome(), Outcome::Abort { .. }));
        let r: Result<(), io::Error> = Err(io_error());
        assert!(matches!(
            r.or_retry_after(d).into_outcome(),
            Outcome::Retry { delay: Some(x), .. } if x == d
        ));
        let r: Result<(), io::Error> = Err(io_error());
        assert!(matches!(
            r.or_defer(d).into_outcome(),
            Outcome::Defer { delay, .. } if delay == d
        ));
    }

    #[test]
    fn boxing_does_not_change_the_classification() {
        // The source is boxed into a general error type before and after
        // classification; the kind travels next to it, not inside it.
        let boxed: BoxError = Box::new(io_error());
        let abort = TaskError::abort(boxed);
        let reboxed: BoxError = abort.into_source();
        let again = TaskError::abort(reboxed);
        assert_eq!(again.kind(), ErrorKind::Abort);
        assert!(matches!(Outcome::from(again), Outcome::Abort { .. }));
        assert_eq!(
            TaskError::abort(io_error()).source().to_string(),
            "disk is gone"
        );
    }

    #[test]
    fn return_types() {
        assert_eq!(().into_outcome(), Outcome::Success);
        assert_eq!(Ok::<(), TaskError>(()).into_outcome(), Outcome::Success);
        assert_eq!(
            Ok::<Outcome, TaskError>(Outcome::abort("no")).into_outcome(),
            Outcome::abort("no")
        );
    }

    #[test]
    fn final_outcomes() {
        assert!(Outcome::Success.is_final());
        assert!(Outcome::abort("x").is_final());
        assert!(
            Outcome::Panic {
                message: "x".into()
            }
            .is_final()
        );
        assert!(!Outcome::retry("x").is_final());
        assert!(!Outcome::defer(Duration::ZERO, "x").is_final());
    }
}
