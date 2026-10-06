//! Standard tower layers on top of a handler, without adapters.

use std::sync::Arc;
use std::time::Duration;

use taskcraft::{
    FinishReason, MetadataRegistry, Outcome, ResultExt, SharedData, Task, TaskError, TaskRequest,
    run_attempt, task_fn,
};
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;

fn request(args: u64) -> TaskRequest<u64> {
    TaskRequest::new(
        Task::new(args),
        Arc::new(MetadataRegistry::new()),
        Arc::new(SharedData::new()),
    )
}

/// Sleeps for `ms` milliseconds; a zero sleep means "the input is bad".
async fn sleepy(ms: u64) -> Result<(), TaskError> {
    if ms == 0 {
        return Err(std::io::Error::other("bad input")).or_abort();
    }
    tokio::time::sleep(Duration::from_millis(ms)).await;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn timeout_and_concurrency_layers_work_as_is() {
    let mut service = ServiceBuilder::new()
        .layer(ConcurrencyLimitLayer::new(2))
        .layer(TimeoutLayer::new(Duration::from_secs(1)))
        .service(task_fn(sleepy));

    assert_eq!(
        run_attempt(&mut service, request(10)).await,
        Outcome::Success
    );

    // The timeout layer turns the error type into a boxed error; that is an
    // unclassified error, so a retry.
    match run_attempt(&mut service, request(5_000)).await {
        Outcome::Retry {
            reason: FinishReason::Handler(text),
            delay: None,
        } => {
            assert!(text.contains("timed out"), "{text}");
        }
        other => panic!("expected a retry, got {other:?}"),
    }

    // An abort travels in the response, so wrapping layers cannot lose it.
    assert!(matches!(
        run_attempt(&mut service, request(0)).await,
        Outcome::Abort { .. }
    ));
}
