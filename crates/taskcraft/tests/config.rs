//! Criterion 37: every rule of spec 2.10 on its own gives a configuration
//! error with the message of 2.10. Rules of the store and the Kafka source
//! are checked in their packages; "reject without a hook" cannot be written
//! (`reject_with` takes the hook).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use taskcraft::{
    AckPoint, ConfigError, IdentityCodec, InMemorySource, MetadataRegistry, Monitor, PollStrategy,
    Queue, QueueBuilder, RetryPolicy, TRACE_PARENT, TaskFn, task_fn,
};

type Builder = QueueBuilder<
    InMemorySource<u32>,
    IdentityCodec<u32>,
    TaskFn<fn(u32) -> std::future::Ready<()>, u32, ()>,
    u32,
>;

fn noop(_: u32) -> std::future::Ready<()> {
    std::future::ready(())
}

fn queue(name: &str) -> Builder {
    Queue::builder(
        name,
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(noop as fn(u32) -> std::future::Ready<()>),
    )
    .no_recovery()
}

fn message<T>(result: Result<T, ConfigError>) -> String {
    match result {
        Ok(_) => panic!("expected a configuration error"),
        Err(e) => e.to_string(),
    }
}

fn retry(change: impl FnOnce(&mut RetryPolicy)) -> String {
    let mut policy = RetryPolicy::default();
    change(&mut policy);
    message(queue("q").retry_policy(policy).build())
}

#[derive(Clone, Serialize, Deserialize)]
struct Region(String);

#[test]
fn every_rule_of_2_10() {
    let rules: Vec<(&str, String)> = vec![
        ("queue name must not be empty", message(queue("").build())),
        (
            "duplicate queue name",
            message(
                Monitor::new()
                    .register(queue("q").build().unwrap())
                    .unwrap()
                    .register(queue("q").build().unwrap()),
            ),
        ),
        (
            "duration must be positive",
            message(queue("q").cancel_grace(Duration::ZERO).build()),
        ),
        (
            "duration must be positive",
            message(queue("q").attempt_timeout(Duration::ZERO).build()),
        ),
        (
            "duration must be positive",
            message(
                queue("q")
                    .poll_strategy(PollStrategy::Interval(Duration::ZERO))
                    .build(),
            ),
        ),
        (
            "capacity must be at least 1",
            message(InMemorySource::<u32>::new(0)),
        ),
        (
            "concurrency must be at least 1",
            message(queue("q").concurrency(0).build()),
        ),
        (
            "ack-on-accept without a store requires a recovery hook",
            message(
                Queue::builder(
                    "q",
                    Arc::new(InMemorySource::<u32>::default()),
                    IdentityCodec::new(),
                    task_fn(noop as fn(u32) -> std::future::Ready<()>),
                )
                .ack_point(AckPoint::OnAccept)
                .build(),
            ),
        ),
        (
            "max attempts must be at least 1",
            retry(|p| p.max_attempts = 0),
        ),
        (
            "backoff factor must be at least 1",
            retry(|p| p.factor = 0.5),
        ),
        (
            "base delay exceeds max delay",
            retry(|p| {
                p.base = Duration::from_secs(10);
                p.max = Duration::from_secs(1);
            }),
        ),
        ("jitter must be within 0..1", retry(|p| p.jitter = 1.5)),
        (
            "unknown pool",
            message(Monitor::new().register(queue("q").pool("missing", 1).build().unwrap())),
        ),
        (
            "permits exceed pool size",
            message(
                Monitor::new()
                    .pool("p", 1)
                    .unwrap()
                    .register(queue("q").pool("p", 2).build().unwrap()),
            ),
        ),
        (
            "pool size must be at least 1",
            message(Monitor::new().pool("p", 0)),
        ),
        (
            "duplicate metadata name",
            message(
                MetadataRegistry::new()
                    .register::<Region>("x")
                    .unwrap()
                    .register::<String>("x"),
            ),
        ),
        (
            "reserved metadata name",
            message(MetadataRegistry::new().register::<Region>(TRACE_PARENT)),
        ),
        (
            "restart delay exceeds max",
            message(Monitor::new().restart_delays(Duration::from_secs(5), Duration::from_secs(1))),
        ),
    ];
    for (expected, actual) in rules {
        assert!(
            actual.contains(expected),
            "expected {expected:?}, got {actual:?}"
        );
    }
}
