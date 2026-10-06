//! The task model used only through the crate root, as a consumer would.

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde::{Deserialize, Serialize};
use taskcraft::{
    AckPoint, Lifecycle, MetadataRegistry, PushOutcome, RejectReason, Task, TaskState,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Customer {
    region: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Priority(u8);

#[derive(Clone, Debug)]
struct Report {
    month: &'static str,
}

#[test]
fn two_scenarios_share_one_registry() {
    let registry = MetadataRegistry::new()
        .register::<Customer>("billing.customer")
        .unwrap()
        .register::<Priority>("report.priority")
        .unwrap();

    // Different scenarios of one application carry different metadata types.
    let billing = Task::new("invoice-17").with_meta(Customer {
        region: "eu".into(),
    });
    let report = Task::new(Report { month: "2026-10" })
        .with_id("report-2026-10")
        .with_meta(Priority(5))
        .with_ack_point(AckPoint::OnCompletion);

    let billing_meta = registry.encode(billing.metadata()).unwrap();
    let report_meta = registry.encode(report.metadata()).unwrap();
    assert_eq!(
        billing_meta.keys().collect::<Vec<_>>(),
        ["billing.customer"]
    );
    assert_eq!(report_meta.keys().collect::<Vec<_>>(), ["report.priority"]);

    let restored = registry.decode(report_meta);
    assert_eq!(
        restored.resolve::<Priority>(&registry).unwrap(),
        Some(Priority(5))
    );
    assert_eq!(restored.resolve::<Customer>(&registry).unwrap(), None);
    assert_eq!(report.args().month, "2026-10");
    assert_eq!(report.ack_point(), Some(AckPoint::OnCompletion));
}

#[test]
fn lifecycle_and_push_outcomes() {
    let mut life = Lifecycle::queued();
    life.advance(TaskState::Accepted).unwrap();
    let err = life.advance(TaskState::Succeeded).unwrap_err();
    assert_eq!(
        err.to_string(),
        "transition accepted -> succeeded is not allowed"
    );
    assert_eq!(life.state(), TaskState::Accepted);

    let task = Task::new(()).with_id("dup");
    let outcome = PushOutcome::AlreadyRunning {
        id: task.id().clone(),
        state: life.state(),
    };
    assert_eq!(outcome.id().as_str(), "dup");
    let full = PushOutcome::Rejected {
        id: task.id().clone(),
        reason: RejectReason::SourceFull,
    };
    assert!(matches!(full, PushOutcome::Rejected { .. }));
}
