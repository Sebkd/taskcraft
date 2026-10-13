//! Which offset each assigned partition may commit (spec 2.3.9 p. 5).
//!
//! Pure bookkeeping, no client: the source feeds it deliveries, acks and
//! rebalances, and commits what it answers.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use taskcraft::source::OffsetTracker;

/// Identifies one delivery: its partition, offset and the assignment of the
/// partition it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KafkaReceipt {
    pub(crate) partition: i32,
    pub(crate) offset: i64,
    pub(crate) assignment: u64,
}

impl KafkaReceipt {
    /// The partition.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// The offset.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

/// How long after a failed commit the boundary goes out again without a new
/// ack (change kafka-commit-retry).
pub(crate) const RETRY_PAUSE: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct Partition {
    assignment: u64,
    tracker: OffsetTracker,
    committed: Option<i64>,
    /// When a failed boundary may go out again; set on a failure.
    retry_at: Option<Instant>,
}

/// Offsets in flight per partition of the current assignment.
#[derive(Debug, Default)]
pub(crate) struct Commits {
    partitions: HashMap<i32, Partition>,
    assignments: u64,
}

impl Commits {
    /// Records a delivered offset; a partition seen for the first time
    /// starts a new assignment.
    pub(crate) fn delivered(&mut self, partition: i32, offset: i64) -> KafkaReceipt {
        let assignments = &mut self.assignments;
        let entry = self.partitions.entry(partition).or_insert_with(|| {
            *assignments += 1;
            Partition {
                assignment: *assignments,
                tracker: OffsetTracker::new(),
                committed: None,
                retry_at: None,
            }
        });
        entry.tracker.delivered(offset);
        KafkaReceipt {
            partition,
            offset,
            assignment: entry.assignment,
        }
    }

    /// Records an ack and returns the offset to commit when the commit point
    /// moved: the next offset to read, one past the last finished one. An ack
    /// from a revoked assignment changes nothing.
    pub(crate) fn acked(&mut self, receipt: KafkaReceipt) -> Option<i64> {
        let partition = self.partitions.get_mut(&receipt.partition)?;
        if partition.assignment != receipt.assignment {
            return None;
        }
        partition.tracker.acked(receipt.offset);
        let point = partition.tracker.commit_point()?;
        if partition.committed.is_some_and(|c| c >= point) {
            return None;
        }
        partition.committed = Some(point);
        partition.retry_at = None;
        Some(point + 1)
    }

    /// A commit up to `next` (the next offset to read) failed at `now`:
    /// unless a later commit already went out, the partition commits its
    /// boundary again on its next ack, or on [`retry`](Self::retry) after
    /// [`RETRY_PAUSE`] — whichever comes first.
    pub(crate) fn commit_failed(&mut self, partition: i32, next: i64, now: Instant) {
        if let Some(entry) = self.partitions.get_mut(&partition)
            && entry.committed == Some(next - 1)
        {
            entry.committed = None;
            entry.retry_at = Some(now + RETRY_PAUSE);
        }
    }

    /// The boundaries whose commit failed and whose pause is over at `now`,
    /// as (partition, next offset to read); each counts as sent again.
    pub(crate) fn retry(&mut self, now: Instant) -> Vec<(i32, i64)> {
        let mut due = Vec::new();
        for (&partition, entry) in &mut self.partitions {
            if entry.committed.is_some() || entry.retry_at.is_none_or(|at| at > now) {
                continue;
            }
            let Some(point) = entry.tracker.commit_point() else {
                continue;
            };
            entry.committed = Some(point);
            entry.retry_at = None;
            due.push((partition, point + 1));
        }
        due
    }

    /// Forgets revoked partitions: their deliveries in flight no longer
    /// commit anything; another consumer of the group gets them again.
    pub(crate) fn revoke(&mut self, partitions: impl IntoIterator<Item = i32>) {
        for partition in partitions {
            self.partitions.remove(&partition);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Criterion 23: offsets 1, 2, 3; 1 and 3 done → commit up to 1; 2 done
    /// → up to 3. Kafka stores the next offset to read.
    #[test]
    fn unfinished_offset_holds_back_the_commit() {
        let mut commits = Commits::default();
        let receipts: Vec<_> = (1..=3).map(|o| commits.delivered(0, o)).collect();
        assert_eq!(commits.acked(receipts[0]), Some(2));
        assert_eq!(commits.acked(receipts[2]), None);
        assert_eq!(commits.acked(receipts[1]), Some(4));
        assert_eq!(commits.acked(receipts[1]), None, "acked twice");
    }

    #[test]
    fn partitions_commit_independently() {
        let mut commits = Commits::default();
        let a = commits.delivered(0, 10);
        let b = commits.delivered(1, 20);
        let a2 = commits.delivered(0, 11);
        assert_eq!(commits.acked(b), Some(21));
        assert_eq!(commits.acked(a2), None);
        assert_eq!(commits.acked(a), Some(12));
    }

    /// Change criterion 2: a failed commit goes out again on the next ack.
    #[test]
    fn failed_commit_is_sent_again() {
        let mut commits = Commits::default();
        let first = commits.delivered(0, 1);
        let second = commits.delivered(0, 2);
        assert_eq!(commits.acked(first), Some(2));
        commits.commit_failed(0, 2, Instant::now());
        assert_eq!(commits.acked(second), Some(3), "boundary sent again");

        // An older failure does not undo a newer commit.
        commits.commit_failed(0, 2, Instant::now());
        let third = commits.delivered(0, 3);
        assert_eq!(commits.acked(third), Some(4));
        commits.commit_failed(7, 1, Instant::now()); // unknown partition: nothing happens
    }

    /// Change kafka-commit-retry, criterion 1: the last boundary of a
    /// partition failed and no ack follows; it goes out again after the
    /// pause, once.
    #[test]
    fn failed_last_boundary_is_retried_without_an_ack() {
        let mut commits = Commits::default();
        let only = commits.delivered(0, 1);
        assert_eq!(commits.acked(only), Some(2));
        let failed = Instant::now();
        commits.commit_failed(0, 2, failed);
        assert_eq!(commits.retry(failed), [], "not before the pause");
        assert_eq!(commits.retry(failed + RETRY_PAUSE), [(0, 2)]);
        assert_eq!(commits.retry(failed + 2 * RETRY_PAUSE), [], "sent once");

        // Failed again: retried again.
        let again = failed + 3 * RETRY_PAUSE;
        commits.commit_failed(0, 2, again);
        assert_eq!(commits.retry(again + RETRY_PAUSE), [(0, 2)]);
    }

    /// Change kafka-commit-retry, criterion 2: a later boundary committed by
    /// an ack replaces the failed one.
    #[test]
    fn a_later_commit_replaces_the_retry() {
        let mut commits = Commits::default();
        let first = commits.delivered(0, 1);
        let second = commits.delivered(0, 2);
        assert_eq!(commits.acked(first), Some(2));
        let failed = Instant::now();
        commits.commit_failed(0, 2, failed);
        assert_eq!(commits.acked(second), Some(3));
        assert_eq!(commits.retry(failed + RETRY_PAUSE), []);
    }

    #[test]
    fn a_revoked_partition_is_not_retried() {
        let mut commits = Commits::default();
        let only = commits.delivered(0, 1);
        assert_eq!(commits.acked(only), Some(2));
        let failed = Instant::now();
        commits.commit_failed(0, 2, failed);
        commits.revoke([0]);
        assert_eq!(commits.retry(failed + RETRY_PAUSE), []);
    }

    #[test]
    fn acks_from_a_revoked_assignment_are_ignored() {
        let mut commits = Commits::default();
        let old = commits.delivered(0, 5);
        commits.revoke([0]);
        assert_eq!(commits.acked(old), None);

        // Assigned again: the same offset is delivered anew.
        let new = commits.delivered(0, 5);
        assert_ne!(old, new);
        assert_eq!(commits.acked(old), None, "the old delivery stays ignored");
        assert_eq!(commits.acked(new), Some(6));
    }
}
