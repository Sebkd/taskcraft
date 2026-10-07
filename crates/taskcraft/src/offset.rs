//! The commit boundary of a log partition (spec 2.3.9 p. 5).

use std::collections::BTreeSet;

/// Tracks delivered and acknowledged offsets of one log partition and tells
/// up to which offset the partition may be committed.
///
/// Committing offset N says "everything up to N is done", so a task not yet
/// acknowledged holds back the commit of every later task of its partition.
///
/// ```
/// use taskcraft::source::OffsetTracker;
///
/// let mut partition = OffsetTracker::new();
/// for offset in 1..=3 {
///     partition.delivered(offset);
/// }
/// partition.acked(1);
/// partition.acked(3);
/// assert_eq!(partition.commit_point(), Some(1));
/// partition.acked(2);
/// assert_eq!(partition.commit_point(), Some(3));
/// ```
///
/// Memory is bounded by the number of offsets in flight.
#[derive(Debug, Clone, Default)]
pub struct OffsetTracker {
    start: Option<i64>,
    max: Option<i64>,
    pending: BTreeSet<i64>,
}

impl OffsetTracker {
    /// An empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a delivered offset.
    pub fn delivered(&mut self, offset: i64) {
        self.start = Some(self.start.map_or(offset, |s| s.min(offset)));
        self.max = Some(self.max.map_or(offset, |m| m.max(offset)));
        self.pending.insert(offset);
    }

    /// Records an acknowledged offset. Acking an offset twice, or one never
    /// delivered, changes nothing.
    pub fn acked(&mut self, offset: i64) {
        self.pending.remove(&offset);
    }

    /// The highest offset N such that every delivered offset up to N is
    /// acknowledged, or `None` while no such prefix exists.
    #[must_use]
    pub fn commit_point(&self) -> Option<i64> {
        let start = self.start?;
        match self.pending.first() {
            None => self.max,
            Some(&first) if first == start => None,
            Some(&first) => Some(first - 1),
        }
    }

    /// Number of delivered offsets not yet acknowledged.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tracker_has_nothing_to_commit() {
        assert_eq!(OffsetTracker::new().commit_point(), None);
    }

    #[test]
    fn a_gap_at_the_start_blocks_the_commit() {
        let mut t = OffsetTracker::new();
        t.delivered(10);
        t.delivered(11);
        t.acked(11);
        assert_eq!(t.commit_point(), None);
        t.acked(10);
        assert_eq!((t.commit_point(), t.in_flight()), (Some(11), 0));
    }

    #[test]
    fn boundary_moves_with_later_deliveries() {
        let mut t = OffsetTracker::new();
        for o in 1..=3 {
            t.delivered(o);
        }
        t.acked(1);
        t.acked(3);
        assert_eq!(t.commit_point(), Some(1));
        t.acked(2);
        assert_eq!(t.commit_point(), Some(3));
        t.delivered(4);
        assert_eq!(t.commit_point(), Some(3));
        t.acked(4);
        t.acked(4);
        assert_eq!(t.commit_point(), Some(4));
    }
}
