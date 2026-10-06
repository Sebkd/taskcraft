//! Counting deliveries to find duplicates and losses.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use crate::task::TaskId;

/// Duplicates and losses found by a [`DeliveryLedger`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LedgerReport {
    /// Tasks executed more than once, with the number of executions, sorted
    /// by id.
    pub duplicates: Vec<(TaskId, u32)>,
    /// Tasks pushed but never executed, sorted by id.
    pub lost: Vec<TaskId>,
}

impl LedgerReport {
    /// Whether every pushed task ran exactly once.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.duplicates.is_empty() && self.lost.is_empty()
    }
}

#[derive(Debug, Default)]
struct Entry {
    pushed: bool,
    executed: u32,
}

/// Records which tasks were pushed and how many times each one ran.
#[derive(Debug, Default)]
pub struct DeliveryLedger {
    entries: Mutex<BTreeMap<TaskId, Entry>>,
}

impl DeliveryLedger {
    /// An empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a pushed task.
    pub fn pushed(&self, id: &TaskId) {
        self.with(id, |e| e.pushed = true);
    }

    /// Records one execution of a task.
    pub fn executed(&self, id: &TaskId) {
        self.with(id, |e| e.executed += 1);
    }

    /// How many times the task ran.
    #[must_use]
    pub fn executions(&self, id: &TaskId) -> u32 {
        self.lock().get(id).map_or(0, |e| e.executed)
    }

    /// Total number of executions of all tasks.
    #[must_use]
    pub fn total_executions(&self) -> u32 {
        self.lock().values().map(|e| e.executed).sum()
    }

    /// The duplicates and losses so far.
    #[must_use]
    pub fn report(&self) -> LedgerReport {
        let entries = self.lock();
        LedgerReport {
            duplicates: entries
                .iter()
                .filter(|(_, e)| e.executed > 1)
                .map(|(id, e)| (id.clone(), e.executed))
                .collect(),
            lost: entries
                .iter()
                .filter(|(_, e)| e.pushed && e.executed == 0)
                .map(|(id, _)| id.clone())
                .collect(),
        }
    }

    fn with(&self, id: &TaskId, f: impl FnOnce(&mut Entry)) {
        f(self.lock().entry(id.clone()).or_default());
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<TaskId, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_lists_duplicates_and_losses_sorted() {
        let ledger = DeliveryLedger::new();
        let [a, b, c, d] = ["a", "b", "c", "d"].map(TaskId::new);
        for id in [&d, &c, &b, &a] {
            ledger.pushed(id);
        }
        ledger.executed(&a);
        ledger.executed(&c);
        ledger.executed(&c);
        ledger.executed(&c);
        ledger.executed(&b);
        ledger.executed(&b);

        let report = ledger.report();
        assert_eq!(report.duplicates, [(b, 2), (c.clone(), 3)]);
        assert_eq!(report.lost, [d]);
        assert!(!report.is_clean());
        assert_eq!((ledger.executions(&c), ledger.total_executions()), (3, 6));
    }

    #[test]
    fn exactly_once_is_clean() {
        let ledger = DeliveryLedger::new();
        let id = TaskId::new("x");
        ledger.pushed(&id);
        ledger.executed(&id);
        assert!(ledger.report().is_clean());
    }
}
