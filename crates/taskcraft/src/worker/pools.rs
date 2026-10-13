//! Monitor pools taken by attempts (spec 2.8).

#[cfg(not(taskcraft_loom))]
use std::sync::Mutex;
use std::sync::{Arc, PoisonError};

// Model checking: every interleaving of the gauge's critical sections.
#[cfg(taskcraft_loom)]
use loom::sync::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::observe::{Event, Observers};

/// Permits of one monitor pool taken by every attempt of a queue.
#[derive(Debug, Clone)]
pub(crate) struct PoolClaim {
    pub(crate) name: Arc<str>,
    pub(crate) size: u32,
    pub(crate) semaphore: Arc<Semaphore>,
    /// Permits held by attempts, shared by every queue of the pool. Not
    /// the semaphore's free count: a released permit may pass to a waiter
    /// that is then cancelled and gives it back without a report.
    pub(crate) in_use: Arc<Gauge>,
    pub(crate) permits: u32,
}

/// A pool's usage and who reports it (rule 2.3.22 p. 1). One thread at a
/// time reports, outside the lock: reports come in order and never hold up
/// a change. Changes made while a report is under way are reported by the
/// reporter, as one value; it stops once its last report is the current
/// value.
#[derive(Debug, Default)]
pub(crate) struct Gauge {
    state: Mutex<GaugeState>,
}

#[derive(Debug, Default)]
struct GaugeState {
    value: u32,
    reporting: bool,
}

impl Gauge {
    /// Applies `change`; reports the value through `report` unless another
    /// thread is reporting already.
    pub(crate) fn change(&self, change: impl FnOnce(u32) -> u32, mut report: impl FnMut(u32)) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.value = change(state.value);
        if state.reporting {
            return;
        }
        state.reporting = true;
        loop {
            let value = state.value;
            drop(state);
            report(value);
            state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.value == value {
                state.reporting = false;
                return;
            }
        }
    }
}

/// Permits of one pool held by an attempt; reports the pool's usage when
/// taken and when given back.
pub(super) struct PoolHeld {
    permit: Option<OwnedSemaphorePermit>,
    claim: PoolClaim,
    observers: Observers,
}

impl PoolHeld {
    pub(super) fn new(
        claim: &PoolClaim,
        permit: OwnedSemaphorePermit,
        observers: &Observers,
    ) -> Self {
        let held = Self {
            permit: Some(permit),
            claim: claim.clone(),
            observers: observers.clone(),
        };
        held.count(|n| n.saturating_add(claim.permits));
        held
    }

    /// Updates the pool's count; the reports of a pool come in order, the
    /// last one is its usage, and none holds the count locked.
    fn count(&self, change: impl FnOnce(u32) -> u32) {
        self.claim.in_use.change(change, |in_use| {
            self.observers.emit(&Event::PoolUsage {
                pool: &self.claim.name,
                in_use,
                total: self.claim.size,
            });
        });
    }
}

impl Drop for PoolHeld {
    fn drop(&mut self) {
        drop(self.permit.take());
        let permits = self.claim.permits;
        self.count(|n| n.saturating_sub(permits));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::observe::Observer;

    /// Records pool usage; sleeps on the first report.
    #[derive(Default)]
    struct Slow {
        seen: StdMutex<Vec<u32>>,
    }

    impl Observer for Slow {
        fn on_event(&self, event: &Event<'_>) {
            if let Event::PoolUsage { in_use, .. } = event {
                let first = {
                    let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
                    seen.push(*in_use);
                    seen.len() == 1
                };
                if first {
                    std::thread::sleep(Duration::from_millis(300));
                }
            }
        }
    }

    fn claim() -> PoolClaim {
        PoolClaim {
            name: "p".into(),
            size: 2,
            semaphore: Arc::new(Semaphore::new(2)),
            in_use: Arc::default(),
            permits: 1,
        }
    }

    /// Change pool-usage-events-outside-lock, criteria 1 and 2: another
    /// attempt takes and gives back a permit while an observer is busy with
    /// a pool event; the last event is the pool's usage.
    #[test]
    fn a_slow_observer_does_not_hold_the_pool_up() {
        let claim = claim();
        let slow = Arc::new(Slow::default());
        let observers = Observers::new(vec![Arc::clone(&slow) as Arc<dyn Observer>]);
        let permit = |claim: &PoolClaim| Arc::clone(&claim.semaphore).try_acquire_owned().unwrap();

        let first = std::thread::scope(|scope| {
            let a = scope.spawn(|| PoolHeld::new(&claim, permit(&claim), &observers));
            // A reports, and its observer sleeps.
            std::thread::sleep(Duration::from_millis(50));
            let started = Instant::now();
            let b = PoolHeld::new(&claim, permit(&claim), &observers);
            drop(b);
            let waited = started.elapsed();
            let a = a.join().unwrap();
            (a, waited)
        });
        let (a, waited) = first;
        assert!(waited < Duration::from_millis(100), "waited {waited:?}");
        drop(a);
        let seen = slow
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        assert_eq!(seen.last(), Some(&0), "{seen:?}");
        assert_eq!(seen.first(), Some(&1), "{seen:?}");
    }
}

/// Rule 2.3.22 p. 1 under every interleaving `loom` finds (change
/// pool-usage-events-outside-lock): reports never overlap, and the last one
/// is the final usage. Run with
/// `RUSTFLAGS="--cfg taskcraft_loom" cargo test -p taskcraft --release --lib loom`.
#[cfg(all(test, taskcraft_loom))]
mod loom_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};
    use loom::thread;

    use super::*;

    #[derive(Default)]
    struct Reports {
        busy: AtomicBool,
        values: Mutex<Vec<u32>>,
    }

    impl Reports {
        fn report(&self, value: u32) {
            assert!(!self.busy.swap(true, Ordering::SeqCst), "reports overlap");
            self.values
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(value);
            self.busy.store(false, Ordering::SeqCst);
        }
    }

    #[test]
    fn pool_reports_come_in_order_and_end_at_the_usage() {
        loom::model(|| {
            let gauge = Arc::new(Gauge::default());
            let reports = Arc::new(Reports::default());
            let changes: [fn(u32) -> u32; 3] = [|n| n + 1, |n| n + 1, |n| n.saturating_sub(1)];
            let threads: Vec<_> = changes
                .into_iter()
                .map(|change| {
                    let (gauge, reports) = (Arc::clone(&gauge), Arc::clone(&reports));
                    thread::spawn(move || gauge.change(change, |v| reports.report(v)))
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            let usage = gauge
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .value;
            let values = reports
                .values
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            assert_eq!(values.last(), Some(&usage), "{values:?}");
        });
    }
}
