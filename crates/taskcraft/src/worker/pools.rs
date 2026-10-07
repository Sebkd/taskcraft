//! Monitor pools taken by attempts (spec 2.8).

use std::sync::{Arc, Mutex, PoisonError};

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
    pub(crate) in_use: Arc<Mutex<u32>>,
    pub(crate) permits: u32,
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

    /// Updates the pool's count and reports it under one lock: the reports
    /// of a pool come in order, so the last one is its usage.
    fn count(&self, change: impl FnOnce(u32) -> u32) {
        let mut in_use = self
            .claim
            .in_use
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *in_use = change(*in_use);
        self.observers.emit(&Event::PoolUsage {
            pool: &self.claim.name,
            in_use: *in_use,
            total: self.claim.size,
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
