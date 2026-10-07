//! Monitor pools taken by attempts (spec 2.8).

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::observe::{Event, Observers};

/// Permits of one monitor pool taken by every attempt of a queue.
#[derive(Debug, Clone)]
pub(crate) struct PoolClaim {
    pub(crate) name: Arc<str>,
    pub(crate) size: u32,
    pub(crate) semaphore: Arc<Semaphore>,
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
        held.report();
        held
    }

    fn report(&self) {
        let free = u32::try_from(self.claim.semaphore.available_permits()).unwrap_or(u32::MAX);
        self.observers.emit(&Event::PoolUsage {
            pool: &self.claim.name,
            in_use: self.claim.size.saturating_sub(free),
            total: self.claim.size,
        });
    }
}

impl Drop for PoolHeld {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.report();
    }
}
