//! Test harness: a source with scripted failures, a delivery ledger and
//! reusable scenarios for workers. Enabled by the `test-util` feature.
//!
//! - [`FaultySource`] fails exactly where the test says: poison messages,
//!   failing polls, failing acks that put the task back for redelivery.
//! - [`DeliveryLedger`] reports tasks run more than once or never.
//! - [`scenarios`] check a worker, through the small [`Runner`] interface,
//!   against the defects this kind of design is known to invite. Run them
//!   with `#[tokio::test(start_paused = true)]`: time is virtual.

mod ledger;
mod runner;
pub mod scenarios;
mod source;

pub use ledger::{DeliveryLedger, LedgerReport};
pub use runner::{Runner, RunnerSetup, ScenarioHandler};
pub use scenarios::ScenarioFailure;
pub use source::{FaultyCodec, FaultySource, InjectedError, Scripted};
/// Re-exported for building a [`RunnerSetup`].
pub use tokio_util::sync::CancellationToken;
