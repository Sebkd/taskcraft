//! Task states and the transitions between them (spec 2.4.1).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::InvalidTransition;

/// The state of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// Pushed into a source that accepts pushes; not yet handed out by a poll.
    Queued,
    /// In the task registry, waiting for a slot and pool permits.
    Accepted,
    /// An attempt is running.
    Running,
    /// The attempt asked to retry; the retry pause is running.
    RetryWaiting,
    /// Handed back to the source with a delay. Over for this process: the
    /// source delivers it again as a new record.
    Deferred,
    /// The handler succeeded.
    Succeeded,
    /// The task failed: aborted, out of retries, bad metadata or timed out.
    Failed,
    /// The handler panicked.
    Panicked,
    /// Cancelled by request, by shutdown, by an overflow reject in a store, or
    /// because the lease was lost.
    Cancelled,
}

/// Every allowed pair, numbered as in spec 2.4.1. The entry transitions
/// (2.4.1.1 from nothing, 2.4.1.12) are [`Lifecycle`] constructors.
const TRANSITIONS: [(TaskState, TaskState); 12] = {
    use TaskState::{
        Accepted, Cancelled, Deferred, Failed, Panicked, Queued, RetryWaiting, Running, Succeeded,
    };
    [
        (Queued, Accepted),        // 2.4.1.1
        (Accepted, Running),       // 2.4.1.2
        (Accepted, Cancelled),     // 2.4.1.3
        (Running, Succeeded),      // 2.4.1.4
        (Running, RetryWaiting),   // 2.4.1.5
        (Running, Failed),         // 2.4.1.6
        (Running, Panicked),       // 2.4.1.7
        (Running, Cancelled),      // 2.4.1.8
        (Running, Deferred),       // 2.4.1.9
        (RetryWaiting, Accepted),  // 2.4.1.10
        (RetryWaiting, Cancelled), // 2.4.1.11
        (Queued, Cancelled),       // 2.4.1.13
    ]
};

impl TaskState {
    /// Every state, in specification order.
    pub const ALL: [Self; 9] = [
        Self::Queued,
        Self::Accepted,
        Self::Running,
        Self::RetryWaiting,
        Self::Deferred,
        Self::Succeeded,
        Self::Failed,
        Self::Panicked,
        Self::Cancelled,
    ];

    /// Whether this is a final state: succeeded, failed, panicked or cancelled.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Panicked | Self::Cancelled
        )
    }

    /// Whether the task is over for this process: a terminal state or
    /// [`Deferred`](Self::Deferred).
    #[must_use]
    pub const fn ends_in_process(self) -> bool {
        self.is_terminal() || matches!(self, Self::Deferred)
    }

    /// Whether the lifecycle table allows going from `self` to `to`.
    #[must_use]
    pub fn can_transition_to(self, to: Self) -> bool {
        TRANSITIONS.contains(&(self, to))
    }

    /// The lower-case name used in `Display` and serialization.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::RetryWaiting => "retry_waiting",
            Self::Deferred => "deferred",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Panicked => "panicked",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The current state of one task, changed only along the lifecycle table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lifecycle {
    state: TaskState,
}

impl Lifecycle {
    /// A task just pushed into a source (transition 2.4.1.12).
    #[must_use]
    pub const fn queued() -> Self {
        Self {
            state: TaskState::Queued,
        }
    }

    /// A task accepted straight from a poll, recovery or a lease takeover
    /// (transition 2.4.1.1 from the start).
    #[must_use]
    pub const fn accepted() -> Self {
        Self {
            state: TaskState::Accepted,
        }
    }

    /// The current state.
    #[must_use]
    pub const fn state(&self) -> TaskState {
        self.state
    }

    /// Moves to `to` if the lifecycle table allows it.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidTransition`] and leaves the state unchanged when the
    /// table has no `current -> to` transition.
    pub fn advance(&mut self, to: TaskState) -> Result<(), InvalidTransition> {
        if self.state.can_transition_to(to) {
            self.state = to;
            Ok(())
        } else {
            Err(InvalidTransition {
                from: self.state,
                to,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use TaskState::{
        Accepted, Cancelled, Deferred, Failed, Panicked, Queued, RetryWaiting, Running, Succeeded,
    };

    const ALLOWED: [(TaskState, TaskState); 12] = [
        (Queued, Accepted),
        (Accepted, Running),
        (Accepted, Cancelled),
        (Running, Succeeded),
        (Running, RetryWaiting),
        (Running, Failed),
        (Running, Panicked),
        (Running, Cancelled),
        (Running, Deferred),
        (RetryWaiting, Accepted),
        (RetryWaiting, Cancelled),
        (Queued, Cancelled),
    ];

    #[test]
    fn exactly_the_table_pairs_are_allowed() {
        for from in TaskState::ALL {
            for to in TaskState::ALL {
                assert_eq!(
                    from.can_transition_to(to),
                    ALLOWED.contains(&(from, to)),
                    "{from} -> {to}"
                );
            }
        }
    }

    #[test]
    fn rejected_transition_leaves_state_unchanged() {
        let mut life = Lifecycle::accepted();
        assert_eq!(
            life.advance(Succeeded),
            Err(InvalidTransition {
                from: Accepted,
                to: Succeeded
            })
        );
        assert_eq!(life.state(), Accepted);
    }

    #[test]
    fn no_way_out_of_states_that_end_the_task() {
        for from in TaskState::ALL.into_iter().filter(|s| s.ends_in_process()) {
            for to in TaskState::ALL {
                assert!(!from.can_transition_to(to), "{from} -> {to}");
            }
        }
    }

    #[test]
    fn queued_never_jumps_to_running() {
        let mut life = Lifecycle::queued();
        assert!(life.advance(Running).is_err());
        assert_eq!(life.state(), Queued);
    }

    #[test]
    fn full_retry_cycle() {
        let mut life = Lifecycle::queued();
        for to in [
            Accepted,
            Running,
            RetryWaiting,
            Accepted,
            Running,
            Succeeded,
        ] {
            life.advance(to).unwrap();
        }
        assert_eq!(life.state(), Succeeded);
    }

    #[test]
    fn terminal_and_in_process_sets() {
        let terminal: Vec<_> = TaskState::ALL
            .into_iter()
            .filter(|s| s.is_terminal())
            .collect();
        assert_eq!(terminal, [Succeeded, Failed, Panicked, Cancelled]);
        assert!(Deferred.ends_in_process() && !Deferred.is_terminal());
    }

    #[test]
    fn display_matches_serde_name() {
        for s in TaskState::ALL {
            assert_eq!(serde_json::to_value(s).unwrap(), s.as_str());
        }
    }
}
