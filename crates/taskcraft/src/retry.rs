//! The retry policy: how many attempts and how long to pause between them
//! (spec 2.3.3–2.3.5).

use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::ConfigError;

/// How a queue retries tasks whose handler answered "retry".
///
/// ```
/// use std::time::Duration;
/// use taskcraft::RetryPolicy;
///
/// let policy = RetryPolicy {
///     max_attempts: 5,
///     base: Duration::from_secs(2),
///     ..RetryPolicy::default()
/// };
/// assert!(policy.validate().is_ok());
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// Runs of the handler at most, counting the first; "defer" does not use
    /// them up. 1 means no retries. Default 1.
    pub max_attempts: u32,
    /// The pause before the first retry. Default 1 s.
    pub base: Duration,
    /// How much the pause grows with every retry. Default 2.
    pub factor: f64,
    /// The longest pause; also caps a pause asked for by the handler.
    /// Default 5 min.
    pub max: Duration,
    /// Random spread of the pause, as a share of it: 0.1 means ±10 %.
    /// Not applied to a pause asked for by the handler. Default 0.1.
    pub jitter: f64,
    /// Keep the concurrency slot during the pause instead of releasing it.
    /// Pool permits are released either way. Default `false`.
    pub hold_slot: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            base: Duration::from_secs(1),
            factor: 2.0,
            max: Duration::from_secs(300),
            jitter: 0.1,
            hold_slot: false,
        }
    }
}

impl RetryPolicy {
    /// Checks the policy (spec 2.10).
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidRetryPolicy`] for no attempts, a factor below 1,
    /// a base above the maximum or a jitter outside 0..=1.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |reason| Err(ConfigError::InvalidRetryPolicy { reason });
        if self.max_attempts == 0 {
            return invalid("max attempts must be at least 1");
        }
        if !(self.factor.is_finite() && self.factor >= 1.0) {
            return invalid("backoff factor must be at least 1");
        }
        if self.base > self.max {
            return invalid("base delay exceeds max delay");
        }
        if !(0.0..=1.0).contains(&self.jitter) {
            return invalid("jitter must be within 0..1");
        }
        Ok(())
    }

    /// Whether a task that has been retried `retries` times may retry again.
    #[must_use]
    pub fn allows_retry(&self, retries: u32) -> bool {
        retries.saturating_add(1) < self.max_attempts
    }

    /// The pause before retry number `retries` (1 for the first retry), or
    /// the pause the handler asked for, capped by the maximum and without
    /// jitter (rule 2.3.4).
    #[must_use]
    pub fn pause(&self, retries: u32, requested: Option<Duration>) -> Duration {
        if let Some(requested) = requested {
            return requested.min(self.max);
        }
        let max = self.max.as_secs_f64();
        let exponent = i32::try_from(retries.saturating_sub(1)).unwrap_or(i32::MAX);
        let mut pause = (self.base.as_secs_f64() * self.factor.powi(exponent)).min(max);
        if !pause.is_finite() {
            pause = max;
        }
        if self.jitter > 0.0 {
            let spread = (2.0 * self.jitter).mul_add(random_unit(), 1.0 - self.jitter);
            pause = (pause * spread).clamp(0.0, max);
        }
        Duration::try_from_secs_f64(pause).unwrap_or(self.max)
    }
}

/// A number in [0, 1) for the jitter. Not for cryptography: only to keep
/// retries of many tasks from lining up.
fn random_unit() -> f64 {
    static CALLS: AtomicU64 = AtomicU64::new(0);
    let mut hasher = std::hash::RandomState::new().build_hasher();
    hasher.write_u64(CALLS.fetch_add(1, Ordering::Relaxed));
    let bits = hasher.finish() >> 11;
    #[allow(clippy::cast_precision_loss)] // 53 bits fit an f64 exactly
    let unit = bits as f64 / (1_u64 << 53) as f64;
    unit
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: Duration = Duration::from_secs(1);

    fn exact() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 10,
            base: SEC,
            factor: 2.0,
            max: 5 * SEC,
            jitter: 0.0,
            hold_slot: false,
        }
    }

    #[test]
    fn pauses_grow_up_to_the_max() {
        let policy = exact();
        let pauses: Vec<_> = (1..=5).map(|k| policy.pause(k, None)).collect();
        assert_eq!(pauses, [SEC, 2 * SEC, 4 * SEC, 5 * SEC, 5 * SEC]);
        assert_eq!(policy.pause(u32::MAX, None), 5 * SEC);
    }

    #[test]
    fn requested_pause_is_capped_and_has_no_jitter() {
        let policy = RetryPolicy {
            max: 300 * SEC,
            jitter: 1.0,
            ..exact()
        };
        assert_eq!(policy.pause(1, Some(600 * SEC)), 300 * SEC);
        assert_eq!(policy.pause(1, Some(7 * SEC)), 7 * SEC);
    }

    #[test]
    fn jitter_stays_in_range() {
        let policy = RetryPolicy {
            jitter: 0.5,
            max: 100 * SEC,
            base: 4 * SEC,
            ..exact()
        };
        for _ in 0..1000 {
            let pause = policy.pause(1, None);
            assert!((2 * SEC..=6 * SEC).contains(&pause), "{pause:?}");
        }
        let capped = RetryPolicy {
            jitter: 1.0,
            base: 5 * SEC,
            ..exact()
        };
        for _ in 0..1000 {
            assert!(capped.pause(1, None) <= 5 * SEC);
        }
    }

    #[test]
    fn retries_allowed_by_max_attempts() {
        let policy = RetryPolicy {
            max_attempts: 3,
            ..exact()
        };
        assert!(policy.allows_retry(0));
        assert!(policy.allows_retry(1));
        assert!(!policy.allows_retry(2));
        assert!(!RetryPolicy::default().allows_retry(0));
    }

    #[test]
    fn validation() {
        let cases = [
            (
                RetryPolicy {
                    max_attempts: 0,
                    ..exact()
                },
                "max attempts must be at least 1",
            ),
            (
                RetryPolicy {
                    factor: 0.5,
                    ..exact()
                },
                "backoff factor must be at least 1",
            ),
            (
                RetryPolicy {
                    factor: f64::NAN,
                    ..exact()
                },
                "backoff factor must be at least 1",
            ),
            (
                RetryPolicy {
                    base: 10 * SEC,
                    ..exact()
                },
                "base delay exceeds max delay",
            ),
            (
                RetryPolicy {
                    jitter: 1.5,
                    ..exact()
                },
                "jitter must be within 0..1",
            ),
        ];
        for (policy, reason) in cases {
            assert_eq!(
                policy.validate().unwrap_err().to_string(),
                format!("invalid retry policy: {reason}")
            );
        }
        assert!(RetryPolicy::default().validate().is_ok());
    }
}
