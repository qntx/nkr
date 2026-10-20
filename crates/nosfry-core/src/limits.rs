//! Relay resource limits (strfry-compatible defaults).

use std::sync::Arc;

use serde::Deserialize;

use crate::auth::AuthRules;
use crate::event::EventLimits;

/// REQ-side limits: filter shape and subscription count. All fields
/// carry strfry's defaults and unknown keys are rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReqLimits {
    /// Maximum number of filters in a single REQ.
    pub max_req_filter_size: usize,
    /// Maximum concurrently open subscriptions per connection.
    pub max_subs_per_connection: usize,
    /// Cap applied to a filter's `limit` when serving stored events.
    pub max_filter_limit: u64,
    /// Maximum number of `#x` tag conditions allowed per filter.
    pub max_tags_per_filter: usize,
}

impl Default for ReqLimits {
    fn default() -> Self {
        Self {
            max_req_filter_size: 200,
            max_subs_per_connection: 200,
            max_filter_limit: 500,
            max_tags_per_filter: 3,
        }
    }
}

/// Per-connection token-bucket rate limits for EVENT and REQ messages.
/// A `*_per_second` of 0 disables that bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateLimits {
    /// EVENT messages accepted per second, sustained.
    pub events_per_second: u32,
    /// EVENT burst capacity: the bucket starts full and refills at
    /// `events_per_second`.
    pub events_burst: u32,
    /// REQ messages accepted per second, sustained.
    pub reqs_per_second: u32,
    /// REQ burst capacity.
    pub reqs_burst: u32,
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            events_per_second: 20,
            events_burst: 100,
            reqs_per_second: 20,
            reqs_burst: 100,
        }
    }
}

/// A token bucket in milli-tokens (1 token = 1000 milli-tokens) so a
/// sub-token refill per millisecond needs no floats. Starts full; a
/// clock that goes backwards refills nothing.
#[derive(Debug, Clone)]
pub(crate) struct TokenBucket {
    /// Milli-tokens refilled per elapsed millisecond (= rate per second).
    refill_ms: u64,
    /// Bucket capacity in milli-tokens (= burst * 1000).
    capacity: u64,
    /// Current balance in milli-tokens.
    tokens: u64,
    /// Timestamp of the last refill, unix milliseconds.
    last_ms: u64,
}

impl TokenBucket {
    /// A full bucket of `burst` tokens refilling at `per_second`.
    /// `per_second` of 0 means "disabled" and is the caller's job.
    pub(crate) fn new(per_second: u32, burst: u32) -> Self {
        Self {
            refill_ms: u64::from(per_second),
            capacity: u64::from(burst) * 1000,
            tokens: u64::from(burst) * 1000,
            last_ms: 0,
        }
    }

    /// Spend one token at `now_ms`; `false` when the bucket is empty.
    pub(crate) fn take(&mut self, now_ms: u64) -> bool {
        if now_ms > self.last_ms {
            let refill = (now_ms - self.last_ms).saturating_mul(self.refill_ms);
            self.tokens = self.capacity.min(self.tokens.saturating_add(refill));
            self.last_ms = now_ms;
        }
        if self.tokens < 1000 {
            return false;
        }
        self.tokens -= 1000;
        true
    }
}

/// The limits a session enforces: ingest ([`EventLimits`]), REQ-side
/// ([`ReqLimits`]), per-connection rate limits ([`RateLimits`]), and
/// authentication rules ([`AuthRules`]).
#[derive(Debug, Clone, Default)]
pub struct Limits {
    /// Event acceptance limits (structure plus `created_at` windows).
    pub event: EventLimits,
    /// REQ limits.
    pub req: ReqLimits,
    /// Per-connection EVENT/REQ rate limits.
    pub rate: RateLimits,
    /// NIP-42 authentication and read restriction.
    pub auth: Arc<AuthRules>,
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "unit tests may panic on failure"
)]
mod tests {
    use super::*;

    #[test]
    fn bucket_burst_exhausts_then_refills() {
        let mut bucket = TokenBucket::new(10, 3);
        assert!(bucket.take(0));
        assert!(bucket.take(0));
        assert!(bucket.take(0));
        assert!(!bucket.take(0), "burst of 3 is exhausted in one instant");
        // +1000 ms at 10 tokens/s refills back to the capacity of 3.
        assert!(bucket.take(1_000));
        assert!(bucket.take(1_000));
        assert!(bucket.take(1_000));
        assert!(!bucket.take(1_000));
    }

    #[test]
    fn bucket_partial_refill() {
        let mut bucket = TokenBucket::new(10, 2);
        assert!(bucket.take(0) && bucket.take(0));
        // 150 ms -> 1.5 tokens: one take succeeds, one fails.
        assert!(bucket.take(150));
        assert!(!bucket.take(150));
    }

    #[test]
    fn bucket_saturates_at_capacity() {
        let mut bucket = TokenBucket::new(1_000, 5);
        assert!(bucket.take(0));
        // A huge jump refills no more than the burst capacity of 5.
        let later = u64::MAX / 2;
        for _ in 0..5 {
            assert!(bucket.take(later));
        }
        assert!(!bucket.take(later));
    }

    #[test]
    fn bucket_backwards_clock_refills_nothing() {
        let mut bucket = TokenBucket::new(10, 1);
        assert!(bucket.take(5_000));
        // Going backwards grants no refill and does not rewind `last`.
        assert!(!bucket.take(1_000));
        // Nor does the same timestamp.
        assert!(!bucket.take(5_000));
        assert!(bucket.take(6_000));
    }

    #[test]
    fn rate_limits_deserialize_defaults() {
        let limits: RateLimits = serde_json::from_str("{}").unwrap();
        assert_eq!(limits, RateLimits::default());
        let custom: RateLimits =
            serde_json::from_str("{\"events_per_second\":0,\"reqs_burst\":5}").unwrap();
        assert_eq!(custom.events_per_second, 0);
        assert_eq!(custom.reqs_per_second, 20);
        assert_eq!(custom.reqs_burst, 5);
        assert!(serde_json::from_str::<RateLimits>("{\"bogus\":1}").is_err());
    }
}
