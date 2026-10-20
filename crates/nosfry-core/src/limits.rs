//! Relay resource limits (strfry-compatible defaults).

use std::sync::{Arc, Mutex, PoisonError};

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
    /// Cap on index entries a `COUNT` scan may examine; `0` disables
    /// `COUNT` entirely. The count itself is never capped — hitting
    /// the budget marks the answer `"approximate": true`.
    pub max_filter_limit_count: u64,
    /// Maximum number of `#x` tag conditions allowed per filter.
    pub max_tags_per_filter: usize,
}

impl Default for ReqLimits {
    fn default() -> Self {
        Self {
            max_req_filter_size: 200,
            max_subs_per_connection: 200,
            max_filter_limit: 500,
            max_filter_limit_count: 1_000_000,
            max_tags_per_filter: 3,
        }
    }
}

/// Per-connection token-bucket rate limits for EVENT and REQ messages,
/// plus shared per-address buckets the adapter hands to every session
/// of one client address. A `*_per_second` of 0 disables that bucket.
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
    /// EVENT messages accepted per second across every connection of
    /// one client address.
    pub ip_events_per_second: u32,
    /// Shared EVENT burst capacity per client address.
    pub ip_events_burst: u32,
    /// REQ messages accepted per second across every connection of
    /// one client address.
    pub ip_reqs_per_second: u32,
    /// Shared REQ burst capacity per client address.
    pub ip_reqs_burst: u32,
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            events_per_second: 20,
            events_burst: 100,
            reqs_per_second: 20,
            reqs_burst: 100,
            ip_events_per_second: 100,
            ip_events_burst: 500,
            ip_reqs_per_second: 100,
            ip_reqs_burst: 500,
        }
    }
}

/// Token buckets shared by every connection of one client address
/// (built from the `ip_*` rate limits). A `None` bucket is disabled;
/// `Default` gives a fully disabled pair.
#[derive(Debug, Default)]
pub struct AddressBuckets {
    events: Option<Mutex<TokenBucket>>,
    reqs: Option<Mutex<TokenBucket>>,
}

impl AddressBuckets {
    /// Buckets for the shared `ip_*` limits of `rate`.
    #[must_use]
    pub fn new(rate: &RateLimits) -> Self {
        Self {
            events: (rate.ip_events_per_second > 0).then(|| {
                Mutex::new(TokenBucket::new(
                    rate.ip_events_per_second,
                    rate.ip_events_burst,
                ))
            }),
            reqs: (rate.ip_reqs_per_second > 0).then(|| {
                Mutex::new(TokenBucket::new(
                    rate.ip_reqs_per_second,
                    rate.ip_reqs_burst,
                ))
            }),
        }
    }

    /// Spend one shared EVENT token at `now_ms`; `true` when the
    /// bucket is disabled.
    pub fn take_event(&self, now_ms: u64) -> bool {
        self.events.as_ref().is_none_or(|bucket| {
            bucket
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(now_ms)
        })
    }

    /// Spend one shared REQ token at `now_ms`; `true` when the
    /// bucket is disabled.
    pub fn take_req(&self, now_ms: u64) -> bool {
        self.reqs.as_ref().is_none_or(|bucket| {
            bucket
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(now_ms)
        })
    }

    /// Every enabled bucket is back at full capacity (disabled buckets
    /// count as full): an address with no live connections has cooled
    /// off, and dropping its state cannot grant a fresh burst.
    pub fn is_idle(&self, now_ms: u64) -> bool {
        self.events.as_ref().is_none_or(|bucket| {
            bucket
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_full(now_ms)
        }) && self.reqs.as_ref().is_none_or(|bucket| {
            bucket
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_full(now_ms)
        })
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

    /// Whether the balance plus the refill accrued since `last_ms`
    /// reaches capacity; a clock that goes backwards refills nothing.
    /// Does not mutate.
    pub(crate) const fn is_full(&self, now_ms: u64) -> bool {
        let refill = now_ms
            .saturating_sub(self.last_ms)
            .saturating_mul(self.refill_ms);
        self.tokens.saturating_add(refill) >= self.capacity
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
    /// Per-connection and per-address EVENT/REQ rate limits.
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
    fn bucket_is_full_only_at_capacity() {
        let mut bucket = TokenBucket::new(10, 3);
        assert!(bucket.is_full(0), "a fresh bucket is full");
        assert!(bucket.take(0));
        assert!(!bucket.is_full(0));
        assert!(!bucket.is_full(99), "0.99 of a token is not full");
        assert!(bucket.is_full(100), "one token refilled in 100 ms");
        // `is_full` does not mutate: a second check agrees.
        assert!(bucket.is_full(100));
    }

    /// `is_idle` is true only when every enabled bucket refilled;
    /// disabled buckets count as full.
    #[test]
    fn address_buckets_is_idle() {
        assert!(AddressBuckets::default().is_idle(0));
        let rate = RateLimits {
            events_per_second: 0,
            events_burst: 0,
            reqs_per_second: 0,
            reqs_burst: 0,
            ip_events_per_second: 1,
            ip_events_burst: 2,
            ip_reqs_per_second: 0,
            ip_reqs_burst: 0,
        };
        let buckets = AddressBuckets::new(&rate);
        assert!(buckets.is_idle(0), "fresh buckets are full");
        assert!(buckets.take_event(0));
        assert!(!buckets.is_idle(0));
        assert!(buckets.is_idle(2_000), "2 tokens at 1/s: idle after 2 s");
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
