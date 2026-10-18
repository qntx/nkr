//! Relay resource limits (strfry-compatible defaults).

use serde::Deserialize;

use crate::event::EventLimits;

/// Per-connection and ingest limits. All fields carry strfry's defaults and
/// unknown keys are rejected.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Reject events whose `created_at` is more than this far in the future.
    pub reject_events_newer_than_seconds: u64,
    /// Reject events older than this many seconds.
    pub reject_events_older_than_seconds: u64,
    /// Reject ephemeral events older than this many seconds.
    pub reject_ephemeral_events_older_than_seconds: u64,
    /// Maximum number of filters in a single REQ.
    pub max_req_filter_size: usize,
    /// Maximum concurrently open subscriptions per connection.
    pub max_subs_per_connection: usize,
    /// Cap applied to a filter's `limit` when serving stored events.
    pub max_filter_limit: u64,
    /// Maximum number of `#x` tag conditions allowed per filter.
    pub max_tags_per_filter: usize,
    /// Cap applied to a filter's `limit` for COUNT responses.
    pub max_filter_limit_count: u64,
    /// Structural event limits enforced at ingest.
    pub event: EventLimits,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            reject_events_newer_than_seconds: 900,
            reject_events_older_than_seconds: 94_608_000,
            reject_ephemeral_events_older_than_seconds: 60,
            max_req_filter_size: 200,
            max_subs_per_connection: 200,
            max_filter_limit: 500,
            max_tags_per_filter: 3,
            max_filter_limit_count: 1_000_000,
            event: EventLimits::default(),
        }
    }
}
