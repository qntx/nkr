//! Relay resource limits (strfry-compatible defaults).

use serde::Deserialize;

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

/// The limits a session enforces: ingest ([`EventLimits`]) plus REQ-side
/// ([`ReqLimits`]).
#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    /// Event acceptance limits (structure plus `created_at` windows).
    pub event: EventLimits,
    /// REQ limits.
    pub req: ReqLimits,
}
