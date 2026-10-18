//! TOML configuration, strfry-style sections and defaults.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use nkr::limits::Limits;
use nkr_lmdb::StoreOptions;
use serde::Deserialize;

use crate::ServerError;

/// The whole `nkr.toml` document.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Database environment section.
    pub db: Db,
    /// Event acceptance limits.
    pub events: Events,
    /// Relay behaviour section.
    pub relay: Relay,
}

/// `[db]` — LMDB environment.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Db {
    /// Environment directory.
    pub path: PathBuf,
    /// LMDB map size in bytes.
    pub map_size: usize,
    /// Maximum concurrent read transactions.
    pub max_readers: u32,
    /// Disable kernel read-ahead for databases larger than RAM.
    pub no_read_ahead: bool,
}

/// `[events]` — event acceptance limits.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Events {
    /// Maximum canonical event size in bytes.
    pub max_event_size: usize,
    /// Reject events this many seconds in the future.
    pub reject_events_newer_than_seconds: u64,
    /// Reject events this many seconds in the past.
    pub reject_events_older_than_seconds: u64,
    /// Reject ephemeral events this many seconds in the past.
    pub reject_ephemeral_events_older_than_seconds: u64,
    /// Maximum tags per event.
    pub max_num_tags: usize,
    /// Maximum tag value size in bytes.
    pub max_tag_val_size: usize,
}

/// `[relay]` — network and runtime behaviour.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Relay {
    /// Bind address.
    pub bind: IpAddr,
    /// Bind port.
    pub port: u16,
    /// Header carrying the real client IP (empty = socket address).
    pub real_ip_header: String,
    /// Maximum inbound WebSocket payload in bytes.
    pub max_websocket_payload_size: usize,
    /// WebSocket ping interval in seconds.
    pub auto_ping_seconds: u64,
    /// Pending outbound bytes above which the connection is closed.
    pub max_pending_outbound_bytes: usize,
    /// Per-slice scan budget in microseconds.
    pub query_timeslice_budget_microseconds: u64,
    /// Subscription limits.
    pub limits: RelayLimits,
    /// Thread pool sizes.
    pub threads: Threads,
    /// WebSocket compression.
    pub compression: Compression,
    /// NIP-11 relay information.
    pub info: Info,
}

/// `[relay.limits]` — subscription limits.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RelayLimits {
    /// Maximum filters per REQ.
    pub max_req_filter_size: usize,
    /// Maximum open subscriptions per connection.
    pub max_subs_per_connection: usize,
    /// Per-filter limit cap for stored scans.
    pub max_filter_limit: u64,
    /// Maximum `#` conditions per filter.
    pub max_tags_per_filter: usize,
    /// Limit cap for COUNT responses.
    pub max_filter_limit_count: u64,
}

/// `[relay.threads]` — worker thread counts.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Threads {
    /// Scan reader threads.
    pub readers: usize,
    /// Monitor shard threads.
    pub monitors: usize,
}

/// `[relay.compression]` — permessage-deflate settings.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Compression {
    /// Negotiate permessage-deflate.
    pub enabled: bool,
    /// Keep the LZ77 sliding window between messages.
    pub sliding_window: bool,
}

/// `[relay.info]` — NIP-11 document fields.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Info {
    /// Relay name.
    pub name: String,
    /// Plain-text description.
    pub description: String,
    /// Relay operator pubkey (hex).
    pub pubkey: String,
    /// Relay operator contact.
    pub contact: String,
    /// Relay icon URL.
    pub icon: String,
}

impl Default for Db {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./nkr-db"),
            map_size: StoreOptions::default().map_size,
            max_readers: StoreOptions::default().max_readers,
            no_read_ahead: false,
        }
    }
}

impl Default for Events {
    fn default() -> Self {
        Self {
            max_event_size: 65_536,
            reject_events_newer_than_seconds: 900,
            reject_events_older_than_seconds: 94_608_000,
            reject_ephemeral_events_older_than_seconds: 60,
            max_num_tags: 2_000,
            max_tag_val_size: 1_024,
        }
    }
}

impl Default for Relay {
    fn default() -> Self {
        Self {
            bind: IpAddr::from([127, 0, 0, 1]),
            port: 7_777,
            real_ip_header: String::new(),
            max_websocket_payload_size: 131_072,
            auto_ping_seconds: 55,
            max_pending_outbound_bytes: 33_554_432,
            query_timeslice_budget_microseconds: 10_000,
            limits: RelayLimits::default(),
            threads: Threads::default(),
            compression: Compression::default(),
            info: Info::default(),
        }
    }
}

impl Default for RelayLimits {
    fn default() -> Self {
        Self {
            max_req_filter_size: 200,
            max_subs_per_connection: 200,
            max_filter_limit: 500,
            max_tags_per_filter: 3,
            max_filter_limit_count: 1_000_000,
        }
    }
}

impl Default for Threads {
    fn default() -> Self {
        Self {
            readers: 3,
            monitors: 3,
        }
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self {
            enabled: true,
            sliding_window: true,
        }
    }
}

impl Default for Info {
    fn default() -> Self {
        Self {
            name: "nkr".into(),
            description: String::new(),
            pubkey: String::new(),
            contact: String::new(),
            icon: String::new(),
        }
    }
}

impl Config {
    /// Parse a TOML file. `path == "-"` reads stdin.
    ///
    /// # Errors
    ///
    /// [`ServerError::Io`] on read failure, [`ServerError::Config`] on
    /// TOML syntax errors or unknown keys.
    pub fn load(path: &Path) -> Result<Self, ServerError> {
        let text = std::fs::read_to_string(path)?;
        toml::from_str(&text).map_err(|e| ServerError::Config(e.to_string()))
    }

    /// `nkr::limits::Limits` assembled from `[events]` + `[relay.limits]`.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        Limits {
            event: nkr::event::EventLimits {
                max_event_size: self.events.max_event_size,
                max_num_tags: self.events.max_num_tags,
                max_tag_val_size: self.events.max_tag_val_size,
            },
            reject_events_newer_than_seconds: self.events.reject_events_newer_than_seconds,
            reject_events_older_than_seconds: self.events.reject_events_older_than_seconds,
            reject_ephemeral_events_older_than_seconds: self
                .events
                .reject_ephemeral_events_older_than_seconds,
            max_req_filter_size: self.relay.limits.max_req_filter_size,
            max_subs_per_connection: self.relay.limits.max_subs_per_connection,
            max_filter_limit: self.relay.limits.max_filter_limit,
            max_tags_per_filter: self.relay.limits.max_tags_per_filter,
            max_filter_limit_count: self.relay.limits.max_filter_limit_count,
        }
    }

    /// `StoreOptions` assembled from `[db]`.
    #[must_use]
    pub const fn store_options(&self) -> StoreOptions {
        StoreOptions {
            map_size: self.db.map_size,
            max_readers: self.db.max_readers,
            no_read_ahead: self.db.no_read_ahead,
        }
    }
}
