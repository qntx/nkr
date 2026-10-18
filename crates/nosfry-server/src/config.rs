//! TOML configuration, strfry-style sections and defaults.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use nosfry::event::EventLimits;
use nosfry::limits::{Limits, ReqLimits};
use nosfry_lmdb::StoreOptions;
use serde::Deserialize;

use crate::ServerError;

/// The whole `nosfry.toml` document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Database environment section.
    pub db: Db,
    /// Event acceptance limits (`nosfry::event::EventLimits` verbatim).
    pub events: EventLimits,
    /// Relay behaviour section.
    pub relay: Relay,
}

/// `[db]` — LMDB environment.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

/// `[relay]` — network and runtime behaviour.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
    /// REQ limits (`nosfry::limits::ReqLimits` verbatim).
    pub limits: ReqLimits,
    /// Thread pool sizes.
    pub threads: Threads,
    /// WebSocket compression.
    pub compression: Compression,
    /// NIP-11 relay information.
    pub info: Info,
}

/// `[relay.threads]` — worker thread counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Threads {
    /// Scan reader threads.
    pub readers: usize,
    /// Monitor shard threads.
    pub monitors: usize,
}

/// `[relay.compression]` — permessage-deflate settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Compression {
    /// Negotiate permessage-deflate.
    pub enabled: bool,
    /// Keep the LZ77 sliding window between messages.
    pub sliding_window: bool,
}

/// `[relay.info]` — NIP-11 document fields.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
            path: PathBuf::from("./nosfry-db"),
            map_size: StoreOptions::default().map_size,
            max_readers: StoreOptions::default().max_readers,
            no_read_ahead: false,
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
            limits: ReqLimits::default(),
            threads: Threads::default(),
            compression: Compression::default(),
            info: Info::default(),
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
            name: "nosfry".into(),
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
        Ok(toml::from_str(&text)?)
    }

    /// `nosfry::limits::Limits` assembled from `[events]` + `[relay.limits]`.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        Limits {
            event: self.events,
            req: self.relay.limits,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "unit tests may panic on failure")]
mod tests {
    use super::*;

    /// The shipped `nosfry.example.toml` parses to exactly `Config::default()`.
    #[test]
    fn example_toml_matches_defaults() {
        let text = include_str!("../../../nosfry.example.toml");
        let parsed: Config = toml::from_str(text).unwrap();
        assert_eq!(parsed, Config::default());
    }
}
