//! TOML configuration, strfry-style sections and defaults.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use nosfry_core::event::EventLimits;
use nosfry_core::limits::{Limits, RateLimits, ReqLimits};
use nosfry_lmdb::StoreOptions;
use serde::Deserialize;

use crate::ServerError;

/// The whole `nosfry.toml` document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Database environment section.
    pub db: Db,
    /// Event acceptance limits (`nosfry_core::event::EventLimits` verbatim).
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
    /// Soft open-file limit to raise to at startup, capped by the hard
    /// limit (0 = leave unchanged).
    pub nofiles: u64,
    /// Header carrying the real client IP; its last comma-separated
    /// value is trusted, so set it only behind a proxy that sets or
    /// appends the header (empty = socket address).
    pub real_ip_header: String,
    /// Maximum inbound WebSocket payload in bytes.
    pub max_websocket_payload_size: usize,
    /// WebSocket ping interval in seconds.
    pub auto_ping_seconds: u64,
    /// Pending outbound bytes above which the connection is closed.
    pub max_pending_outbound_bytes: usize,
    /// Process-wide pending outbound byte budget: when the sum of
    /// every connection's pending exceeds it, scans pause on
    /// connections already more than one chunk behind (restart-only;
    /// 0 = unlimited).
    pub max_pending_outbound_bytes_total: usize,
    /// Process-wide budget for sealed negentropy sync storages,
    /// accounted at 40 bytes per scanned record (restart-only;
    /// 0 = unlimited).
    pub max_sync_memory_bytes: usize,
    /// Maximum simultaneous WebSocket connections (0 = unlimited).
    pub max_connections: usize,
    /// Maximum simultaneous WebSocket connections per client IP
    /// (0 = unlimited). IPv4 counts per address; IPv6 per /64.
    pub max_connections_per_ip: usize,
    /// Serve `GET /metrics` (Prometheus text exposition format) on the
    /// relay port. Unauthenticated — keep it behind a reverse proxy or
    /// firewall.
    pub metrics: bool,
    /// Per-slice scan budget in microseconds.
    pub query_timeslice_budget_microseconds: u64,
    /// REQ limits (`nosfry_core::limits::ReqLimits` verbatim).
    pub limits: ReqLimits,
    /// Per-connection EVENT/REQ rate limits
    /// (`nosfry_core::limits::RateLimits` verbatim).
    pub rate_limits: RateLimits,
    /// Thread pool sizes.
    pub threads: Threads,
    /// WebSocket compression.
    pub compression: Compression,
    /// Write-policy plugin.
    pub write_policy: WritePolicy,
    /// NIP-42 authentication and restricted read kinds.
    pub auth: Auth,
    /// NIP-11 relay information.
    pub info: Info,
}

/// `[relay.auth]` — NIP-42 authentication (strfry `relay.auth.*`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Auth {
    /// The relay's canonical `ws://`/`wss://` URL clients sign into an
    /// AUTH event's `relay` tag. Empty disables AUTH: restricted kinds
    /// are stored but never served, and protected events are rejected.
    pub service_url: String,
    /// Kinds served only to authenticated readers.
    pub restricted_read_kinds: Vec<u16>,
    /// Restricted kinds are served only to authenticated pubkeys
    /// involved in the event (author or `p` tag); `false` serves them
    /// to any authenticated reader.
    pub restrict_read_to_involved: bool,
}

impl Default for Auth {
    fn default() -> Self {
        Self {
            service_url: String::new(),
            restricted_read_kinds: vec![4, 1059],
            restrict_read_to_involved: true,
        }
    }
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
    /// Negotiate permessage-deflate. Off by default: yawc bounds the
    /// *compressed* frame sizes only, so a small frame can inflate far
    /// beyond `max_websocket_payload_size`
    /// (<https://github.com/infinitefield/yawc/issues/55>) — enable it
    /// only for trusted clients.
    pub enabled: bool,
    /// Keep the LZ77 sliding window between messages.
    pub sliding_window: bool,
}

/// `[relay.write_policy]` — strfry-compatible write-policy plugin.
///
/// Plugins written for strfry run unchanged: each verified event is
/// sent to `/bin/sh -c <plugin>` as one JSON line and the plugin's
/// `accept`/`reject`/`shadowReject` answer decides its fate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WritePolicy {
    /// The plugin command (empty = off). A command without spaces is
    /// a path and reloads when the file's mtime changes.
    pub plugin: String,
    /// Seconds the plugin has to answer each request.
    pub timeout_seconds: u64,
}

impl Default for WritePolicy {
    fn default() -> Self {
        Self {
            plugin: String::new(),
            timeout_seconds: 10,
        }
    }
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
            nofiles: 1_000_000,
            real_ip_header: String::new(),
            max_websocket_payload_size: 131_072,
            auto_ping_seconds: 55,
            max_pending_outbound_bytes: 33_554_432,
            max_pending_outbound_bytes_total: 1_073_741_824,
            max_sync_memory_bytes: 1_073_741_824,
            max_connections: 0,
            max_connections_per_ip: 128,
            metrics: false,
            query_timeslice_budget_microseconds: 10_000,
            limits: ReqLimits::default(),
            rate_limits: RateLimits::default(),
            threads: Threads::default(),
            compression: Compression::default(),
            write_policy: WritePolicy::default(),
            auth: Auth::default(),
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
            enabled: false,
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
    /// TOML syntax errors or unknown keys, [`ServerError::Auth`] on an
    /// invalid `[relay.auth] service_url`.
    pub fn load(path: &Path) -> Result<Self, ServerError> {
        let text = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&text)?;
        config.limits()?;
        Ok(config)
    }

    /// `nosfry_core::limits::Limits` assembled from `[events]`,
    /// `[relay.limits]`, `[relay.rate_limits]`, and `[relay.auth]`.
    ///
    /// # Errors
    ///
    /// [`ServerError::Auth`] on an invalid `[relay.auth] service_url`.
    pub fn limits(&self) -> Result<Limits, ServerError> {
        Ok(Limits {
            event: self.events,
            req: self.relay.limits,
            rate: self.relay.rate_limits,
            auth: std::sync::Arc::new(nosfry_core::auth::AuthRules::new(
                &self.relay.auth.service_url,
                &self.relay.auth.restricted_read_kinds,
                self.relay.auth.restrict_read_to_involved,
            )?),
        })
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

    /// Merge a reloaded config into the running one: every restart-only
    /// field keeps the running value, everything else follows `new`.
    /// Returns the effective config plus the names of the restart-only
    /// keys that differed (the server warns about them once per reload).
    ///
    /// Restart-only: `[db]`, `relay.bind`, `relay.port`,
    /// `relay.nofiles`, `relay.metrics` (route registration),
    /// `relay.query_timeslice_budget_microseconds`, `relay.threads`,
    /// `relay.limits.max_filter_limit`,
    /// `relay.limits.max_filter_limit_count`,
    /// `relay.limits.max_sync_events`, and
    /// `relay.max_sync_memory_bytes` (reader threads hold them),
    /// `relay.max_pending_outbound_bytes_total` (ditto).
    #[must_use]
    pub fn reloadable_from(&self, mut new: Self) -> (Self, Vec<&'static str>) {
        let mut ignored = Vec::new();
        if new.db != self.db {
            ignored.push("db");
            new.db.clone_from(&self.db);
        }
        let running = &self.relay;
        let relay = &mut new.relay;
        if relay.bind != running.bind {
            ignored.push("relay.bind");
            relay.bind = running.bind;
        }
        if relay.port != running.port {
            ignored.push("relay.port");
            relay.port = running.port;
        }
        if relay.nofiles != running.nofiles {
            ignored.push("relay.nofiles");
            relay.nofiles = running.nofiles;
        }
        if relay.metrics != running.metrics {
            ignored.push("relay.metrics");
            relay.metrics = running.metrics;
        }
        if relay.query_timeslice_budget_microseconds != running.query_timeslice_budget_microseconds
        {
            ignored.push("relay.query_timeslice_budget_microseconds");
            relay.query_timeslice_budget_microseconds = running.query_timeslice_budget_microseconds;
        }
        if relay.threads != running.threads {
            ignored.push("relay.threads");
            relay.threads = running.threads;
        }
        if relay.limits.max_filter_limit != running.limits.max_filter_limit {
            ignored.push("relay.limits.max_filter_limit");
            relay.limits.max_filter_limit = running.limits.max_filter_limit;
        }
        if relay.limits.max_filter_limit_count != running.limits.max_filter_limit_count {
            ignored.push("relay.limits.max_filter_limit_count");
            relay.limits.max_filter_limit_count = running.limits.max_filter_limit_count;
        }
        if relay.limits.max_sync_events != running.limits.max_sync_events {
            ignored.push("relay.limits.max_sync_events");
            relay.limits.max_sync_events = running.limits.max_sync_events;
        }
        if relay.max_sync_memory_bytes != running.max_sync_memory_bytes {
            ignored.push("relay.max_sync_memory_bytes");
            relay.max_sync_memory_bytes = running.max_sync_memory_bytes;
        }
        if relay.max_pending_outbound_bytes_total != running.max_pending_outbound_bytes_total {
            ignored.push("relay.max_pending_outbound_bytes_total");
            relay.max_pending_outbound_bytes_total = running.max_pending_outbound_bytes_total;
        }
        (new, ignored)
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

    /// `relay.nofiles` defaults to strfry's `1_000_000`; `0` disables.
    #[test]
    fn nofiles_default_and_zero() {
        assert_eq!(Config::default().relay.nofiles, 1_000_000);
        let parsed: Config = toml::from_str("[relay]\nnofiles = 0\n").unwrap();
        assert_eq!(parsed.relay.nofiles, 0);
    }

    /// Unknown keys stay rejected.
    #[test]
    fn unknown_key_rejected() {
        assert!(toml::from_str::<Config>("[relay]\nbogus = 1\n").is_err());
    }

    /// `Config::load` validates `[relay.auth]` too, so a file whose
    /// `service_url` is not a ws/wss URL fails with `ServerError::Auth`
    /// and a broken SIGHUP reload keeps the running configuration.
    #[test]
    fn load_rejects_invalid_auth_service_url() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nosfry.toml");
        std::fs::write(&path, "[relay.auth]\nservice_url = \"http://bad\"\n").unwrap();
        assert!(matches!(Config::load(&path), Err(ServerError::Auth(_))));
    }

    /// Restart-only fields keep the running value and are reported;
    /// reloadable fields pass through.
    #[test]
    fn reloadable_from() {
        let running = Config::default();
        let mut new = Config::default();
        new.db.path = PathBuf::from("/elsewhere");
        new.relay.port = 9_999;
        new.relay.nofiles = 0;
        new.relay.metrics = true;
        new.relay.query_timeslice_budget_microseconds = 1;
        new.relay.threads.readers = 9;
        new.relay.limits.max_filter_limit = 1;
        new.relay.limits.max_filter_limit_count = 1;
        new.relay.limits.max_sync_events = 1;
        new.relay.max_sync_memory_bytes = 1;
        new.relay.max_pending_outbound_bytes_total = 1;
        new.relay.info.name = "reloaded".into();
        new.relay.rate_limits.events_per_second = 5;
        new.relay.write_policy.plugin = "/bin/true".into();
        new.relay.auth.service_url = "wss://relay.example.com".into();
        let (merged, ignored) = running.reloadable_from(new);
        assert_eq!(
            ignored,
            [
                "db",
                "relay.port",
                "relay.nofiles",
                "relay.metrics",
                "relay.query_timeslice_budget_microseconds",
                "relay.threads",
                "relay.limits.max_filter_limit",
                "relay.limits.max_filter_limit_count",
                "relay.limits.max_sync_events",
                "relay.max_sync_memory_bytes",
                "relay.max_pending_outbound_bytes_total",
            ]
        );
        assert_eq!(merged.db.path, running.db.path);
        assert_eq!(merged.relay.port, running.relay.port);
        assert_eq!(merged.relay.info.name, "reloaded");
        assert_eq!(merged.relay.rate_limits.events_per_second, 5);
        assert_eq!(merged.relay.write_policy.plugin, "/bin/true");
        assert_eq!(merged.relay.auth.service_url, "wss://relay.example.com");
    }

    /// An unchanged reload reports nothing.
    #[test]
    fn reloadable_from_noop() {
        let running = Config::default();
        let (merged, ignored) = running.clone().reloadable_from(running);
        let expected: [&str; 0] = [];
        assert_eq!(ignored, expected);
        assert_eq!(merged, Config::default());
    }
}
