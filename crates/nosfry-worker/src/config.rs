//! TOML configuration: the native `[relay]`/`[events]` keys Workers
//! keeps, plus the `[worker]` section.

use nosfry_core::auth::{AuthConfig, AuthRules};
use nosfry_core::event::EventLimits;
use nosfry_core::info::Info;
use nosfry_core::limits::{Limits, RateLimits, ReqLimits};
use nosfry_core::url::RelayUrlError;
use serde::Deserialize;

/// The `NOSFRY_CONFIG` document. Every section rejects unknown keys,
/// so native-only keys (`bind`, `port`, `db`, `threads`, …) fail
/// fast instead of being silently ignored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Event acceptance limits (`nosfry_core::event::EventLimits` verbatim).
    pub events: EventLimits,
    /// Relay behaviour section.
    pub relay: Relay,
    /// Workers platform section.
    pub worker: Worker,
}

/// `[relay]` — the subset of the native relay section Workers keeps.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Relay {
    /// The relay's canonical `ws://`/`wss://` URL clients sign into an
    /// AUTH event's `relay` tag. Empty disables AUTH: restricted
    /// kinds are stored but never served, and protected events are
    /// rejected.
    pub service_url: String,
    /// Maximum inbound WebSocket payload in bytes.
    pub max_websocket_payload_size: usize,
    /// Maximum simultaneous WebSocket connections (0 = unlimited).
    /// Workers keeps a tighter default than native's 0 because a
    /// Durable Object serializes all of them on one thread.
    pub max_connections: usize,
    /// Maximum simultaneous connections from one client address
    /// (0 = unlimited), the native default.
    pub max_connections_per_ip: usize,
    /// REQ limits (`nosfry_core::limits::ReqLimits` verbatim).
    pub limits: ReqLimits,
    /// Per-connection EVENT/REQ rate limits
    /// (`nosfry_core::limits::RateLimits` verbatim).
    pub rate_limits: RateLimits,
    /// NIP-42 authentication and restricted read kinds.
    pub auth: AuthConfig,
    /// NIP-11 relay information.
    pub info: Info,
}

/// `[worker]` — Workers platform knobs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Worker {
    /// Durable Object `locationHint`; empty = none.
    pub location_hint: String,
    /// Per-REQ index-row budget, the Workers substitute for native
    /// time slicing (one thread cannot yield inside a message). It
    /// also bounds a `COUNT` scan together with `[relay.limits]
    /// max_filter_limit_count`: the effective COUNT budget is their
    /// minimum, `0` disabling COUNT entirely.
    pub max_scan_rows: usize,
}

impl Default for Relay {
    fn default() -> Self {
        Self {
            service_url: String::new(),
            max_websocket_payload_size: 131_072,
            max_connections: 5_000,
            max_connections_per_ip: 128,
            limits: ReqLimits::default(),
            rate_limits: RateLimits::default(),
            auth: AuthConfig::default(),
            info: Info::default(),
        }
    }
}

impl Default for Worker {
    fn default() -> Self {
        Self {
            location_hint: String::new(),
            max_scan_rows: 100_000,
        }
    }
}

/// Configuration failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// TOML syntax error or an unknown key.
    #[error("config: {0}")]
    Toml(#[from] toml::de::Error),
    /// `[relay] service_url` is not a `ws://`/`wss://` URL.
    #[error(transparent)]
    ServiceUrl(#[from] RelayUrlError),
    /// `[worker] max_scan_rows` is 0 — every REQ/COUNT would return
    /// nothing, so the configuration is refused.
    #[error("[worker] max_scan_rows must be at least 1")]
    MaxScanRows,
}

impl Config {
    /// Parse a TOML document.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Toml`] on syntax errors or unknown keys,
    /// [`ConfigError::ServiceUrl`] on an invalid
    /// `[relay] service_url`, [`ConfigError::MaxScanRows`] when
    /// `[worker] max_scan_rows` is 0.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text)?;
        config.limits()?;
        if config.worker.max_scan_rows == 0 {
            return Err(ConfigError::MaxScanRows);
        }
        Ok(config)
    }

    /// `nosfry_core::limits::Limits` assembled from `[events]`,
    /// `[relay.limits]`, `[relay.rate_limits]`, and `[relay.auth]`.
    ///
    /// COUNT (NIP-45) is served; the session still refuses negentropy
    /// itself because `max_sync_events` is forced to 0 until slice
    /// 3.5.
    ///
    /// # Errors
    ///
    /// [`ConfigError::ServiceUrl`] on an invalid
    /// `[relay] service_url`.
    pub fn limits(&self) -> Result<Limits, ConfigError> {
        let mut req = self.relay.limits;
        req.max_sync_events = 0;
        Ok(Limits {
            event: self.events,
            req,
            rate: self.relay.rate_limits,
            auth: self.auth_rules()?,
        })
    }

    /// The shared `AuthRules` from `service_url`, the restricted
    /// kinds, and the involved-only setting.
    ///
    /// # Errors
    ///
    /// [`ConfigError::ServiceUrl`] on an invalid
    /// `[relay] service_url`.
    pub fn auth_rules(&self) -> Result<std::sync::Arc<AuthRules>, ConfigError> {
        let service_url = if self.relay.service_url.is_empty() {
            None
        } else {
            Some(nosfry_core::url::RelayUrl::parse(&self.relay.service_url)?)
        };
        Ok(std::sync::Arc::new(AuthRules::new(
            service_url,
            &self.relay.auth.restricted_read_kinds,
            self.relay.auth.restrict_read_to_involved,
        )))
    }
}
