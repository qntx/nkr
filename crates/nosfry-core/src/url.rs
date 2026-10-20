//! Relay service URLs (`ws://`/`wss://`), normalized like strfry's
//! `normalizeRelayUrl`.

/// The host part of a relay URL: strip through `://`, cut at the
/// first `/`, `?`, or `#` (strfry `normalizeRelayUrl`).
fn normalize(url: &str) -> &str {
    let rest = url.find("://").map_or(url, |pos| &url[pos + 3..]);
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    &rest[..end]
}

/// A validated relay URL, kept as its normalized lowercase host.
/// Tag values are compared under the same normalization, so scheme
/// and path differences never matter.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelayUrl(Box<str>);

impl RelayUrl {
    /// Parse `url` as a `ws://`/`wss://` URL with a non-empty host.
    ///
    /// # Errors
    ///
    /// [`RelayUrlError`] on a wrong scheme or empty host.
    pub fn parse(url: &str) -> Result<Self, RelayUrlError> {
        if !url.starts_with("ws://") && !url.starts_with("wss://") {
            return Err(RelayUrlError);
        }
        let host = normalize(url).to_ascii_lowercase();
        if host.is_empty() {
            return Err(RelayUrlError);
        }
        Ok(Self(host.into_boxed_str()))
    }

    /// Whether `tag_value` normalizes to this relay's host
    /// (NIP-42 `relay` tags, NIP-62 vanish targets).
    #[must_use]
    pub fn matches(&self, tag_value: &str) -> bool {
        normalize(tag_value).eq_ignore_ascii_case(&self.0)
    }
}

/// The URL is not a `ws://`/`wss://` URL with a host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("service_url must be a ws:// or wss:// URL with a host")]
pub struct RelayUrlError;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "unit tests may panic on failure"
)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_match() {
        let url = RelayUrl::parse("wss://Relay.Example.com/path?x").unwrap();
        assert!(url.matches("wss://relay.example.com"));
        assert!(url.matches("wss://Relay.Example.com/sub#frag"));
        assert!(!url.matches("wss://other.example.com"));
        for bad in ["http://x", "wss://", "relay.example.com", "ws://"] {
            assert!(RelayUrl::parse(bad).is_err(), "{bad} must fail");
        }
    }
}
