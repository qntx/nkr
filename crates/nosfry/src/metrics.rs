//! Prometheus metrics: fixed, lock-free counters rendered in the text
//! exposition format for `GET /metrics`.
//!
//! Every label set is fixed and bounded — `verb` labels index the
//! `ClientVerb`/`RelayVerb` enums, `kind` is a u16 — and counting sits
//! on the per-message hot path, so storage is flat arrays of atomics:
//! no locks, no allocation, all `Relaxed`.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use nosfry_core::message::{ClientVerb, RelayVerb};

/// Process-wide counters, shared as `Arc<Metrics>`.
#[derive(Debug)]
pub(crate) struct Metrics {
    /// `nostr_client_messages_total{verb}`.
    client: [AtomicU64; ClientVerb::ALL.len()],
    /// `nostr_relay_messages_total{verb}` — only frames actually
    /// written to a socket count.
    relay: [AtomicU64; RelayVerb::ALL.len()],
    /// `nosfry_rate_limited_total{verb}`.
    rate_limited: [AtomicU64; ClientVerb::ALL.len()],
    /// `nostr_events_total{kind}` — one counter per u16 (512 KiB,
    /// allocated once) so the label never echoes client strings.
    events: Box<[AtomicU64]>,
    /// `nosfry_connections_rejected_total`.
    rejected: AtomicU64,
}

impl Metrics {
    /// All counters at zero.
    pub(crate) fn new() -> Self {
        Self {
            client: [const { AtomicU64::new(0) }; ClientVerb::ALL.len()],
            relay: [const { AtomicU64::new(0) }; RelayVerb::ALL.len()],
            rate_limited: [const { AtomicU64::new(0) }; ClientVerb::ALL.len()],
            events: (0..=u16::MAX)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            rejected: AtomicU64::new(0),
        }
    }

    /// One inbound text frame with a recognized client verb, counted
    /// even when the message is then rejected.
    pub(crate) fn client_message(&self, verb: ClientVerb) {
        if let Some(counter) = self.client.get(verb as usize) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One frame written to a client socket.
    pub(crate) fn relay_message(&self, verb: RelayVerb) {
        if let Some(counter) = self.relay.get(verb as usize) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One message refused by a session rate limit.
    pub(crate) fn rate_limited(&self, verb: ClientVerb) {
        if let Some(counter) = self.rate_limited.get(verb as usize) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One event that passed verification.
    pub(crate) fn event(&self, kind: u16) {
        if let Some(counter) = self.events.get(usize::from(kind)) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One WebSocket upgrade refused by the connection caps.
    pub(crate) fn connection_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Render the Prometheus text exposition format (0.0.4);
    /// `connections` is the live gauge read from the connection
    /// limiter. Counter series at zero are omitted; the gauge is
    /// always emitted.
    pub(crate) fn render(&self, connections: usize) -> String {
        let mut out = String::with_capacity(2 * 1024);
        family(
            &mut out,
            "nostr_client_messages_total",
            "Inbound client messages by verb.",
            "counter",
        );
        for (verb, counter) in ClientVerb::ALL.iter().zip(&self.client) {
            verb_series(
                &mut out,
                "nostr_client_messages_total",
                verb.as_str(),
                counter,
            );
        }
        family(
            &mut out,
            "nostr_relay_messages_total",
            "Frames written to clients by verb.",
            "counter",
        );
        for (verb, counter) in RelayVerb::ALL.iter().zip(&self.relay) {
            verb_series(
                &mut out,
                "nostr_relay_messages_total",
                verb.as_str(),
                counter,
            );
        }
        family(
            &mut out,
            "nostr_events_total",
            "Verified events by kind.",
            "counter",
        );
        for (kind, counter) in self.events.iter().enumerate() {
            let value = counter.load(Ordering::Relaxed);
            if value > 0 {
                writeln!(out, "nostr_events_total{{kind=\"{kind}\"}} {value}").ok();
            }
        }
        family(
            &mut out,
            "nosfry_rate_limited_total",
            "Messages refused by the session rate limits.",
            "counter",
        );
        for (verb, counter) in ClientVerb::ALL.iter().zip(&self.rate_limited) {
            verb_series(
                &mut out,
                "nosfry_rate_limited_total",
                verb.as_str(),
                counter,
            );
        }
        family(
            &mut out,
            "nosfry_connections_rejected_total",
            "Upgrades refused by the connection caps.",
            "counter",
        );
        let rejected = self.rejected.load(Ordering::Relaxed);
        if rejected > 0 {
            writeln!(out, "nosfry_connections_rejected_total {rejected}").ok();
        }
        family(
            &mut out,
            "nosfry_connections",
            "Current WebSocket connections.",
            "gauge",
        );
        writeln!(out, "nosfry_connections {connections}").ok();
        out
    }
}

/// `# HELP`/`# TYPE` header lines for one family.
fn family(out: &mut String, name: &str, help: &str, kind: &str) {
    writeln!(out, "# HELP {name} {help}").ok();
    writeln!(out, "# TYPE {name} {kind}").ok();
}

/// One `{verb="..."}` counter line, omitted while at zero.
fn verb_series(out: &mut String, name: &str, verb: &str, counter: &AtomicU64) {
    let value = counter.load(Ordering::Relaxed);
    if value > 0 {
        writeln!(out, "{name}{{verb=\"{verb}\"}} {value}").ok();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "unit tests may panic on failure")]
mod tests {
    use super::*;

    #[test]
    fn empty_render_has_families_and_gauge_only() {
        let out = Metrics::new().render(0);
        assert!(out.contains("# HELP nostr_client_messages_total"));
        assert!(out.contains("# TYPE nostr_events_total counter"));
        assert!(out.ends_with("nosfry_connections 0\n"));
        assert!(!out.contains('{'), "no series while every counter is 0");
    }

    #[test]
    fn render_exact_text() {
        let metrics = Metrics::new();
        metrics.client_message(ClientVerb::Event);
        metrics.client_message(ClientVerb::Event);
        metrics.client_message(ClientVerb::Req);
        metrics.relay_message(RelayVerb::Ok);
        metrics.relay_message(RelayVerb::Eose);
        metrics.relay_message(RelayVerb::Auth);
        metrics.event(1);
        metrics.event(7);
        metrics.rate_limited(ClientVerb::Event);
        metrics.connection_rejected();
        assert_eq!(
            metrics.render(3),
            "# HELP nostr_client_messages_total Inbound client messages by verb.\n\
             # TYPE nostr_client_messages_total counter\n\
             nostr_client_messages_total{verb=\"EVENT\"} 2\n\
             nostr_client_messages_total{verb=\"REQ\"} 1\n\
             # HELP nostr_relay_messages_total Frames written to clients by verb.\n\
             # TYPE nostr_relay_messages_total counter\n\
             nostr_relay_messages_total{verb=\"EOSE\"} 1\n\
             nostr_relay_messages_total{verb=\"OK\"} 1\n\
             nostr_relay_messages_total{verb=\"AUTH\"} 1\n\
             # HELP nostr_events_total Verified events by kind.\n\
             # TYPE nostr_events_total counter\n\
             nostr_events_total{kind=\"1\"} 1\n\
             nostr_events_total{kind=\"7\"} 1\n\
             # HELP nosfry_rate_limited_total Messages refused by the session rate limits.\n\
             # TYPE nosfry_rate_limited_total counter\n\
             nosfry_rate_limited_total{verb=\"EVENT\"} 1\n\
             # HELP nosfry_connections_rejected_total Upgrades refused by the connection caps.\n\
             # TYPE nosfry_connections_rejected_total counter\n\
             nosfry_connections_rejected_total 1\n\
             # HELP nosfry_connections Current WebSocket connections.\n\
             # TYPE nosfry_connections gauge\n\
             nosfry_connections 3\n"
        );
    }
}
