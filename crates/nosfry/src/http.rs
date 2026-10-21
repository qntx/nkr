//! axum HTTP surface: WebSocket upgrade, NIP-11 document, landing page.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, DefaultBodyLimit, FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodFilter, get};
use nosfry_lmdb::Moderation;
use tokio::sync::watch;
use yawc::{IncomingUpgrade, Options};

use crate::Live;
use crate::config::Config;
use crate::connection;
use crate::limiter::ConnectionLimiter;
use crate::management;
use crate::metrics::Metrics;

/// Everything a connection handler needs.
#[derive(Debug, Clone)]
pub(crate) struct AppState {
    /// Live-reloadable state (config, limits, NIP-11, policy).
    pub(crate) live: watch::Receiver<Arc<Live>>,
    /// Connection dispatch.
    pub(crate) conn: connection::Handle,
    /// Process-wide counters.
    pub(crate) metrics: Arc<Metrics>,
    /// Simultaneous-connection caps.
    pub(crate) limiter: Arc<ConnectionLimiter>,
    /// Process-wide reserved sync-storage bytes (the
    /// `nosfry_sync_memory_bytes` gauge).
    pub(crate) sync_memory: Arc<std::sync::atomic::AtomicUsize>,
    /// Latest NIP-86 snapshot; read-only copies for the handlers.
    pub(crate) moderation: watch::Receiver<Arc<Moderation>>,
    /// Moderation snapshot sender — the management handler publishes
    /// the snapshot the writer returned after each applied op.
    pub(crate) mod_tx: watch::Sender<Arc<Moderation>>,
    /// [`Live`] sender — `changerelay*` ops rebuild `nip11`.
    pub(crate) live_tx: watch::Sender<Arc<Live>>,
}

/// Build the application router. `/metrics` exists only when
/// `[relay] metrics` is enabled — otherwise it falls through to the
/// default 404 like any unknown path.
pub(crate) fn router(state: AppState) -> Router {
    let router = Router::new().route(
        "/",
        get(root)
            .post(management::handle)
            .on(MethodFilter::OPTIONS, preflight)
            .route_layer(DefaultBodyLimit::max(management::BODY_LIMIT)),
    );
    let router = if state.live.borrow().config.relay.metrics {
        router.route("/metrics", get(metrics))
    } else {
        router
    };
    router.with_state(state)
}

/// Prometheus text exposition format (0.0.4), no CORS.
async fn metrics(State(state): State<AppState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(
            state.limiter.total(),
            state
                .conn
                .outbound_total
                .load(std::sync::atomic::Ordering::Relaxed),
            state.sync_memory.load(std::sync::atomic::Ordering::Relaxed),
        ),
    )
        .into_response()
}

const CORS_HEADERS: [(&str, &str); 3] = [
    ("access-control-allow-origin", "*"),
    ("access-control-allow-headers", "*"),
    ("access-control-allow-methods", "GET, OPTIONS"),
];

/// Apply the CORS headers shared by the preflight and NIP-11 responses.
fn with_cors(mut response: Response) -> Response {
    for (name, value) in CORS_HEADERS {
        response.headers_mut().insert(
            header::HeaderName::from_static(name),
            header::HeaderValue::from_static(value),
        );
    }
    response
}

async fn preflight() -> Response {
    with_cors(StatusCode::NO_CONTENT.into_response())
}

async fn root(State(state): State<AppState>, request: Request) -> Response {
    let (mut parts, _body) = request.into_parts();
    // The accept loop always inserts `ConnectInfo`; missing means a
    // broken wiring path, not a client error.
    let Some(peer) = parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0)
    else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if ip_blocked(&state, &parts.headers, peer.ip()) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let wants_upgrade = parts
        .headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    if wants_upgrade && let Some(response) = websocket(&state, &mut parts, peer).await {
        return response;
    }
    let accepts_nostr = parts
        .headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("application/nostr+json"));
    if accepts_nostr {
        return with_cors(
            (
                [(header::CONTENT_TYPE, "application/nostr+json")],
                state.live.borrow().nip11.clone(),
            )
                .into_response(),
        );
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(
            "nosfry: a nostr relay. connect with a websocket client.\n",
        ))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Whether the effective client IP (the same address the per-IP
/// limiter charges, `real_ip_header`-aware) is on the NIP-86 block
/// list. Checked before upgrades, NIP-11, and the management RPC —
/// existing WebSocket connections are not cut off (block applies to
/// new requests only).
pub(crate) fn ip_blocked(state: &AppState, headers: &header::HeaderMap, peer: IpAddr) -> bool {
    let ip = client_ip(&state.live.borrow().config, headers, peer);
    state.moderation.borrow().ip_blocked(&ip)
}

/// WebSocket upgrade branch of `root`; `None` when the upgrade headers
/// were malformed and the request falls through to NIP-11/plain text.
/// The deflate level is fixed at `fast()` (level 1, strfry's
/// `deflateInit2` level) — query responses dominate and level 6 costs
/// CPU without buying meaningful bytes at these message sizes.
async fn websocket(state: &AppState, parts: &mut Parts, peer: SocketAddr) -> Option<Response> {
    let Ok(upgrade) = IncomingUpgrade::from_request_parts(parts, &()).await else {
        return None;
    };
    // One snapshot per upgrade: existing connections keep the
    // options they started with.
    let live = state.live.borrow().clone();
    let compression = &live.config.relay.compression;
    let options =
        Options::default().with_max_payload_read(live.config.relay.max_websocket_payload_size);
    let options = if compression.enabled {
        Options {
            compression: Some(yawc::DeflateOptions {
                level: yawc::CompressionLevel::fast(),
                server_no_context_takeover: !compression.sliding_window,
                client_no_context_takeover: !compression.sliding_window,
            }),
            ..options
        }
    } else {
        options
    };
    // The cap is charged before the upgrade. Rejections log at
    // debug: a hostile peer may hammer the cap and must not flood
    // the log.
    let ip = client_ip(&live.config, &parts.headers, peer.ip());
    let Some(permit) = state.limiter.try_acquire(ip) else {
        tracing::debug!(?ip, "connection cap reached");
        state.metrics.connection_rejected();
        return Some((StatusCode::TOO_MANY_REQUESTS, "too many connections").into_response());
    };
    let Ok((response, fut)) = upgrade.upgrade(options) else {
        return Some(StatusCode::BAD_REQUEST.into_response());
    };
    let buckets = permit.buckets();
    let handle = state.conn.clone();
    let tracker = handle.tracker.clone();
    tracker.spawn(async move {
        let _permit = permit;
        match fut.await {
            Ok(mut ws) => {
                tracing::debug!(?ip, "connection open");
                connection::run(&mut ws, &handle, ip, buckets).await;
            }
            Err(err) => tracing::debug!(%err, "upgrade failed"),
        }
    });
    Some(response.into_response())
}

/// The client IP for logging, per-IP limits, and the write-policy
/// plugin. When `relay.real_ip_header` is set, the header's LAST
/// comma-separated value is trusted — a proxy appends its peer's
/// address, so the last entry is the one written by the trusted proxy
/// in front of nosfry; earlier entries are client-controlled. Set it
/// only behind a proxy that sets or appends the header. Missing or
/// unparsable values fall back to the socket peer.
fn client_ip(config: &Config, headers: &header::HeaderMap, peer: IpAddr) -> IpAddr {
    let header_name = &config.relay.real_ip_header;
    if !header_name.is_empty()
        && let Some(value) = headers.get(header_name.as_str())
        && let Ok(value) = value.to_str()
        && let Some(last) = value.split(',').next_back()
        && let Ok(ip) = last.trim().parse()
    {
        return ip;
    }
    peer
}

/// The NIP-11 relay information document, serialized once at startup
/// (it is static for the process lifetime). The NIP list follows the
/// configured features; the NIP-86 moderation snapshot overrides
/// `name`/`description`/`icon` through an effective [`Info`].
pub(crate) fn nip11(
    config: &Config,
    limits: &nosfry_core::limits::Limits,
    moderation: &Moderation,
) -> String {
    let relay = &config.relay;
    let mut nips = vec![1, 9, 11, 40, 62, 70];
    if relay.limits.max_filter_limit_count > 0 {
        nips.push(45);
    }
    if relay.limits.max_sync_events > 0 {
        nips.push(77);
    }
    if !relay.service_url.is_empty() {
        nips.push(42);
    }
    if !relay.management.admins.is_empty() {
        nips.push(86);
    }
    nips.sort_unstable();
    let info = nosfry_core::info::Info {
        name: moderation
            .name
            .as_deref()
            .unwrap_or(&relay.info.name)
            .to_owned(),
        description: moderation
            .description
            .as_deref()
            .unwrap_or(&relay.info.description)
            .to_owned(),
        pubkey: relay.info.pubkey.clone(),
        contact: relay.info.contact.clone(),
        icon: moderation
            .icon
            .as_deref()
            .unwrap_or(&relay.info.icon)
            .to_owned(),
    };
    nosfry_core::info::document(&info, &nips, limits, relay.max_websocket_payload_size)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "unit tests may panic on failure")]
mod tests {
    use super::*;

    fn config_with(header_name: &str) -> Config {
        let mut config = Config::default();
        config.relay.real_ip_header = header_name.to_owned();
        config
    }

    fn headers(name: &str, value: &str) -> header::HeaderMap {
        let mut map = header::HeaderMap::new();
        map.insert(
            header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            header::HeaderValue::from_str(value).unwrap(),
        );
        map
    }

    #[test]
    fn client_ip_single_value() {
        let config = config_with("x-forwarded-for");
        let peer: IpAddr = "127.0.0.1".parse().unwrap();
        let got = client_ip(&config, &headers("x-forwarded-for", " 1.2.3.4 "), peer);
        assert_eq!(got, "1.2.3.4".parse::<IpAddr>().unwrap());
    }

    /// A proxy appends to X-Forwarded-For: the last entry is the one the
    /// trusted proxy wrote; earlier ones are client-controlled.
    #[test]
    fn client_ip_appended_list_takes_last() {
        let config = config_with("x-forwarded-for");
        let peer: IpAddr = "127.0.0.1".parse().unwrap();
        let got = client_ip(
            &config,
            &headers("x-forwarded-for", "9.9.9.9, 10.0.0.1, 203.0.113.7"),
            peer,
        );
        assert_eq!(got, "203.0.113.7".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn client_ip_garbage_falls_back_to_peer() {
        let config = config_with("x-forwarded-for");
        let peer: IpAddr = "127.0.0.1".parse().unwrap();
        let got = client_ip(&config, &headers("x-forwarded-for", "not-an-ip"), peer);
        assert_eq!(got, peer);
    }

    #[test]
    fn client_ip_header_unset_or_absent() {
        // Header name configured but absent from the request.
        let config = config_with("x-forwarded-for");
        let peer: IpAddr = "127.0.0.1".parse().unwrap();
        assert_eq!(client_ip(&config, &header::HeaderMap::new(), peer), peer);
        // No header name configured.
        let unset = config_with("");
        let got = client_ip(&unset, &headers("x-forwarded-for", "1.2.3.4"), peer);
        assert_eq!(got, peer);
    }
}
