//! axum HTTP surface: WebSocket upgrade, NIP-11 document, landing page.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodFilter, get};
use serde_json::json;
use yawc::{IncomingUpgrade, Options};

use crate::config::Config;
use crate::connection;

/// Everything a connection handler needs.
#[derive(Debug, Clone)]
pub(crate) struct AppState {
    /// Server configuration.
    pub(crate) config: Arc<Config>,
    /// Connection dispatch.
    pub(crate) conn: connection::Handle,
    /// Pre-serialized NIP-11 document; static for the process lifetime.
    pub(crate) nip11: Bytes,
}

/// Build the application router.
pub(crate) fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root).on(MethodFilter::OPTIONS, preflight))
        .with_state(state)
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
    let peer = parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0);
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
                state.nip11.clone(),
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

/// WebSocket upgrade branch of `root`; `None` when the upgrade headers
/// were malformed and the request falls through to NIP-11/plain text.
/// The deflate level is fixed at `fast()` (level 1, strfry's
/// `deflateInit2` level) — query responses dominate and level 6 costs
/// CPU without buying meaningful bytes at these message sizes.
async fn websocket(
    state: &AppState,
    parts: &mut Parts,
    peer: Option<SocketAddr>,
) -> Option<Response> {
    let Ok(upgrade) = IncomingUpgrade::from_request_parts(parts, &()).await else {
        return None;
    };
    let compression = &state.config.relay.compression;
    let options =
        Options::default().with_max_payload_read(state.config.relay.max_websocket_payload_size);
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
    let Ok((response, fut)) = upgrade.upgrade(options) else {
        return Some(StatusCode::BAD_REQUEST.into_response());
    };
    let handle = state.conn.clone();
    let ip = client_ip(state, &parts.headers, peer);
    let tracker = handle.tracker.clone();
    tracker.spawn(async move {
        match fut.await {
            Ok(mut ws) => {
                tracing::debug!(?ip, "connection open");
                connection::run(&mut ws, &handle).await;
            }
            Err(err) => tracing::debug!(%err, "upgrade failed"),
        }
    });
    Some(response.into_response())
}

/// The client IP for logging and per-IP limits. When
/// `relay.real_ip_header` is set, the header's LAST comma-separated
/// value is trusted — a proxy appends its peer's address, so the last
/// entry is the one written by the trusted proxy in front of nosfry;
/// earlier entries are client-controlled. Set it only behind a proxy
/// that sets or appends the header. Missing or unparsable values fall
/// back to the socket peer.
fn client_ip(
    state: &AppState,
    headers: &header::HeaderMap,
    peer: Option<SocketAddr>,
) -> Option<IpAddr> {
    let header_name = &state.config.relay.real_ip_header;
    if !header_name.is_empty()
        && let Some(value) = headers.get(header_name.as_str())
        && let Ok(value) = value.to_str()
        && let Some(last) = value.split(',').next_back()
        && let Ok(ip) = last.trim().parse()
    {
        return Some(ip);
    }
    peer.map(|p| p.ip())
}

/// The NIP-11 relay information document, serialized once at startup
/// (it is static for the process lifetime). `description`, `pubkey`,
/// `contact`, and `icon` are optional and omitted when empty; `name`
/// stays unconditionally.
pub(crate) fn nip11(config: &Config) -> String {
    let relay = &config.relay;
    let mut doc = json!({
        "name": relay.info.name,
        "supported_nips": [1, 9, 11, 40],
        "software": "https://github.com/qntx/nosfry",
        "version": env!("CARGO_PKG_VERSION"),
        "limitation": {
            "max_message_length": relay.max_websocket_payload_size,
            "max_subscriptions": relay.limits.max_subs_per_connection,
            "max_limit": relay.limits.max_filter_limit,
            "default_limit": relay.limits.max_filter_limit,
            "max_subid_length": nosfry_core::message::SUB_ID_MAX_CHARS,
            "max_event_tags": config.events.max_num_tags,
            "created_at_lower_limit": config.events.reject_events_older_than_seconds,
            "created_at_upper_limit": config.events.reject_events_newer_than_seconds,
            "auth_required": false,
            "restricted_writes": false,
            "payment_required": false,
        },
    });
    if let Some(map) = doc.as_object_mut() {
        for (key, value) in [
            ("description", &relay.info.description),
            ("pubkey", &relay.info.pubkey),
            ("contact", &relay.info.contact),
            ("icon", &relay.info.icon),
        ] {
            if !value.is_empty() {
                map.insert(key.to_owned(), json!(value));
            }
        }
    }
    doc.to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "unit tests may panic on failure")]
mod tests {
    use nosfry_core::limits::Limits;

    use super::*;

    fn state_with(header_name: &str) -> AppState {
        let mut config = Config::default();
        config.relay.real_ip_header = header_name.to_owned();
        AppState {
            nip11: Bytes::new(),
            config: Arc::new(config),
            conn: connection::Handle {
                writer: tokio::sync::mpsc::channel(1).0,
                shards: Arc::from(vec![]),
                limits: Arc::new(Limits::default()),
                ping_interval: std::time::Duration::from_secs(1),
                max_pending_outbound: 0,
                shutdown: tokio::sync::watch::channel(false).1,
                tracker: tokio_util::task::TaskTracker::new(),
            },
        }
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
        let state = state_with("x-forwarded-for");
        let peer: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let got = client_ip(&state, &headers("x-forwarded-for", " 1.2.3.4 "), Some(peer));
        assert_eq!(got, Some("1.2.3.4".parse::<IpAddr>().unwrap()));
    }

    /// A proxy appends to X-Forwarded-For: the last entry is the one the
    /// trusted proxy wrote; earlier ones are client-controlled.
    #[test]
    fn client_ip_appended_list_takes_last() {
        let state = state_with("x-forwarded-for");
        let peer: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let got = client_ip(
            &state,
            &headers("x-forwarded-for", "9.9.9.9, 10.0.0.1, 203.0.113.7"),
            Some(peer),
        );
        assert_eq!(got, Some("203.0.113.7".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn client_ip_garbage_falls_back_to_peer() {
        let state = state_with("x-forwarded-for");
        let peer: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let got = client_ip(&state, &headers("x-forwarded-for", "not-an-ip"), Some(peer));
        assert_eq!(got, Some(peer.ip()));
    }

    #[test]
    fn client_ip_header_unset_or_absent() {
        // Header name configured but absent from the request.
        let state = state_with("x-forwarded-for");
        let peer: SocketAddr = "127.0.0.1:9".parse().unwrap();
        assert_eq!(
            client_ip(&state, &header::HeaderMap::new(), Some(peer)),
            Some(peer.ip())
        );
        // No header name configured.
        let unset = state_with("");
        let got = client_ip(&unset, &headers("x-forwarded-for", "1.2.3.4"), Some(peer));
        assert_eq!(got, Some(peer.ip()));
    }
}
