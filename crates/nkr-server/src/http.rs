//! axum HTTP surface: WebSocket upgrade, NIP-11 document, landing page.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, FromRequestParts, Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodFilter, get};
use axum::{Router, body::Body};
use serde_json::{Value, json};
use yawc::{IncomingUpgrade, Options};

use crate::config::Config;
use crate::connection;

/// Everything a connection handler needs.
#[derive(Debug, Clone)]
pub struct AppState {
    /// Server configuration.
    pub config: Arc<Config>,
    /// Connection dispatch.
    pub conn: connection::Handle,
}

/// Build the application router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root).on(MethodFilter::OPTIONS, preflight))
        .with_state(state)
}

const CORS_HEADERS: [(&str, &str); 3] = [
    ("access-control-allow-origin", "*"),
    ("access-control-allow-headers", "*"),
    ("access-control-allow-methods", "GET, OPTIONS"),
];

async fn preflight() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    for (name, value) in CORS_HEADERS {
        response.headers_mut().insert(
            header::HeaderName::from_static(name),
            header::HeaderValue::from_static(value),
        );
    }
    response
}

#[allow(
    clippy::excessive_nesting,
    reason = "the three-way dispatch is clearest inline"
)]
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
    if wants_upgrade && let Ok(upgrade) = IncomingUpgrade::from_request_parts(&mut parts, &()).await
    {
        let compression = &state.config.relay.compression;
        let options =
            Options::default().with_max_payload_read(state.config.relay.max_websocket_payload_size);
        let options = if compression.enabled {
            Options {
                compression: Some(yawc::DeflateOptions {
                    level: yawc::CompressionLevel::default(),
                    server_no_context_takeover: !compression.sliding_window,
                    client_no_context_takeover: !compression.sliding_window,
                }),
                ..options
            }
        } else {
            options
        };
        let Ok((response, fut)) = upgrade.upgrade(options) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let handle = state.conn.clone();
        let ip = client_ip(&state, &parts.headers, peer);
        let tracker = handle.tracker.clone();
        tracker.spawn(async move {
            match fut.await {
                Ok(mut ws) => {
                    tracing::debug!(%ip, "connection open");
                    connection::run(&mut ws, &handle).await;
                }
                Err(err) => tracing::debug!(%err, "upgrade failed"),
            }
        });
        return response.into_response();
    }
    let accepts_nostr = parts
        .headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("application/nostr+json"));
    if accepts_nostr {
        let mut response = Json(nip11(&state.config)).into_response();
        for (name, value) in CORS_HEADERS {
            response.headers_mut().insert(
                header::HeaderName::from_static(name),
                header::HeaderValue::from_static(value),
            );
        }
        return response;
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(
            "nkr: a nostr relay. connect with a websocket client.\n",
        ))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn client_ip(state: &AppState, headers: &header::HeaderMap, peer: Option<SocketAddr>) -> String {
    let header_name = &state.config.relay.real_ip_header;
    if !header_name.is_empty()
        && let Some(value) = headers.get(header_name.as_str())
        && let Ok(value) = value.to_str()
        && let Some(first) = value.split(',').next()
    {
        return first.trim().to_owned();
    }
    peer.map_or_else(|| "-".to_owned(), |p| p.ip().to_string())
}

/// The NIP-11 relay information document.
fn nip11(config: &Config) -> Value {
    let relay = &config.relay;
    json!({
        "name": relay.info.name,
        "description": relay.info.description,
        "pubkey": relay.info.pubkey,
        "contact": relay.info.contact,
        "icon": relay.info.icon,
        "supported_nips": [1, 11],
        "software": "https://github.com/qntx/nkr",
        "version": env!("CARGO_PKG_VERSION"),
        "limitation": {
            "max_message_length": relay.max_websocket_payload_size,
            "max_subscriptions": relay.limits.max_subs_per_connection,
            "max_limit": relay.limits.max_filter_limit,
            "default_limit": relay.limits.max_filter_limit,
            "max_subid_length": 64,
            "max_event_tags": config.events.max_num_tags,
            "created_at_lower_limit": config.events.reject_events_older_than_seconds,
            "created_at_upper_limit": config.events.reject_events_newer_than_seconds,
            "auth_required": false,
            "restricted_writes": false,
            "payment_required": false,
        },
    })
}
