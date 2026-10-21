//! NIP-86 relay management API: `POST /` with
//! `Content-Type: application/nostr+json+rpc`, authorized by a NIP-98
//! event (`Authorization: Nostr <base64(event)>`) from a configured
//! admin pubkey. Writes go through the single writer thread; reads
//! answer from the in-memory [`nosfry_lmdb::Moderation`] snapshot.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use nosfry_core::event::{EventId, PublicKey, UnverifiedEvent};
use nosfry_core::url::RelayUrl;
use nosfry_lmdb::{InfoField, ModOp};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use crate::channels::WriterMsg;
use crate::config::Config;
use crate::http::{AppState, ip_blocked};

/// Request body cap in bytes — `413` above (enforced by the route's
/// `DefaultBodyLimit`).
pub(crate) const BODY_LIMIT: usize = 64 * 1024;

/// NIP-98 `created_at` tolerance in seconds.
const AUTH_WINDOW_SECONDS: u64 = 60;

/// The implemented NIP-86 method set (`supportedmethods` result).
const METHODS: &[&str] = &[
    "allowkind",
    "allowpubkey",
    "banevent",
    "banpubkey",
    "blockip",
    "changerelaydescription",
    "changerelayicon",
    "changerelayname",
    "disallowkind",
    "listallowedkinds",
    "listallowedpubkeys",
    "listbannedevents",
    "listbannedpubkeys",
    "listblockedips",
    "listdisallowedkinds",
    "supportedmethods",
    "unallowpubkey",
    "unbanevent",
    "unbanpubkey",
    "unblockip",
];

/// Warn-once flag for a management endpoint without `service_url`
/// (every request then fails the `u`-tag check).
static NO_SERVICE_URL_WARNED: AtomicBool = AtomicBool::new(false);

/// `POST /` — the NIP-86 endpoint exists only with a
/// `application/nostr+json+rpc` content type and a non-empty
/// `[relay.management] admins`; anything else falls through to the
/// pre-NIP-86 behavior (405).
pub(crate) async fn handle(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if ip_blocked(&state, &headers, peer.ip()) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let live = state.live.borrow().clone();
    let rpc = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|t| t.trim().eq_ignore_ascii_case("application/nostr+json+rpc"));
    if !rpc || live.config.relay.management.admins.is_empty() {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if let Err(reason) = authorize(&headers, &body, &live.config, live.limits.auth.relay()) {
        return unauthorized(reason);
    }
    let Ok(text) = std::str::from_utf8(&body) else {
        return (StatusCode::BAD_REQUEST, "invalid utf-8").into_response();
    };
    match serde_json::from_str::<Request<'_>>(text) {
        Err(_) => (StatusCode::BAD_REQUEST, "invalid request json").into_response(),
        Ok(request) => match dispatch(&state, request.method, &request.params).await {
            Ok(result) => Json(json!({ "result": result })).into_response(),
            Err(error) => Json(json!({ "error": error })).into_response(),
        },
    }
}

/// The NIP-86 request envelope.
#[derive(Debug, Deserialize)]
struct Request<'a> {
    /// Method name.
    method: &'a str,
    /// Positional parameters (`[]` when absent).
    #[serde(default)]
    params: Vec<Value>,
}

/// `401 {"error": "unauthorized: <reason>"}`.
fn unauthorized(reason: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": format!("unauthorized: {reason}") })),
    )
        .into_response()
}

/// NIP-98 gate, as NIP-86 requires: `kind 27235`, `created_at` within
/// ±60 s, `method` = `POST`, `payload` = the SHA-256 of the raw body,
/// `u` matching `[relay] service_url` host-normalized, pubkey in
/// `admins`, and a valid BIP-340 signature. `Err` is the `unauthorized:
/// <reason>` tail for the 401 body.
fn authorize(
    headers: &HeaderMap,
    body: &[u8],
    config: &Config,
    service_url: Option<&RelayUrl>,
) -> Result<(), &'static str> {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return Err("missing authorization header");
    };
    let mut parts = value.splitn(2, ' ');
    if parts
        .next()
        .is_none_or(|s| !s.eq_ignore_ascii_case("Nostr"))
    {
        return Err("expected the `Nostr` authorization scheme");
    }
    let json = BASE64
        .decode(parts.next().unwrap_or_default().trim())
        .map_err(|_| "malformed base64 auth event")?;
    let text = std::str::from_utf8(&json).map_err(|_| "auth event is not utf-8")?;
    let event = UnverifiedEvent::parse(text, &config.events).map_err(|_| "invalid auth event")?;
    // Cheap checks first; the BIP-340 verify runs last.
    if u16::from(event.kind()) != 27235 {
        return Err("auth event must be kind 27235");
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    if event.created_at().abs_diff(now) > AUTH_WINDOW_SECONDS {
        return Err("stale auth event timestamp");
    }
    if event.tag_values("method").next() != Some("POST") {
        return Err("auth event `method` tag must be POST");
    }
    let digest = faster_hex::hex_string(&Sha256::digest(body));
    match event.tag_values("payload").next() {
        None => return Err("missing `payload` tag"),
        Some(tag) if tag != digest => {
            return Err("`payload` tag does not match the body");
        }
        Some(_) => {}
    }
    let Some(url) = service_url else {
        if !NO_SERVICE_URL_WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "relay.management configured without relay.service_url; \
                            NIP-98 `u` checks cannot pass"
            );
        }
        return Err("no service_url configured");
    };
    if !event.tag_values("u").next().is_some_and(|u| url.matches(u)) {
        return Err("`u` tag does not match this relay");
    }
    let pubkey = event.pubkey();
    // Verify before the admin check, so an unsigned probe cannot learn
    // which pubkeys are admins from the error.
    event
        .verify(&config.events)
        .map_err(|_| "invalid auth event signature")?;
    if !config
        .relay
        .management
        .admins
        .iter()
        .any(|a| PublicKey::from_hex(a) == Some(pubkey))
    {
        return Err("pubkey is not an admin");
    }
    Ok(())
}

/// Dispatch one request. `Err` becomes the `{"error": …}` response
/// body at HTTP 200 (method-level errors are not HTTP errors).
async fn dispatch(state: &AppState, method: &str, params: &[Value]) -> Result<Value, String> {
    let m = state.moderation.borrow().clone();
    let op = match method {
        "supportedmethods" => return Ok(json!(METHODS)),
        "listbannedpubkeys" => return Ok(listed(m.banned_pubkeys.iter(), "pubkey")),
        "listallowedpubkeys" => return Ok(listed(m.allowed_pubkeys.iter(), "pubkey")),
        "listbannedevents" => return Ok(listed(m.banned_events.iter(), "id")),
        "listblockedips" => return Ok(listed(m.blocked_ips.iter(), "ip")),
        "listallowedkinds" => return Ok(kinds(&m.allowed_kinds)),
        "listdisallowedkinds" => return Ok(kinds(&m.disallowed_kinds)),
        "banpubkey" => ModOp::BanPubkey {
            pubkey: pubkey_param(params)?,
            reason: reason_param(params),
        },
        "unbanpubkey" => ModOp::UnbanPubkey {
            pubkey: pubkey_param(params)?,
        },
        "allowpubkey" => ModOp::AllowPubkey {
            pubkey: pubkey_param(params)?,
            reason: reason_param(params),
        },
        "unallowpubkey" => ModOp::UnallowPubkey {
            pubkey: pubkey_param(params)?,
        },
        "banevent" => ModOp::BanEvent {
            id: event_id_param(params)?,
            reason: reason_param(params),
        },
        "unbanevent" => ModOp::UnbanEvent {
            id: event_id_param(params)?,
        },
        "allowkind" => ModOp::AllowKind {
            kind: kind_param(params)?,
        },
        "disallowkind" => ModOp::DisallowKind {
            kind: kind_param(params)?,
        },
        "blockip" => ModOp::BlockIp {
            ip: ip_param(params)?,
            reason: reason_param(params),
        },
        "unblockip" => ModOp::UnblockIp {
            ip: ip_param(params)?,
        },
        "changerelayname" => ModOp::SetInfo {
            field: InfoField::Name,
            value: info_param(params)?,
        },
        "changerelaydescription" => ModOp::SetInfo {
            field: InfoField::Description,
            value: info_param(params)?,
        },
        "changerelayicon" => ModOp::SetInfo {
            field: InfoField::Icon,
            value: info_param(params)?,
        },
        other => return Err(format!("unsupported method: {other}")),
    };
    apply(state, op).await
}

/// Send `op` to the writer thread, publish the returned snapshot, and
/// rebuild the NIP-11 document when an info field changed.
async fn apply(state: &AppState, op: ModOp) -> Result<Value, String> {
    let is_info = matches!(op, ModOp::SetInfo { .. });
    let (reply, rx) = oneshot::channel();
    state
        .conn
        .writer
        .send(WriterMsg::Moderate { op, reply })
        .await
        .map_err(|_| "error: internal error".to_owned())?;
    let moderation = rx
        .await
        .map_err(|_| "error: internal error".to_owned())?
        .map_err(|err| format!("error: {err}"))?;
    let moderation = Arc::new(moderation);
    state.mod_tx.send_replace(Arc::clone(&moderation));
    if is_info {
        let mut next = (**state.live.borrow()).clone();
        next.nip11 = crate::http::nip11(&next.config, &next.limits, &moderation).into();
        state.live_tx.send_replace(Arc::new(next));
    }
    Ok(Value::Bool(true))
}

/// Render a `mod_*` map as the NIP-86 `[{field, reason?}]` list,
/// sorted by key for a stable output.
fn listed<'a, K>(rows: impl Iterator<Item = (&'a K, &'a Option<Box<str>>)>, field: &str) -> Value
where
    K: Ord + fmt::Display + 'a,
{
    let mut rows: Vec<_> = rows.collect();
    rows.sort_by(|a, b| a.0.cmp(b.0));
    Value::Array(
        rows.into_iter()
            .map(|(key, reason)| {
                let mut obj = Map::new();
                obj.insert(field.to_owned(), Value::String(key.to_string()));
                if let Some(reason) = reason {
                    obj.insert("reason".to_owned(), Value::String(reason.to_string()));
                }
                Value::Object(obj)
            })
            .collect(),
    )
}

/// Render a kind set as `[kind, …]` (sorted — `BTreeSet` order).
fn kinds(set: &std::collections::BTreeSet<u16>) -> Value {
    Value::Array(set.iter().map(|kind| json!(kind)).collect())
}

/// `params[0]` as a hex pubkey.
fn pubkey_param(params: &[Value]) -> Result<PublicKey, String> {
    params
        .first()
        .and_then(Value::as_str)
        .and_then(PublicKey::from_hex)
        .ok_or_else(|| "invalid params: expected a 64-hex pubkey".to_owned())
}

/// `params[0]` as a hex event id.
fn event_id_param(params: &[Value]) -> Result<EventId, String> {
    params
        .first()
        .and_then(Value::as_str)
        .and_then(EventId::from_hex)
        .ok_or_else(|| "invalid params: expected a 64-hex event id".to_owned())
}

/// `params[0]` as a kind number.
fn kind_param(params: &[Value]) -> Result<u16, String> {
    params
        .first()
        .and_then(Value::as_u64)
        .and_then(|k| u16::try_from(k).ok())
        .ok_or_else(|| "invalid params: expected a kind number".to_owned())
}

/// `params[0]` as an IP address.
fn ip_param(params: &[Value]) -> Result<std::net::IpAddr, String> {
    params
        .first()
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "invalid params: expected an IP address".to_owned())
}

/// `params[0]` as a string (info fields).
fn info_param(params: &[Value]) -> Result<String, String> {
    params
        .first()
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "invalid params: expected a string value".to_owned())
}

/// `params[1]` as an optional reason string.
fn reason_param(params: &[Value]) -> Option<String> {
    params.get(1).and_then(Value::as_str).map(str::to_owned)
}
