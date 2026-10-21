//! Black-box NIP conformance: the identical assertions run against
//! the native and the Workers editions. Every test needs `NOSFRY_URL`
//! (`ws://…`) pointing at a relay configured with `profile.toml` plus
//! an injected `service_url`; `run.sh` provides both.
//!
//! Every test signs with a fresh random key, so tests isolate on a
//! shared relay and may run in parallel.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::tests_outside_test_module,
    clippy::shadow_unrelated,
    reason = "conformance assertions may panic on black-box replies"
)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use nosfry_core::event::Event;
use nosfry_core::sign;
use secp256k1::Keypair;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TIMEOUT: Duration = Duration::from_secs(10);
fn url() -> String {
    std::env::var("NOSFRY_URL").expect("NOSFRY_URL must point at the relay under test")
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A fresh keypair — never the deterministic pool, so reruns against
/// the same relay do not collide with stale state.
fn key() -> Keypair {
    loop {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).unwrap();
        if let Ok(secret) = secp256k1::SecretKey::from_secret_bytes(bytes) {
            return Keypair::from_secret_key(&secret);
        }
    }
}

fn pubkey(k: &Keypair) -> String {
    format!("{}", k.x_only_public_key().0)
}

fn event_wire(ev: &Event) -> String {
    format!("[\"EVENT\",{}]", ev.json())
}

fn req_wire(id: &str, filter: &str) -> String {
    format!("[\"REQ\",\"{id}\",{filter}]")
}

async fn connect() -> yawc::TcpWebSocket {
    yawc::WebSocket::connect(url().parse().unwrap())
        .await
        .unwrap()
}

async fn send(ws: &mut yawc::TcpWebSocket, text: &str) {
    ws.send(yawc::Frame::text(text.to_owned())).await.unwrap();
}

/// The next text frame, parsed as JSON; non-text frames are skipped.
async fn recv_json(ws: &mut yawc::TcpWebSocket) -> Value {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let frame = tokio::time::timeout_at(deadline, ws.next())
            .await
            .expect("timed out waiting for a frame")
            .expect("connection closed while waiting for a frame");
        if frame.opcode() == yawc::OpCode::Text {
            return serde_json::from_slice(frame.payload()).unwrap();
        }
    }
}

/// Frames until `pred` matches one; returns everything collected.
async fn collect_until(ws: &mut yawc::TcpWebSocket, pred: impl Fn(&Value) -> bool) -> Vec<Value> {
    let mut frames = Vec::new();
    while !frames.iter().any(&pred) {
        frames.push(recv_json(ws).await);
    }
    frames
}

/// Publish and assert the OK frame, returning its message field.
async fn publish(ws: &mut yawc::TcpWebSocket, ev: &Event) -> (bool, String) {
    send(ws, &event_wire(ev)).await;
    let ok = collect_until(ws, |v| v[0] == "OK").await;
    let ok = &ok[ok.len() - 1];
    assert_eq!(ok[1], ev.id().to_string(), "OK id mismatch");
    (ok[2].as_bool().unwrap(), ok[3].as_str().unwrap().to_owned())
}

/// Issue a REQ and return the frames up to and including EOSE or
/// CLOSED.
async fn req(ws: &mut yawc::TcpWebSocket, id: &str, filter: &str) -> Vec<Value> {
    send(ws, &req_wire(id, filter)).await;
    collect_until(ws, |v| v[0] == "EOSE" || v[0] == "CLOSED").await
}

fn events(frames: &[Value]) -> Vec<&Value> {
    frames
        .iter()
        .filter(|v| v[0] == "EVENT")
        .map(|v| &v[2])
        .collect()
}

/// The relay's NIP-11 document over plain HTTP.
async fn nip11() -> Value {
    let url = url();
    let authority = url
        .strip_prefix("ws://")
        .expect("NOSFRY_URL must use ws://")
        .trim_end_matches('/');
    let mut tcp = tokio::net::TcpStream::connect(authority).await.unwrap();
    tcp.write_all(
        b"GET / HTTP/1.1\r\nHost: x\r\nAccept: application/nostr+json\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    tcp.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8(buf).unwrap();
    let body = text.split("\r\n\r\n").nth(1).unwrap();
    serde_json::from_str(body).unwrap()
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn nip11_document() {
    let doc = nip11().await;
    assert_eq!(doc["name"], "nosfry conformance");
    assert_eq!(doc["software"], "https://github.com/qntx/nosfry");
    assert!(doc["version"].is_string());
    assert!(doc["limitation"].is_object());
    let nips: Vec<u64> = doc["supported_nips"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    for nip in [1, 9, 11, 40, 42, 70] {
        assert!(nips.contains(&nip), "missing NIP-{nip}: {nips:?}");
    }
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn publish_ok_duplicate_and_bad_signature() {
    let k = key();
    let ev = sign::event(&k, 1, now(), &json!([]), "conformance publish");
    let mut ws = connect().await;
    let (ok, _) = publish(&mut ws, &ev).await;
    assert!(ok, "first publish rejected");

    let (ok, reason) = publish(&mut ws, &ev).await;
    // A byte-identical re-publish is accepted with `duplicate:`.
    assert!(
        ok && reason.starts_with("duplicate:"),
        "re-publish: {reason}"
    );

    // Corrupt one hex nibble inside the signature; the JSON stays valid.
    let mut doc = serde_json::from_str::<Value>(ev.json()).unwrap();
    let sig = doc["sig"].as_str().unwrap();
    let last = sig.chars().last().unwrap();
    let flipped = if last == '0' { '1' } else { '0' };
    doc["sig"] = json!(format!("{}{}", &sig[..sig.len() - 1], flipped));
    send(
        &mut ws,
        &format!("[\"EVENT\",{}]", serde_json::to_string(&doc).unwrap()),
    )
    .await;
    let ok = recv_json(&mut ws).await;
    assert_eq!(ok[0], "OK");
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap().starts_with("invalid:"), "{ok}");
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn req_newest_first_limit_eose() {
    let k = key();
    let base = now() - 4;
    let mut ws = connect().await;
    for i in 0..3u64 {
        let ev = sign::event(&k, 1, base + i, &json!([]), &format!("e{i}"));
        let (ok, reason) = publish(&mut ws, &ev).await;
        assert!(ok, "publish {i}: {reason}");
    }
    let filter = format!("{{\"authors\":[\"{}\"],\"limit\":2}}", pubkey(&k));
    let frames = req(&mut ws, "s", &filter).await;
    assert_eq!(frames.last().unwrap()[0], "EOSE");
    let got = events(&frames);
    assert_eq!(got.len(), 2);
    // Newest first: created_at descending.
    assert!(
        got[0]["created_at"].as_u64().unwrap() > got[1]["created_at"].as_u64().unwrap(),
        "{got:?}"
    );
    assert_eq!(got[0]["content"], "e2");
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn live_delivery_after_eose() {
    let k = key();
    let mut sub = connect().await;
    let mut writer = connect().await;
    let filter = format!("{{\"authors\":[\"{}\"]}}", pubkey(&k));
    let frames = req(&mut sub, "s", &filter).await;
    assert_eq!(frames.last().unwrap()[0], "EOSE");
    assert!(events(&frames).is_empty(), "unexpected events: {frames:?}");

    let ev = sign::event(&k, 1, now(), &json!([]), "live");
    let (ok, reason) = publish(&mut writer, &ev).await;
    assert!(ok, "publish: {reason}");
    let frame = recv_json(&mut sub).await;
    assert_eq!(frame[0], "EVENT");
    assert_eq!(frame[2]["id"], ev.id().to_string());
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn close_stops_delivery() {
    let k = key();
    let mut sub = connect().await;
    let mut writer = connect().await;
    let filter = format!("{{\"authors\":[\"{}\"]}}", pubkey(&k));
    req(&mut sub, "s", &filter).await;
    send(&mut sub, "[\"CLOSE\",\"s\"]").await;
    // Inbound frames apply in order, so the barrier's EOSE proves the
    // CLOSE took effect before the other connection publishes.
    req(&mut sub, "barrier", "{\"ids\":[]}").await;

    let ev = sign::event(&k, 1, now(), &json!([]), "after close");
    let (ok, reason) = publish(&mut writer, &ev).await;
    assert!(ok, "publish: {reason}");
    // After CLOSE no EVENT may arrive; a quiet two seconds proves it
    // (pings and pongs are not relay traffic and don't count).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while let Ok(Some(f)) = tokio::time::timeout_at(deadline, sub.next()).await {
        assert_ne!(
            f.opcode(),
            yawc::OpCode::Text,
            "frame after CLOSE: {:?}",
            String::from_utf8_lossy(f.payload())
        );
    }
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn replaceable_newer_wins() {
    let k = key();
    let base = now() - 10;
    let older = sign::event(&k, 0, base, &json!([]), "older");
    let newer = sign::event(&k, 0, base + 5, &json!([]), "newer");
    let mut ws = connect().await;

    let (ok, reason) = publish(&mut ws, &older).await;
    assert!(ok, "older: {reason}");
    let (ok, reason) = publish(&mut ws, &newer).await;
    assert!(ok, "newer: {reason}");
    let (ok, reason) = publish(&mut ws, &older).await;
    assert!(
        !ok && reason == "duplicate: have a newer version",
        "older again: {reason}"
    );

    let filter = format!("{{\"kinds\":[0],\"authors\":[\"{}\"]}}", pubkey(&k));
    let frames = req(&mut ws, "s", &filter).await;
    let got = events(&frames);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["id"], newer.id().to_string());
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn addressable_replaced_by_d() {
    let k = key();
    let base = now() - 10;
    let older = sign::event(&k, 30023, base, &json!([["d", "conformance"]]), "v1");
    let newer = sign::event(&k, 30023, base + 5, &json!([["d", "conformance"]]), "v2");
    let mut ws = connect().await;
    assert!(publish(&mut ws, &older).await.0);
    assert!(publish(&mut ws, &newer).await.0);

    let filter = format!("{{\"kinds\":[30023],\"authors\":[\"{}\"]}}", pubkey(&k));
    let frames = req(&mut ws, "s", &filter).await;
    let got = events(&frames);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["content"], "v2");
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn ephemeral_live_only() {
    let k = key();
    let mut sub = connect().await;
    let mut writer = connect().await;
    let filter = format!("{{\"authors\":[\"{}\"],\"kinds\":[20001]}}", pubkey(&k));
    req(&mut sub, "s", &filter).await;

    let ev = sign::event(&k, 20001, now(), &json!([]), "ephemeral");
    let (ok, reason) = publish(&mut writer, &ev).await;
    assert!(ok, "ephemeral publish: {reason}");
    let frame = recv_json(&mut sub).await;
    assert_eq!(frame[0], "EVENT");
    assert_eq!(frame[2]["id"], ev.id().to_string());

    let frames = req(&mut writer, "t", &filter).await;
    assert_eq!(frames.last().unwrap()[0], "EOSE");
    assert!(events(&frames).is_empty(), "ephemeral event was stored");
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn nip09_e_deletion() {
    let k = key();
    let ev = sign::event(&k, 1, now() - 5, &json!([]), "to be deleted");
    let mut ws = connect().await;
    assert!(publish(&mut ws, &ev).await.0);

    let del = sign::event(
        &k,
        5,
        now(),
        &json!([["e", &*ev.id().to_string()]]),
        "deleted",
    );
    let (ok, reason) = publish(&mut ws, &del).await;
    assert!(ok, "deletion: {reason}");

    let filter = format!("{{\"ids\":[\"{}\"]}}", ev.id());
    let frames = req(&mut ws, "s", &filter).await;
    assert!(
        events(&frames).is_empty(),
        "deleted event served: {frames:?}"
    );

    let (ok, reason) = publish(&mut ws, &ev).await;
    assert!(
        !ok && reason == "blocked: deleted by its author",
        "re-publish of a deleted event: {reason}"
    );
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn nip09_a_deletion() {
    let k = key();
    let ev = sign::event(&k, 30023, now() - 5, &json!([["d", "nip09a"]]), "v1");
    let mut ws = connect().await;
    assert!(publish(&mut ws, &ev).await.0);

    let del = sign::event(
        &k,
        5,
        now(),
        &json!([["a", format!("30023:{}:nip09a", pubkey(&k))]]),
        "deleted",
    );
    assert!(publish(&mut ws, &del).await.0);

    let filter = format!("{{\"kinds\":[30023],\"authors\":[\"{}\"]}}", pubkey(&k));
    let frames = req(&mut ws, "s", &filter).await;
    assert!(events(&frames).is_empty(), "deleted article served");
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn nip40_expired_rejected() {
    let k = key();
    let ev = sign::event(
        &k,
        1,
        now() - 30,
        &json!([["expiration", (now() - 10).to_string()]]),
        "already expired",
    );
    let mut ws = connect().await;
    let (ok, reason) = publish(&mut ws, &ev).await;
    assert!(!ok && reason.starts_with("invalid:"), "{reason}");
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn nip40_expiry_removes_from_results() {
    let k = key();
    let ev = sign::event(
        &k,
        1,
        now() - 1,
        &json!([["expiration", (now() + 2).to_string()]]),
        "expiring soon",
    );
    let mut ws = connect().await;
    let (ok, reason) = publish(&mut ws, &ev).await;
    assert!(ok, "publish: {reason}");

    tokio::time::sleep(Duration::from_secs(3)).await;
    let filter = format!("{{\"authors\":[\"{}\"]}}", pubkey(&k));
    let frames = req(&mut ws, "s", &filter).await;
    assert!(
        events(&frames).is_empty(),
        "expired event served: {frames:?}"
    );
}

/// The NIP-42 flow shared by the AUTH tests: REQ a restricted kind,
/// capture the challenge from the AUTH frame, sign kind 22242 as
/// `signer`, and assert acceptance.
async fn authenticate(ws: &mut yawc::TcpWebSocket, signer: &Keypair, req_filter: &str) {
    let frames = req(ws, "r", req_filter).await;
    let challenge = frames
        .iter()
        .find(|v| v[0] == "AUTH")
        .map(|v| v[1].as_str().unwrap().to_owned())
        .expect("no AUTH challenge sent");
    assert!(
        frames
            .iter()
            .any(|v| v[0] == "CLOSED" && v[2].as_str().unwrap().starts_with("auth-required:")),
        "restricted REQ not refused: {frames:?}"
    );
    let auth = sign::event(
        signer,
        22242,
        now(),
        &json!([["relay", url()], ["challenge", challenge]]),
        "",
    );
    send(ws, &format!("[\"AUTH\",{}]", auth.json())).await;
    let ok = collect_until(ws, |v| v[0] == "OK").await;
    assert_eq!(ok.last().unwrap()[2], true, "AUTH rejected");
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn nip42_restricted_reads() {
    let author = key();
    let reader = key();
    let involved = sign::event(
        &author,
        4,
        now() - 5,
        &json!([["p", pubkey(&reader)]]),
        "restricted dm",
    );
    {
        let mut publisher = connect().await;
        let (ok, reason) = publish(&mut publisher, &involved).await;
        assert!(ok, "restricted publish: {reason}");
    }

    let mut ws = connect().await;
    let filter = format!("{{\"kinds\":[4],\"authors\":[\"{}\"]}}", pubkey(&author));
    authenticate(&mut ws, &reader, &filter).await;

    let frames = req(&mut ws, "s", &filter).await;
    let got = events(&frames);
    assert_eq!(got.len(), 1, "restricted event not served: {frames:?}");
    assert_eq!(got[0]["id"], involved.id().to_string());
}

#[tokio::test]
#[ignore = "needs NOSFRY_URL"]
async fn nip70_protected_events() {
    let k = key();
    let ev = sign::event(&k, 1, now(), &json!([["-"]]), "protected");
    let mut ws = connect().await;
    send(&mut ws, &event_wire(&ev)).await;
    let frames = collect_until(&mut ws, |v| v[0] == "OK").await;
    let ok = frames.last().unwrap();
    let reason = ok[3].as_str().unwrap();
    assert!(
        ok[2] == false && reason.starts_with("auth-required:"),
        "unauthenticated protected publish: {reason}"
    );

    // The relay sent its challenge alongside the rejection.
    let challenge = frames
        .iter()
        .find(|v| v[0] == "AUTH")
        .map(|v| v[1].as_str().unwrap().to_owned())
        .expect("no AUTH challenge sent");
    let auth = sign::event(
        &k,
        22242,
        now(),
        &json!([["relay", url()], ["challenge", challenge]]),
        "",
    );
    send(&mut ws, &format!("[\"AUTH\",{}]", auth.json())).await;
    let ok = collect_until(&mut ws, |v| v[0] == "OK").await;
    assert_eq!(ok.last().unwrap()[2], true, "AUTH rejected");

    let (ok, reason) = publish(&mut ws, &ev).await;
    assert!(ok, "protected publish after AUTH: {reason}");
}
