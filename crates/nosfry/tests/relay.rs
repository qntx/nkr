//! In-process relay integration tests over real websockets.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::tests_outside_test_module,
    clippy::shadow_unrelated,
    clippy::excessive_nesting,
    clippy::too_many_lines,
    unused_crate_dependencies,
    reason = "tests may panic on failure paths; deps cover every test binary"
)]

mod common;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::{Relay, close_wire, event_wire, key, recv_text, req_wire, send_text};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const T: Duration = Duration::from_secs(15);

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn json(frame: &str) -> Value {
    serde_json::from_str(frame).unwrap()
}

#[tokio::test]
async fn publish_ok_and_duplicate() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let ev = common::event(&key(1), 1, now(), &json!([]), "hi");
    send_text(&mut ws, &event_wire(&ev)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[0], "OK");
    assert_eq!(
        ok[1].as_str().unwrap(),
        json(ev.json())["id"].as_str().unwrap()
    );
    assert_eq!(ok[2], true);

    send_text(&mut ws, &event_wire(&ev)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[0], "OK");
    assert_eq!(ok[2], true);
    assert!(ok[3].as_str().unwrap().starts_with("duplicate:"));
    relay.stop().await;
}

#[tokio::test]
async fn publish_bad_signature_and_time_window() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;

    // Corrupt one hex nibble inside the signature, JSON stays valid.
    let ev = common::event(&key(2), 1, now(), &json!([]), "x");
    let mut doc = json(ev.json());
    let sig = doc["sig"].as_str().unwrap();
    let last = sig.chars().last().unwrap();
    let flipped = if last == '0' { '1' } else { '0' };
    doc["sig"] = json!(format!("{}{}", &sig[..sig.len() - 1], flipped));
    send_text(
        &mut ws,
        &format!("[\"EVENT\",{}]", serde_json::to_string(&doc).unwrap()),
    )
    .await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[0], "OK");
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap().starts_with("invalid:"));

    // Far-future event.
    let future = common::event(&key(2), 1, now() + 60 * 60 * 24, &json!([]), "f");
    send_text(&mut ws, &event_wire(&future)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[0], "OK");
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap().starts_with("invalid:"));
    relay.stop().await;
}

#[tokio::test]
async fn req_returns_stored_events_then_eose() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let base = now() - 100;
    let mut events = Vec::new();
    for i in 0..5u64 {
        let ev = common::event(&key(3), 1, base + i, &json!([]), &format!("e{i}"));
        send_text(&mut ws, &event_wire(&ev)).await;
        let _ok = recv_text(&mut ws, T).await;
        events.push(ev);
    }
    send_text(&mut ws, &req_wire("s1", "{\"kinds\":[1]}")).await;
    let mut got = Vec::new();
    loop {
        let frame = json(&recv_text(&mut ws, T).await);
        match frame[0].as_str().unwrap() {
            "EVENT" => got.push(frame[2]["id"].as_str().unwrap().to_owned()),
            "EOSE" => break,
            other => panic!("unexpected frame {other}"),
        }
    }
    // Newest first.
    let want: Vec<_> = events.iter().rev().map(|e| e.json().to_owned()).collect();
    let want_ids: Vec<String> = want
        .iter()
        .map(|j| json(j)["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(got, want_ids);
    relay.stop().await;
}

/// A 500-event stored result arrives complete and in newest-first
/// order before EOSE through the reader's chunked `Out::Frames` path.
#[tokio::test]
async fn req_500_events_complete_in_order_before_eose() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let base = now() - 10_000;
    let mut events = Vec::new();
    for i in 0..500u64 {
        let ev = common::event(&key(5), 1, base + i, &json!([]), &format!("e{i}"));
        send_text(&mut ws, &event_wire(&ev)).await;
        let _ok = recv_text(&mut ws, T).await;
        events.push(ev);
    }
    send_text(&mut ws, &req_wire("s", "{\"kinds\":[1],\"limit\":500}")).await;
    let mut got = Vec::new();
    loop {
        let frame = json(&recv_text(&mut ws, T).await);
        match frame[0].as_str().unwrap() {
            "EVENT" => got.push(frame[2]["id"].as_str().unwrap().to_owned()),
            "EOSE" => break,
            other => panic!("unexpected frame {other}"),
        }
    }
    let want: Vec<String> = events
        .iter()
        .rev()
        .map(|e| json(e.json())["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(got, want);
    relay.stop().await;
}

#[tokio::test]
async fn req_limit_and_multi_filter_dedup() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let base = now() - 100;
    for i in 0..10u64 {
        let ev = common::event(&key(4), 1, base + i, &json!([]), "x");
        send_text(&mut ws, &event_wire(&ev)).await;
        let _ok = recv_text(&mut ws, T).await;
    }
    send_text(
        &mut ws,
        &req_wire(
            "s",
            "{\"kinds\":[1],\"limit\":3},{\"kinds\":[1],\"limit\":3}",
        ),
    )
    .await;
    let mut got = std::collections::HashSet::new();
    loop {
        let frame = json(&recv_text(&mut ws, T).await);
        match frame[0].as_str().unwrap() {
            "EVENT" => {
                got.insert(frame[2]["id"].as_str().unwrap().to_owned());
            }
            "EOSE" => break,
            other => panic!("unexpected {other}"),
        }
    }
    // Two identical filters dedup to the first one's 3 events.
    assert_eq!(got.len(), 3);
    relay.stop().await;
}

#[tokio::test]
async fn live_event_after_eose_and_close_stops() {
    let relay = Relay::start().await;
    let mut sub_ws = relay.connect().await;
    let mut pub_ws = relay.connect().await;
    send_text(&mut sub_ws, &req_wire("s", "{\"kinds\":[1]}")).await;
    loop {
        let frame = json(&recv_text(&mut sub_ws, T).await);
        if frame[0] == "EOSE" {
            break;
        }
    }
    let ev = common::event(&key(5), 1, now(), &json!([]), "live");
    send_text(&mut pub_ws, &event_wire(&ev)).await;
    let _ok = recv_text(&mut pub_ws, T).await;
    let frame = json(&recv_text(&mut sub_ws, T).await);
    assert_eq!(frame[0], "EVENT");
    assert_eq!(
        frame[2]["id"],
        json(ev.json()).get("id").unwrap().as_str().unwrap()
    );

    // CLOSE stops delivery. There is no cross-connection ordering, so
    // barrier on sub_ws first: inbound frames are processed in order,
    // and the EOSE for the impossible filter proves the CLOSE was
    // applied in the session before the barrier's scan ran.
    send_text(&mut sub_ws, &close_wire("s")).await;
    send_text(&mut sub_ws, &req_wire("barrier", "{\"ids\":[]}")).await;
    loop {
        let frame = json(&recv_text(&mut sub_ws, T).await);
        if frame[0] == "EOSE" && frame[1] == "barrier" {
            break;
        }
    }
    let ev2 = common::event(&key(5), 1, now(), &json!([]), "live2");
    send_text(&mut pub_ws, &event_wire(&ev2)).await;
    let _ok = recv_text(&mut pub_ws, T).await;
    let frame = tokio::time::timeout(Duration::from_millis(800), sub_ws.next()).await;
    if let Ok(Some(f)) = frame
        && f.opcode() == yawc::OpCode::Text
    {
        panic!("unexpected delivery after CLOSE: {:?}", f.payload());
    }
    relay.stop().await;
}

/// Read-your-writes: the writer queues each batch on every shard
/// before pushing the publisher's `OK`, so a `REQ` sent right after
/// `OK` must find the event in its snapshot and emit it before `EOSE`.
#[tokio::test]
async fn req_after_ok_sees_event_before_eose() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    for i in 0..200u32 {
        let ev = common::event(&key(10), 1, now(), &json!([]), &format!("ryw{i}"));
        let id = json(ev.json())["id"].as_str().unwrap().to_owned();
        send_text(&mut ws, &event_wire(&ev)).await;
        let ok = json(&recv_text(&mut ws, T).await);
        assert_eq!(ok[0], "OK");
        assert_eq!(ok[2], true);
        let sub = format!("s{i}");
        send_text(&mut ws, &req_wire(&sub, &format!("{{\"ids\":[\"{id}\"]}}"))).await;
        let frame = json(&recv_text(&mut ws, T).await);
        assert_eq!(frame[0], "EVENT", "iteration {i}: event must precede EOSE");
        assert_eq!(frame[2]["id"], id);
        assert_eq!(json(&recv_text(&mut ws, T).await)[0], "EOSE");
    }
    relay.stop().await;
}

#[tokio::test]
async fn re_req_same_id_replaces_generation() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    send_text(&mut ws, &req_wire("s", "{\"kinds\":[1]}")).await;
    // Drain EOSE.
    loop {
        if json(&recv_text(&mut ws, T).await)[0] == "EOSE" {
            break;
        }
    }
    // Re-REQ under the same id with a different filter.
    send_text(&mut ws, &req_wire("s", "{\"kinds\":[2]}")).await;
    loop {
        if json(&recv_text(&mut ws, T).await)[0] == "EOSE" {
            break;
        }
    }
    // A kind-1 event must no longer be delivered on "s".
    let ev1 = common::event(&key(6), 1, now(), &json!([]), "old-filter");
    send_text(&mut ws, &event_wire(&ev1)).await;
    let _ok = recv_text(&mut ws, T).await;
    let ev2 = common::event(&key(6), 2, now(), &json!([]), "new-filter");
    send_text(&mut ws, &event_wire(&ev2)).await;
    let frame = loop {
        let frame = json(&recv_text(&mut ws, T).await);
        if frame[0] == "EVENT" {
            break frame;
        }
    };
    assert_eq!(frame[1], "s");
    assert_eq!(frame[2]["id"], json(ev2.json())["id"]);
    relay.stop().await;
}

#[tokio::test]
async fn replaceable_superseded_and_latest_only() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let k = key(7);
    let v2 = common::event(&k, 0, now(), &json!([]), "newer");
    let v1 = common::event(&k, 0, now() - 100, &json!([]), "older");
    let v3 = common::event(&k, 0, now() + 1, &json!([]), "newest");
    send_text(&mut ws, &event_wire(&v2)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], true);
    // Older version rejected as superseded.
    send_text(&mut ws, &event_wire(&v1)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[0], "OK");
    assert_eq!(ok[2], false);
    assert_eq!(ok[3], "duplicate: have a newer version");
    // Newer version replaces.
    send_text(&mut ws, &event_wire(&v3)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], true);
    send_text(&mut ws, &req_wire("s", "{\"kinds\":[0]}")).await;
    let frame = json(&recv_text(&mut ws, T).await);
    assert_eq!(frame[0], "EVENT");
    assert_eq!(frame[2]["id"], json(v3.json())["id"]);
    assert_eq!(json(&recv_text(&mut ws, T).await)[0], "EOSE");
    relay.stop().await;
}

#[tokio::test]
async fn ephemeral_live_only() {
    let relay = Relay::start().await;
    let mut sub_ws = relay.connect().await;
    let mut pub_ws = relay.connect().await;
    send_text(&mut sub_ws, &req_wire("s", "{\"kinds\":[20000]}")).await;
    loop {
        if json(&recv_text(&mut sub_ws, T).await)[0] == "EOSE" {
            break;
        }
    }
    let ev = common::event(&key(8), 20_000, now(), &json!([]), "eph");
    send_text(&mut pub_ws, &event_wire(&ev)).await;
    let ok = json(&recv_text(&mut pub_ws, T).await);
    assert_eq!(ok[2], true);
    let frame = json(&recv_text(&mut sub_ws, T).await);
    assert_eq!(frame[0], "EVENT");
    // A later REQ finds nothing stored.
    send_text(&mut sub_ws, &req_wire("s2", "{\"kinds\":[20000]}")).await;
    assert_eq!(json(&recv_text(&mut sub_ws, T).await)[0], "EOSE");
    relay.stop().await;
}

#[tokio::test]
async fn durability_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = |path: &std::path::Path| {
        let mut c = nosfry::config::Config::default();
        c.db.path = path.to_path_buf();
        c
    };
    let ev = common::event(&key(9), 1, now(), &json!([]), "durable");
    {
        let relay = Relay::start_with(config(dir.path())).await;
        let mut ws = relay.connect().await;
        send_text(&mut ws, &event_wire(&ev)).await;
        let _ok = recv_text(&mut ws, T).await;
        relay.stop().await;
    }
    {
        let relay = Relay::start_with(config(dir.path())).await;
        let mut ws = relay.connect().await;
        send_text(&mut ws, &req_wire("s", "{\"kinds\":[1]}")).await;
        let frame = json(&recv_text(&mut ws, T).await);
        assert_eq!(frame[0], "EVENT");
        assert_eq!(frame[2]["id"], json(ev.json())["id"]);
        assert_eq!(json(&recv_text(&mut ws, T).await)[0], "EOSE");
        relay.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exactly_once_across_stored_and_live() {
    const PUBLISHERS: usize = 4;
    const PER_PUB: usize = 250;
    let mut config = nosfry::config::Config::default();
    config.relay.query_timeslice_budget_microseconds = 1;
    config.relay.limits.max_filter_limit = 5_000;
    let relay = Relay::start_with(config).await;

    // Publishers send continuously.
    let mut pub_tasks = Vec::new();
    for p in 0..PUBLISHERS {
        let addr = relay.addr;
        pub_tasks.push(tokio::spawn(async move {
            let url = format!("ws://{addr}/");
            let mut ws = yawc::WebSocket::connect(url.parse().unwrap())
                .await
                .unwrap();
            let k = key(20 + u8::try_from(p).unwrap());
            let base = now();
            let mut oks = 0usize;
            for i in 0..PER_PUB as u64 {
                let ev = common::event(&k, 1, base + i, &json!([]), &format!("p{p}e{i}"));
                ws.send(yawc::Frame::text(event_wire(&ev))).await.unwrap();
                // Drain OKs lazily to keep the socket writable.
                if i % 50 == 49 {
                    while let Ok(Some(f)) =
                        tokio::time::timeout(Duration::from_millis(50), ws.next()).await
                    {
                        if f.opcode() == yawc::OpCode::Text
                            && json(std::str::from_utf8(f.payload()).unwrap())[0] == "OK"
                        {
                            oks += 1;
                        }
                    }
                }
            }
            // Await every OK so the peer's replies do not linger in our
            // receive buffer; dropping the socket with unread inbound data
            // sends a TCP RST that discards our un-acked tail frames.
            let deadline = Instant::now() + Duration::from_secs(30);
            while oks < PER_PUB && Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
                    Ok(Some(f)) if f.opcode() == yawc::OpCode::Text => {
                        let v = json(std::str::from_utf8(f.payload()).unwrap());
                        if v[0] == "OK" {
                            assert_eq!(v[2], true, "pub{p} rejected: {v}");
                            oks += 1;
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            assert_eq!(oks, PER_PUB, "pub{p} missing OKs");
            // Graceful close handshake, then drain until EOF so the FIN is
            // not upgraded to an RST by lingering inbound data.
            ws.send(yawc::Frame::close(yawc::close::CloseCode::Normal, &[][..]))
                .await
                .ok();
            while let Ok(Some(_)) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await {}
        }));
    }

    // Subscribers issue REQs at staggered moments.
    let mut sub_tasks = Vec::new();
    for s in 0..3usize {
        let addr = relay.addr;
        sub_tasks.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100 * s as u64)).await;
            let url = format!("ws://{addr}/");
            let mut ws = yawc::WebSocket::connect(url.parse().unwrap())
                .await
                .unwrap();
            send_text(&mut ws, &req_wire("s", "{\"kinds\":[1],\"limit\":5000}")).await;
            let mut ids = std::collections::HashSet::new();
            let deadline = Instant::now() + Duration::from_secs(60);
            while Instant::now() < deadline && ids.len() < PUBLISHERS * PER_PUB {
                let Ok(Some(frame)) =
                    tokio::time::timeout(Duration::from_secs(10), ws.next()).await
                else {
                    continue;
                };
                if frame.opcode() != yawc::OpCode::Text {
                    continue;
                }
                let v = json(std::str::from_utf8(frame.payload()).unwrap());
                if v[0] == "EVENT" {
                    let id = v[2]["id"].as_str().unwrap().to_owned();
                    assert!(ids.insert(id), "duplicate delivery: {v}");
                }
            }
            ids.len()
        }));
    }
    for t in pub_tasks {
        t.await.unwrap();
    }
    for t in sub_tasks {
        let count = t.await.unwrap();
        assert_eq!(count, PUBLISHERS * PER_PUB);
    }
    relay.stop().await;
}

#[tokio::test]
async fn too_many_subs_and_unknown_filter_key() {
    let mut config = nosfry::config::Config::default();
    config.relay.limits.max_subs_per_connection = 1;
    let relay = Relay::start_with(config).await;
    let mut ws = relay.connect().await;
    send_text(&mut ws, &req_wire("a", "{\"kinds\":[1]}")).await;
    loop {
        if json(&recv_text(&mut ws, T).await)[0] == "EOSE" {
            break;
        }
    }
    send_text(&mut ws, &req_wire("b", "{\"kinds\":[2]}")).await;
    let frame = json(&recv_text(&mut ws, T).await);
    assert_eq!(frame[0], "CLOSED");
    assert_eq!(frame[1], "b");
    assert!(frame[2].as_str().unwrap().starts_with("error:"));

    send_text(&mut ws, &req_wire("a", "{\"kinds\":[1],\"bogus\":true}")).await;
    let frame = json(&recv_text(&mut ws, T).await);
    assert_eq!(frame[0], "CLOSED");
    assert!(frame[2].as_str().unwrap().starts_with("unsupported:"));
    relay.stop().await;
}

#[tokio::test]
async fn oversized_frame_closes_connection() {
    let mut config = nosfry::config::Config::default();
    config.relay.max_websocket_payload_size = 1024;
    let relay = Relay::start_with(config).await;
    let mut ws = relay.connect().await;
    let big = format!(
        "[\"REQ\",\"s\",{{\"kinds\":[1],\"search\":\"{}\"}}]",
        "x".repeat(4096)
    );
    send_text(&mut ws, &big).await;
    // The connection should be torn down without a valid relay reply.
    let deadline = Instant::now() + T;
    loop {
        let Some(frame) = tokio::time::timeout(T, ws.next()).await.ok().flatten() else {
            break;
        };
        if Instant::now() > deadline || frame.opcode() == yawc::OpCode::Close {
            break;
        }
        assert_ne!(
            frame.opcode(),
            yawc::OpCode::Text,
            "unexpected reply to oversized frame"
        );
    }
    relay.stop().await;
}

/// One HTTP GET returning the raw response text.
async fn http_get(addr: std::net::SocketAddr) -> String {
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(
        b"GET / HTTP/1.1\r\nHost: x\r\nAccept: application/nostr+json\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    tcp.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}

fn nip11_body(text: &str) -> Value {
    let (_, body) = text.split_once("\r\n\r\n").expect("http head/body split");
    serde_json::from_str(body).expect("nip11 json")
}

#[tokio::test]
async fn nip11_document() {
    // Empty info fields are omitted; `name` stays unconditionally.
    let relay = Relay::start().await;
    let text = http_get(relay.addr).await;
    assert!(text.contains("content-type: application/nostr+json"));
    assert!(text.contains("access-control-allow-origin: *"));
    let doc = nip11_body(&text);
    assert_eq!(doc["name"], "nosfry");
    for key in ["description", "pubkey", "contact", "icon"] {
        assert!(doc.get(key).is_none(), "{key} must be omitted when empty");
    }
    assert_eq!(doc["supported_nips"], json!([1, 9, 11, 40]));
    assert_eq!(doc["software"], "https://github.com/qntx/nosfry");
    assert_eq!(doc["limitation"]["max_subscriptions"], 200);
    assert_eq!(
        doc["limitation"]["max_subid_length"],
        nosfry_core::message::SUB_ID_MAX_CHARS as u64
    );
    relay.stop().await;

    // Configured fields are emitted verbatim.
    let mut config = nosfry::config::Config::default();
    config.relay.info.description = "test relay".to_owned();
    config.relay.info.pubkey = "ab".repeat(32);
    config.relay.info.contact = "admin@example.com".to_owned();
    config.relay.info.icon = "https://example.com/icon.png".to_owned();
    let relay = Relay::start_with(config).await;
    let doc = nip11_body(&http_get(relay.addr).await);
    assert_eq!(doc["description"], "test relay");
    assert_eq!(doc["pubkey"], "ab".repeat(32));
    assert_eq!(doc["contact"], "admin@example.com");
    assert_eq!(doc["icon"], "https://example.com/icon.png");
    relay.stop().await;
}

#[tokio::test]
async fn slow_consumer_is_disconnected() {
    let mut config = nosfry::config::Config::default();
    config.relay.max_pending_outbound_bytes = 4096;
    let relay = Relay::start_with(config).await;
    // Fill the store: the scan will push more than 4 KiB of events.
    {
        let mut ws = relay.connect().await;
        for i in 0..50u64 {
            let ev = common::event(&key(30), 1, now(), &json!([]), &format!("e{i:03}"));
            send_text(&mut ws, &event_wire(&ev)).await;
            let _ok = recv_text(&mut ws, T).await;
        }
    }
    // This client never reads; its outbound queue must overflow.
    let mut ws = relay.connect().await;
    {
        ws.send(yawc::Frame::text(req_wire("s", "{\"kinds\":[1]}")))
            .await
            .unwrap();
    }
    let close = tokio::time::timeout(T, async {
        loop {
            match ws.next().await {
                Some(f) if f.opcode() == yawc::OpCode::Close => break Some(f),
                Some(_) => {}
                None => break None,
            }
        }
    })
    .await;
    match close {
        Ok(Some(frame)) => {
            let reason = String::from_utf8_lossy(frame.payload());
            assert!(reason.contains("slow consumer") || frame.close_code().is_some());
        }
        Ok(None) => {} // server dropped the connection outright
        Err(err) => panic!("connection stayed open past the outbound cap: {err}"),
    }
    relay.stop().await;
}

#[tokio::test]
async fn half_open_connection_times_out() {
    let mut config = nosfry::config::Config::default();
    config.relay.auto_ping_seconds = 1;
    let relay = Relay::start_with(config).await;

    // A raw client that completes the upgrade and then goes silent:
    // it never reads or answers the server's pings.
    let mut tcp = tokio::net::TcpStream::connect(relay.addr).await.unwrap();
    let request = format!(
        "GET / HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n",
        relay.addr
    );
    tcp.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    let mut chunk = [0u8; 1024];
    while !response.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = tcp.read(&mut chunk).await.unwrap();
        assert!(n > 0, "upgrade failed: {response:?}");
        response.extend_from_slice(&chunk[..n]);
    }
    assert!(response.starts_with(b"HTTP/1.1 101"));

    // The server must close the silent connection after two ping
    // intervals. Read until EOF and pull the code out of the close
    // frame (server frames are unmasked).
    let mut close_code = None;
    let mut buf = Vec::new();
    tokio::time::timeout(T, async {
        tcp.read_to_end(&mut buf).await.unwrap();
        let mut i = 0;
        while i + 2 <= buf.len() {
            let opcode = buf[i] & 0x0f;
            let (len, hdr) = match buf[i + 1] & 0x7f {
                126 if i + 4 <= buf.len() => {
                    (usize::from(u16::from_be_bytes([buf[i + 2], buf[i + 3]])), 4)
                }
                127 if i + 10 <= buf.len() => (
                    usize::try_from(u64::from_be_bytes(buf[i + 2..i + 10].try_into().unwrap()))
                        .unwrap(),
                    10,
                ),
                n @ 0..=125 => (usize::from(n), 2),
                _ => break,
            };
            if i + hdr + len > buf.len() {
                break;
            }
            if opcode == 0x8 && len >= 2 {
                close_code = Some(u16::from_be_bytes([buf[i + hdr], buf[i + hdr + 1]]));
            }
            i += hdr + len;
        }
    })
    .await
    .expect("half-open connection was not closed");
    assert_eq!(close_code, Some(1001));
    relay.stop().await;
}

#[tokio::test]
async fn shutdown_with_stuck_client_returns() {
    let mut config = nosfry::config::Config::default();
    config.relay.auto_ping_seconds = 1;
    let relay = Relay::start_with(config).await;
    // Fill the store so the REQ produces a large outbound backlog.
    {
        let mut ws = relay.connect().await;
        for i in 0..200u64 {
            let ev = common::event(&key(40), 1, now(), &json!([]), &format!("e{i:03}"));
            send_text(&mut ws, &event_wire(&ev)).await;
            let _ok = recv_text(&mut ws, T).await;
        }
    }
    // This client issues the REQ and never reads another frame.
    let ws = relay.connect().await;
    {
        let mut ws = ws;
        ws.send(yawc::Frame::text(req_wire("s", "{\"kinds\":[1]}")))
            .await
            .unwrap();
    }
    // `serve` must return even though the client never drains its
    // socket: write timeouts and the tracker deadline bound it.
    tokio::time::timeout(Duration::from_secs(15), relay.stop())
        .await
        .expect("serve did not return with a stuck client");
}

/// Collect the event ids of one REQ until EOSE.
async fn req_ids(ws: &mut yawc::TcpWebSocket, sub: &str, filter: &str) -> Vec<String> {
    send_text(ws, &req_wire(sub, filter)).await;
    let mut got = Vec::new();
    loop {
        let frame = json(&recv_text(ws, T).await);
        match frame[0].as_str().unwrap() {
            "EVENT" => got.push(frame[2]["id"].as_str().unwrap().to_owned()),
            "EOSE" => return got,
            other => panic!("unexpected frame {other}"),
        }
    }
}

#[tokio::test]
async fn nip09_delete_by_id_hides_and_blocks() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let k = key(30);
    let note = common::event(&k, 1, now(), &json!([]), "gone");
    let note_id = json(note.json())["id"].as_str().unwrap().to_owned();
    send_text(&mut ws, &event_wire(&note)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], true);

    let kind5 = common::event(&k, 5, now(), &json!([["e", note_id]]), "");
    send_text(&mut ws, &event_wire(&kind5)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], true);

    // The deleted event no longer matches a REQ; the kind 5 remains.
    let got = req_ids(&mut ws, "d1", "{}").await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0], json(kind5.json())["id"].as_str().unwrap());

    // Republishing the deleted id is blocked.
    send_text(&mut ws, &event_wire(&note)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[0], "OK");
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap().starts_with("blocked:"));
    relay.stop().await;
}

#[tokio::test]
async fn nip09_delete_by_address() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let k = key(31);
    let pk = format!("{}", k.x_only_public_key().0);
    let v1 = common::event(&k, 30_023, now(), &json!([["d", "art"]]), "v1");
    let v1_id = json(v1.json())["id"].as_str().unwrap().to_owned();
    send_text(&mut ws, &event_wire(&v1)).await;
    let _ok = recv_text(&mut ws, T).await;

    let kind5 = common::event(&k, 5, now(), &json!([["a", format!("30023:{pk}:art")]]), "");
    send_text(&mut ws, &event_wire(&kind5)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], true);

    let got = req_ids(&mut ws, "a1", "{\"kinds\":[30023]}").await;
    assert!(got.is_empty(), "addressable event still visible: {got:?}");

    // Same-address republish at an older timestamp is blocked; a newer
    // version is accepted.
    send_text(&mut ws, &event_wire(&v1)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap().starts_with("blocked:"));
    let v2 = common::event(&k, 30_023, now() + 1, &json!([["d", "art"]]), "v2");
    send_text(&mut ws, &event_wire(&v2)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], true, "newer version rejected: {ok}");
    let _ = v1_id;
    relay.stop().await;
}

#[tokio::test]
async fn nip40_expired_event_rejected_on_ingest() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let ev = common::event(
        &key(32),
        1,
        now(),
        &json!([["expiration", (now() - 10).to_string()]]),
        "stale",
    );
    send_text(&mut ws, &event_wire(&ev)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[0], "OK");
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap().starts_with("invalid:"));
    relay.stop().await;
}

#[tokio::test]
async fn nip40_expiring_event_hidden_after_expiry() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let ev = common::event(
        &key(33),
        1,
        now(),
        &json!([["expiration", (now() + 2).to_string()]]),
        "short-lived",
    );
    let ev_id = json(ev.json())["id"].as_str().unwrap().to_owned();
    send_text(&mut ws, &event_wire(&ev)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], true);

    // Visible before expiry.
    let got = req_ids(&mut ws, "x1", "{\"kinds\":[1]}").await;
    assert_eq!(got, vec![ev_id.clone()]);

    // Past the expiration the same REQ hides it — before any sweep.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let got = req_ids(&mut ws, "x2", "{\"kinds\":[1]}").await;
    assert!(got.is_empty(), "expired event still served: {got:?}");
    relay.stop().await;
}

/// A client holding a TCP connection without finishing its request
/// head is dropped once the header-read deadline passes (slowloris).
#[tokio::test]
async fn incomplete_request_headers_time_out() {
    let relay = Relay::start().await;
    let mut tcp = tokio::net::TcpStream::connect(relay.addr).await.unwrap();
    // A partial request line and one header, never completed.
    tcp.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap();
    let start = Instant::now();
    let mut buf = [0u8; 64];
    let read = tokio::time::timeout(Duration::from_secs(12), tcp.read(&mut buf)).await;
    match read {
        Ok(Ok(0) | Err(..)) => {}
        Ok(Ok(n)) => panic!("server kept a stalled request head alive: {n} bytes read"),
        Err(..) => panic!("header-read deadline never fired"),
    }
    assert!(
        start.elapsed() >= Duration::from_secs(9),
        "connection closed early: {:?}",
        start.elapsed()
    );
    relay.stop().await;
}

/// A client-sent (masked) frame; `first` carries FIN/RSV/opcode.
fn masked_frame(first: u8, payload: &[u8]) -> Vec<u8> {
    let mask = [0x11, 0x22, 0x33, 0x44];
    let mut frame = vec![first];
    match payload.len() {
        0..=125 => frame.push(0x80 | u8::try_from(payload.len()).unwrap()),
        126..=65535 => {
            frame.push(0x80 | 0x7E);
            frame.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_be_bytes());
        }
        n => {
            frame.push(0x80 | 0x7F);
            frame.extend_from_slice(&u64::try_from(n).unwrap().to_be_bytes());
        }
    }
    frame.extend_from_slice(&mask);
    frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    frame
}

/// Read raw (unmasked) server frames into `(opcode, payload)` pairs.
async fn read_server_frames(tcp: &mut tokio::net::TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut buf = Vec::new();
    tcp.read_to_end(&mut buf).await.unwrap();
    let mut frames = Vec::new();
    let mut i = 0;
    while i + 2 <= buf.len() {
        let opcode = buf[i] & 0x0f;
        let (len, hdr) = match buf[i + 1] & 0x7f {
            126 if i + 4 <= buf.len() => {
                (usize::from(u16::from_be_bytes([buf[i + 2], buf[i + 3]])), 4)
            }
            127 if i + 10 <= buf.len() => (
                usize::try_from(u64::from_be_bytes(buf[i + 2..i + 10].try_into().unwrap()))
                    .unwrap(),
                10,
            ),
            n @ 0..=125 => (usize::from(n), 2),
            _ => break,
        };
        if i + hdr + len > buf.len() {
            break;
        }
        frames.push((opcode, buf[i + hdr..i + hdr + len].to_vec()));
        i += hdr + len;
    }
    frames
}

/// With the default config permessage-deflate is off: a client offering
/// it gets no extension header back, and a compressed (RSV1) frame is a
/// protocol error.
#[tokio::test]
async fn compression_not_negotiated_and_rsv1_rejected() {
    let relay = Relay::start().await;
    let mut tcp = tokio::net::TcpStream::connect(relay.addr).await.unwrap();
    let request = format!(
        "GET / HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n",
        relay.addr
    );
    tcp.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    let mut chunk = [0u8; 1024];
    while !response.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = tcp.read(&mut chunk).await.unwrap();
        assert!(n > 0, "upgrade failed: {response:?}");
        response.extend_from_slice(&chunk[..n]);
    }
    assert!(response.starts_with(b"HTTP/1.1 101"), "{response:?}");
    let head = String::from_utf8_lossy(&response).to_lowercase();
    assert!(
        !head.contains("sec-websocket-extensions"),
        "compression negotiated despite being off: {head}"
    );

    // A compressed frame (FIN|RSV1|text) must close the connection,
    // not be processed. yawc queues a 1002 close frame but drops the
    // socket without flushing it, so EOF is the observable signal.
    tcp.write_all(&masked_frame(0xC1, b"hi")).await.unwrap();
    let mut close_code = None;
    tokio::time::timeout(T, async {
        for (opcode, payload) in read_server_frames(&mut tcp).await {
            if opcode == 0x8 && payload.len() >= 2 {
                close_code = Some(u16::from_be_bytes([payload[0], payload[1]]));
            }
        }
    })
    .await
    .expect("RSV1 frame did not close the connection");
    assert!(close_code.is_none() || close_code == Some(1002));
    relay.stop().await;
}

/// Automatic pongs flush on inbound pings: 1000 back-to-back pings get
/// their pongs without waiting for other outbound traffic.
#[tokio::test]
async fn pings_are_answered_while_idle() {
    const N: u64 = 1_000;
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    for i in 0..N {
        ws.send(yawc::frame::Frame::ping(i.to_le_bytes().to_vec()))
            .await
            .unwrap();
    }
    let mut pongs = 0u64;
    tokio::time::timeout(Duration::from_secs(10), async {
        while pongs < N {
            let frame = ws.next().await.expect("connection closed");
            match frame.opcode() {
                yawc::OpCode::Pong => pongs += 1,
                yawc::OpCode::Ping => ws.send(yawc::frame::Frame::pong(Vec::new())).await.unwrap(),
                other => panic!("unexpected frame: {other:?}"),
            }
        }
    })
    .await
    .expect("pongs did not arrive");
    assert_eq!(pongs, N);
    relay.stop().await;
}
