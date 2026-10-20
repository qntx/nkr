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

use std::future::Future;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::{Relay, close_wire, count_wire, event_wire, key, recv_text, req_wire, send_text};
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
        let mut c = common::config();
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
    let mut config = common::config();
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
    let mut config = common::config();
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
    let mut config = common::config();
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
async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(
        format!(
            "GET {path} HTTP/1.1\r\nHost: x\r\nAccept: application/nostr+json\r\nConnection: close\r\n\r\n"
        )
        .as_bytes(),
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
    let text = http_get(relay.addr, "/").await;
    assert!(text.contains("content-type: application/nostr+json"));
    assert!(text.contains("access-control-allow-origin: *"));
    let doc = nip11_body(&text);
    assert_eq!(doc["name"], "nosfry");
    for key in ["description", "pubkey", "contact", "icon"] {
        assert!(doc.get(key).is_none(), "{key} must be omitted when empty");
    }
    assert_eq!(doc["supported_nips"], json!([1, 9, 11, 40, 45, 70]));
    assert_eq!(doc["software"], "https://github.com/qntx/nosfry");
    assert_eq!(doc["limitation"]["max_subscriptions"], 200);
    assert_eq!(
        doc["limitation"]["max_subid_length"],
        nosfry_core::message::SUB_ID_MAX_CHARS as u64
    );
    relay.stop().await;

    // Configured fields are emitted verbatim.
    let mut config = common::config();
    config.relay.info.description = "test relay".to_owned();
    config.relay.info.pubkey = "ab".repeat(32);
    config.relay.info.contact = "admin@example.com".to_owned();
    config.relay.info.icon = "https://example.com/icon.png".to_owned();
    let relay = Relay::start_with(config).await;
    let doc = nip11_body(&http_get(relay.addr, "/").await);
    assert_eq!(doc["description"], "test relay");
    assert_eq!(doc["pubkey"], "ab".repeat(32));
    assert_eq!(doc["contact"], "admin@example.com");
    assert_eq!(doc["icon"], "https://example.com/icon.png");
    relay.stop().await;
}

#[tokio::test]
async fn slow_consumer_is_disconnected() {
    let mut config = common::config();
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
    let mut config = common::config();
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
    let mut config = common::config();
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
    // Close a1: a stored republish would otherwise be delivered live
    // on this socket, racing the OK frame the reads below expect.
    send_text(&mut ws, &close_wire("a1")).await;

    // Same-address republish at an older timestamp is blocked; a newer
    // version is accepted.
    send_text(&mut ws, &event_wire(&v1)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], false);
    assert!(ok[3].as_str().unwrap().starts_with("blocked:"));
    let v2 = common::event(&k, 30_023, now() + 1, &json!([["d", "art"]]), "v2");
    send_text(&mut ws, &event_wire(&v2)).await;
    let ok = recv_ok(&mut ws, &v2).await;
    assert_eq!(ok[2], true, "newer version rejected: {ok}");
    let _ = v1_id;
    relay.stop().await;
}

#[tokio::test]
async fn req_addressable_kinds_without_d_returns_all_addresses() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let k = key(33);
    let pk = format!("{}", k.x_only_public_key().0);
    let mut ids = Vec::new();
    let mut b_id = String::new();
    for d in ["a", "b", "c"] {
        let ev = common::event(&k, 30_023, now(), &json!([["d", d]]), d);
        send_text(&mut ws, &event_wire(&ev)).await;
        assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
        if d == "b" {
            b_id = ev.id().to_string();
        }
        ids.push(ev.id().to_string());
    }
    // `intrinsic_limit` must not bound an addressable author+kind
    // query that has no `#d` condition.
    let mut got = req_ids(
        &mut ws,
        "al",
        &format!("{{\"kinds\":[30023],\"authors\":[\"{pk}\"]}}"),
    )
    .await;
    got.sort();
    ids.sort();
    assert_eq!(got, ids);
    // With `#d` only the listed addresses match.
    let got = req_ids(
        &mut ws,
        "dx",
        &format!("{{\"kinds\":[30023],\"authors\":[\"{pk}\"],\"#d\":[\"b\"]}}"),
    )
    .await;
    assert_eq!(got, vec![b_id]);
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

/// Raw HTTP/1.1 WebSocket upgrade attempt; returns the response
/// status line ("HTTP/1.1 101 ..." / "... 429 ..."). The connection
/// is dropped right after the status line is read.
async fn upgrade_status(addr: std::net::SocketAddr) -> String {
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(
        b"GET / HTTP/1.1\r\nHost: nosfry\r\nUpgrade: websocket\r\n\
          Connection: upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
          Sec-WebSocket-Version: 13\r\n\r\n",
    )
    .await
    .unwrap();
    let mut buf = [0u8; 512];
    let n = tcp.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n])
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned()
}

#[tokio::test]
async fn connection_cap_per_ip() {
    let mut config = common::config();
    config.relay.max_connections_per_ip = 2;
    let relay = Relay::start_with(config).await;
    let ws1 = relay.connect().await;
    let ws2 = relay.connect().await;
    // A third upgrade from the same IP is refused before the upgrade.
    let status = upgrade_status(relay.addr).await;
    assert!(status.contains("429"), "{status}");
    // Freeing a slot admits the next connection. The release happens
    // when the server task observes the close, so poll briefly.
    drop(ws1);
    let mut status = String::new();
    for _ in 0..30 {
        status = upgrade_status(relay.addr).await;
        if status.contains("101") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(status.contains("101"), "{status}");
    drop(ws2);
    relay.stop().await;
}

#[tokio::test]
async fn connection_cap_total() {
    let mut config = common::config();
    config.relay.max_connections = 1;
    let relay = Relay::start_with(config).await;
    let _ws1 = relay.connect().await;
    let status = upgrade_status(relay.addr).await;
    assert!(status.contains("429"), "{status}");
    relay.stop().await;
}

#[tokio::test]
async fn event_rate_limit_is_per_connection() {
    let mut config = common::config();
    config.relay.rate_limits = nosfry_core::limits::RateLimits::default();
    let relay = Relay::start_with(config).await;

    // Client A floods 500 events: past the 100-event burst the OKs
    // turn `rate-limited:` while the connection stays open.
    let mut a = relay.connect().await;
    let base = now();
    for i in 0..500u64 {
        let ev = common::event(&key(30), 1, base, &json!([]), &format!("a{i}"));
        send_text(&mut a, &event_wire(&ev)).await;
    }
    let (mut accepted, mut limited) = (0usize, 0usize);
    for _ in 0..500 {
        let ok = recv_text(&mut a, T).await;
        if ok.contains("rate-limited: slow down") {
            limited += 1;
        } else {
            accepted += 1;
        }
    }
    assert!(limited > 0, "500 events never hit the rate limit");
    assert!(
        accepted <= 150,
        "burst is 100 plus ~20/s refill while reading: {accepted}"
    );

    // Client B on its own connection keeps a full-rate bucket: it
    // publishes and receives `OK true` and its live subscriber gets
    // the event inside a second.
    let mut b = relay.connect().await;
    send_text(&mut b, &req_wire("sub", "{\"kinds\":[1]}")).await;
    let ev_b = common::event(&key(31), 1, now(), &json!([]), "b");
    let id_b = json(ev_b.json())["id"].clone();
    send_text(&mut b, &event_wire(&ev_b)).await;
    let (mut saw_ok, mut saw_event) = (false, false);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !(saw_ok && saw_event) {
            let frame = ws_recv(&mut b).await;
            match frame[0].as_str() {
                Some("OK") => saw_ok = frame[2].as_bool() == Some(true),
                Some("EVENT") => saw_event = frame[2]["id"] == id_b,
                _ => {}
            }
        }
    })
    .await
    .expect("client B was not served within 1s");
    relay.stop().await;
}

/// Shared per-address EVENT buckets: two connections of one address
/// drain one bucket between them.
#[tokio::test]
async fn ip_rate_limit_shared_across_connections() {
    let mut config = common::config();
    config.relay.rate_limits.ip_events_per_second = 1;
    config.relay.rate_limits.ip_events_burst = 3;
    let relay = Relay::start_with(config).await;
    let mut a = relay.connect().await;
    let mut b = relay.connect().await;
    // The shared burst admits exactly 3 events across both sockets
    // (per-connection buckets are disabled by `common::config`).
    for i in 0..3u64 {
        let ws = if i == 1 { &mut b } else { &mut a };
        let ev = common::event(&key(50), 1, now(), &json!([]), &format!("burst{i}"));
        send_text(ws, &event_wire(&ev)).await;
        assert_eq!(recv_ok(ws, &ev).await[2], true, "burst {i}");
    }
    let ev = common::event(&key(50), 1, now(), &json!([]), "over");
    send_text(&mut b, &event_wire(&ev)).await;
    let ok = recv_ok(&mut b, &ev).await;
    assert_eq!(ok[2], false, "{ok}");
    assert_eq!(ok[3], "rate-limited: slow down");
    relay.stop().await;
}

/// A reconnect cannot buy a fresh address burst: the spent entry
/// survives at count 0 and the next connection reuses its buckets.
#[tokio::test]
async fn ip_rate_limit_survives_reconnect() {
    let mut config = common::config();
    config.relay.metrics = true;
    config.relay.rate_limits.ip_events_per_second = 1;
    config.relay.rate_limits.ip_events_burst = 3;
    let relay = Relay::start_with(config).await;
    {
        let mut a = relay.connect().await;
        for i in 0..3u64 {
            let ev = common::event(&key(51), 1, now(), &json!([]), &format!("x{i}"));
            send_text(&mut a, &event_wire(&ev)).await;
            assert_eq!(recv_ok(&mut a, &ev).await[2], true, "burst {i}");
        }
    }
    // Wait until the relay accounts the close before reconnecting.
    let deadline = Instant::now() + T;
    loop {
        let body = http_get(relay.addr, "/metrics").await;
        if metric_value(&body, "nosfry_connections") == Some(0) {
            break;
        }
        assert!(Instant::now() < deadline, "close was not processed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut b = relay.connect().await;
    let ev = common::event(&key(51), 1, now(), &json!([]), "over");
    send_text(&mut b, &event_wire(&ev)).await;
    let ok = recv_ok(&mut b, &ev).await;
    assert_eq!(ok[2], false, "{ok}");
    assert_eq!(ok[3], "rate-limited: slow down");
    relay.stop().await;
}

async fn ws_recv(ws: &mut yawc::TcpWebSocket) -> Value {
    json(&recv_text(ws, T).await)
}

/// The integer value of one Prometheus series line (`<series> <value>`),
/// matched on the whole label block so `{kind="1"}` cannot match
/// `{kind="11"}`.
fn metric_value(body: &str, series: &str) -> Option<u64> {
    body.lines().find_map(|line| {
        let (head, value) = line.rsplit_once(' ')?;
        (head == series).then(|| value.parse().ok()).flatten()
    })
}

#[tokio::test]
async fn metrics_endpoint_reports_prometheus_series() {
    let mut config = common::config();
    config.relay.metrics = true;
    let relay = Relay::start_with(config).await;
    let mut ws = relay.connect().await;

    let ev = common::event(&key(40), 1, now(), &json!([]), "m");
    send_text(&mut ws, &event_wire(&ev)).await;
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[0], "OK");
    assert_eq!(ok[2], true);

    send_text(&mut ws, &req_wire("m", "{\"kinds\":[1]}")).await;
    loop {
        let frame = json(&recv_text(&mut ws, T).await);
        assert!(frame[0] == "EVENT" || frame[0] == "EOSE", "{frame}");
        if frame[0] == "EOSE" {
            break;
        }
    }

    let body = http_get(relay.addr, "/metrics").await;
    assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    assert!(
        body.contains("content-type: text/plain; version=0.0.4"),
        "{body}"
    );
    assert_eq!(
        metric_value(&body, "nostr_client_messages_total{verb=\"EVENT\"}"),
        Some(1),
        "{body}"
    );
    assert_eq!(
        metric_value(&body, "nostr_client_messages_total{verb=\"REQ\"}"),
        Some(1),
        "{body}"
    );
    assert_eq!(
        metric_value(&body, "nostr_events_total{kind=\"1\"}"),
        Some(1),
        "{body}"
    );
    assert_eq!(
        metric_value(&body, "nostr_relay_messages_total{verb=\"OK\"}"),
        Some(1),
        "{body}"
    );
    assert_eq!(
        metric_value(&body, "nostr_relay_messages_total{verb=\"EOSE\"}"),
        Some(1),
        "{body}"
    );
    // The metrics GET itself is plain HTTP and must not count.
    assert_eq!(metric_value(&body, "nosfry_connections"), Some(1), "{body}");
    relay.stop().await;
}

#[tokio::test]
async fn metrics_endpoint_is_404_by_default() {
    let relay = Relay::start().await;
    let body = http_get(relay.addr, "/metrics").await;
    assert!(body.starts_with("HTTP/1.1 404"), "{body}");
    relay.stop().await;
}

#[tokio::test]
async fn metrics_count_rate_limited_events() {
    let mut config = common::config();
    config.relay.metrics = true;
    config.relay.rate_limits = nosfry_core::limits::RateLimits::default();
    let relay = Relay::start_with(config).await;
    let mut ws = relay.connect().await;

    let base = now();
    for i in 0..300u64 {
        let ev = common::event(&key(41), 1, base, &json!([]), &format!("f{i}"));
        send_text(&mut ws, &event_wire(&ev)).await;
    }
    let mut saw_limited = false;
    for _ in 0..300 {
        if recv_text(&mut ws, T).await.contains("rate-limited:") {
            saw_limited = true;
        }
    }
    assert!(saw_limited, "300 events never hit the rate limit");

    let body = http_get(relay.addr, "/metrics").await;
    assert!(
        metric_value(&body, "nosfry_rate_limited_total{verb=\"EVENT\"}").unwrap_or(0) > 0,
        "{body}"
    );
    relay.stop().await;
}

#[tokio::test]
async fn metrics_count_rejected_upgrades() {
    let mut config = common::config();
    config.relay.metrics = true;
    config.relay.max_connections_per_ip = 1;
    let relay = Relay::start_with(config).await;
    let _ws = relay.connect().await;
    let status = upgrade_status(relay.addr).await;
    assert!(status.contains("429"), "{status}");
    let body = http_get(relay.addr, "/metrics").await;
    assert_eq!(
        metric_value(&body, "nosfry_connections_rejected_total"),
        Some(1),
        "{body}"
    );
    assert_eq!(metric_value(&body, "nosfry_connections"), Some(1), "{body}");
    relay.stop().await;
}

// Write-policy plugin tests: real `/bin/sh -c` scripts in tempdirs.

/// The `sed` extraction every test plugin shares: `event.id` is the
/// first field of the canonical event JSON.
const PLUGIN_SED: &str =
    r#"id=$(printf '%s' "$line" | sed -n 's/.*"event":{"id":"\([0-9a-f]\{64\}\)".*/\1/p')"#;

/// A `while read` plugin loop running `body` once per request line.
fn loop_plugin(body: &str) -> String {
    format!("while IFS= read -r line; do\n  {PLUGIN_SED}\n  {body}\ndone\n")
}

/// Write a POSIX `sh` plugin into a tempdir. `/bin/sh -c <path>`
/// execs the file, so it needs the exec bit; the returned dir keeps
/// it alive.
fn write_plugin(script: &str) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy.sh");
    std::fs::write(&path, script).unwrap();
    make_executable(&path);
    (dir, path.to_str().unwrap().to_owned())
}

/// `chmod +x` for plugin files.
fn make_executable(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions).unwrap();
}

/// Relay config with the write-policy plugin enabled.
fn policy_config(plugin: &str, timeout_seconds: u64) -> nosfry::config::Config {
    let mut config = common::config();
    config.relay.write_policy = nosfry::config::WritePolicy {
        plugin: plugin.to_owned(),
        timeout_seconds,
    };
    config
}

/// Receive one frame and assert it is `["OK", <event id>, ...]`.
async fn recv_ok(ws: &mut yawc::TcpWebSocket, event: &nosfry_core::event::Event) -> Value {
    let ok = json(&recv_text(ws, T).await);
    assert_eq!(ok[0], "OK", "{ok}");
    assert_eq!(ok[1], event.id().to_string(), "{ok}");
    ok
}

#[tokio::test]
async fn write_policy_accept_stores_and_delivers() {
    let (_dir, plugin) = write_plugin(&loop_plugin(
        r#"printf '{"id":"%s","action":"accept"}\n' "$id""#,
    ));
    let relay = Relay::start_with(policy_config(&plugin, 10)).await;
    let mut sub = relay.connect().await;
    send_text(&mut sub, &req_wire("s", r#"{"kinds":[1]}"#)).await;
    assert_eq!(json(&recv_text(&mut sub, T).await), json!(["EOSE", "s"]));

    let mut pub_ws = relay.connect().await;
    let ev = common::event(&key(7), 1, now(), &json!([]), "hi");
    send_text(&mut pub_ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut pub_ws, &ev).await[2], true);

    let live = json(&recv_text(&mut sub, T).await);
    assert_eq!(live[0], "EVENT");
    assert_eq!(live[2]["id"], ev.id().to_string());

    let mut query = relay.connect().await;
    send_text(
        &mut query,
        &req_wire("q", &format!(r#"{{"ids":["{}"]}}"#, ev.id())),
    )
    .await;
    assert_eq!(
        json(&recv_text(&mut query, T).await)[2]["id"],
        ev.id().to_string()
    );
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_reject_message_and_default() {
    let (_dir, plugin) = write_plugin(&loop_plugin(
        r#"case "$line" in
    *'"kind":2'*) printf '{"id":"%s","action":"reject"}\n' "$id" ;;
    *) printf '{"id":"%s","action":"reject","msg":"blocked: spam"}\n' "$id" ;;
  esac"#,
    ));
    let relay = Relay::start_with(policy_config(&plugin, 10)).await;
    let mut ws = relay.connect().await;

    let ev = common::event(&key(7), 1, now(), &json!([]), "spam");
    send_text(&mut ws, &event_wire(&ev)).await;
    let ok = recv_ok(&mut ws, &ev).await;
    assert_eq!(
        ok,
        json!(["OK", ev.id().to_string(), false, "blocked: spam"])
    );

    // An absent `msg` falls back to the NIP-01 default reason.
    let ev = common::event(&key(7), 2, now(), &json!([]), "quiet");
    send_text(&mut ws, &event_wire(&ev)).await;
    let ok = recv_ok(&mut ws, &ev).await;
    assert_eq!(
        ok,
        json!([
            "OK",
            ev.id().to_string(),
            false,
            "blocked: rejected by write policy"
        ])
    );
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_shadow_reject() {
    let (_dir, plugin) = write_plugin(&loop_plugin(
        r#"printf '{"id":"%s","action":"shadowReject","msg":"shh"}\n' "$id""#,
    ));
    let relay = Relay::start_with(policy_config(&plugin, 10)).await;
    let mut sub = relay.connect().await;
    send_text(&mut sub, &req_wire("s", r#"{"kinds":[1]}"#)).await;
    assert_eq!(json(&recv_text(&mut sub, T).await), json!(["EOSE", "s"]));

    let mut pub_ws = relay.connect().await;
    let ev = common::event(&key(7), 1, now(), &json!([]), "ghost");
    send_text(&mut pub_ws, &event_wire(&ev)).await;
    assert_eq!(
        recv_ok(&mut pub_ws, &ev).await,
        json!(["OK", ev.id().to_string(), true, "shh"])
    );

    // Not broadcast to the live subscriber and not stored.
    let mut text_frame = None;
    drop(
        tokio::time::timeout(Duration::from_millis(400), async {
            while let Some(frame) = sub.next().await {
                if frame.opcode() == yawc::OpCode::Text {
                    text_frame = Some(String::from_utf8_lossy(frame.payload()).into_owned());
                }
            }
        })
        .await,
    );
    assert!(
        text_frame.is_none(),
        "shadow-rejected event reached a subscriber: {text_frame:?}"
    );
    let ids = req_ids(&mut pub_ws, "q", r#"{"kinds":[1]}"#).await;
    assert!(ids.is_empty(), "{ids:?}");
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_ephemeral_rejected_not_broadcast() {
    let (_dir, plugin) = write_plugin(&loop_plugin(
        r#"case "$line" in
    *'"kind":20000'*) printf '{"id":"%s","action":"reject","msg":"blocked: eph"}\n' "$id" ;;
    *) printf '{"id":"%s","action":"accept"}\n' "$id" ;;
  esac"#,
    ));
    let relay = Relay::start_with(policy_config(&plugin, 10)).await;
    let mut sub = relay.connect().await;
    send_text(&mut sub, &req_wire("s", r#"{"kinds":[20000,20001]}"#)).await;
    assert_eq!(json(&recv_text(&mut sub, T).await), json!(["EOSE", "s"]));

    let mut pub_ws = relay.connect().await;
    let rejected = common::event(&key(8), 20_000, now(), &json!([]), "no");
    send_text(&mut pub_ws, &event_wire(&rejected)).await;
    let ok = recv_ok(&mut pub_ws, &rejected).await;
    assert_eq!(
        ok,
        json!(["OK", rejected.id().to_string(), false, "blocked: eph"])
    );

    let accepted = common::event(&key(8), 20_001, now(), &json!([]), "yes");
    send_text(&mut pub_ws, &event_wire(&accepted)).await;
    assert_eq!(recv_ok(&mut pub_ws, &accepted).await[2], true);

    // Single-stream ordering: had the rejected event been broadcast
    // it would precede the accepted one.
    let live = json(&recv_text(&mut sub, T).await);
    assert_eq!(live[0], "EVENT");
    assert_eq!(live[2]["id"], accepted.id().to_string());
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_timeout_then_fresh_process() {
    let (dir, plugin) = write_plugin("exec sleep 60\n");
    let relay = Relay::start_with(policy_config(&plugin, 1)).await;
    let mut ws = relay.connect().await;

    let ev = common::event(&key(7), 1, now(), &json!([]), "slow");
    let started = Instant::now();
    send_text(&mut ws, &event_wire(&ev)).await;
    let ok = recv_ok(&mut ws, &ev).await;
    assert_eq!(
        ok,
        json!(["OK", ev.id().to_string(), false, "error: internal error"])
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    // Fix the plugin at the same path: the next event spawns a fresh
    // process running the new script.
    std::fs::write(
        dir.path().join("policy.sh"),
        loop_plugin(r#"printf '{"id":"%s","action":"accept"}\n' "$id""#),
    )
    .unwrap();
    let ev = common::event(&key(7), 1, now(), &json!([]), "fixed");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_eof_recovers_with_fresh_process() {
    let (_dir, plugin) = write_plugin(&format!(
        "IFS= read -r line\n  {PLUGIN_SED}\n  \
         printf '{{\"id\":\"%s\",\"action\":\"accept\"}}\\n' \"$id\"\n"
    ));
    let relay = Relay::start_with(policy_config(&plugin, 10)).await;
    let mut ws = relay.connect().await;

    // The plugin answers once and exits; the next event discovers the
    // dead child and is rejected, the one after spawns a fresh plugin.
    let ev = common::event(&key(7), 1, now(), &json!([]), "one");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    let ev = common::event(&key(7), 1, now(), &json!([]), "two");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(
        recv_ok(&mut ws, &ev).await,
        json!(["OK", ev.id().to_string(), false, "error: internal error"])
    );
    let ev = common::event(&key(7), 1, now(), &json!([]), "three");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_noise_line_skipped() {
    let (_dir, plugin) = write_plugin(&loop_plugin(
        r#"echo "plugin startup noise"
  printf '{"id":"%s","action":"accept"}\n' "$id""#,
    ));
    let relay = Relay::start_with(policy_config(&plugin, 10)).await;
    let mut ws = relay.connect().await;
    let ev = common::event(&key(7), 1, now(), &json!([]), "hi");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_id_mismatch_is_internal_error() {
    let (_dir, plugin) = write_plugin(&loop_plugin(
        r#"printf '{"id":"0000000000000000000000000000000000000000000000000000000000000000","action":"accept"}\n'"#,
    ));
    let relay = Relay::start_with(policy_config(&plugin, 10)).await;
    let mut ws = relay.connect().await;
    let ev = common::event(&key(7), 1, now(), &json!([]), "hi");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(
        recv_ok(&mut ws, &ev).await,
        json!(["OK", ev.id().to_string(), false, "error: internal error"])
    );
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_mtime_reload() {
    let (dir, plugin) = write_plugin(&loop_plugin(
        r#"printf '{"id":"%s","action":"accept"}\n' "$id""#,
    ));
    let relay = Relay::start_with(policy_config(&plugin, 10)).await;
    let mut ws = relay.connect().await;
    let ev = common::event(&key(7), 1, now(), &json!([]), "before");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);

    // Rewriting a no-space plugin command reloads it on the next event.
    tokio::time::sleep(Duration::from_millis(50)).await;
    std::fs::write(
        dir.path().join("policy.sh"),
        loop_plugin(r#"printf '{"id":"%s","action":"reject","msg":"blocked: reloaded"}\n' "$id""#),
    )
    .unwrap();
    let ev = common::event(&key(7), 1, now(), &json!([]), "after");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(
        recv_ok(&mut ws, &ev).await,
        json!(["OK", ev.id().to_string(), false, "blocked: reloaded"])
    );
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_request_line_shape() {
    /// The plugin protocol's wire shape, for assertions.
    #[derive(serde::Deserialize)]
    struct PluginRequest<'a> {
        #[serde(rename = "type")]
        kind: String,
        #[serde(borrow)]
        event: &'a serde_json::value::RawValue,
        #[serde(rename = "receivedAt")]
        received_at: u64,
        #[serde(rename = "sourceType")]
        source_type: String,
        #[serde(rename = "sourceInfo")]
        source_info: String,
    }

    let dir = tempfile::tempdir().unwrap();
    let seen = dir.path().join("requests.jsonl");
    let plugin = dir.path().join("policy.sh");
    std::fs::write(
        &plugin,
        format!(
            "while IFS= read -r line; do\n  printf '%s\\n' \"$line\" >> '{}'\n  \
             {PLUGIN_SED}\n  \
             printf '{{\"id\":\"%s\",\"action\":\"accept\"}}\\n' \"$id\"\ndone\n",
            seen.display()
        ),
    )
    .unwrap();
    make_executable(&plugin);
    let relay = Relay::start_with(policy_config(plugin.to_str().unwrap(), 10)).await;
    let mut ws = relay.connect().await;
    let before = now();
    let ev = common::event(&key(7), 1, now(), &json!([]), "probe");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    relay.stop().await;

    let line = std::fs::read_to_string(&seen).unwrap();
    let request: PluginRequest<'_> = serde_json::from_str(line.trim_end()).unwrap();
    assert_eq!(request.kind, "new");
    // The event is embedded raw: byte-identical to the canonical JSON.
    assert_eq!(request.event.get(), ev.json());
    assert!(request.received_at >= before && request.received_at <= now());
    assert_eq!(request.source_type, "IP4");
    assert_eq!(request.source_info, "127.0.0.1");
}

#[tokio::test]
async fn write_policy_does_not_block_shutdown() {
    let (_dir, plugin) = write_plugin(&loop_plugin(
        r#"printf '{"id":"%s","action":"accept"}\n' "$id""#,
    ));
    let relay = Relay::start_with(policy_config(&plugin, 10)).await;
    let mut ws = relay.connect().await;
    let ev = common::event(&key(7), 1, now(), &json!([]), "hi");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    // `stop` joins `serve()`; a stuck policy task would hang this.
    tokio::time::timeout(T, relay.stop()).await.unwrap();
}

// Config hot-reload tests: `relay.reload(config)` is what the binary
// sends down the channel on SIGHUP. Each reload also bumps
// `relay.info.name`, so the NIP-11 document is the deterministic
// "reload applied" marker for the tests below.

/// Poll `check` until it holds or ~5 s pass (reload is applied by a
/// background task).
async fn wait_until<F, Fut>(mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..100 {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// `relay.info.name` as served by NIP-11 right now.
async fn nip11_name(addr: std::net::SocketAddr) -> String {
    nip11_body(&http_get(addr, "/").await)["name"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// A base config with the marker `name` set, so `wait_until` can
/// detect that this exact reload was applied.
fn reload_config(marker: &str) -> nosfry::config::Config {
    let mut config = common::config();
    config.relay.info.name = String::from(marker);
    config
}

#[tokio::test]
async fn reload_updates_nip11() {
    let relay = Relay::start().await;
    assert_eq!(nip11_name(relay.addr).await, "nosfry");
    relay.reload(reload_config("hot reload"));
    assert!(
        wait_until(|| async { nip11_name(relay.addr).await == "hot reload" }).await,
        "NIP-11 never picked up the reload"
    );
    relay.stop().await;
}

#[tokio::test]
async fn reload_enables_policy_on_existing_connection() {
    let (_dir, plugin) = write_plugin(&loop_plugin(
        r#"printf '{"id":"%s","action":"reject","msg":"blocked: nope"}\n' "$id""#,
    ));
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;

    let ev = common::event(&key(7), 1, now(), &json!([]), "before");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);

    // Enable the rejecting plugin: the next event on THIS already-open
    // connection is refused.
    let mut config = reload_config("policy on");
    config.relay.write_policy.plugin = plugin.clone();
    relay.reload(config);
    assert!(wait_until(|| async { nip11_name(relay.addr).await == "policy on" }).await);
    let ev = common::event(&key(7), 1, now(), &json!([]), "during");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(
        recv_ok(&mut ws, &ev).await,
        json!(["OK", ev.id().to_string(), false, "blocked: nope"])
    );

    // Disable it again: the same connection accepts right away.
    relay.reload(reload_config("policy off"));
    assert!(wait_until(|| async { nip11_name(relay.addr).await == "policy off" }).await);
    let ev = common::event(&key(7), 1, now(), &json!([]), "after");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    relay.stop().await;
}

#[tokio::test]
async fn reload_lowers_per_ip_cap() {
    let mut config = common::config();
    config.relay.max_connections_per_ip = 5;
    let relay = Relay::start_with(config).await;
    let mut first = relay.connect().await;
    let _second = relay.connect().await;

    let mut config = reload_config("cap lowered");
    config.relay.max_connections_per_ip = 2;
    relay.reload(config);
    assert!(wait_until(|| async { nip11_name(relay.addr).await == "cap lowered" }).await);

    // The next upgrade is refused; connections already above the
    // lowered cap stay open and usable.
    let status = upgrade_status(relay.addr).await;
    assert!(status.contains("429"), "{status}");
    let ev = common::event(&key(7), 1, now(), &json!([]), "still alive");
    send_text(&mut first, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut first, &ev).await[2], true);
    relay.stop().await;
}

#[tokio::test]
async fn reload_rate_limits_apply_to_new_connections() {
    // `common::config()` disables the buckets; a reload enables a
    // strict one. The existing connection keeps its unlimited
    // session; a fresh connection is limited.
    let relay = Relay::start().await;
    let mut old_ws = relay.connect().await;

    let mut config = reload_config("limits on");
    config.relay.rate_limits = nosfry_core::limits::RateLimits {
        events_per_second: 1,
        events_burst: 1,
        reqs_per_second: 0,
        reqs_burst: 0,
        ip_events_per_second: 0,
        ip_events_burst: 0,
        ip_reqs_per_second: 0,
        ip_reqs_burst: 0,
    };
    relay.reload(config);
    assert!(wait_until(|| async { nip11_name(relay.addr).await == "limits on" }).await);

    for content in ["a", "b", "c"] {
        let ev = common::event(&key(7), 1, now(), &json!([]), content);
        send_text(&mut old_ws, &event_wire(&ev)).await;
        assert_eq!(recv_ok(&mut old_ws, &ev).await[2], true, "{content}");
    }

    let mut new_ws = relay.connect().await;
    let ev = common::event(&key(8), 1, now(), &json!([]), "first");
    send_text(&mut new_ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut new_ws, &ev).await[2], true);
    let ev = common::event(&key(8), 1, now(), &json!([]), "second");
    send_text(&mut new_ws, &event_wire(&ev)).await;
    assert_eq!(
        recv_ok(&mut new_ws, &ev).await,
        json!(["OK", ev.id().to_string(), false, "rate-limited: slow down"])
    );
    relay.stop().await;
}

#[tokio::test]
async fn reload_port_change_is_ignored() {
    let relay = Relay::start().await;
    let mut config = reload_config("port changed");
    config.relay.port = 1;
    relay.reload(config);
    assert!(wait_until(|| async { nip11_name(relay.addr).await == "port changed" }).await);
    // Restart-only fields are kept: the relay still serves on the
    // original port.
    let mut ws = relay.connect().await;
    let ev = common::event(&key(7), 1, now(), &json!([]), "hi");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    relay.stop().await;
}

/// A reload whose merged config fails validation is rejected as a
/// whole: no field — not the NIP-11 name, not the connection caps —
/// is half-applied.
#[tokio::test]
async fn invalid_auth_reload_rejects_everything() {
    let mut config = common::config();
    config.relay.max_connections_per_ip = 2;
    let relay = Relay::start_with(config).await;

    let mut bad = reload_config("must not apply");
    bad.relay.max_connections_per_ip = 1;
    bad.relay.auth.service_url = "http://bad".into();
    relay.reload(bad);
    // Let the reload task consume (and reject) it. The name stays the
    // startup value, and the lowered cap never applies: both checks
    // must run before the sentinel reload below — a valid reload
    // would set the caps again and hide a half-applied `1`.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(nip11_name(relay.addr).await, "nosfry");
    let _first = relay.connect().await;
    let _second = relay.connect().await;

    // The channel kept moving: a valid reload applies afterwards.
    relay.reload(reload_config("sentinel"));
    assert!(wait_until(|| async { nip11_name(relay.addr).await == "sentinel" }).await);
    relay.stop().await;
}

/// The binary re-reads its config file on SIGHUP: a broken file logs
/// an error and keeps the running configuration, a fixed one applies.
#[cfg(unix)]
#[tokio::test]
async fn sighup_reloads_config_and_ignores_broken() {
    use std::process::{Command, Stdio};

    let dir = tempfile::tempdir().unwrap();
    // Grab a free port for the spawned relay.
    let port = std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let cfg_path = dir.path().join("nosfry.toml");
    let write_config = |name: &str| {
        std::fs::write(
            &cfg_path,
            format!(
                "[db]\npath = {:?}\n[relay]\nport = {port}\n\
                 [relay.info]\nname = \"{name}\"\n",
                dir.path().join("db")
            ),
        )
        .unwrap();
    };
    write_config("before");
    let err_log = dir.path().join("stderr.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_nosfry"))
        .args(["relay", "--config"])
        .arg(&cfg_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(std::fs::File::create(&err_log).unwrap()))
        .spawn()
        .unwrap();
    let sighup = |child: &std::process::Child| {
        let status = Command::new("kill")
            .args(["-HUP", &child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    };
    let up = wait_until(|| async { tokio::net::TcpStream::connect(addr).await.is_ok() }).await;
    assert!(up, "spawned relay never listened");

    // A broken config is logged and ignored.
    std::fs::write(&cfg_path, "not = [valid\n").unwrap();
    sighup(&child);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_ok(),
        "relay died on a broken reload"
    );
    let log = std::fs::read_to_string(&err_log).unwrap();
    assert!(log.contains("config reload failed"), "{log}");
    assert_eq!(nip11_name(addr).await, "before");

    // A fixed config applies on the next SIGHUP.
    write_config("after");
    sighup(&child);
    let applied = wait_until(|| async { nip11_name(addr).await == "after" }).await;
    drop(child.kill());
    drop(child.wait());
    assert!(applied, "SIGHUP never applied the fixed config");
}

/// `["AUTH", <signed 22242>]` answering `challenge` for `relay_url`.
fn auth_wire(keypair: &secp256k1::Keypair, challenge: &str, relay_url: &str) -> String {
    let ev = common::event(
        keypair,
        22_242,
        now(),
        &json!([["challenge", challenge], ["relay", relay_url]]),
        "",
    );
    format!("[\"AUTH\",{}]", ev.json())
}

/// Relay config with NIP-42 enabled.
fn auth_config() -> nosfry::config::Config {
    let mut config = common::config();
    config.relay.auth.service_url = "wss://relay.example.com".into();
    config
}

#[tokio::test]
async fn nip11_lists_42_only_with_auth_service_url() {
    let relay = Relay::start_with(auth_config()).await;
    let text = http_get(relay.addr, "/").await;
    let doc = nip11_body(&text);
    assert_eq!(doc["supported_nips"], json!([1, 9, 11, 40, 42, 45, 70]));
    relay.stop().await;
}

#[tokio::test]
async fn auth_dm_flow_restricts_reads_to_involved() {
    let relay = Relay::start_with(auth_config()).await;
    let url = "wss://relay.example.com";
    // Two stored DMs: key1->key2 and key3->key4.
    let pk2 = faster_hex::hex_string(&key(2).x_only_public_key().0.to_byte_array());
    let pk4 = faster_hex::hex_string(&key(4).x_only_public_key().0.to_byte_array());
    let dm12 = common::event(&key(1), 4, now(), &json!([["p", pk2]]), "for 2");
    let dm34 = common::event(&key(3), 4, now(), &json!([["p", pk4]]), "for 4");
    let id12 = dm12.id().to_string();
    let id34 = dm34.id().to_string();
    let mut pub_ws = relay.connect().await;
    for ev in [&dm12, &dm34] {
        send_text(&mut pub_ws, &event_wire(ev)).await;
        assert_eq!(recv_ok(&mut pub_ws, ev).await[2], true);
    }

    // Unauthenticated REQ for restricted kinds: challenge + CLOSED.
    let mut ws_a = relay.connect().await;
    send_text(&mut ws_a, &req_wire("sub", "{\"kinds\":[4]}")).await;
    let auth = json(&recv_text(&mut ws_a, T).await);
    assert_eq!(auth[0], "AUTH", "{auth}");
    let challenge = auth[1].as_str().unwrap().to_owned();
    let closed = json(&recv_text(&mut ws_a, T).await);
    assert_eq!(closed[0], "CLOSED");
    assert_eq!(
        closed[2],
        "auth-required: requested filter requires authentication"
    );

    // AUTH as key2, then the same REQ returns only DMs involving key2.
    send_text(&mut ws_a, &auth_wire(&key(2), &challenge, url)).await;
    let ok = json(&recv_text(&mut ws_a, T).await);
    assert_eq!(ok[2], true, "{ok}");
    let got = req_ids(&mut ws_a, "sub", "{\"kinds\":[4]}").await;
    assert_eq!(got, vec![id12.clone()], "{got:?}");

    // A third party authed as key3 sees only their own DM.
    let mut ws_b = relay.connect().await;
    send_text(&mut ws_b, &req_wire("probe", "{\"kinds\":[4]}")).await;
    let auth = json(&recv_text(&mut ws_b, T).await);
    let challenge_b = auth[1].as_str().unwrap().to_owned();
    drop(json(&recv_text(&mut ws_b, T).await)); // CLOSED
    send_text(&mut ws_b, &auth_wire(&key(3), &challenge_b, url)).await;
    assert_eq!(json(&recv_text(&mut ws_b, T).await)[2], true);
    let got = req_ids(&mut ws_b, "sub", "{\"kinds\":[4]}").await;
    assert_eq!(got, vec![id34], "{got:?}");

    // A live DM to key2 reaches ws_a's subscription but not ws_b's.
    // OK on the publisher means the batch is already queued at every
    // shard, so ws_b's barrier EOSE proves the probe ran.
    let dm2 = common::event(&key(1), 4, now(), &json!([["p", pk2]]), "live");
    send_text(&mut pub_ws, &event_wire(&dm2)).await;
    assert_eq!(recv_ok(&mut pub_ws, &dm2).await[2], true);
    send_text(&mut ws_b, &req_wire("barrier", "{\"kinds\":[1]}")).await;
    let mut ws_b_events = Vec::new();
    loop {
        let frame = json(&recv_text(&mut ws_b, T).await);
        match frame[0].as_str().unwrap() {
            "EVENT" => ws_b_events.push(frame),
            "EOSE" if frame[1] == "barrier" => break,
            _ => {}
        }
    }
    assert!(
        ws_b_events.is_empty(),
        "uninvolved conn got {ws_b_events:?}"
    );
    let live = json(&recv_text(&mut ws_a, T).await);
    assert_eq!(live[0], "EVENT");
    assert_eq!(live[1], "sub");
    assert_eq!(live[2]["id"], dm2.id().to_string());

    relay.stop().await;
}

#[tokio::test]
async fn protected_event_requires_auth() {
    let relay = Relay::start_with(auth_config()).await;
    let url = "wss://relay.example.com";
    let mut ws = relay.connect().await;
    let protected = common::event(&key(7), 1, now(), &json!([["-"]]), "secret");
    send_text(&mut ws, &event_wire(&protected)).await;
    let auth = json(&recv_text(&mut ws, T).await);
    assert_eq!(auth[0], "AUTH", "{auth}");
    let challenge = auth[1].as_str().unwrap().to_owned();
    let ok = json(&recv_text(&mut ws, T).await);
    assert_eq!(ok[2], false);
    assert_eq!(ok[3], "auth-required: event marked as protected");
    // Authenticate as the author, then the same event is accepted.
    send_text(&mut ws, &auth_wire(&key(7), &challenge, url)).await;
    assert_eq!(json(&recv_text(&mut ws, T).await)[2], true);
    send_text(&mut ws, &event_wire(&protected)).await;
    assert_eq!(recv_ok(&mut ws, &protected).await[2], true);
    let got = req_ids(&mut ws, "s", "{}").await;
    assert_eq!(got, vec![protected.id().to_string()]);
    relay.stop().await;
}

#[tokio::test]
async fn protected_event_blocked_without_auth_service() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let protected = common::event(&key(7), 1, now(), &json!([["-"]]), "secret");
    send_text(&mut ws, &event_wire(&protected)).await;
    let ok = recv_ok(&mut ws, &protected).await;
    assert_eq!(ok[2], false);
    assert_eq!(ok[3], "blocked: event marked as protected");
    relay.stop().await;
}

#[tokio::test]
async fn write_policy_receives_authed_pubkey() {
    let out_file = tempfile::tempdir().unwrap();
    let out_path = out_file.path().join("req.jsonl");
    let (_dir, plugin) = write_plugin(&format!(
        "while IFS= read -r line; do printf '%s\\n' \"$line\" >> {}; printf '{{\"id\":\"%s\",\"action\":\"accept\"}}\\n' \"$(printf '%s' \"$line\" | sed -n 's/.*\"event\":{{\"id\":\"\\([0-9a-f]\\{{64\\}}\\)\".*/\\1/p')\"; done",
        out_path.display()
    ));
    let mut config = policy_config(&plugin, 10);
    config.relay.auth.service_url = "wss://relay.example.com".into();
    let relay = Relay::start_with(config).await;
    let mut ws = relay.connect().await;
    // Trigger the challenge with a restricted REQ, then AUTH.
    send_text(&mut ws, &req_wire("x", "{\"kinds\":[4]}")).await;
    let auth = json(&recv_text(&mut ws, T).await);
    let challenge = auth[1].as_str().unwrap().to_owned();
    drop(json(&recv_text(&mut ws, T).await));
    send_text(
        &mut ws,
        &auth_wire(&key(5), &challenge, "wss://relay.example.com"),
    )
    .await;
    assert_eq!(json(&recv_text(&mut ws, T).await)[2], true);
    let ev = common::event(&key(5), 1, now(), &json!([]), "authed publish");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    relay.stop().await;
    let lines = std::fs::read_to_string(&out_path).unwrap();
    let request: Value = serde_json::from_str(lines.trim()).unwrap();
    assert_eq!(
        request["authed"],
        faster_hex::hex_string(&key(5).x_only_public_key().0.to_byte_array())
    );
}

#[tokio::test]
async fn restricted_kinds_stored_but_never_served_by_default() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    // Fully restricted REQ: refused outright, no AUTH (disabled).
    send_text(&mut ws, &req_wire("sub", "{\"kinds\":[4]}")).await;
    let closed = json(&recv_text(&mut ws, T).await);
    assert_eq!(closed[0], "CLOSED");
    assert_eq!(closed[2], "restricted: requested kinds are not served");
    // A kind-4 event is still stored.
    let dm = common::event(&key(8), 4, now(), &json!([]), "dm");
    send_text(&mut ws, &event_wire(&dm)).await;
    assert_eq!(recv_ok(&mut ws, &dm).await[2], true);
    let note = common::event(&key(8), 1, now(), &json!([]), "note");
    send_text(&mut ws, &event_wire(&note)).await;
    assert_eq!(recv_ok(&mut ws, &note).await[2], true);
    // A mixed REQ serves kind 1 but hides the kind 4.
    let got = req_ids(&mut ws, "m", "{\"kinds\":[1,4]}").await;
    assert_eq!(got, vec![note.id().to_string()], "{got:?}");
    relay.stop().await;
}

/// Send a `COUNT` and read its one `["COUNT",<sub>,{..}]` answer body.
async fn recv_count(ws: &mut yawc::TcpWebSocket, sub: &str) -> Value {
    let frame = json(&recv_text(ws, T).await);
    assert_eq!(frame[0], "COUNT", "{frame}");
    assert_eq!(frame[1], sub, "{frame}");
    frame[2].clone()
}

fn pk_hex(n: u8) -> String {
    faster_hex::hex_string(&key(n).x_only_public_key().0.to_byte_array())
}

#[tokio::test]
async fn count_reports_union_once() {
    let relay = Relay::start().await;
    let mut pub_ws = relay.connect().await;
    // key 11: two kind-1s + one kind-3; key 12: one kind-1.
    let events = [
        common::event(&key(11), 1, now(), &json!([]), "a"),
        common::event(&key(11), 1, now(), &json!([]), "b"),
        common::event(&key(12), 1, now(), &json!([]), "c"),
        common::event(&key(11), 3, now(), &json!([]), "d"),
    ];
    for ev in &events {
        send_text(&mut pub_ws, &event_wire(ev)).await;
        assert_eq!(recv_ok(&mut pub_ws, ev).await[2], true);
    }
    let mut ws = relay.connect().await;
    send_text(&mut ws, &count_wire("c", "{\"kinds\":[1]}")).await;
    let body = recv_count(&mut ws, "c").await;
    assert_eq!(body["count"], 3, "{body}");
    assert!(body.get("approximate").is_none(), "{body}");

    // Overlapping filters count each event once: 3 kind-1s plus key
    // 11's kind-3 — not 3 + 3.
    let filters = format!("{{\"kinds\":[1]}},{{\"authors\":[\"{}\"]}}", pk_hex(11));
    send_text(&mut ws, &count_wire("d", &filters)).await;
    assert_eq!(recv_count(&mut ws, "d").await["count"], 4);

    // A filter `limit` is ignored for COUNT.
    send_text(&mut ws, &count_wire("e", "{\"kinds\":[1],\"limit\":1}")).await;
    assert_eq!(recv_count(&mut ws, "e").await["count"], 3);
    relay.stop().await;
}

#[tokio::test]
async fn count_after_ok_includes_the_event() {
    let relay = Relay::start().await;
    let mut ws = relay.connect().await;
    let ev = common::event(&key(21), 1, now(), &json!([]), "fresh");
    send_text(&mut ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut ws, &ev).await[2], true);
    // Read-your-writes: the COUNT's snapshot covers the acked write.
    send_text(&mut ws, &count_wire("c", "{\"kinds\":[1]}")).await;
    assert_eq!(recv_count(&mut ws, "c").await["count"], 1);
    relay.stop().await;
}

#[tokio::test]
async fn count_excludes_deleted_and_expired() {
    let relay = Relay::start().await;
    let mut pub_ws = relay.connect().await;
    let k = key(31);
    let gone = common::event(&k, 1, now(), &json!([]), "gone");
    let keep = common::event(&k, 1, now(), &json!([]), "keep");
    let expiring = common::event(
        &k,
        1,
        now(),
        &json!([["expiration", (now() + 2).to_string()]]),
        "exp",
    );
    for ev in [&gone, &keep, &expiring] {
        send_text(&mut pub_ws, &event_wire(ev)).await;
        assert_eq!(recv_ok(&mut pub_ws, ev).await[2], true);
    }
    let deletion = common::event(&k, 5, now(), &json!([["e", gone.id().to_string()]]), "");
    send_text(&mut pub_ws, &event_wire(&deletion)).await;
    assert_eq!(recv_ok(&mut pub_ws, &deletion).await[2], true);

    let mut ws = relay.connect().await;
    send_text(&mut ws, &count_wire("c", "{\"kinds\":[1]}")).await;
    assert_eq!(recv_count(&mut ws, "c").await["count"], 2);

    // Past the expiration the event is hidden even before the sweep.
    tokio::time::sleep(Duration::from_secs(3)).await;
    send_text(&mut ws, &count_wire("d", "{\"kinds\":[1]}")).await;
    assert_eq!(recv_count(&mut ws, "d").await["count"], 1);
    relay.stop().await;
}

#[tokio::test]
async fn count_budget_marks_approximate() {
    let mut config = common::config();
    config.relay.limits.max_filter_limit_count = 10;
    let relay = Relay::start_with(config).await;
    let mut pub_ws = relay.connect().await;
    for i in 0..20u64 {
        let ev = common::event(&key(33), 1, now() + i, &json!([]), &format!("e{i}"));
        send_text(&mut pub_ws, &event_wire(&ev)).await;
        assert_eq!(recv_ok(&mut pub_ws, &ev).await[2], true);
    }
    let mut ws = relay.connect().await;
    send_text(&mut ws, &count_wire("c", "{\"kinds\":[1]}")).await;
    let body = recv_count(&mut ws, "c").await;
    assert_eq!(body["approximate"], true, "{body}");
    let partial = body["count"].as_u64().unwrap();
    assert!(partial <= 20, "{body}");
    relay.stop().await;
}

#[tokio::test]
async fn count_disabled_closes_unsupported() {
    let mut config = common::config();
    config.relay.limits.max_filter_limit_count = 0;
    let relay = Relay::start_with(config).await;
    let mut ws = relay.connect().await;
    send_text(&mut ws, &count_wire("c", "{\"kinds\":[1]}")).await;
    let frame = json(&recv_text(&mut ws, T).await);
    assert_eq!(frame, json!(["CLOSED", "c", "unsupported: COUNT"]));
    relay.stop().await;
}

#[tokio::test]
async fn count_respects_the_read_gate() {
    let relay = Relay::start_with(auth_config()).await;
    let url = "wss://relay.example.com";
    // Two stored DMs plus two public notes.
    let dm12 = common::event(&key(1), 4, now(), &json!([["p", pk_hex(2)]]), "for 2");
    let dm34 = common::event(&key(3), 4, now(), &json!([["p", pk_hex(4)]]), "for 4");
    let note = common::event(&key(5), 1, now(), &json!([]), "n1");
    let note2 = common::event(&key(6), 1, now(), &json!([]), "n2");
    let mut pub_ws = relay.connect().await;
    for ev in [&dm12, &dm34, &note, &note2] {
        send_text(&mut pub_ws, &event_wire(ev)).await;
        assert_eq!(recv_ok(&mut pub_ws, ev).await[2], true);
    }

    // Unauthenticated all-restricted COUNT: challenge + CLOSED.
    let mut ws = relay.connect().await;
    send_text(&mut ws, &count_wire("c", "{\"kinds\":[4]}")).await;
    let auth = json(&recv_text(&mut ws, T).await);
    assert_eq!(auth[0], "AUTH", "{auth}");
    let challenge = auth[1].as_str().unwrap().to_owned();
    let closed = json(&recv_text(&mut ws, T).await);
    assert_eq!(
        closed,
        json!([
            "CLOSED",
            "c",
            "auth-required: requested filter requires authentication"
        ])
    );

    // A mixed count sees only what the requester may read.
    send_text(&mut ws, &count_wire("m", "{}")).await;
    assert_eq!(recv_count(&mut ws, "m").await["count"], 2);

    // Authenticated as key 2, only the involved DM is counted.
    send_text(&mut ws, &auth_wire(&key(2), &challenge, url)).await;
    assert_eq!(json(&recv_text(&mut ws, T).await)[2], true);
    send_text(&mut ws, &count_wire("d", "{\"kinds\":[4]}")).await;
    assert_eq!(recv_count(&mut ws, "d").await["count"], 1);
    relay.stop().await;
}

#[tokio::test]
async fn count_hll_matches_local_sketch() {
    let relay = Relay::start().await;
    let target = pk_hex(2);
    let mut pub_ws = relay.connect().await;
    // Three followers tag key 2 in kind-3 contact lists; a kind-1 with
    // the same tag does not match the filter.
    let mut followers = Vec::new();
    for n in [1u8, 3, 5] {
        let ev = common::event(&key(n), 3, now(), &json!([["p", target]]), "");
        send_text(&mut pub_ws, &event_wire(&ev)).await;
        assert_eq!(recv_ok(&mut pub_ws, &ev).await[2], true);
        followers.push(n);
    }
    let stray = common::event(&key(7), 1, now(), &json!([["p", target]]), "");
    send_text(&mut pub_ws, &event_wire(&stray)).await;
    assert_eq!(recv_ok(&mut pub_ws, &stray).await[2], true);

    let mut ws = relay.connect().await;
    let filters = format!("{{\"#p\":[\"{target}\"],\"kinds\":[3]}}");
    send_text(&mut ws, &count_wire("c", &filters)).await;
    let body = recv_count(&mut ws, "c").await;
    assert_eq!(body["count"], 3, "{body}");

    // The relay's hll equals the sketch built locally over the same
    // pubkeys with nosfry-core's Hll.
    let filter = nosfry_core::filter::Filter::parse(&filters).unwrap();
    let mut hll = nosfry_core::hll::Hll::for_filters(&[filter]).unwrap();
    for n in followers {
        hll.add(&nosfry_core::event::PublicKey(
            key(n).x_only_public_key().0.to_byte_array(),
        ));
    }
    assert_eq!(
        body["hll"].as_str().unwrap(),
        faster_hex::hex_string(hll.registers()),
        "{body}"
    );
    relay.stop().await;
}

#[tokio::test]
async fn count_never_answers_for_a_replaced_id() {
    let relay = Relay::start().await;
    let mut pub_ws = relay.connect().await;
    let ev = common::event(&key(41), 1, now(), &json!([]), "x");
    send_text(&mut pub_ws, &event_wire(&ev)).await;
    assert_eq!(recv_ok(&mut pub_ws, &ev).await[2], true);

    let mut ws = relay.connect().await;
    // The REQ re-uses the id: the COUNT's generation goes stale and
    // its answer, whenever the scan finishes, must be dropped.
    send_text(&mut ws, &count_wire("c", "{}")).await;
    send_text(&mut ws, &req_wire("c", "{\"kinds\":[1]}")).await;
    let mut events = 0usize;
    loop {
        let frame = json(&recv_text(&mut ws, T).await);
        match frame[0].as_str().unwrap() {
            "EVENT" => {
                assert_eq!(frame[1], "c", "{frame}");
                events += 1;
            }
            "EOSE" => break,
            "COUNT" => panic!("stale generation answered: {frame}"),
            other => panic!("unexpected frame {other}"),
        }
    }
    assert_eq!(events, 1);
    relay.stop().await;
}
