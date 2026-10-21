//! Relay object over the rusqlite executor: protocol flow, delivery,
//! expiry, and hibernation restore.

#![allow(
    unused_crate_dependencies,
    reason = "each test binary uses a subset of the workspace dependencies"
)]
#![allow(
    clippy::tests_outside_test_module,
    reason = "each file is a single integration test"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "tests may panic on failure"
)]
#![allow(
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::assert_is_empty,
    reason = "tests discard Effects and re-bind short names freely"
)]

use std::cell::RefCell;
use std::ops::ControlFlow;
use std::rc::Rc;

use nosfry_core::sign::{event, key};
use nosfry_sql::rusqlite::Connection;
use nosfry_sql::{Executor, SqlStore, Value};
use nosfry_worker::{Attachment, Config, Relay, Socket};
use serde_json::json;

const NOW: u64 = 1_700_000_000;

type TestRelay = Relay<Connection, FakeSocket>;

#[derive(Clone, Default)]
struct FakeSocket {
    sent: Rc<RefCell<Vec<String>>>,
    closed: Rc<RefCell<Vec<(u16, String)>>>,
}

impl FakeSocket {
    fn sent(&self) -> Vec<String> {
        self.sent.borrow().clone()
    }

    fn closed(&self) -> Vec<(u16, String)> {
        self.closed.borrow().clone()
    }
}

impl Socket for FakeSocket {
    fn send(&self, text: &str) {
        self.sent.borrow_mut().push(text.to_owned());
    }

    fn close(&self, code: u16, reason: &str) {
        self.closed.borrow_mut().push((code, reason.to_owned()));
    }
}

fn relay(config: &str) -> TestRelay {
    relay_on(Connection::open_in_memory().unwrap(), config, vec![])
}

fn relay_on(exec: Connection, config: &str, sockets: Vec<(FakeSocket, Attachment)>) -> TestRelay {
    Relay::open(
        SqlStore::open(exec).unwrap(),
        Config::parse(config).unwrap(),
        sockets,
    )
    .unwrap()
}

fn connect(relay: &mut TestRelay, conn: u64) -> FakeSocket {
    let socket = FakeSocket::default();
    relay
        .accept(socket.clone(), conn, None, format!("challenge-{conn}"))
        .unwrap();
    socket
}

/// `Relay::message` with the `Effects` discarded.
fn msg(relay: &mut TestRelay, conn: u64, text: &str) {
    let _ = relay.message(conn, NOW * 1000, text);
}

/// The hex pubkey of `key(n)` (pool index `n - 1`).
fn pubkey_of(n: u8) -> String {
    format!("{}", key(n).x_only_public_key().0)
}

fn event_frame(event: &nosfry_core::event::Event) -> String {
    format!("[\"EVENT\",{}]", event.json())
}

fn req_frame(sub: &str, filter: &serde_json::Value) -> String {
    format!("[\"REQ\",\"{sub}\",{filter}]")
}

fn auth_frame(secret: u8, challenge: &str, at: u64) -> String {
    let auth = event(
        &key(secret),
        22_242,
        at,
        &json!([
            ["challenge", challenge],
            ["relay", "wss://relay.example.com"]
        ]),
        "",
    );
    format!("[\"AUTH\",{}]", auth.json())
}

fn ok_frames(frames: &[String]) -> Vec<String> {
    frames
        .iter()
        .filter(|f| f.starts_with("[\"OK\""))
        .cloned()
        .collect()
}

fn event_frames(frames: &[String], sub: &str) -> Vec<String> {
    frames
        .iter()
        .filter(|f| f.starts_with(&format!("[\"EVENT\",\"{sub}\"")))
        .cloned()
        .collect()
}

#[test]
fn publish_ok_and_duplicate() {
    let mut r = relay("");
    let socket = connect(&mut r, 1);
    let e = event(&key(1), 1, NOW, &json!([]), "hi");
    msg(&mut r, 1, &event_frame(&e));
    let frames = ok_frames(&socket.sent());
    assert_eq!(frames.len(), 1, "{:?}", socket.sent());
    assert!(frames[0].ends_with(",true,\"\"]"), "{:?}", socket.sent());
    msg(&mut r, 1, &event_frame(&e));
    let frames = ok_frames(&socket.sent());
    assert_eq!(frames.len(), 2);
    assert!(frames[1].contains("duplicate: have this event"));
}

#[test]
fn req_newest_first_eose_and_limit() {
    let mut r = relay("");
    let publisher = connect(&mut r, 1);
    let reader = connect(&mut r, 2);
    for i in 0..5 {
        let e = event(&key(1), 1, NOW - 10 + i, &json!([]), &format!("m{i}"));
        let _ = r.message(1, NOW * 1000, &event_frame(&e));
    }
    drop(publisher);
    msg(
        &mut r,
        2,
        &req_frame("s", &json!({"kinds": [1], "limit": 3})),
    );
    let frames = reader.sent();
    let hits = event_frames(&frames, "s");
    assert_eq!(hits.len(), 3, "{frames:?}");
    assert!(frames[3].starts_with("[\"EOSE\",\"s\"]"), "{frames:?}");
    assert!(hits[0].contains("m4") && hits[1].contains("m3") && hits[2].contains("m2"));
}

#[test]
fn live_delivery_to_other_and_self() {
    let mut r = relay("");
    let a = connect(&mut r, 1);
    let b = connect(&mut r, 2);
    msg(&mut r, 2, &req_frame("s", &json!({"kinds": [1]})));
    msg(&mut r, 1, &req_frame("t", &json!({"kinds": [1]})));
    let e = event(&key(1), 1, NOW, &json!([]), "live");
    msg(&mut r, 1, &event_frame(&e));
    let b_hits = event_frames(&b.sent(), "s");
    assert_eq!(b_hits.len(), 1, "{:?}", b.sent());
    assert!(b_hits[0].contains("\"live\""));
    // The publisher's own subscription receives it too.
    let a_hits = event_frames(&a.sent(), "t");
    assert_eq!(a_hits.len(), 1, "{:?}", a.sent());
}

#[test]
fn close_stops_delivery() {
    let mut r = relay("");
    connect(&mut r, 1);
    let b = connect(&mut r, 2);
    msg(&mut r, 2, &req_frame("s", &json!({"kinds": [1]})));
    let _ = r.message(2, NOW * 1000, "[\"CLOSE\",\"s\"]");
    let e = event(&key(1), 1, NOW, &json!([]), "gone");
    msg(&mut r, 1, &event_frame(&e));
    assert!(event_frames(&b.sent(), "s").is_empty(), "{:?}", b.sent());
}

#[test]
fn replaceable_event_is_superseded() {
    let mut r = relay("");
    let socket = connect(&mut r, 1);
    let old = event(&key(1), 0, NOW - 100, &json!([]), "old profile");
    let new = event(&key(1), 0, NOW - 50, &json!([]), "new profile");
    msg(&mut r, 1, &event_frame(&old));
    msg(&mut r, 1, &event_frame(&new));
    // The stale version loses.
    msg(&mut r, 1, &event_frame(&old));
    let oks = ok_frames(&socket.sent());
    assert_eq!(oks.len(), 3);
    assert!(
        oks[2].contains("duplicate: have a newer version"),
        "{oks:?}"
    );
    let reader = connect(&mut r, 2);
    msg(&mut r, 2, &req_frame("s", &json!({"kinds": [0]})));
    let hits = event_frames(&reader.sent(), "s");
    assert_eq!(hits.len(), 1);
    assert!(hits[0].contains("new profile"));
}

#[test]
fn deletion_hides_and_blocks_republication() {
    let mut r = relay("");
    let socket = connect(&mut r, 1);
    let victim = event(&key(1), 1, NOW - 100, &json!([]), "victim");
    msg(&mut r, 1, &event_frame(&victim));
    let del = event(
        &key(1),
        5,
        NOW - 50,
        &json!([["e", victim.id().to_string()]]),
        "bye",
    );
    msg(&mut r, 1, &event_frame(&del));
    // The deleted event is gone from scans.
    let reader = connect(&mut r, 2);
    msg(&mut r, 2, &req_frame("s", &json!({"kinds": [1]})));
    assert!(event_frames(&reader.sent(), "s").is_empty());
    // Re-publication is blocked.
    msg(&mut r, 1, &event_frame(&victim));
    let oks = ok_frames(&socket.sent());
    assert!(
        oks.last()
            .unwrap()
            .contains("blocked: deleted by its author"),
        "{oks:?}"
    );
}

#[test]
fn expiry_alarm_and_effects() {
    let mut r = relay("");
    connect(&mut r, 1);
    let soon = event(
        &key(1),
        1,
        NOW,
        &json!([["expiration", (NOW + 150).to_string()]]),
        "soon",
    );
    let later = event(
        &key(2),
        1,
        NOW,
        &json!([["expiration", (NOW + 500).to_string()]]),
        "later",
    );
    let never = event(&key(3), 1, NOW, &json!([]), "never");
    let effects = r.message(1, NOW * 1000, &event_frame(&soon));
    assert_eq!(effects.expiration, Some(NOW + 150));
    msg(&mut r, 1, &event_frame(&later));
    msg(&mut r, 1, &event_frame(&never));
    // Alarm before `soon` expires reports it.
    assert_eq!(r.alarm(NOW + 100).unwrap(), Some(NOW + 150));
    // At NOW + 200 `soon` is gone; `later` is next.
    assert_eq!(r.alarm(NOW + 200).unwrap(), Some(NOW + 500));
    // The sweep removed it for good.
    let reader = connect(&mut r, 2);
    let _ = r.message(
        2,
        (NOW + 600) * 1000,
        &req_frame("s", &json!({"kinds": [1]})),
    );
    let hits = event_frames(&reader.sent(), "s");
    assert_eq!(hits.len(), 1, "{:?}", reader.sent());
    assert!(hits[0].contains("never"));
    // Past everything, the alarm disarms.
    assert_eq!(r.alarm(NOW + 600).unwrap(), None);
}

#[test]
fn auth_challenge_flow() {
    let mut r = relay("[relay]\nservice_url = \"wss://relay.example.com\"\n");
    let socket = connect(&mut r, 1);
    let effects = r.message(1, NOW * 1000, &req_frame("a", &json!({"kinds": [4]})));
    let frames = socket.sent();
    assert_eq!(frames[0], "[\"AUTH\",\"challenge-1\"]", "{frames:?}");
    assert!(frames[1].contains("auth-required:"), "{frames:?}");
    // The sent challenge flips the attachment.
    let attachment = effects.attachment.expect("attachment changed");
    assert!(attachment.challenge_sent);
    assert!(attachment.authed.is_empty());
    // AUTH with the challenge succeeds and grows the authed set.
    let effects = r.message(1, NOW * 1000, &auth_frame(9, "challenge-1", NOW));
    let frames = socket.sent();
    assert!(frames[2].contains(",true,\"\"]"), "{frames:?}");
    let attachment = effects.attachment.expect("authed grew");
    assert_eq!(attachment.authed.len(), 1);
}

#[test]
fn restricted_kind_served_only_to_involved_authed() {
    let mut r = relay("[relay]\nservice_url = \"wss://relay.example.com\"\n");
    connect(&mut r, 1);
    let b = connect(&mut r, 2);
    // Conn 2 authenticates as key(9), then subscribes to kind 4.
    msg(&mut r, 2, &req_frame("x", &json!({"kinds": [4]})));
    msg(&mut r, 2, &auth_frame(9, "challenge-2", NOW));
    msg(&mut r, 2, &req_frame("s", &json!({"kinds": [4]})));
    assert_eq!(b.sent().last().unwrap(), "[\"EOSE\",\"s\"]");
    // A kind-4 p-tagging conn 2's pubkey is delivered; one tagging a
    // stranger is not.
    let me = pubkey_of(9);
    let other = pubkey_of(1);
    let for_me = event(&key(1), 4, NOW, &json!([["p", me]]), "secret for you");
    let for_other = event(&key(1), 4, NOW, &json!([["p", other]]), "not yours");
    msg(&mut r, 1, &event_frame(&for_me));
    msg(&mut r, 1, &event_frame(&for_other));
    let hits = event_frames(&b.sent(), "s");
    assert_eq!(hits.len(), 1, "{:?}", b.sent());
    assert!(hits[0].contains("secret for you"));
}

#[test]
fn protected_event_requires_auth() {
    let mut r = relay("[relay]\nservice_url = \"wss://relay.example.com\"\n");
    let socket = connect(&mut r, 1);
    let protected = event(&key(1), 1, NOW, &json!([["-"]]), "for me only");
    msg(&mut r, 1, &event_frame(&protected));
    let frames = socket.sent();
    assert_eq!(frames[0], "[\"AUTH\",\"challenge-1\"]", "{frames:?}");
    assert!(frames[1].contains("auth-required: event marked as protected"));
}

#[test]
fn oversized_message_closes_1009() {
    let mut r = relay("[relay]\nmax_websocket_payload_size = 64\n");
    let socket = connect(&mut r, 1);
    msg(&mut r, 1, &"x".repeat(100));
    assert_eq!(socket.closed(), vec![(1009, "message too big".to_owned())]);
    // The connection is gone: a following REQ produces nothing.
    msg(&mut r, 1, &req_frame("s", &json!({"kinds": [1]})));
    assert!(socket.sent().is_empty(), "{:?}", socket.sent());
}

#[test]
fn admits_enforces_the_global_cap() {
    let mut r = relay("[relay]\nmax_connections = 1\n");
    assert!(r.admits(None));
    connect(&mut r, 1);
    assert!(!r.admits(None));
    assert!(!r.admits(Some("10.0.0.1".parse().unwrap())));
    r.close(1);
    assert!(r.admits(None));
    // 0 disables the cap.
    let mut r = relay("[relay]\nmax_connections = 0\n");
    connect(&mut r, 1);
    assert!(r.admits(None));
}

#[test]
fn admits_enforces_the_per_ip_cap() {
    let ip = "10.0.0.7".parse().unwrap();
    let mut r = relay("[relay]\nmax_connections_per_ip = 1\n");
    let socket = FakeSocket::default();
    r.accept(socket, 1, Some(ip), "c".into()).unwrap();
    // A second socket from the same address is refused; another
    // address or an address-less connection is fine.
    assert!(!r.admits(Some(ip)));
    assert!(r.admits(Some("10.0.0.8".parse().unwrap())));
    assert!(r.admits(None));
    r.close(1);
    assert!(r.admits(Some(ip)));
    // 0 disables the cap.
    let mut r = relay("[relay]\nmax_connections_per_ip = 0\n");
    let socket = FakeSocket::default();
    r.accept(socket, 1, Some(ip), "c".into()).unwrap();
    assert!(r.admits(Some(ip)));
}

#[test]
fn attachment_stays_under_16_kib() {
    let mut attachment = Attachment {
        conn: 7,
        ip: Some("192.168.1.1".parse().unwrap()),
        challenge: "challenge".to_owned(),
        challenge_sent: true,
        authed: Vec::new(),
    };
    for i in 0..nosfry_core::auth::MAX_AUTHED_PUBKEYS {
        attachment.authed.push(format!("{i:064x}"));
    }
    let bytes = serde_json::to_vec(&attachment).unwrap();
    assert!(bytes.len() < 16 * 1024, "{}", bytes.len());
}

#[test]
fn hibernation_restores_subscriptions() {
    let config = "[relay]\nservice_url = \"wss://relay.example.com\"\n";
    let exec = Connection::open_in_memory().unwrap();
    let a = FakeSocket::default();
    let b = FakeSocket::default();
    let c = FakeSocket::default();
    let (att_a, att_b, att_c);
    {
        let mut r = relay_on(exec, config, vec![]);
        att_a = r.accept(a.clone(), 1, None, "challenge-a".into()).unwrap();
        let mut b_attachment = r.accept(b.clone(), 2, None, "challenge-b".into()).unwrap();
        att_c = r.accept(c.clone(), 3, None, "challenge-c".into()).unwrap();
        // Conn 1 subscribes before AUTH (open kinds plus kind 4, so
        // the REQ is not fully restricted); conn 2 after AUTH. The
        // glue rewrites the socket's attachment each time Effects
        // reports one.
        let _ = r.message(1, NOW * 1000, &req_frame("pre", &json!({"kinds": [1, 4]})));
        let effects = r.message(2, NOW * 1000, &auth_frame(9, "challenge-b", NOW));
        if let Some(attachment) = effects.attachment {
            b_attachment = attachment;
        }
        let _ = r.message(2, NOW * 1000, &req_frame("post", &json!({"kinds": [4]})));
        att_b = b_attachment;
        // Conn 3's row is stranded by the wake below.
        let _ = r.message(3, NOW * 1000, &req_frame("s", &json!({"kinds": [1]})));
        let exec = r.into_store().into_exec();
        // Wake with conn 1 and 2 only; conn 3's socket is gone.
        let mut r = relay_on(exec, config, vec![(a.clone(), att_a), (b.clone(), att_b)]);
        // A new kind-4 p-tagging conn 2's authed pubkey arrives as
        // before: conn 2's post-AUTH sub sees it, conn 1's pre-AUTH
        // sub does not.
        let me = pubkey_of(9);
        let dm = event(&key(1), 4, NOW + 10, &json!([["p", me]]), "restored dm");
        let _ = r.message(1, NOW * 1000 + 10_000, &event_frame(&dm));
        let post = event_frames(&b.sent(), "post");
        assert_eq!(post.len(), 1, "{:?}", b.sent());
        assert!(post[0].contains("restored dm"));
        assert!(event_frames(&a.sent(), "pre").is_empty(), "{:?}", a.sent());
        // And conn 3's stranded rows were purged on wake.
        let exec = r.into_store().into_exec();
        let mut count = 0u64;
        exec.query("SELECT COUNT(*) FROM subs", &[], &mut |row| {
            if let Some(Value::Int(v)) = row.first() {
                count = u64::try_from(*v).unwrap();
            }
            ControlFlow::Continue(())
        })
        .unwrap();
        assert_eq!(count, 2, "one row per live subscription");
    }
    drop((att_c, c));
}

#[test]
fn corrupt_subs_rows_are_purged_not_fatal() {
    let exec = Connection::open_in_memory().unwrap();
    let socket = FakeSocket::default();
    let attachment;
    {
        let mut r = relay_on(exec, "", vec![]);
        attachment = r.accept(socket.clone(), 1, None, "c".into()).unwrap();
        let _ = r.message(1, NOW * 1000, &req_frame("s", &json!({"kinds": [1]})));
        let exec = r.into_store().into_exec();
        // Garbage row for the live connection, plus one for a dead
        // connection.
        exec.execute(
            "INSERT INTO subs (conn, sub_id, filters, authed)
             VALUES (1, 'bad', 'not json', 0), (99, 'x', '[]', 0)",
            &[],
        )
        .unwrap();
        let mut r = relay_on(exec, "", vec![(socket.clone(), attachment)]);
        // The valid subscription still delivers.
        let e = event(&key(1), 1, NOW, &json!([]), "still here");
        let _ = r.message(1, NOW * 1000, &event_frame(&e));
        let hits = event_frames(&socket.sent(), "s");
        assert_eq!(hits.len(), 1, "{:?}", socket.sent());
        let exec = r.into_store().into_exec();
        let mut count = 0u64;
        exec.query("SELECT COUNT(*) FROM subs", &[], &mut |row| {
            if let Some(Value::Int(v)) = row.first() {
                count = u64::try_from(*v).unwrap();
            }
            ControlFlow::Continue(())
        })
        .unwrap();
        assert_eq!(count, 1, "only the valid row remains");
    }
}
