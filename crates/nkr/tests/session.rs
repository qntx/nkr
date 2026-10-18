//! Session state machine branch coverage (sans-IO).

#![allow(
    unused_crate_dependencies,
    reason = "each test binary uses a subset of the workspace dependencies"
)]
#![allow(
    clippy::tests_outside_test_module,
    reason = "each file is a single vector runner or property test"
)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "vector fixtures may panic on failure"
)]
#![allow(
    clippy::shadow_unrelated,
    reason = "the loop variables intentionally shadow per-case bindings"
)]

mod common;

use std::fmt::Write as _;
use std::sync::Arc;

use nkr::event::{PublicKey, event_id, serialize_for_id};
use nkr::kind::Kind;
use nkr::limits::Limits;
use nkr::session::{Command, Session};
use secp256k1::Keypair;
use secp256k1::schnorr;

fn session() -> Session {
    Session::new(Arc::new(Limits::default()))
}

fn feed(session: &mut Session, now: u64, text: &str) -> Vec<Command> {
    let mut out = Vec::new();
    session.on_text(now, text, &mut out);
    out
}

fn event_json(created_at: u64, kind: u16, content: &str) -> String {
    let keypair = Keypair::from_secret_bytes([9u8; 32]).expect("secret key");
    let pubkey = PublicKey(keypair.x_only_public_key().0.to_byte_array());
    let preimage = serialize_for_id(&pubkey, created_at, Kind(kind), &[], content);
    let id = event_id(preimage.as_bytes());
    let sig = schnorr::sign_no_aux_rand(&id.0, &keypair);
    let mut sig_hex = String::with_capacity(128);
    for b in sig.to_byte_array() {
        write!(sig_hex, "{b:02x}").expect("write");
    }
    format!(
        "{{\"id\":\"{id}\",\"pubkey\":\"{pubkey}\",\"created_at\":{created_at},\
         \"kind\":{kind},\"tags\":[],\"content\":\"{content}\",\"sig\":\"{sig_hex}\"}}"
    )
}

fn signed_event(created_at: u64, kind: u16, content: &str) -> String {
    format!("[\"EVENT\",{}]", event_json(created_at, kind, content))
}

fn sends(cmds: &[Command]) -> Vec<&str> {
    cmds.iter()
        .filter_map(|c| match c {
            Command::Send(s) => Some(s.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn garbage_produces_notice() {
    let mut s = session();
    let out = feed(&mut s, 1, "not json");
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].starts_with("[\"NOTICE\",\"invalid:"),
        "{}",
        frames[0]
    );
}

#[test]
fn req_subscribes_and_is_current() {
    let mut s = session();
    let out = feed(&mut s, 1, "[\"REQ\",\"a\",{\"kinds\":[1]}]");
    assert_eq!(out.len(), 1);
    let Command::Subscribe { sub, filters } = &out[0] else {
        panic!("expected Subscribe, got {out:?}");
    };
    assert_eq!(sub.id.as_str(), "a");
    assert_eq!(sub.generation, 1);
    assert_eq!(filters.len(), 1);
    assert!(s.is_current(sub));
}

#[test]
fn req_replace_unsubscribes_then_subscribes_new_generation() {
    let mut s = session();
    let out = feed(&mut s, 1, "[\"REQ\",\"a\",{}]");
    let Command::Subscribe { sub: first, .. } = &out[0] else {
        panic!("expected Subscribe");
    };
    let out = feed(&mut s, 2, "[\"REQ\",\"a\",{\"kinds\":[2]}]");
    assert_eq!(out.len(), 2);
    let Command::Unsubscribe { sub_id } = &out[0] else {
        panic!("expected Unsubscribe first, got {:?}", out[0]);
    };
    assert_eq!(sub_id.as_str(), "a");
    let Command::Subscribe { sub: second, .. } = &out[1] else {
        panic!("expected Subscribe second");
    };
    assert_eq!(second.id.as_str(), "a");
    assert_ne!(first.generation, second.generation);
    assert!(!s.is_current(first));
    assert!(s.is_current(second));
}

#[test]
fn req_over_cap_closes() {
    let limits = Limits {
        max_subs_per_connection: 1,
        ..Limits::default()
    };
    let mut s = Session::new(Arc::new(limits));
    feed(&mut s, 1, "[\"REQ\",\"a\",{}]");
    let out = feed(&mut s, 2, "[\"REQ\",\"b\",{}]");
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].contains("\"CLOSED\"") && frames[0].contains("too many open subscriptions"),
        "{}",
        frames[0]
    );
    // Replacing the still-open sub stays allowed.
    let out = feed(&mut s, 3, "[\"REQ\",\"a\",{}]");
    assert!(matches!(out.last(), Some(Command::Subscribe { .. })));
}

#[test]
fn req_too_many_filters_closes() {
    let mut s = session();
    let mut text = String::from("[\"REQ\",\"a\"");
    for _ in 0..=200 {
        text.push_str(",{}");
    }
    text.push(']');
    let out = feed(&mut s, 1, &text);
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(frames[0].contains("\"CLOSED\""), "{}", frames[0]);
}

#[test]
fn req_too_many_tag_filters_closes() {
    let mut s = session();
    let out = feed(
        &mut s,
        1,
        "[\"REQ\",\"a\",{\"#a\":[\"x\"],\"#b\":[\"x\"],\"#c\":[\"x\"],\"#d\":[\"x\"]}]",
    );
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(frames[0].contains("\"CLOSED\""), "{}", frames[0]);
}

#[test]
fn req_bad_filter_closes_with_reason() {
    let mut s = session();
    let out = feed(&mut s, 1, "[\"REQ\",\"a\",{\"kinds\":\"x\"}]");
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].starts_with("[\"CLOSED\",\"a\",\"invalid:"),
        "{}",
        frames[0]
    );
}

#[test]
fn close_removes_and_ignores_unknown() {
    let mut s = session();
    feed(&mut s, 1, "[\"REQ\",\"a\",{}]");
    let out = feed(&mut s, 2, "[\"CLOSE\",\"a\"]");
    assert!(matches!(out.as_slice(), [Command::Unsubscribe { .. }]));
    let out = feed(&mut s, 3, "[\"CLOSE\",\"a\"]");
    assert!(out.is_empty());
    let out = feed(&mut s, 4, "[\"CLOSE\",\"never\"]");
    assert!(out.is_empty());
}

#[test]
fn count_auth_neg_are_unsupported() {
    let mut s = session();
    let out = feed(&mut s, 1, "[\"COUNT\",\"c\",{}]");
    assert!(
        sends(&out)[0].starts_with("[\"CLOSED\",\"c\",\"unsupported:"),
        "{out:?}"
    );
    let auth = format!("[\"AUTH\",{}]", event_json(1, 22_242, "auth"));
    let out = feed(&mut s, 1, &auth);
    assert!(
        sends(&out)[0].starts_with("[\"NOTICE\",\"unsupported:"),
        "{out:?}"
    );
    for (text, sub) in [
        ("[\"NEG-OPEN\",\"n\",{},\"aabb\"]", "n"),
        ("[\"NEG-MSG\",\"m\",\"aabb\"]", "m"),
        ("[\"NEG-CLOSE\",\"x\"]", "x"),
    ] {
        let out = feed(&mut s, 1, text);
        assert!(
            sends(&out)[0].starts_with(&format!("[\"NEG-ERR\",\"{sub}\",\"unsupported:")),
            "{text}: {out:?}"
        );
    }
}

#[test]
fn event_publishes_when_valid() {
    let mut s = session();
    let out = feed(&mut s, 1_700_000_000, &signed_event(1_700_000_000, 1, "gm"));
    assert!(matches!(out.as_slice(), [Command::Publish(_)]));
}

#[test]
fn event_future_created_at_rejected() {
    let mut s = session();
    let now = 1_700_000_000u64;
    let out = feed(&mut s, now, &signed_event(now + 901, 1, "gm"));
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].contains(",false,") && frames[0].contains("invalid:"),
        "{}",
        frames[0]
    );
}

#[test]
fn event_ancient_created_at_rejected() {
    let mut s = session();
    let now = 1_700_000_000u64;
    let out = feed(&mut s, now, &signed_event(now - 94_608_001, 1, "gm"));
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(frames[0].contains(",false,"), "{}", frames[0]);
}

#[test]
fn ephemeral_event_past_window_rejected() {
    let mut s = session();
    let now = 1_700_000_000u64;
    let out = feed(&mut s, now, &signed_event(now - 61, 20_000, "gm"));
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(frames[0].contains(",false,"), "{}", frames[0]);
    // Same age on a regular kind passes the ephemeral window.
    let out = feed(&mut s, now, &signed_event(now - 61, 1, "gm"));
    assert!(matches!(out.as_slice(), [Command::Publish(_)]));
}

#[test]
fn event_bad_signature_rejected() {
    let mut s = session();
    let text = signed_event(1_700_000_000, 1, "gm");
    let tampered = text.replace("\"content\":\"gm\"", "\"content\":\"gn\"");
    let out = feed(&mut s, 1_700_000_000, &tampered);
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(frames[0].contains(",false,"), "{}", frames[0]);
}

#[test]
fn event_malformed_reports_id_or_notice() {
    // Message-level structural failure -> NOTICE (no id extracted).
    let mut s = session();
    let out = feed(&mut s, 1, "[\"EVENT\",{}]");
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(frames[0].starts_with("[\"NOTICE\""), "{}", frames[0]);

    // Limit failure inside `on_event` with a recoverable id -> OK false.
    let limits = Limits {
        event: nkr::event::EventLimits {
            max_num_tags: 0,
            ..nkr::event::EventLimits::default()
        },
        ..Limits::default()
    };
    let mut s = Session::new(Arc::new(limits));
    let tagged =
        signed_event(1_700_000_000, 1, "gm").replace("\"tags\":[]", "\"tags\":[[\"t\",\"x\"]]");
    // The event id/signature are stale after tampering, but the limit
    // failure must fire before any hashing so the OK still reports the id.
    let out = feed(&mut s, 1_700_000_000, &tagged);
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].starts_with("[\"OK\"") && frames[0].contains(",false,"),
        "{}",
        frames[0]
    );
}

#[test]
fn sub_id_emoji_length_counts_chars() {
    let mut s = session();
    let sub = "\u{1f600}".repeat(64);
    let text = format!("[\"REQ\",\"{sub}\",{{}}]");
    let out = feed(&mut s, 1, &text);
    assert!(matches!(out.as_slice(), [Command::Subscribe { .. }]));
    let sub = "\u{1f600}".repeat(65);
    let text = format!("[\"REQ\",\"{sub}\",{{}}]");
    let out = feed(&mut s, 1, &text);
    assert!(sends(&out)[0].starts_with("[\"NOTICE\",\"invalid:"));
}

#[test]
fn close_by_relay_only_removes_current_generation() {
    use nkr::session::SubKey;
    let mut s = session();
    let out = feed(&mut s, 1, "[\"REQ\",\"a\",{}]");
    let Command::Subscribe { sub: first, .. } = &out[0] else {
        panic!("expected Subscribe");
    };
    let first = first.clone();
    // Re-REQ bumps the generation; a relay close for the stale key is a no-op.
    feed(&mut s, 2, "[\"REQ\",\"a\",{}]");
    assert!(!s.close_by_relay(&first));
    let current = SubKey {
        id: first.id.clone(),
        generation: first.generation + 1,
    };
    assert!(s.is_current(&current));
    assert!(s.close_by_relay(&current));
    assert!(!s.is_current(&current));
    // Closing an unknown id is also a no-op.
    assert!(!s.close_by_relay(&SubKey {
        id: first.id,
        generation: 99,
    }));
}

#[test]
fn subkey_generation_monotonic() {
    let mut s = session();
    for n in 1..=3u64 {
        let out = feed(&mut s, n, "[\"REQ\",\"a\",{}]");
        let Command::Subscribe { sub, .. } = out.last().expect("subscribe") else {
            panic!("expected Subscribe");
        };
        assert_eq!(sub.generation, n);
    }
}
