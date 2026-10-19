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

use nosfry_core::event::{PublicKey, event_id, serialize_for_id};
use nosfry_core::kind::Kind;
use nosfry_core::limits::Limits;
use nosfry_core::message::ClientVerb;
use nosfry_core::session::{Command, Received, Session};
use secp256k1::Keypair;
use secp256k1::schnorr;

fn session() -> Session {
    Session::new(Arc::new(Limits::default()))
}

fn feed(session: &mut Session, now: u64, text: &str) -> Vec<Command> {
    feed_ms(session, now * 1000, text)
}

/// `feed` at millisecond resolution, for the rate-limit tests.
fn feed_ms(session: &mut Session, now_ms: u64, text: &str) -> Vec<Command> {
    receive(session, now_ms, text).0
}

/// `feed` plus the returned [`Received`].
fn receive(session: &mut Session, now_ms: u64, text: &str) -> (Vec<Command>, Received) {
    let mut out = Vec::new();
    let received = session.on_text(now_ms, text, &mut out);
    (out, received)
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

/// A signed event carrying arbitrary tags (raw JSON tag array).
fn signed_event_tags(created_at: u64, kind: u16, tags: &str, content: &str) -> String {
    let keypair = Keypair::from_secret_bytes([9u8; 32]).expect("secret key");
    let pubkey = PublicKey(keypair.x_only_public_key().0.to_byte_array());
    let tags_v: Vec<Vec<String>> = serde_json::from_str(tags).expect("tags json");
    let preimage = serialize_for_id(&pubkey, created_at, Kind(kind), &tags_v, content);
    let id = event_id(preimage.as_bytes());
    let sig = schnorr::sign_no_aux_rand(&id.0, &keypair);
    let mut sig_hex = String::with_capacity(128);
    for b in sig.to_byte_array() {
        write!(sig_hex, "{b:02x}").expect("write");
    }
    let content_json = serde_json::to_string(content).expect("content");
    format!(
        "[\"EVENT\",{{\"id\":\"{id}\",\"pubkey\":\"{pubkey}\",\"created_at\":{created_at},\
         \"kind\":{kind},\"tags\":{tags},\"content\":{content_json},\"sig\":\"{sig_hex}\"}}]"
    )
}

fn sends(cmds: &[Command]) -> Vec<&str> {
    cmds.iter()
        .filter_map(|c| match c {
            Command::Send { text, .. } => Some(text.as_str()),
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
        req: nosfry_core::limits::ReqLimits {
            max_subs_per_connection: 1,
            ..nosfry_core::limits::ReqLimits::default()
        },
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
        event: nosfry_core::event::EventLimits {
            max_num_tags: 0,
            ..nosfry_core::event::EventLimits::default()
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
    use nosfry_core::session::SubKey;
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

#[test]
fn expired_event_is_rejected_invalid() {
    let mut s = session();
    let now = 1_700_000_000u64;
    let tags = format!("[[\"expiration\",\"{}\"]]", now - 5);
    let out = feed(&mut s, now, &signed_event_tags(now - 10, 1, &tags, "old"));
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].contains("\"OK\"") && frames[0].contains("invalid: event has expired"),
        "{}",
        frames[0]
    );

    // Still-valid expiration reaches verification (Publish).
    let tags = format!("[[\"expiration\",\"{}\"]]", now + 100);
    let out = feed(&mut s, now, &signed_event_tags(now, 1, &tags, "fresh"));
    assert!(matches!(out.last(), Some(Command::Publish(_))), "{out:?}");
}

/// Session with caller-chosen rate limits over the default `Limits`.
fn session_with_rate(rate: nosfry_core::limits::RateLimits) -> Session {
    Session::new(Arc::new(Limits {
        rate,
        ..Limits::default()
    }))
}

#[test]
fn event_burst_exhausts_and_refills() {
    let mut s = session();
    let t = 1_000u64;
    let text = signed_event(1_700_000_000, 1, "gm");
    // The burst of 100 passes; message 101 is rate-limited.
    for _ in 0..100 {
        feed_ms(&mut s, t, &text);
    }
    let out = feed_ms(&mut s, t, &text);
    let frames = sends(&out);
    assert_eq!(frames.len(), 1);
    assert!(
        frames[0].contains("\"OK\"") && frames[0].contains("rate-limited: slow down"),
        "{}",
        frames[0]
    );
    // +1000 ms at 20 events/s refills exactly 20 takes.
    for _ in 0..20 {
        let out = feed_ms(&mut s, t + 1_000, &text);
        assert!(
            !sends(&out).iter().any(|f| f.contains("rate-limited:")),
            "{out:?}"
        );
    }
    let out = feed_ms(&mut s, t + 1_000, &text);
    assert!(
        sends(&out)[0].contains("rate-limited: slow down"),
        "{out:?}"
    );
}

#[test]
fn ephemeral_events_share_the_bucket() {
    let mut s = session();
    let t = 1_000u64;
    let stored = signed_event(1_700_000_000, 1, "gm");
    for _ in 0..100 {
        feed_ms(&mut s, t, &stored);
    }
    let ephemeral = signed_event(1_700_000_000, 20_000, "gm");
    let out = feed_ms(&mut s, t, &ephemeral);
    assert!(
        sends(&out)[0].contains("rate-limited: slow down"),
        "{out:?}"
    );
}

#[test]
fn req_over_limit_is_closed_rate_limited() {
    let mut s = session();
    let t = 1_000u64;
    for _ in 0..100 {
        feed_ms(&mut s, t, "[\"REQ\",\"a\",{}]");
    }
    let out = feed_ms(&mut s, t, "[\"REQ\",\"a\",{}]");
    let frames = sends(&out);
    assert!(
        frames[0].starts_with("[\"CLOSED\",\"a\",\"rate-limited: slow down\"")
            || frames[0].contains("rate-limited: slow down"),
        "{}",
        frames[0]
    );
    // CLOSE is never charged: it still unsubscribes on an empty bucket.
    let out = feed_ms(&mut s, t, "[\"CLOSE\",\"a\"]");
    assert!(
        matches!(out.as_slice(), [Command::Unsubscribe { .. }]),
        "{out:?}"
    );
    // Unsupported verbs are not charged either.
    let out = feed_ms(&mut s, t, "[\"NEG-OPEN\",\"n\",{},\"aabb\"]");
    assert!(sends(&out)[0].contains("unsupported:"), "{out:?}");
}

#[test]
fn zero_rate_per_second_disables() {
    let rate = nosfry_core::limits::RateLimits {
        events_per_second: 0,
        events_burst: 0,
        reqs_per_second: 0,
        reqs_burst: 0,
    };
    let mut s = session_with_rate(rate);
    let text = signed_event(1_700_000_000, 1, "gm");
    for _ in 0..200 {
        let out = feed_ms(&mut s, 1_700_000_000_000, &text);
        assert!(matches!(out.as_slice(), [Command::Publish(_)]), "{out:?}");
    }
    for _ in 0..200 {
        let out = feed_ms(&mut s, 1_000, "[\"REQ\",\"a\",{}]");
        assert!(
            !sends(&out).iter().any(|f| f.contains("rate-limited:")),
            "{out:?}"
        );
    }
}

/// Every recognized client verb surfaces through `Received.verb`, even
/// when the message fails validation.
#[test]
fn received_verb_covers_every_client_verb() {
    let mut s = session();
    let t = 1_700_000_000_000u64;
    let cases: [(String, ClientVerb); 8] = [
        (signed_event(1_700_000_000, 1, "gm"), ClientVerb::Event),
        ("[\"REQ\",\"a\",{}]".to_owned(), ClientVerb::Req),
        ("[\"CLOSE\",\"a\"]".to_owned(), ClientVerb::Close),
        ("[\"COUNT\",\"a\",{}]".to_owned(), ClientVerb::Count),
        (
            format!("[\"AUTH\",{}]", event_json(1_700_000_000, 1, "gm")),
            ClientVerb::Auth,
        ),
        (
            "[\"NEG-OPEN\",\"a\",{},\"aa\"]".to_owned(),
            ClientVerb::NegOpen,
        ),
        ("[\"NEG-MSG\",\"a\",\"aa\"]".to_owned(), ClientVerb::NegMsg),
        ("[\"NEG-CLOSE\",\"a\"]".to_owned(), ClientVerb::NegClose),
    ];
    for (text, want) in cases {
        let (_, received) = receive(&mut s, t, &text);
        assert_eq!(received.verb, Some(want), "{text}");
        assert!(!received.rate_limited, "{text}");
    }
}

/// Rejected messages still report their verb: parser arity/type errors
/// and session-level rejections alike.
#[test]
fn rejected_messages_still_report_their_verb() {
    let mut s = session();
    for (text, want) in [
        ("[\"EVENT\",\"bad\"]", ClientVerb::Event),
        ("[\"REQ\"]", ClientVerb::Req),
        ("[\"REQ\",\"a\"]", ClientVerb::Req),
        ("[\"CLOSE\",1]", ClientVerb::Close),
        ("[\"NEG-OPEN\",\"a\",{},\"zz\"]", ClientVerb::NegOpen),
    ] {
        let (_, received) = receive(&mut s, 1_000, text);
        assert_eq!(received.verb, Some(want), "{text}");
    }
    // A verified-format event rejected by the session time window.
    let (_, received) = receive(&mut s, 1_000_000, &signed_event(u64::MAX, 1, "x"));
    assert_eq!(received.verb, Some(ClientVerb::Event));
    assert!(
        sends(&receive(&mut s, 1_000_000, &signed_event(u64::MAX, 1, "x")).0)[0]
            .contains("invalid:"),
    );
}

/// Unparseable frames and unknown verbs report `None`.
#[test]
fn unparseable_and_unknown_verbs_report_none() {
    let mut s = session();
    for text in ["not json", "[]", "[1,2]", "[\"BOGUS\",\"x\"]"] {
        let (_, received) = receive(&mut s, 1_000, text);
        assert_eq!(received.verb, None, "{text}");
        assert!(!received.rate_limited, "{text}");
    }
}

/// `rate_limited` flags exactly the messages the buckets refuse.
#[test]
fn rate_limited_flag_marks_refusals() {
    let rate = nosfry_core::limits::RateLimits {
        events_per_second: 1_000,
        events_burst: 1,
        reqs_per_second: 1_000,
        reqs_burst: 1,
    };
    let mut s = session_with_rate(rate);
    let t = 1_000u64;
    let event = signed_event(1_700_000_000, 1, "gm");
    let (_, received) = receive(&mut s, t, &event);
    assert!(!received.rate_limited);
    let (_, received) = receive(&mut s, t, &event);
    assert!(received.rate_limited);
    assert_eq!(received.verb, Some(ClientVerb::Event));
    let (_, received) = receive(&mut s, t, "[\"REQ\",\"a\",{}]");
    assert!(!received.rate_limited);
    let (_, received) = receive(&mut s, t, "[\"REQ\",\"b\",{}]");
    assert!(received.rate_limited);
    assert_eq!(received.verb, Some(ClientVerb::Req));
    // CLOSE is never charged.
    let (_, received) = receive(&mut s, t, "[\"CLOSE\",\"a\"]");
    assert_eq!(received.verb, Some(ClientVerb::Close));
    assert!(!received.rate_limited);
}
