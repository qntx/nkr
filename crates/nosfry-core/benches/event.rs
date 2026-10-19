//! Criterion bench: event parse+verify and compiled filter matching.

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

use std::fmt::Write as _;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use nosfry_core::event::{EventLimits, PublicKey, UnverifiedEvent, event_id, serialize_for_id};
use nosfry_core::filter::Filter;
use nosfry_core::kind::Kind;
use nosfry_core::message::SubId;
use nosfry_core::monitor::Monitors;
use nosfry_core::packed::PackedEvent;
use nosfry_core::session::SubKey;
use secp256k1::Keypair;
use secp256k1::schnorr;

fn signed_event() -> String {
    let keypair = Keypair::from_secret_bytes([9u8; 32]).expect("secret key");
    let pubkey = PublicKey(keypair.x_only_public_key().0.to_byte_array());
    let tags = vec![vec!["t".to_owned(), "nostr".to_owned()]];
    let preimage = serialize_for_id(&pubkey, 1_700_000_000, Kind(1), &tags, "gm");
    let id = event_id(preimage.as_bytes());
    let sig = schnorr::sign_no_aux_rand(&id.0, &keypair);
    let sig_hex = sig
        .to_byte_array()
        .iter()
        .fold(String::with_capacity(128), |mut out, b| {
            write!(out, "{b:02x}").expect("write");
            out
        });
    format!(
        "{{\"id\":\"{id}\",\"pubkey\":\"{pubkey}\",\"created_at\":1700000000,\
         \"kind\":1,\"tags\":[[\"t\",\"nostr\"]],\"content\":\"gm\",\"sig\":\"{sig_hex}\"}}"
    )
}

fn parse_verify(c: &mut Criterion) {
    let json = signed_event();
    let limits = EventLimits::default();
    c.bench_function("event parse+verify kind-1", |b| {
        b.iter(|| {
            UnverifiedEvent::parse(&json, &limits)
                .expect("parse")
                .verify(&limits)
                .expect("verify")
        });
    });
}

fn filter_match(c: &mut Criterion) {
    let authors: Vec<String> = (0u64..100).map(|i| format!("{i:064x}")).collect();
    let filter_json = format!(
        "{{\"authors\":{}}}",
        serde_json::to_string(&authors).unwrap()
    );
    let filter = Filter::parse(&filter_json).expect("filter");
    let keypair = Keypair::from_secret_bytes([9u8; 32]).expect("secret key");
    let pubkey = PublicKey(keypair.x_only_public_key().0.to_byte_array());
    let tags: Vec<Vec<String>> = Vec::new();
    let packed = PackedEvent::build(&event_id(b"x"), &pubkey, 1_700_000_000, Kind(1), &tags);
    c.bench_function("filter match 100 authors", |b| {
        b.iter(|| filter.matches(packed.packed()));
    });
}

/// 10k live subscriptions on the popular `kinds:[1]` index list:
/// churn every subscription once (remove + insert).
fn monitor_churn(c: &mut Criterion) {
    let filters: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").expect("filter")]);
    let sub_id = SubId::parse("s").expect("sub id");
    let key = |generation: u64| SubKey {
        id: sub_id.clone(),
        generation,
    };
    let mut monitors: Monitors<u64> = Monitors::new();
    for conn in 0..10_000u64 {
        monitors.insert(conn, key(0), Arc::clone(&filters));
    }
    let mut group = c.benchmark_group("monitor");
    group.sample_size(10);
    group.bench_function("kinds1 10k churn", |b| {
        let mut generation = 0u64;
        b.iter(|| {
            for conn in 0..10_000u64 {
                monitors.remove(conn, &sub_id);
                generation += 1;
                monitors.insert(conn, key(generation), Arc::clone(&filters));
            }
        });
    });
    group.finish();
}

criterion_group!(benches, parse_verify, filter_match, monitor_churn);
criterion_main!(benches);
