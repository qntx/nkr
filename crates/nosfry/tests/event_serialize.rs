//! `vectors/event-serialize.json` runner: the id preimage is the oracle.

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

use nosfry::event::{PublicKey, event_id, serialize_for_id};
use nosfry::kind::Kind;
use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    unsigned: Unsigned,
    serialized: String,
    id: String,
}

#[derive(Deserialize)]
struct Unsigned {
    pubkey: String,
    created_at: u64,
    kind: u16,
    tags: Vec<Vec<String>>,
    content: String,
}

#[test]
fn event_serialize() {
    let cases = common::cases::<Case>("event-serialize.json");
    assert_eq!(cases.len(), 5, "vector count drift");
    for case in &cases {
        let pubkey = PublicKey::from_hex(&case.unsigned.pubkey)
            .unwrap_or_else(|| panic!("bad pubkey hex: {}", case.unsigned.pubkey));
        let serialized = serialize_for_id(
            &pubkey,
            case.unsigned.created_at,
            Kind(case.unsigned.kind),
            &case.unsigned.tags,
            &case.unsigned.content,
        );
        assert_eq!(serialized, case.serialized, "preimage mismatch");
        assert_eq!(
            event_id(serialized.as_bytes()).to_string(),
            case.id,
            "id mismatch"
        );
    }
}
