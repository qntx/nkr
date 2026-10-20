#![no_main]

//! Build a correctly-signed event from arbitrary parts: whenever the
//! strict parser accepts it, verification must succeed and the
//! canonical JSON must be a stable fixed point.

use std::sync::LazyLock;

use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use nosfry_core::event::{self, EventLimits, PublicKey, UnverifiedEvent};
use nosfry_core::kind::Kind;
use nosfry_core::packed::PackedRef;
use secp256k1::{Keypair, SecretKey, schnorr};
use serde_json::{Value, json};

static KEYPAIR: LazyLock<Keypair> =
    LazyLock::new(|| Keypair::from_secret_key(&SecretKey::from_secret_bytes([0x42; 32]).unwrap()));

static LIMITS: LazyLock<EventLimits> = LazyLock::new(|| EventLimits {
    max_event_size: usize::MAX,
    max_num_tags: usize::MAX,
    max_tag_val_size: usize::MAX,
    ..EventLimits::default()
});

#[derive(Arbitrary, Debug)]
struct Input {
    created_at: u64,
    kind: u16,
    tags: Vec<Vec<String>>,
    content: String,
}

fuzz_target!(|input: Input| {
    let created_at = input.created_at % (1 << 53);
    let mut tags = input.tags;
    tags.truncate(16);
    for tag in &mut tags {
        tag.truncate(4);
    }
    let pubkey = PublicKey(KEYPAIR.x_only_public_key().0.to_byte_array());
    let preimage =
        event::serialize_for_id(&pubkey, created_at, Kind(input.kind), &tags, &input.content);
    let id = event::event_id(preimage.as_bytes());
    let sig = schnorr::sign_no_aux_rand(&id.0, &KEYPAIR);
    let assembled = json!({
        "id": faster_hex::hex_string(&id.0),
        "pubkey": faster_hex::hex_string(&pubkey.0),
        "created_at": created_at,
        "kind": input.kind,
        "tags": tags,
        "content": input.content,
        "sig": faster_hex::hex_string(&sig.to_byte_array()),
    });
    let text = assembled.to_string();
    // Structural rejection is legitimate (empty tags, bad e/p hex,
    // malformed expiration); only acceptance is checked.
    let Ok(unverified) = UnverifiedEvent::parse(&text, &LIMITS) else {
        return;
    };
    let event = unverified
        .verify(&LIMITS)
        .expect("a correctly-signed event must verify");
    // Canonicalization is idempotent: re-parse the emitted JSON.
    let canonical = event.json().to_owned();
    let reverified = UnverifiedEvent::parse(&canonical, &LIMITS)
        .and_then(|u| u.verify(&LIMITS))
        .expect("canonical JSON must re-verify");
    assert_eq!(reverified.json(), canonical, "canonical JSON is not stable");
    assert_eq!(
        serde_json::from_str::<Value>(&canonical).unwrap(),
        assembled,
        "canonical JSON lost a field"
    );
    let packed = PackedRef::new(event.packed().as_bytes()).expect("packed record must validate");
    assert_eq!(packed.id(), event.id());
    assert_eq!(packed.pubkey(), event.pubkey());
    assert_eq!(packed.kind(), event.kind());
    assert_eq!(packed.created_at(), event.created_at());
});
