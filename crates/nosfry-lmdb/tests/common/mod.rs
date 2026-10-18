//! Shared test helpers: deterministic event signer.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    missing_docs,
    unreachable_pub,
    reason = "test helpers may panic and are only partially used per binary"
)]

use std::sync::Arc;

use nosfry::event::{Event, EventLimits, UnverifiedEvent};
use secp256k1::{Keypair, SecretKey, schnorr};
use serde_json::json;
use sha2::{Digest, Sha256};

/// A fixed secret key derived from a small integer.
pub fn key(n: u8) -> Keypair {
    let mut bytes = [0u8; 32];
    bytes[31] = n.max(1);
    Keypair::from_secret_key(&SecretKey::from_secret_bytes(bytes).unwrap())
}

/// Build a signed, verified event. `tags` is a JSON value like
/// `json!([["t","abc"]])`.
pub fn event(
    keypair: &Keypair,
    kind: u16,
    created_at: u64,
    tags: &serde_json::Value,
    content: &str,
) -> Arc<Event> {
    let pubkey = faster_hex::hex_string(&keypair.x_only_public_key().0.to_byte_array());
    let preimage =
        serde_json::to_string(&json!([0, pubkey, created_at, kind, tags, content])).unwrap();
    let id = Sha256::digest(preimage.as_bytes());
    let sig = schnorr::sign_no_aux_rand(&id, keypair);
    let event_json = serde_json::to_string(&json!({
        "id": faster_hex::hex_string(&id),
        "pubkey": pubkey,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": faster_hex::hex_string(&sig.to_byte_array()),
    }))
    .unwrap();
    Arc::new(
        UnverifiedEvent::parse(&event_json, &EventLimits::default())
            .unwrap()
            .verify(&EventLimits::default())
            .unwrap(),
    )
}
