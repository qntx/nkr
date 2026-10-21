//! Deterministic event signers.
//!
//! A fixed key pool and a canonical JSON builder that parses and
//! verifies every event it makes. Test and tooling support — the
//! stores and the differential suite share these helpers.

#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "deterministic test signers: a failure is a caller bug"
)]

use std::sync::Arc;

use secp256k1::{Keypair, SecretKey, schnorr};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::event::{Event, EventLimits, UnverifiedEvent};

/// A fixed secret key derived from a small integer.
///
/// # Panics
///
/// Panics when the byte string is not a valid secret key; every
/// nonzero `n` produces one, so this is unreachable.
#[must_use]
pub fn key(n: u8) -> Keypair {
    let mut bytes = [0u8; 32];
    bytes[31] = n.max(1);
    Keypair::from_secret_key(&SecretKey::from_secret_bytes(bytes).unwrap())
}

/// The shared pool of signers (small, so filters overlap).
#[must_use]
pub fn keys() -> Vec<Keypair> {
    (1..=4u8).map(key).collect()
}

/// The hex of pool signer `i`'s public key.
#[must_use]
pub fn pubkey_hex(i: usize) -> String {
    format!("{}", keys()[i].x_only_public_key().0)
}

/// Build a signed, verified event. `tags` is a JSON value like
/// `json!([["t","abc"]])`.
///
/// # Panics
///
/// Panics when `tags` is not a tags array or the event fails to
/// verify — both are caller bugs in test code.
#[must_use]
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
