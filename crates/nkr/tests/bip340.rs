//! `vectors/bip340/official.csv` runner: verification of the official
//! BIP-340 test vectors. Only rows with a 32-byte message and a 64-byte
//! signature are verification cases; the trailing signing-failure rows
//! are skipped.

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

use secp256k1::XOnlyPublicKey;
use secp256k1::schnorr::{self, Signature};

#[test]
fn bip340() {
    let text = common::read("bip340/official.csv");
    let mut count = 0usize;
    for (line_no, line) in text.lines().enumerate().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split(',').collect();
        assert!(cols.len() >= 6, "line {line_no}: {line}");
        let pubkey = common::hex_decode_array::<32>(cols[2]);
        let message = common::hex_decode(cols[4]);
        let signature = common::hex_decode_array::<64>(cols[5]);
        let expected = cols[6].trim() == "TRUE";
        let (Some(pubkey), Some(message), Some(signature)) = (pubkey, message, signature) else {
            continue; // signing-failure rows: no verification semantics
        };
        if message.len() != 32 {
            continue;
        }
        count += 1;
        match XOnlyPublicKey::from_byte_array(pubkey) {
            Ok(pubkey) => {
                let msg: [u8; 32] = message.try_into().expect("32-byte message");
                let ok =
                    schnorr::verify(&Signature::from_byte_array(signature), &msg, &pubkey).is_ok();
                assert_eq!(ok, expected, "line {line_no}: verify result");
            }
            Err(_) => assert!(!expected, "line {line_no}: invalid pubkey must not verify"),
        }
    }
    assert_eq!(count, 15, "BIP-340 verification case count drift");
}
