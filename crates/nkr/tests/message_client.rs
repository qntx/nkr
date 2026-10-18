//! `vectors/message-client.json` runner.
//!
//! `error` cases must fail to parse; `encoded` cases must parse and
//! re-encode to the canonical wire form (rebuilt from the public accessors
//! by `common::message_wire`); bare `raw` cases must parse. The
//! serde/strfry-vs-JSON.parse divergences below are the only permitted
//! deviations, each pinned to a raw substring.

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

use nkr::message::ClientMessage;
use serde::Deserialize;

/// `(raw substring, why)` — each pattern identifies exactly one case.
const DIVERGENCES: &[(&str, &str)] = &[
    (
        "\"kinds\":[9],\"kinds\":[1]",
        "duplicate `kinds` key and float `1e2`: serde rejects, JSON.parse \
         takes the last value",
    ),
    (
        "\"since\":\"x\",\"since\":5",
        "duplicate `since` key: serde rejects, JSON.parse takes the last value",
    ),
    (
        "\"kinds\":[{}],\"kinds\":[7]",
        "duplicate `kinds` key: serde rejects, JSON.parse takes the last value",
    ),
    (
        "\"ids\":[123],\"ids\"",
        "duplicate `ids` key: serde rejects, JSON.parse takes the last value",
    ),
    (
        "\"limit\":null,\"limit\":3",
        "duplicate `limit` key: serde rejects, JSON.parse takes the last value",
    ),
    (
        "1e3",
        "float spellings `1e3`/`2.0`/`-0`: serde rejects, JSON.parse accepts",
    ),
    (
        "9007199254740991.0",
        "float spelling: serde rejects, JSON.parse accepts",
    ),
    (
        "\"foo\":1",
        "relay policy (strfry): unknown filter keys are rejected, not ignored",
    ),
    (
        "\"#custom\"",
        "relay policy (strfry): unknown filter keys are rejected, not ignored",
    ),
    (
        "9007199254740992",
        "values >= 2^53 accepted as u64 where the SDK rejects them",
    ),
];

#[derive(Deserialize)]
struct Case {
    raw: String,
    encoded: Option<String>,
    error: Option<String>,
}

#[test]
fn message_client() {
    let cases = common::cases::<Case>("message-client.json");
    assert_eq!(cases.len(), 92, "vector count drift");
    let mut divergent = 0usize;
    for (i, case) in cases.iter().enumerate() {
        let outcome = ClientMessage::parse(&case.raw);
        let divergence = DIVERGENCES.iter().find(|(pat, _)| case.raw.contains(pat));
        if case.error.is_some() {
            if let Some((_, why)) = divergence {
                assert!(
                    outcome.is_ok(),
                    "case {i}: oracle expects an error but nkr accepts ({why})"
                );
                divergent += 1;
            } else {
                assert!(
                    outcome.is_err(),
                    "case {i} ({}): expected error, got {:?}",
                    case.raw,
                    outcome.map(|m| common::message_wire(&m))
                );
            }
            continue;
        }
        match outcome {
            Ok(message) => {
                assert!(
                    divergence.is_none(),
                    "case {i}: parsed yet listed as divergent",
                );
                if let Some(encoded) = &case.encoded {
                    assert_eq!(
                        &common::message_wire(&message),
                        encoded,
                        "case {i}: {}",
                        case.raw
                    );
                }
            }
            Err(e) => {
                if divergence.is_some() {
                    divergent += 1;
                    continue;
                }
                panic!("case {i} ({}): expected success, got {e}", case.raw);
            }
        }
    }
    assert_eq!(divergent, DIVERGENCES.len(), "divergence count drift");
}
