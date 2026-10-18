//! `vectors/event-validate.json` runner.
//!
//! Cases with `error` must fail to parse; cases with `parsed` must parse and
//! produce the same canonical object. The serde-vs-JSON.parse divergences
//! below are the only permitted skips.

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

use nkr::event::{EventLimits, UnverifiedEvent};
use serde::Deserialize;

/// `(reason, why)` — legitimate serde/JSON.parse divergences.
const DIVERGENCES: &[(&str, &str)] = &[
    (
        "dup",
        "serde derives reject duplicate keys; JSON.parse takes the last value",
    ),
    (
        ".0",
        "serde rejects float spellings where JSON.parse accepts integral floats",
    ),
    (
        "1e3",
        "serde rejects exponent spellings where JSON.parse accepts integral floats",
    ),
    (
        "-0",
        "serde rejects the float spelling -0 where JSON.parse yields 0",
    ),
];

fn is_divergent(reason: &str) -> bool {
    DIVERGENCES.iter().any(|(pat, _)| reason.contains(pat))
}

#[derive(Deserialize)]
struct Case {
    reason: String,
    raw: String,
    parsed: Option<String>,
    error: Option<String>,
}

#[test]
fn event_validate() {
    let cases = common::cases::<Case>("event-validate.json");
    assert_eq!(cases.len(), 59, "vector count drift");
    let mut divergent = 0usize;
    for case in &cases {
        let outcome = UnverifiedEvent::parse(&case.raw, &EventLimits::default());
        if let Some(expected) = &case.parsed {
            match outcome {
                Ok(ev) => {
                    let expected: serde_json::Value =
                        serde_json::from_str(expected).expect("parsed is a JSON object");
                    let parsed = expected.as_object().expect("parsed is an object");
                    assert_eq!(
                        parsed.get("id").and_then(|v| v.as_str()),
                        Some(ev.id().to_string().as_str()),
                        "{}: id",
                        case.reason
                    );
                    assert_eq!(
                        parsed.get("pubkey").and_then(|v| v.as_str()),
                        Some(ev.pubkey().to_string().as_str()),
                        "{}: pubkey",
                        case.reason
                    );
                    assert_eq!(
                        parsed.get("created_at").and_then(serde_json::Value::as_u64),
                        Some(ev.created_at()),
                        "{}: created_at",
                        case.reason
                    );
                    assert_eq!(
                        parsed.get("kind").and_then(serde_json::Value::as_u64),
                        Some(u64::from(ev.kind().0)),
                        "{}: kind",
                        case.reason
                    );
                    assert_eq!(
                        parsed.get("content").and_then(|v| v.as_str()),
                        Some(ev.content()),
                        "{}: content",
                        case.reason
                    );
                    assert_eq!(
                        parsed.get("sig").and_then(|v| v.as_str()).map(str::len),
                        Some(128),
                        "{}: sig",
                        case.reason
                    );
                    let tags: Vec<Vec<String>> = ev
                        .tags()
                        .map(|tag| tag.iter().map(ToString::to_string).collect())
                        .collect();
                    assert_eq!(
                        parsed.get("tags"),
                        Some(&serde_json::json!(tags)),
                        "{}: tags",
                        case.reason
                    );
                }
                Err(e) => {
                    assert!(
                        is_divergent(&case.reason),
                        "{}: expected success, got {e} ({})",
                        case.reason,
                        e.reason()
                    );
                    divergent += 1;
                }
            }
        } else {
            assert!(
                case.error.is_some(),
                "{}: neither parsed nor error",
                case.reason
            );
            assert!(
                outcome.is_err(),
                "{}: expected error, got successful parse",
                case.reason
            );
        }
    }
    assert_eq!(divergent, 14, "divergence count drift");
}
