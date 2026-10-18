//! `vectors/filter-limit.json` runner.

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

use nkr::filter::Filter;
use serde::Deserialize;

/// `(filter JSON substring, why)` — oracle SDK cases the relay resolves
/// differently.
const DIVERGENCES: &[(&str, &str)] = &[(
    "\"#d\":[]",
    "empty `#` value lists make the filter impossible (NIP-01 MUST), so the      intrinsic limit is 0 rather than unbounded",
)];

#[derive(Deserialize)]
struct Case {
    filter: serde_json::Value,
    limit: Option<u64>,
}

#[test]
fn filter_limit() {
    let cases = common::cases::<Case>("filter-limit.json");
    assert_eq!(cases.len(), 18, "vector count drift");
    let mut divergent = 0usize;
    for (i, case) in cases.iter().enumerate() {
        let filter_json = serde_json::to_string(&case.filter).expect("filter json");
        let filter = Filter::parse(&filter_json)
            .unwrap_or_else(|e| panic!("case {i}: filter parse: {e} ({filter_json})"));
        if DIVERGENCES.iter().any(|(pat, _)| filter_json.contains(pat)) {
            divergent += 1;
            assert!(
                filter.is_impossible() && filter.intrinsic_limit() == Some(0),
                "case {i}: {filter_json}"
            );
            continue;
        }
        assert_eq!(
            filter.intrinsic_limit(),
            case.limit,
            "case {i}: {filter_json}"
        );
    }
    assert_eq!(divergent, DIVERGENCES.len(), "divergence count drift");
}
