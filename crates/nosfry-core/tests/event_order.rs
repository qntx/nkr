//! `vectors/event-order.json` runner.

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

use nosfry_core::event::{EventId, cmp_newest_first, supersedes};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "op")]
enum Case {
    #[serde(rename = "sort")]
    Sort {
        events: Vec<WireEvent>,
        order: Vec<String>,
    },
    #[serde(rename = "item")]
    Item {
        events: Vec<WireEvent>,
        order: Vec<String>,
    },
    #[serde(rename = "winner")]
    Winner {
        candidate: WireEvent,
        incumbent: WireEvent,
        wins: bool,
    },
}

#[derive(Deserialize)]
struct WireEvent {
    id: String,
    created_at: u64,
}

impl WireEvent {
    fn key(&self) -> (u64, EventId) {
        (
            self.created_at,
            EventId::from_hex(&self.id).expect("vector id is hex"),
        )
    }
}

#[test]
fn event_order() {
    let cases = common::cases::<Case>("event-order.json");
    assert_eq!(cases.len(), 9, "vector count drift");
    for (i, case) in cases.iter().enumerate() {
        match case {
            Case::Sort { events, order } => {
                let mut keys: Vec<(u64, EventId)> = events.iter().map(WireEvent::key).collect();
                keys.sort_by(|a, b| cmp_newest_first((a.0, &a.1), (b.0, &b.1)));
                let actual: Vec<String> = keys.iter().map(|(_, id)| id.to_string()).collect();
                assert_eq!(&actual, order, "case {i}: sort order");
            }
            Case::Item { events, order } => {
                let mut keys: Vec<(u64, EventId)> = events.iter().map(WireEvent::key).collect();
                // Negentropy item order: created_at ascending, id ascending.
                keys.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                let actual: Vec<String> = keys.iter().map(|(_, id)| id.to_string()).collect();
                assert_eq!(&actual, order, "case {i}: item order");
            }
            Case::Winner {
                candidate,
                incumbent,
                wins,
            } => {
                let new = candidate.key();
                let old = incumbent.key();
                assert_eq!(
                    supersedes((new.0, &new.1), (old.0, &old.1)),
                    *wins,
                    "case {i}: candidate {} vs incumbent {}",
                    candidate.id,
                    incumbent.id
                );
            }
        }
    }
}
