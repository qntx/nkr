//! Differential test: `Scan` output vs a naive in-memory model.

#![allow(
    unused_crate_dependencies,
    reason = "each test binary uses a subset of the workspace dependencies"
)]
#![allow(
    clippy::tests_outside_test_module,
    reason = "each file is a single integration test"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "tests may panic on failure"
)]

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;

use nosfry_core::event::{Event, PublicKey};
use nosfry_core::filter::Filter;
use nosfry_core::sign::{event, keys};
use nosfry_core::store::WriteOutcome;
use nosfry_lmdb::{CountScan, Progress, Scan, Store, StoreOptions};
use nosfry_testkit::driver::{Aftermath, StoreDriver, build_case, check};
use nosfry_testkit::model::model_union;
use nosfry_testkit::spec::{event_spec_strategy, filter_json, filter_spec_strategy};
use proptest::prelude::*;
use serde_json::json;

fn store_scan_gated(
    store: &Store,
    filters: Arc<[Filter]>,
    gate: nosfry_core::auth::ReadGate,
    snapshot: u64,
    max_limit: u64,
    now: u64,
) -> Vec<String> {
    let mut scan = Scan::new(filters, gate, snapshot, max_limit, now);
    let out = RefCell::new(Vec::new());
    let mut guard = 0usize;
    loop {
        guard += 1;
        assert!(guard < 10_000, "scan did not terminate");
        let progress = scan
            .step(store, Duration::ZERO, &mut |_seq, json| {
                out.borrow_mut().push(json.to_owned());
            })
            .unwrap();
        if progress == Progress::Done {
            break;
        }
    }
    out.into_inner()
}

/// Run a [`CountScan`] to completion (`0` entry budget = uncapped).
fn store_count(
    store: &Store,
    filters: Arc<[Filter]>,
    gate: nosfry_core::auth::ReadGate,
    snapshot: u64,
    budget_entries: u64,
    now: u64,
) -> nosfry_lmdb::CountResult {
    let mut scan = CountScan::new(filters, gate, snapshot, budget_entries, now, None);
    let mut guard = 0usize;
    loop {
        guard += 1;
        assert!(guard < 100_000, "count scan did not terminate");
        let progress = scan.step(store, Duration::ZERO).unwrap();
        if progress == Progress::Done {
            break;
        }
    }
    scan.result()
}

/// The LMDB store driven through the shared suite.
struct LmdbDriver<'a> {
    store: &'a Store,
}

impl StoreDriver for LmdbDriver<'_> {
    fn write(&mut self, events: &[Arc<Event>]) -> Vec<WriteOutcome> {
        self.store.write(events, None).unwrap().outcomes
    }

    fn snapshot(&mut self) -> u64 {
        self.store.snapshot().unwrap()
    }

    fn scan(
        &mut self,
        filters: Arc<[Filter]>,
        gate: &nosfry_core::auth::ReadGate,
        snapshot: u64,
        max_limit: u64,
        now: u64,
    ) -> Vec<String> {
        store_scan_gated(self.store, filters, gate.clone(), snapshot, max_limit, now)
    }

    fn expire(&mut self, now: u64) {
        self.store.expire(now, usize::MAX).unwrap();
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn scan_matches_model(
        specs in proptest::collection::vec(event_spec_strategy(), 1..40),
        batches in proptest::collection::vec(1usize..20, 1..5),
        filter_specs in proptest::collection::vec(filter_spec_strategy(), 1..4),
        max_limit in prop_oneof![Just(3u64), Just(7u64), Just(500u64)],
        now in 0u64..600,
        now2 in 0u64..600,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();

        let case = build_case(&specs, batches, filter_specs, max_limit, now, now2);
        let Aftermath { model, snapshot } = {
            let mut driver = LmdbDriver { store: &store };
            check(&mut driver, &case)?
        };

        // COUNT ignores `limit`: it must equal the deduplicated union
        // a limit-less Scan returns.
        let unlimited: Arc<[Filter]> = Arc::from(
            case.filter_specs
                .iter()
                .map(|s| {
                    let mut json = filter_json(s, &case.ids);
                    json.remove("limit");
                    Filter::parse(&serde_json::to_string(&json).unwrap()).unwrap()
                })
                .collect::<Vec<_>>(),
        );
        let union = model_union(
            &model.stored,
            &unlimited,
            snapshot,
            &nosfry_core::auth::ReadGate::default(),
            case.now2,
        );
        let counted = store_count(
            &store,
            Arc::clone(&unlimited),
            nosfry_core::auth::ReadGate::default(),
            snapshot,
            0,
            case.now2,
        );
        prop_assert!(!counted.approximate);
        prop_assert_eq!(counted.count, union as u64);

        // A restricted-kind gate shrinks the count exactly as it
        // shrinks the scan union.
        let rules = nosfry_core::auth::AuthRules::new(
            Some(nosfry_core::url::RelayUrl::parse("wss://relay.example.com").unwrap()),
            &[3],
            true,
        );
        let authed: Arc<[PublicKey]> = Arc::from(vec![PublicKey(
            keys()[0].x_only_public_key().0.to_byte_array(),
        )]);
        let gate = nosfry_core::auth::ReadGate::new(Arc::new(rules), authed);
        let union_gated = model_union(&model.stored, &unlimited, snapshot, &gate, case.now2);
        let counted_gated = store_count(&store, unlimited, gate, snapshot, 0, case.now2);
        prop_assert!(!counted_gated.approximate);
        prop_assert_eq!(counted_gated.count, union_gated as u64);
    }
}

/// Hitting the entry budget ends the scan early and reports a partial
/// count — never a wrong one.
#[test]
fn count_budget_marks_approximate() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
    let events: Vec<Arc<Event>> = (0..40u64)
        .map(|i| {
            event(
                &keys()[usize::try_from(i).unwrap() % 4],
                1,
                1_700_000_000 + i,
                &json!([]),
                &format!("e{i}"),
            )
        })
        .collect();
    store.write(&events, None).unwrap();

    let filters: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()]);
    let snapshot = store.snapshot().unwrap();
    let full = store_count(
        &store,
        Arc::clone(&filters),
        nosfry_core::auth::ReadGate::default(),
        snapshot,
        0,
        u64::MAX,
    );
    assert!(!full.approximate);
    assert_eq!(full.count, events.len() as u64);

    // Fewer index entries than there are matches: the answer is
    // approximate and never over-counts.
    let partial = store_count(
        &store,
        filters,
        nosfry_core::auth::ReadGate::default(),
        snapshot,
        10,
        u64::MAX,
    );
    assert!(partial.approximate);
    assert!(partial.count <= full.count);
}
