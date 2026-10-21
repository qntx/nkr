//! The differential driver: one generated case replayed identically
//! against a real store and the in-memory [`Model`].

use std::sync::Arc;

use nosfry_core::auth::{AuthRules, ReadGate};
use nosfry_core::event::{Event, PublicKey};
use nosfry_core::filter::Filter;
use nosfry_core::hll::Hll;
use nosfry_core::sign::{keys, pubkey_hex};
use nosfry_core::store::WriteOutcome;
use nosfry_core::url::RelayUrl;
use proptest::prelude::*;

use crate::model::{Model, model_expire, model_scan, model_store, model_union};
use crate::spec::{EventSpec, FilterSpec, build_event, filter_json};

/// One event store under test. Implementations translate each call
/// into their store's API; panics and unwraps are the driver's way of
/// saying the store itself failed.
pub trait StoreDriver {
    /// Store `events`; one outcome per input, in arrival order.
    fn write(&mut self, events: &[Arc<Event>]) -> Vec<WriteOutcome>;

    /// The seq bound covering everything written so far.
    fn snapshot(&mut self) -> u64;

    /// The event JSONs `filters` emit under `gate`, bounded by
    /// `snapshot` and `max_limit`, at `now`.
    fn scan(
        &mut self,
        filters: Arc<[Filter]>,
        gate: &ReadGate,
        snapshot: u64,
        max_limit: u64,
        now: u64,
    ) -> Vec<String>;

    /// The union `filters` match at `snapshot`/`now` under `gate`,
    /// counted — a filter `limit` does not apply. `Hll::for_filters`
    /// decides sketch eligibility; the driver runs its count
    /// uncapped and to completion.
    fn count(&mut self, filters: Arc<[Filter]>, gate: &ReadGate, snapshot: u64, now: u64) -> Count;

    /// Sweep events expired at `now`.
    fn expire(&mut self, now: u64);
}

/// The count [`StoreDriver::count`] reports, mirrored per store.
#[derive(Debug)]
pub struct Count {
    /// Events counted (the exact union, or a partial count when
    /// `approximate`).
    pub count: u64,
    /// `true` when the store's budget stopped the count early.
    pub approximate: bool,
    /// The HLL registers of counted authors, when the query was
    /// eligible for a sketch.
    pub hll: Option<[u8; 256]>,
}

/// A fully materialized differential case: the events to store, the
/// write batches to drain, and the filters to query.
#[derive(Debug)]
pub struct Case {
    /// Verified events, in write order.
    pub events: Vec<Arc<Event>>,
    /// Hex ids of `events`, position-aligned.
    pub ids: Vec<String>,
    /// Write-batch sizes drained in order before the rest.
    pub batches: Vec<usize>,
    /// The filters to query (`e`/`id` references resolved in `ids`).
    pub filter_specs: Vec<FilterSpec>,
    /// The `max_limit` passed to every scan.
    pub max_limit: u64,
    /// `now` for the pre-expiry scan.
    pub now: u64,
    /// `now` for the post-expiry scan.
    pub now2: u64,
}

/// Materialize one generated case. The two-pass build makes `e`/`a`
/// tag references point at real event ids.
#[must_use]
pub fn build_case(
    specs: &[EventSpec],
    batches: Vec<usize>,
    filter_specs: Vec<FilterSpec>,
    max_limit: u64,
    now: u64,
    now2: u64,
) -> Case {
    // Build events; ids depend on the tags so we need two passes.
    // The second pass's ids are what filters should reference.
    let placeholder = vec!["00".repeat(32)];
    let draft_ids: Vec<String> = specs
        .iter()
        .map(|s| build_event(s, &placeholder, specs))
        .map(|e| format!("{}", e.id()))
        .collect();
    let draft: Vec<Arc<Event>> = specs
        .iter()
        .map(|s| build_event(s, &draft_ids, specs))
        .collect();
    let tagged_ids: Vec<String> = draft.iter().map(|e| format!("{}", e.id())).collect();
    let events: Vec<Arc<Event>> = specs
        .iter()
        .map(|s| build_event(s, &tagged_ids, specs))
        .collect();
    let ids: Vec<String> = events.iter().map(|e| format!("{}", e.id())).collect();
    Case {
        events,
        ids,
        batches,
        filter_specs,
        max_limit,
        now,
        now2,
    }
}

/// Model state left after `check`, for store-specific extra
/// assertions (e.g. the LMDB COUNT scans).
#[derive(Debug)]
pub struct Aftermath {
    /// The reference model after `expire` ran at `now`.
    pub model: Model,
    /// The seq bound after the expiry sweep.
    pub snapshot: u64,
}

/// The gate the shared restricted scan applies: kind 3 is restricted
/// to involved keys and the authed set is the first pool signer, the
/// same rules the LMDB COUNT check uses.
fn restricted_gate() -> ReadGate {
    let rules = AuthRules::new(
        Some(RelayUrl::parse("wss://relay.example.com").unwrap()),
        &[3],
        true,
    );
    let authed: Arc<[PublicKey]> = Arc::from(vec![PublicKey(
        keys()[0].x_only_public_key().0.to_byte_array(),
    )]);
    ReadGate::new(Arc::new(rules), authed)
}

/// Run the shared write/scan/expire/scan assertions: every batch's
/// outcomes are checked, then both a pre- and post-expiry scan must
/// equal the model's answer — with and without a [`ReadGate`].
///
/// # Panics
///
/// Panics if a filter spec fails to serialize or parse.
///
/// # Errors
///
/// Fails the proptest case on any divergence.
pub fn check(store: &mut impl StoreDriver, case: &Case) -> Result<Aftermath, TestCaseError> {
    // Write in random batch splits.
    let mut all: Vec<Arc<Event>> = case.events.clone();
    for chunk in &case.batches {
        let take = all.len().min(*chunk);
        let batch: Vec<Arc<Event>> = all.drain(..take).collect();
        let outcomes = store.write(&batch);
        prop_assert_eq!(outcomes.len(), batch.len());
        for outcome in &outcomes {
            let known = matches!(
                outcome,
                WriteOutcome::Stored { .. }
                    | WriteOutcome::Duplicate
                    | WriteOutcome::Superseded
                    | WriteOutcome::Ephemeral
                    | WriteOutcome::Deleted
            );
            prop_assert!(known, "unknown outcome {outcome:?}");
        }
    }
    let outcomes = store.write(&all);
    prop_assert_eq!(outcomes.len(), all.len());

    let mut stored = model_store(&case.events);

    let filters: Arc<[Filter]> = Arc::from(
        case.filter_specs
            .iter()
            .map(|s| serde_json::to_string(&filter_json(s, &case.ids)).unwrap())
            .map(|j| Filter::parse(&j).unwrap())
            .collect::<Vec<_>>(),
    );

    let snapshot = store.snapshot();
    let expected = model_scan(
        &stored.stored,
        &filters,
        snapshot,
        &ReadGate::default(),
        case.max_limit,
        case.now,
    );
    let got = store.scan(
        Arc::clone(&filters),
        &ReadGate::default(),
        snapshot,
        case.max_limit,
        case.now,
    );
    prop_assert_eq!(got, expected);

    // A restricted gate must shrink the scan exactly as it shrinks
    // the model's answer.
    let gate = restricted_gate();
    let expected_gated = model_scan(
        &stored.stored,
        &filters,
        snapshot,
        &gate,
        case.max_limit,
        case.now,
    );
    let got_gated = store.scan(
        Arc::clone(&filters),
        &gate,
        snapshot,
        case.max_limit,
        case.now,
    );
    prop_assert_eq!(got_gated, expected_gated);

    // After `expire`, a scan must still match the swept model.
    store.expire(case.now);
    model_expire(&mut stored, case.now);
    let swept = store.snapshot();
    let expected_swept = model_scan(
        &stored.stored,
        &filters,
        swept,
        &ReadGate::default(),
        case.max_limit,
        case.now2,
    );
    let got_swept = store.scan(
        filters,
        &ReadGate::default(),
        swept,
        case.max_limit,
        case.now2,
    );
    prop_assert_eq!(got_swept, expected_swept);

    count_checks(store, case, &stored, swept, &gate);

    Ok(Aftermath {
        model: stored,
        snapshot,
    })
}

/// The COUNT assertions of [`check`]: the union under an open and a
/// restricted gate, plus the HLL sketch of an eligible filter set.
///
/// # Panics
///
/// Panics on any divergence — a failure fails the proptest case.
fn count_checks(
    store: &mut impl StoreDriver,
    case: &Case,
    stored: &Model,
    swept: u64,
    gate: &ReadGate,
) {
    // COUNT ignores `limit`: it must equal the deduplicated union a
    // limit-less scan returns.
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
        &stored.stored,
        &unlimited,
        swept,
        &ReadGate::default(),
        case.now2,
    );
    let counted = store.count(
        Arc::clone(&unlimited),
        &ReadGate::default(),
        swept,
        case.now2,
    );
    assert!(!counted.approximate, "uncapped count marked approximate");
    assert_eq!(counted.count, union as u64, "count != model union");

    let union_gated = model_union(&stored.stored, &unlimited, swept, gate, case.now2);
    let counted_gated = store.count(unlimited, gate, swept, case.now2);
    assert!(
        !counted_gated.approximate,
        "uncapped count marked approximate"
    );
    assert_eq!(
        counted_gated.count, union_gated as u64,
        "gated count != model union"
    );

    // An HLL-eligible COUNT (one filter, one tag condition, one
    // value) returns exactly the sketch the model computes over the
    // union's authors.
    let eligible: Arc<[Filter]> = Arc::from(vec![
        Filter::parse(&format!("{{\"#p\":[\"{}\"]}}", pubkey_hex(0))).unwrap(),
    ]);
    let mut sketch = Hll::for_filters(&eligible)
        .unwrap_or_else(|| panic!("single tag-value filter set is HLL-eligible"));
    let mut hits = 0u64;
    for (seq, ev) in &stored.stored {
        if *seq <= swept
            && ev.expiration().is_none_or(|exp| exp > case.now2)
            && eligible[0].matches(ev.packed())
        {
            sketch.add(&ev.pubkey());
            hits += 1;
        }
    }
    let sketched = store.count(eligible, &ReadGate::default(), swept, case.now2);
    assert!(!sketched.approximate, "uncapped count marked approximate");
    assert_eq!(sketched.count, hits, "eligible count != model union");
    assert_eq!(
        sketched.hll,
        Some(*sketch.registers()),
        "hll registers != model sketch"
    );
}
