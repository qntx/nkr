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

mod common;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use common::{event, key};
use nosfry_core::event::{Event, EventId, cmp_newest_first, supersedes};
use nosfry_core::filter::Filter;
use nosfry_core::kind::KindClass;
use nosfry_lmdb::{Progress, Scan, Store, StoreOptions, WriteOutcome};
use proptest::prelude::*;
use serde_json::json;

/// The shared pool of signers (small, so filters overlap).
fn keys() -> Vec<secp256k1::Keypair> {
    (1..=4u8).map(key).collect()
}

fn pubkey_hex(i: usize) -> String {
    format!("{}", keys()[i].x_only_public_key().0)
}

/// Model the store's write semantics in arrival order.
fn model_store(events: &[Arc<Event>]) -> HashMap<u64, Arc<Event>> {
    let mut stored: HashMap<u64, Arc<Event>> = HashMap::new();
    let mut by_id: HashMap<EventId, u64> = HashMap::new();
    let mut by_addr: HashMap<[u8; 32], u64> = HashMap::new();
    let mut next_seq = 1u64;
    for ev in events {
        if ev.kind().class() == KindClass::Ephemeral || by_id.contains_key(&ev.id()) {
            continue;
        }
        if let Some(addr) = ev.address()
            && let Some(&old_seq) = by_addr.get(&addr)
        {
            let old = stored
                .get(&old_seq)
                .unwrap_or_else(|| panic!("model: missing seq {old_seq}"));
            if !supersedes((ev.created_at(), &ev.id()), (old.created_at(), &old.id())) {
                continue;
            }
            by_id.remove(&old.id());
            stored.remove(&old_seq);
        }
        let seq = next_seq;
        next_seq += 1;
        by_id.insert(ev.id(), seq);
        if let Some(addr) = ev.address() {
            by_addr.insert(addr, seq);
        }
        stored.insert(seq, Arc::clone(ev));
    }
    stored
}

/// Naive scan: per filter, match, sort newest first, truncate, dedup.
fn model_scan(
    stored: &HashMap<u64, Arc<Event>>,
    filters: &[Filter],
    snapshot: u64,
    max_limit: u64,
) -> Vec<String> {
    let mut emitted = HashSet::new();
    let mut out = Vec::new();
    for filter in filters {
        let limit = filter.effective_limit(max_limit);
        if filter.is_impossible() || limit == 0 {
            continue;
        }
        let mut matched: Vec<(u64, EventId, u64)> = stored
            .iter()
            .filter(|(seq, ev)| **seq <= snapshot && filter.matches(ev.packed()))
            .map(|(seq, ev)| (ev.created_at(), ev.id(), *seq))
            .collect();
        matched.sort_by(|a, b| cmp_newest_first((a.0, &a.1), (b.0, &b.1)));
        matched.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        for (_, _, seq) in matched {
            // Already-emitted seqs consume the limit but are not sent twice.
            if !emitted.insert(seq) {
                continue;
            }
            out.push(
                stored
                    .get(&seq)
                    .map_or_else(String::new, |e| e.json().to_owned()),
            );
        }
    }
    out
}

fn store_scan(store: &Store, filters: Arc<[Filter]>, snapshot: u64, max_limit: u64) -> Vec<String> {
    let mut scan = Scan::new(filters, snapshot, max_limit);
    let out = RefCell::new(Vec::new());
    let mut guard = 0usize;
    loop {
        guard += 1;
        assert!(guard < 10_000, "scan did not terminate");
        let progress = scan
            .step(store, Duration::ZERO, &mut |json| {
                out.borrow_mut().push(json.to_owned());
            })
            .unwrap();
        if progress == Progress::Done {
            break;
        }
    }
    out.into_inner()
}

#[derive(Debug, Clone)]
enum TagSpec {
    E(usize), // reference into corpus ids
    P(usize), // reference into the key pool
    T(String),
    D(String),
}

#[derive(Debug, Clone)]
struct EventSpec {
    key: usize,
    kind: u16,
    created_at: u64,
    tags: Vec<TagSpec>,
    content: String,
}

#[derive(Debug, Clone)]
enum FilterSpec {
    Any,
    Kinds(Vec<u16>),
    Authors(Vec<usize>),
    AuthorsKinds(Vec<usize>, Vec<u16>),
    Ids(Vec<usize>),
    TagE(Vec<usize>),
    TagP(Vec<usize>),
    TagStr(u8, Vec<String>),
    Range(Option<u64>, Option<u64>),
    Limit(u64),
    EmptyKinds,
    EmptyTag,
    Mixed(Box<Self>, Box<Self>),
}

fn tag_json(tag: &TagSpec, ids: &[String]) -> serde_json::Value {
    match tag {
        TagSpec::E(i) => json!(["e", ids[i % ids.len().max(1)]]),
        TagSpec::P(i) => json!(["p", pubkey_hex(i % 4)]),
        TagSpec::T(s) => json!(["t", s]),
        TagSpec::D(s) => json!(["d", s]),
    }
}

fn build_event(spec: &EventSpec, ids: &[String]) -> Arc<Event> {
    let tags: Vec<serde_json::Value> = spec.tags.iter().map(|t| tag_json(t, ids)).collect();
    event(
        &keys()[spec.key % 4],
        spec.kind,
        spec.created_at,
        &json!(tags),
        &spec.content,
    )
}

fn filter_json(spec: &FilterSpec, ids: &[String]) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    let mut merge = |s: &FilterSpec| map.extend(filter_json(s, ids));
    match spec {
        FilterSpec::Any => {}
        FilterSpec::Kinds(kinds) => {
            map.insert("kinds".into(), json!(kinds));
        }
        FilterSpec::Authors(ks) => {
            map.insert(
                "authors".into(),
                json!(ks.iter().map(|k| pubkey_hex(k % 4)).collect::<Vec<_>>()),
            );
        }
        FilterSpec::AuthorsKinds(ks, kinds) => {
            map.insert(
                "authors".into(),
                json!(ks.iter().map(|k| pubkey_hex(k % 4)).collect::<Vec<_>>()),
            );
            map.insert("kinds".into(), json!(kinds));
        }
        FilterSpec::Ids(is) => {
            map.insert(
                "ids".into(),
                json!(
                    is.iter()
                        .map(|i| ids[i % ids.len().max(1)].clone())
                        .collect::<Vec<_>>()
                ),
            );
        }
        FilterSpec::TagE(is) => {
            map.insert(
                "#e".into(),
                json!(
                    is.iter()
                        .map(|i| ids[i % ids.len().max(1)].clone())
                        .collect::<Vec<_>>()
                ),
            );
        }
        FilterSpec::TagP(is) => {
            map.insert(
                "#p".into(),
                json!(is.iter().map(|i| pubkey_hex(i % 4)).collect::<Vec<_>>()),
            );
        }
        FilterSpec::TagStr(name, vals) => {
            map.insert(format!("#{}", *name as char), json!(vals));
        }
        FilterSpec::Range(since, until) => {
            if let Some(s) = since {
                map.insert("since".into(), json!(s));
            }
            if let Some(u) = until {
                map.insert("until".into(), json!(u));
            }
        }
        FilterSpec::Limit(n) => {
            map.insert("limit".into(), json!(n));
        }
        FilterSpec::EmptyKinds => {
            map.insert("kinds".into(), json!([]));
        }
        FilterSpec::EmptyTag => {
            map.insert("#t".into(), json!([]));
        }
        FilterSpec::Mixed(a, b) => {
            merge(a);
            merge(b);
        }
    }
    map
}

fn tag_strategy() -> impl Strategy<Value = TagSpec> {
    prop_oneof![
        2 => (0usize..8).prop_map(TagSpec::E),
        2 => (0usize..4).prop_map(TagSpec::P),
        4 => prop_oneof![
            Just("a".to_owned()),
            Just("b".to_owned()),
            Just("nostr".to_owned()),
            Just("x".repeat(300)),
        ].prop_map(TagSpec::T),
        3 => prop_oneof![
            Just("d1".to_owned()),
            Just("d2".to_owned()),
            Just("long".repeat(80)),
        ].prop_map(TagSpec::D),
    ]
}

fn event_spec_strategy() -> impl Strategy<Value = EventSpec> {
    (
        0usize..4,
        prop_oneof![
            4 => Just(1u16),
            2 => Just(0u16),
            1 => Just(3u16),
            1 => Just(20_001u16),
            3 => Just(30_001u16),
            1 => Just(30_002u16),
        ],
        0u64..30,
        proptest::collection::vec(tag_strategy(), 0..4),
        proptest::collection::vec(0u8..8, 1..6),
    )
        .prop_map(|(key, kind, created_at, tags, content)| EventSpec {
            key,
            kind,
            created_at,
            tags,
            content: content.iter().map(|b| char::from(b'a' + b)).collect(),
        })
}

fn filter_spec_strategy() -> impl Strategy<Value = FilterSpec> {
    let leaf = prop_oneof![
        3 => Just(FilterSpec::Any),
        3 => proptest::collection::vec(
            prop_oneof![Just(1u16), Just(0u16), Just(30_001u16), Just(20_001u16), Just(7u16)],
            1..3,
        ).prop_map(FilterSpec::Kinds),
        2 => proptest::collection::vec(0usize..4, 1..3).prop_map(FilterSpec::Authors),
        2 => (
            proptest::collection::vec(0usize..4, 1..3),
            proptest::collection::vec(prop_oneof![Just(1u16), Just(30_001u16)], 1..3),
        ).prop_map(|(a, k)| FilterSpec::AuthorsKinds(a, k)),
        2 => proptest::collection::vec(0usize..8, 1..4).prop_map(FilterSpec::Ids),
        2 => proptest::collection::vec(0usize..8, 1..3).prop_map(FilterSpec::TagE),
        2 => proptest::collection::vec(0usize..4, 1..3).prop_map(FilterSpec::TagP),
        2 => (
            prop_oneof![Just(b't'), Just(b'd')],
            proptest::collection::vec(
                prop_oneof![
                    Just("a".to_owned()),
                    Just("b".to_owned()),
                    Just("nostr".to_owned()),
                    Just("zzz".to_owned()),
                ],
                1..3,
            ),
        ).prop_map(|(n, v)| FilterSpec::TagStr(n, v)),
        2 => (
            proptest::option::of(0u64..20),
            proptest::option::of(10u64..40),
        ).prop_map(|(s, u)| FilterSpec::Range(s, u)),
        2 => (0u64..8).prop_map(FilterSpec::Limit),
        1 => Just(FilterSpec::EmptyKinds),
        1 => Just(FilterSpec::EmptyTag),
    ];
    leaf.prop_recursive(2, 8, 3, |inner| {
        (inner.clone(), inner).prop_map(|(a, b)| FilterSpec::Mixed(Box::new(a), Box::new(b)))
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn scan_matches_model(
        specs in proptest::collection::vec(event_spec_strategy(), 1..40),
        batches in proptest::collection::vec(1usize..20, 1..5),
        filter_specs in proptest::collection::vec(filter_spec_strategy(), 1..4),
        max_limit in prop_oneof![Just(3u64), Just(7u64), Just(500u64)],
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();

        // Build events; ids depend on the tags so we need two passes.
        // The second pass's ids are what filters should reference.
        let placeholder = vec!["00".repeat(32)];
        let draft_ids: Vec<String> = specs
            .iter()
            .map(|s| build_event(s, &placeholder))
            .map(|e| format!("{}", e.id()))
            .collect();
        let draft: Vec<Arc<Event>> =
            specs.iter().map(|s| build_event(s, &draft_ids)).collect();
        let tagged_ids: Vec<String> =
            draft.iter().map(|e| format!("{}", e.id())).collect();
        let events: Vec<Arc<Event>> =
            specs.iter().map(|s| build_event(s, &tagged_ids)).collect();
        let ids: Vec<String> = events.iter().map(|e| format!("{}", e.id())).collect();

        // Write in random batch splits.
        let mut all: Vec<Arc<Event>> = events.clone();
        for chunk in &batches {
            let take = all.len().min(*chunk);
            let batch: Vec<Arc<Event>> = all.drain(..take).collect();
            let outcomes = store.write(&batch).unwrap();
            prop_assert_eq!(outcomes.len(), batch.len());
            for outcome in &outcomes {
                let known = matches!(
                    outcome,
                    WriteOutcome::Stored { .. }
                        | WriteOutcome::Duplicate
                        | WriteOutcome::Superseded
                        | WriteOutcome::Ephemeral
                );
                prop_assert!(known, "unknown outcome {outcome:?}");
            }
        }
        let outcomes = store.write(&all).unwrap();
        prop_assert_eq!(outcomes.len(), all.len());

        let stored = model_store(&events);

        let filters: Vec<Filter> = filter_specs
            .iter()
            .map(|s| serde_json::to_string(&filter_json(s, &ids)).unwrap())
            .map(|j| Filter::parse(&j).unwrap())
            .collect();
        let filters: Arc<[Filter]> = Arc::from(filters);

        let snapshot = store.snapshot().unwrap();
        let expected = model_scan(&stored, &filters, snapshot, max_limit);
        let got = store_scan(&store, filters, snapshot, max_limit);
        prop_assert_eq!(got, expected);
    }
}
