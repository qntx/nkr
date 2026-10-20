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
use nosfry_core::deletion::DeletionTarget;
use nosfry_core::event::{Event, EventId, PublicKey, cmp_newest_first, supersedes};
use nosfry_core::filter::Filter;
use nosfry_lmdb::{CountScan, Progress, Scan, Store, StoreOptions, WriteOutcome};
use proptest::prelude::*;
use serde_json::json;

/// The shared pool of signers (small, so filters overlap).
fn keys() -> Vec<secp256k1::Keypair> {
    (1..=4u8).map(key).collect()
}

fn pubkey_hex(i: usize) -> String {
    format!("{}", keys()[i].x_only_public_key().0)
}

/// The store's state model: stored events plus the deletion blocks a
/// kind 5 leaves behind.
#[derive(Default)]
struct Model {
    stored: HashMap<u64, Arc<Event>>,
    by_id: HashMap<EventId, u64>,
    by_addr: HashMap<[u8; 32], u64>,
    /// `id ‖ author` → seqs of the kind-5 events blocking it.
    blocked_ids: HashMap<(EventId, PublicKey), HashSet<u64>>,
    /// `address` → `(created_at, seq)` of the kind-5 events blocking it.
    blocked_addrs: HashMap<[u8; 32], Vec<(u64, u64)>>,
}

/// Remove `seq` from the model, lifting the blocks it carried.
fn model_delete_seq(m: &mut Model, seq: u64) {
    let Some(ev) = m.stored.remove(&seq) else {
        return;
    };
    m.by_id.remove(&ev.id());
    if let Some(addr) = ev.address()
        && m.by_addr.get(&addr) == Some(&seq)
    {
        m.by_addr.remove(&addr);
    }
    for target in ev.deletion_targets() {
        match target {
            DeletionTarget::Event(id) => {
                if let Some(set) = m.blocked_ids.get_mut(&(*id, ev.pubkey())) {
                    set.remove(&seq);
                }
            }
            DeletionTarget::Address(addr) => {
                if let Some(list) = m.blocked_addrs.get_mut(addr) {
                    list.retain(|(_, s)| *s != seq);
                }
            }
        }
    }
}

/// Model `Store::write` in arrival order, NIP-09 included.
fn model_store(events: &[Arc<Event>]) -> Model {
    let mut m = Model::default();
    let mut next_seq = 1u64;
    for ev in events {
        if ev.kind().is_ephemeral() || m.by_id.contains_key(&ev.id()) {
            continue;
        }
        if ev.kind().0 != 5
            && m.blocked_ids
                .get(&(ev.id(), ev.pubkey()))
                .is_some_and(|set| !set.is_empty())
        {
            continue; // Deleted by an earlier e target
        }
        if let Some(addr) = ev.address()
            && m.blocked_addrs
                .get(&addr)
                .is_some_and(|list| list.iter().any(|(ca, _)| *ca >= ev.created_at()))
        {
            continue; // Deleted by an earlier a target
        }
        if let Some(addr) = ev.address()
            && let Some(&old_seq) = m.by_addr.get(&addr)
        {
            let old = m
                .stored
                .get(&old_seq)
                .unwrap_or_else(|| panic!("model: missing seq {old_seq}"));
            if !supersedes((ev.created_at(), &ev.id()), (old.created_at(), &old.id())) {
                continue;
            }
            model_delete_seq(&mut m, old_seq);
        }
        let seq = next_seq;
        next_seq += 1;
        m.stored.insert(seq, Arc::clone(ev));
        m.by_id.insert(ev.id(), seq);
        if let Some(addr) = ev.address() {
            m.by_addr.insert(addr, seq);
        }
        if ev.kind().0 == 5 {
            model_apply_deletions(&mut m, ev, seq);
        }
    }
    m
}

/// Model a stored kind 5: register its blocks, then delete its targets.
fn model_apply_deletions(m: &mut Model, ev: &Event, seq: u64) {
    for target in ev.deletion_targets() {
        match target {
            DeletionTarget::Event(id) => {
                m.blocked_ids
                    .entry((*id, ev.pubkey()))
                    .or_default()
                    .insert(seq);
            }
            DeletionTarget::Address(addr) => {
                m.blocked_addrs
                    .entry(*addr)
                    .or_default()
                    .push((ev.created_at(), seq));
            }
        }
    }
    let targets: Vec<DeletionTarget> = ev.deletion_targets().to_vec();
    for target in targets {
        let tseq = match target {
            DeletionTarget::Event(id) => m.by_id.get(&id).copied(),
            DeletionTarget::Address(addr) => m.by_addr.get(&addr).copied(),
        };
        let Some(tseq) = tseq else {
            continue;
        };
        let deletable = m.stored.get(&tseq).is_some_and(|t| {
            t.pubkey() == ev.pubkey()
                && match target {
                    DeletionTarget::Event(_) => t.kind().0 != 5,
                    DeletionTarget::Address(_) => t.created_at() <= ev.created_at(),
                }
        });
        if deletable {
            model_delete_seq(m, tseq);
        }
    }
}

/// Model `Store::expire`: every event with `expiration <= now` goes.
fn model_expire(m: &mut Model, now: u64) {
    let due: Vec<u64> = m
        .stored
        .iter()
        .filter(|(_, ev)| ev.expiration().is_some_and(|exp| exp <= now))
        .map(|(seq, _)| *seq)
        .collect();
    for seq in due {
        model_delete_seq(m, seq);
    }
}

/// Naive scan: per filter, match, sort newest first, truncate, dedup.
/// Events with `expiration <= now` are hidden and never counted.
fn model_scan(
    stored: &HashMap<u64, Arc<Event>>,
    filters: &[Filter],
    snapshot: u64,
    max_limit: u64,
    now: u64,
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
            .filter(|(seq, ev)| {
                **seq <= snapshot
                    && ev.expiration().is_none_or(|exp| exp > now)
                    && filter.matches(ev.packed())
            })
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

/// The true count `CountScan` must return: unique stored seqs
/// matching at least one filter, visible at `snapshot`/`now`/`gate`.
/// `Scan`'s union is not the reference: `Filter::intrinsic_limit`
/// caps replaceable/addressable author+kind queries at
/// `kinds × authors × #d values`, which is a serving optimization a
/// true count must not inherit.
fn model_union(
    stored: &HashMap<u64, Arc<Event>>,
    filters: &[Filter],
    snapshot: u64,
    gate: &nosfry_core::auth::ReadGate,
    now: u64,
) -> usize {
    let mut seen = HashSet::new();
    for (seq, ev) in stored {
        if *seq <= snapshot
            && ev.expiration().is_none_or(|exp| exp > now)
            && gate.allows(ev.packed())
            && filters.iter().any(|f| f.matches(ev.packed()))
        {
            seen.insert(*seq);
        }
    }
    seen.len()
}

fn store_scan(
    store: &Store,
    filters: Arc<[Filter]>,
    snapshot: u64,
    max_limit: u64,
    now: u64,
) -> Vec<String> {
    store_scan_gated(
        store,
        filters,
        nosfry_core::auth::ReadGate::default(),
        snapshot,
        max_limit,
        now,
    )
}

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

#[derive(Debug, Clone)]
enum TagSpec {
    E(usize), // reference into corpus ids
    P(usize), // reference into the key pool
    T(String),
    D(String),
    A(usize),        // `a` value of an earlier spec's kind/author/d
    AForeign(usize), // same, but with a foreign pubkey
    ABad(String),    // malformed `a` value
}

#[derive(Debug, Clone)]
struct EventSpec {
    key: usize,
    kind: u16,
    created_at: u64,
    tags: Vec<TagSpec>,
    content: String,
    expiration: Option<u64>,
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

/// The `a` value of spec `i` — its kind, an (optionally foreign)
/// author, and its first `d` value, matching `Event::address`.
fn a_value(i: usize, foreign: bool, specs: &[EventSpec]) -> String {
    let s = &specs[i % specs.len().max(1)];
    let key = if foreign { (s.key + 1) % 4 } else { s.key };
    let d = s
        .tags
        .iter()
        .find_map(|t| match t {
            TagSpec::D(d) => Some(d.as_str()),
            _ => None,
        })
        .unwrap_or("");
    format!("{}:{}:{d}", s.kind, pubkey_hex(key))
}

fn tag_json(tag: &TagSpec, ids: &[String], specs: &[EventSpec]) -> serde_json::Value {
    match tag {
        TagSpec::E(i) => json!(["e", ids[i % ids.len().max(1)]]),
        TagSpec::P(i) => json!(["p", pubkey_hex(i % 4)]),
        TagSpec::T(s) => json!(["t", s]),
        TagSpec::D(s) => json!(["d", s]),
        TagSpec::A(i) => json!(["a", a_value(*i, false, specs)]),
        TagSpec::AForeign(i) => json!(["a", a_value(*i, true, specs)]),
        TagSpec::ABad(s) => json!(["a", s]),
    }
}

fn build_event(spec: &EventSpec, ids: &[String], specs: &[EventSpec]) -> Arc<Event> {
    let mut tags: Vec<serde_json::Value> =
        spec.tags.iter().map(|t| tag_json(t, ids, specs)).collect();
    if let Some(exp) = spec.expiration {
        tags.push(json!(["expiration", exp.to_string()]));
    }
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
        2 => (0usize..40).prop_map(TagSpec::A),
        1 => (0usize..40).prop_map(TagSpec::AForeign),
        1 => prop_oneof![
            Just("garbage".to_owned()),
            Just("1:xyz:d".to_owned()),
            Just("abc::d".to_owned()),
            Just("70000:00:d".to_owned()),
            Just("1:".to_owned()),
        ].prop_map(TagSpec::ABad),
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
            2 => Just(5u16),
        ],
        0u64..30,
        proptest::collection::vec(tag_strategy(), 0..4),
        proptest::collection::vec(0u8..8, 1..6),
        prop_oneof![3 => Just(None), 1 => (100u64..400).prop_map(Some)],
    )
        .prop_map(
            |(key, kind, created_at, tags, content, expiration)| EventSpec {
                key,
                kind,
                created_at,
                tags,
                content: content.iter().map(|b| char::from(b'a' + b)).collect(),
                expiration,
            },
        )
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
        now in 0u64..600,
        now2 in 0u64..600,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();

        // Build events; ids depend on the tags so we need two passes.
        // The second pass's ids are what filters should reference.
        let placeholder = vec!["00".repeat(32)];
        let draft_ids: Vec<String> = specs
            .iter()
            .map(|s| build_event(s, &placeholder, &specs))
            .map(|e| format!("{}", e.id()))
            .collect();
        let draft: Vec<Arc<Event>> =
            specs.iter().map(|s| build_event(s, &draft_ids, &specs)).collect();
        let tagged_ids: Vec<String> =
            draft.iter().map(|e| format!("{}", e.id())).collect();
        let events: Vec<Arc<Event>> =
            specs.iter().map(|s| build_event(s, &tagged_ids, &specs)).collect();
        let ids: Vec<String> = events.iter().map(|e| format!("{}", e.id())).collect();

        // Write in random batch splits.
        let mut all: Vec<Arc<Event>> = events.clone();
        for chunk in &batches {
            let take = all.len().min(*chunk);
            let batch: Vec<Arc<Event>> = all.drain(..take).collect();
            let written = store.write(&batch).unwrap();
            prop_assert_eq!(written.outcomes.len(), batch.len());
            for outcome in &written.outcomes {
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
        let written = store.write(&all).unwrap();
        prop_assert_eq!(written.outcomes.len(), all.len());

        let mut stored = model_store(&events);

        let filters: Vec<Filter> = filter_specs
            .iter()
            .map(|s| serde_json::to_string(&filter_json(s, &ids)).unwrap())
            .map(|j| Filter::parse(&j).unwrap())
            .collect();
        let filters: Arc<[Filter]> = Arc::from(filters);

        let snapshot = store.snapshot().unwrap();
        let expected = model_scan(&stored.stored, &filters, snapshot, max_limit, now);
        let got = store_scan(&store, Arc::clone(&filters), snapshot, max_limit, now);
        prop_assert_eq!(got, expected);

        // After `expire`, a scan must still match the swept model.
        store.expire(now, usize::MAX).unwrap();
        model_expire(&mut stored, now);
        let snapshot2 = store.snapshot().unwrap();
        let expected2 = model_scan(&stored.stored, &filters, snapshot2, max_limit, now2);
        let got2 = store_scan(&store, filters, snapshot2, max_limit, now2);
        prop_assert_eq!(got2, expected2);

        // COUNT ignores `limit`: it must equal the deduplicated union
        // a limit-less Scan returns.
        let unlimited: Arc<[Filter]> = Arc::from(
            filter_specs
                .iter()
                .map(|s| {
                    let mut json = filter_json(s, &ids);
                    json.remove("limit");
                    Filter::parse(&serde_json::to_string(&json).unwrap()).unwrap()
                })
                .collect::<Vec<_>>(),
        );
        let union = model_union(
            &stored.stored,
            &unlimited,
            snapshot2,
            &nosfry_core::auth::ReadGate::default(),
            now2,
        );
        let counted = store_count(
            &store,
            Arc::clone(&unlimited),
            nosfry_core::auth::ReadGate::default(),
            snapshot2,
            0,
            now2,
        );
        prop_assert!(!counted.approximate);
        prop_assert_eq!(counted.count, union as u64);

        // A restricted-kind gate shrinks the count exactly as it
        // shrinks the scan union.
        let rules = nosfry_core::auth::AuthRules::new("wss://relay.example.com", &[3], true)
            .unwrap();
        let authed: Arc<[PublicKey]> = Arc::from(vec![PublicKey(
            keys()[0].x_only_public_key().0.to_byte_array(),
        )]);
        let gate = nosfry_core::auth::ReadGate::new(Arc::new(rules), authed);
        let union_gated = model_union(&stored.stored, &unlimited, snapshot2, &gate, now2);
        let counted_gated = store_count(&store, unlimited, gate, snapshot2, 0, now2);
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
    store.write(&events).unwrap();

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
