//! Property tests: `Filter::matches` vs a naive owned-field matcher, packed
//! record round trips, and `Monitors::matches` vs brute force.

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

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;

use nosfry_core::event::{EventId, PublicKey};
use nosfry_core::filter::Filter;
use nosfry_core::kind::Kind;
use nosfry_core::message::SubId;
use nosfry_core::monitor::Monitors;
use nosfry_core::packed::{PackedEvent, PackedRef, encode_tag_value};
use nosfry_core::session::SubKey;
use proptest::prelude::*;

fn hex64(bytes: &[u8; 32]) -> String {
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        write!(out, "{b:02x}").expect("write");
        out
    })
}

fn arb_tag_name() -> impl Strategy<Value = char> {
    prop::char::range('a', 'z')
}

/// `(name, first value)` — the only part of a tag the index can see.
fn arb_event_tag() -> impl Strategy<Value = (char, String)> {
    prop_oneof![
        // e/p tags with valid lowercase hex.
        (prop::sample::select(vec!['e', 'p']), any::<[u8; 32]>())
            .prop_map(|(name, bytes)| (name, hex64(&bytes))),
        // e/p tags with uppercase hex — unencodable, must be skipped.
        (prop::sample::select(vec!['e', 'p']), any::<[u8; 32]>())
            .prop_map(|(name, bytes)| (name, hex64(&bytes).to_ascii_uppercase())),
        // ordinary tags, short utf-8 values.
        (arb_tag_name(), "[a-z0-9]{0,24}").prop_map(|(name, v)| (name, v)),
        // ordinary tags too long to pack.
        (arb_tag_name(), "[a-z]{256,300}").prop_map(|(name, v)| (name, v)),
    ]
}

fn arb_event() -> impl Strategy<Value = (EventId, PublicKey, u64, u16, Vec<Vec<String>>)> {
    (
        any::<[u8; 32]>(),
        any::<[u8; 32]>(),
        any::<u64>(),
        any::<u16>(),
        prop::collection::vec(arb_event_tag(), 0..6),
    )
        .prop_map(|(id, pk, at, kind, tags)| {
            let tags = tags
                .into_iter()
                .map(|(name, value)| vec![name.to_string(), value])
                .collect();
            (EventId(id), PublicKey(pk), at, kind, tags)
        })
}

fn arb_filter_json() -> impl Strategy<Value = serde_json::Value> {
    (
        prop::option::of(prop::collection::vec(any::<[u8; 32]>(), 0..4)),
        prop::option::of(prop::collection::vec(any::<[u8; 32]>(), 0..4)),
        prop::option::of(prop::collection::vec(any::<u16>(), 0..4)),
        prop::option::of(any::<u64>().prop_map(|v| v % 9_007_199_254_740_992)),
        prop::option::of(any::<u64>().prop_map(|v| v % 9_007_199_254_740_992)),
        prop::collection::vec(
            (
                prop::sample::select(vec!['e', 'p']),
                prop::collection::vec(any::<[u8; 32]>(), 0..3),
            ),
            0..3,
        ),
        prop::collection::vec(
            (
                prop::sample::select(vec!['t', 'x']),
                prop::collection::vec("[a-z0-9]{0,24}", 0..3),
            ),
            0..3,
        ),
    )
        .prop_map(|(ids, authors, kinds, since, until, hex_tags, text_tags)| {
            let mut obj = serde_json::Map::new();
            if let Some(ids) = ids {
                obj.insert(
                    "ids".into(),
                    ids.iter()
                        .map(|b| serde_json::Value::from(hex64(b)))
                        .collect::<Vec<_>>()
                        .into(),
                );
            }
            if let Some(authors) = authors {
                obj.insert(
                    "authors".into(),
                    authors
                        .iter()
                        .map(|b| serde_json::Value::from(hex64(b)))
                        .collect::<Vec<_>>()
                        .into(),
                );
            }
            if let Some(kinds) = kinds {
                obj.insert(
                    "kinds".into(),
                    kinds
                        .iter()
                        .map(|k| serde_json::Value::from(*k))
                        .collect::<Vec<_>>()
                        .into(),
                );
            }
            if let Some(since) = since {
                obj.insert("since".into(), since.into());
            }
            if let Some(until) = until {
                obj.insert("until".into(), until.into());
            }
            for (name, values) in hex_tags {
                obj.insert(
                    format!("#{name}"),
                    values
                        .iter()
                        .map(|b| serde_json::Value::from(hex64(b)))
                        .collect::<Vec<_>>()
                        .into(),
                );
            }
            for (name, values) in text_tags {
                obj.insert(
                    format!("#{name}"),
                    values
                        .iter()
                        .map(|v| serde_json::Value::from(v.clone()))
                        .collect::<Vec<_>>()
                        .into(),
                );
            }
            serde_json::Value::Object(obj)
        })
}

/// Naive matcher over the raw generated values (the oracle).
fn naive_match(
    filter: &serde_json::Value,
    id: &EventId,
    pubkey: &PublicKey,
    created_at: u64,
    kind: u16,
    tags: &[Vec<String>],
) -> bool {
    let obj = filter.as_object().expect("filter object");
    let id_hex = id.to_string();
    let pk_hex = pubkey.to_string();
    if let Some(ids) = obj.get("ids") {
        let ids = ids.as_array().expect("ids array");
        if ids.is_empty()
            || !ids
                .iter()
                .any(|v| v.as_str().expect("id string").eq_ignore_ascii_case(&id_hex))
        {
            return false;
        }
    }
    if let Some(authors) = obj.get("authors") {
        let authors = authors.as_array().expect("authors array");
        if authors.is_empty()
            || !authors.iter().any(|v| {
                v.as_str()
                    .expect("author string")
                    .eq_ignore_ascii_case(&pk_hex)
            })
        {
            return false;
        }
    }
    if let Some(kinds) = obj.get("kinds") {
        let kinds = kinds.as_array().expect("kinds array");
        if kinds.is_empty()
            || !kinds
                .iter()
                .any(|v| v.as_u64().expect("kind") == u64::from(kind))
        {
            return false;
        }
    }
    if let Some(since) = obj.get("since")
        && created_at < since.as_u64().expect("since")
    {
        return false;
    }
    if let Some(until) = obj.get("until")
        && created_at > until.as_u64().expect("until")
    {
        return false;
    }
    for (key, values) in obj {
        let Some(name) = key.strip_prefix('#') else {
            continue;
        };
        let mut chars = name.chars();
        let Some(letter) = chars.next() else {
            continue;
        };
        if chars.next().is_some() || !letter.is_ascii_alphabetic() {
            continue; // non-letter names are dropped by the wire parser
        }
        let values = values.as_array().expect("tag values");
        if values.is_empty() {
            return false; // empty tag lists make the filter impossible
        }
        // A condition value can only match when it packs; `e`/`p` compare
        // as hex, everything else as raw bytes.
        let packed_values: Vec<Vec<u8>> = values
            .iter()
            .filter_map(|v| {
                let v = v.as_str().expect("tag value string");
                let normalized = if letter == 'e' || letter == 'p' {
                    v.to_ascii_lowercase()
                } else {
                    v.to_owned()
                };
                encode_tag_value(letter as u8, &normalized).map(|b| b.as_bytes().to_vec())
            })
            .collect();
        if packed_values.is_empty() {
            return false; // condition can never match -> impossible filter
        }
        let hit = tags.iter().any(|tag| {
            tag.first().is_some_and(|n| n.as_str() == name)
                && tag.get(1).is_some_and(|v| {
                    encode_tag_value(letter as u8, v)
                        .is_some_and(|ev| packed_values.iter().any(|pv| pv == ev.as_bytes()))
                })
        });
        if !hit {
            return false;
        }
    }
    true
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn filter_matches_vs_naive(
        (id, pubkey, created_at, kind, tags) in arb_event(),
        filter_json in arb_filter_json(),
    ) {
        let filter = Filter::parse(&serde_json::to_string(&filter_json).unwrap())
            .expect("generated filters are always valid");
        let packed = PackedEvent::build(&id, &pubkey, created_at, Kind(kind), &tags);
        let expected = naive_match(&filter_json, &id, &pubkey, created_at, kind, &tags);
        prop_assert_eq!(filter.matches(packed.packed()), expected);
    }

    #[test]
    fn packed_round_trip(
        (id, pubkey, created_at, kind, tags) in arb_event(),
    ) {
        let packed = PackedEvent::build(&id, &pubkey, created_at, Kind(kind), &tags);
        let view = packed.packed();
        prop_assert_eq!(view.id(), id);
        prop_assert_eq!(view.pubkey(), pubkey);
        prop_assert_eq!(view.created_at(), created_at);
        prop_assert_eq!(view.kind(), Kind(kind));
        // Expected packed tags: first value of every single-letter tag that
        // encodes within 255 bytes.
        let expected: Vec<(u8, Vec<u8>)> = tags
            .iter()
            .filter_map(|tag| {
                let name = tag.first()?;
                let value = tag.get(1)?;
                let mut chars = name.chars();
                let letter = chars.next()?;
                if chars.next().is_some() || !letter.is_ascii_alphabetic() {
                    return None;
                }
                encode_tag_value(letter as u8, value)
                    .map(|b| (letter as u8, b.as_bytes().to_vec()))
            })
            .collect();
        let actual: Vec<(u8, Vec<u8>)> = view
            .tags()
            .map(|(name, value)| (name, value.to_vec()))
            .collect();
        prop_assert_eq!(actual, expected);
        // The record validates under PackedRef::new.
        prop_assert!(PackedRef::new(view.as_bytes()).is_some());
    }
}

// --- Monitors vs brute force -------------------------------------------------

type Model = HashMap<(u8, String), (SubKey, Arc<[Filter]>)>;

fn sub_key(text: &str, generation: u64) -> SubKey {
    SubKey {
        id: SubId::parse(text).expect("sub id"),
        generation,
    }
}

fn arb_filter() -> impl Strategy<Value = Filter> {
    prop_oneof![
        3 => Just(serde_json::json!({})),
        2 => any::<u16>().prop_map(|k| serde_json::json!({"kinds": [k]})),
        2 => any::<[u8; 32]>()
            .prop_map(|b| serde_json::json!({"authors": [hex64(&b)]})),
        1 => any::<[u8; 32]>().prop_map(|b| serde_json::json!({"ids": [hex64(&b)]})),
        1 => "[a-z]{1,4}".prop_map(|v| serde_json::json!({"#t": [v]})),
        1 => Just(serde_json::json!({"ids": []})),
    ]
    .prop_map(|v| Filter::parse(&serde_json::to_string(&v).unwrap()).expect("filter"))
}

#[derive(Debug)]
enum Op {
    Insert(u8, String, u64, Vec<Filter>),
    Remove(u8, String),
    RemoveConn(u8),
    Check,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (
            0u8..=2u8,
            prop::sample::select(vec!['a', 'b', 'c']).prop_map(|c| c.to_string()),
            any::<u64>(),
            prop::collection::vec(arb_filter(), 1..3),
        )
            .prop_map(|(conn, sub, generation, filters)| Op::Insert(conn, sub, generation, filters)),
        2 => (
            0u8..=2u8,
            prop::sample::select(vec!['a', 'b', 'c']).prop_map(|c| c.to_string()),
        )
            .prop_map(|(conn, sub)| Op::Remove(conn, sub)),
        1 => (0u8..=2u8).prop_map(Op::RemoveConn),
        3 => Just(()).prop_map(|()| Op::Check),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(80))]

    #[test]
    fn monitors_vs_brute_force(
        ops in prop::collection::vec(arb_op(), 0..40),
        (id, pubkey, created_at, kind, tags) in arb_event(),
    ) {
        let mut monitors = Monitors::<u8>::new();
        let mut model: Model = HashMap::new();
        for op in ops {
            match op {
                Op::Insert(conn, sub, generation, filters) => {
                    let key = sub_key(&sub, generation);
                    let filters: Arc<[Filter]> = filters.into();
                    monitors.insert(conn, key.clone(), Arc::clone(&filters));
                    model.insert((conn, sub), (key, filters));
                }
                Op::Remove(conn, sub) => {
                    monitors.remove(conn, &SubId::parse(&sub).expect("sub id"));
                    model.remove(&(conn, sub));
                }
                Op::RemoveConn(conn) => {
                    monitors.remove_connection(conn);
                    model.retain(|(c, _), _| *c != conn);
                }
                Op::Check => break,
            }
        }
        prop_assert_eq!(monitors.len(), model.len());
        let packed = PackedEvent::build(&id, &pubkey, created_at, Kind(kind), &tags);
        let mut out = Vec::new();
        monitors.matches(packed.packed(), &mut out);
        let got: HashSet<(u8, String, u64)> = out
            .iter()
            .map(|(conn, key)| (*conn, key.id.as_str().to_owned(), key.generation))
            .collect();
        let want: HashSet<(u8, String, u64)> = model
            .iter()
            .filter(|(_, (_, filters))| {
                filters.iter().any(|f| f.matches(packed.packed()))
            })
            .map(|((conn, sub), (key, _))| (*conn, sub.clone(), key.generation))
            .collect();
        prop_assert_eq!(got, want);
    }
}
