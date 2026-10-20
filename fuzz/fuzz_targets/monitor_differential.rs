#![no_main]

//! Differential test: a `Monitors` index against a naive model that
//! evaluates every subscription's filters on each event.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;
use nosfry_core::auth::ReadGate;
use nosfry_core::event::{EventId, PublicKey};
use nosfry_core::filter::Filter;
use nosfry_core::kind::Kind;
use nosfry_core::message::SubId;
use nosfry_core::monitor::Monitors;
use nosfry_core::packed::{PackedEvent, PackedRef};
use nosfry_core::session::SubKey;
use serde_json::{Value, json};

const NUM_SUBS: u8 = 8;
const POOL: usize = 4;
const T_POOL: [&str; POOL] = ["red", "green", "blue", "white"];

static SUB_IDS: LazyLock<Vec<SubId>> = LazyLock::new(|| {
    (0..NUM_SUBS)
        .map(|i| SubId::parse(&format!("s{i}")).unwrap())
        .collect()
});

fn hex32(byte: u8) -> String {
    faster_hex::hex_string(&[byte; 32])
}

#[derive(Arbitrary, Debug)]
struct Input {
    ops: Vec<Op>,
}

#[derive(Arbitrary, Debug)]
enum Op {
    Insert { sub: u8, filters: Vec<FilterSpec> },
    Remove { sub: u8 },
    Match(EventSpec),
}

#[derive(Arbitrary, Debug)]
struct FilterSpec {
    ids: Option<u8>,
    authors: Option<u8>,
    kinds: Vec<u8>,
    tag_name: Option<u8>,
    tag_value: u8,
    since: Option<u8>,
    until: Option<u8>,
    limit: Option<u8>,
}

impl FilterSpec {
    fn to_json(&self) -> Value {
        let mut filter = json!({});
        if let Some(i) = self.ids {
            filter["ids"] = json!([hex32(0x10 + i % POOL as u8)]);
        }
        if let Some(i) = self.authors {
            filter["authors"] = json!([hex32(0x20 + i % POOL as u8)]);
        }
        if !self.kinds.is_empty() {
            let mut kinds: Vec<u16> = self.kinds.iter().map(|k| u16::from(k % 8)).collect();
            kinds.sort_unstable();
            kinds.dedup();
            filter["kinds"] = json!(kinds);
        }
        if let Some(name) = self.tag_name {
            let (key, value) = match name % 3 {
                0 => ("#e", hex32(0x30 + self.tag_value % POOL as u8)),
                1 => ("#p", hex32(0x40 + self.tag_value % POOL as u8)),
                _ => (
                    "#t",
                    T_POOL[usize::from(self.tag_value % POOL as u8)].to_owned(),
                ),
            };
            filter[key] = json!([value]);
        }
        if let Some(since) = self.since {
            filter["since"] = json!(u64::from(since % 16));
        }
        if let Some(until) = self.until {
            filter["until"] = json!(u64::from(until % 16));
        }
        if let Some(limit) = self.limit {
            filter["limit"] = json!(u64::from(limit % 16));
        }
        filter
    }
}

#[derive(Arbitrary, Debug)]
struct EventSpec {
    id: u8,
    pubkey: u8,
    created_at: u8,
    kind: u8,
    e_tag: Option<u8>,
    p_tag: Option<u8>,
    t_tag: Option<u8>,
}

impl EventSpec {
    fn packed(&self) -> PackedEvent {
        let mut tags: Vec<Vec<String>> = Vec::new();
        if let Some(v) = self.e_tag {
            tags.push(vec!["e".to_owned(), hex32(0x30 + v % POOL as u8)]);
        }
        if let Some(v) = self.p_tag {
            tags.push(vec!["p".to_owned(), hex32(0x40 + v % POOL as u8)]);
        }
        if let Some(v) = self.t_tag {
            tags.push(vec![
                "t".to_owned(),
                T_POOL[usize::from(v % POOL as u8)].to_owned(),
            ]);
        }
        PackedEvent::build(
            &EventId([0x10 + self.id % POOL as u8; 32]),
            &PublicKey([0x20 + self.pubkey % POOL as u8; 32]),
            u64::from(self.created_at % 16),
            Kind(u16::from(self.kind % 8)),
            0,
            tags,
        )
    }
}

fn expected(model: &HashMap<u8, (u64, Arc<[Filter]>)>, ev: PackedRef<'_>) -> Vec<u8> {
    let mut subs: Vec<u8> = model
        .iter()
        .filter(|(_, (_, filters))| filters.iter().any(|f| !f.is_impossible() && f.matches(ev)))
        .map(|(sub, _)| *sub)
        .collect();
    subs.sort_unstable();
    subs
}

fuzz_target!(|input: Input| {
    let mut monitors = Monitors::<u8>::new();
    let mut model: HashMap<u8, (u64, Arc<[Filter]>)> = HashMap::new();
    let mut generation = 0;
    let mut out = Vec::new();
    for op in input.ops.iter().take(64) {
        match op {
            Op::Insert { sub, filters } => {
                let sub = sub % NUM_SUBS;
                if filters.is_empty() {
                    continue;
                }
                let parsed: Arc<[Filter]> = filters
                    .iter()
                    .take(3)
                    .filter_map(|f| Filter::parse(&f.to_json().to_string()).ok())
                    .collect();
                generation += 1;
                let key = SubKey {
                    id: SUB_IDS[usize::from(sub)].clone(),
                    generation,
                };
                monitors.insert(sub, key, Arc::clone(&parsed), ReadGate::default());
                model.insert(sub, (generation, parsed));
            }
            Op::Remove { sub } => {
                let sub = sub % NUM_SUBS;
                monitors.remove(sub, &SUB_IDS[usize::from(sub)]);
                model.remove(&sub);
            }
            Op::Match(spec) => {
                let event = spec.packed();
                out.clear();
                monitors.matches(event.packed(), &mut out);
                let mut actual: Vec<u8> = out.iter().map(|(conn, _)| *conn).collect();
                let total = actual.len();
                actual.sort_unstable();
                actual.dedup();
                assert_eq!(total, actual.len(), "monitor emitted a duplicate");
                assert_eq!(actual, expected(&model, event.packed()));
                for (conn, key) in &out {
                    assert_eq!(key.id, SUB_IDS[usize::from(*conn)]);
                    assert_eq!(key.generation, model[conn].0);
                }
            }
        }
    }
});
