//! Live-subscription inverted index (strfry `ActiveMonitors`).
//!
//! Every non-impossible filter is indexed by exactly one field, in priority
//! order `ids` > `authors` > the tag key with the fewest values > `kinds` >
//! a match-all list. `matches` probes the event's id, pubkey, packed tags
//! and kind plus the match-all list, then fully checks each candidate with
//! [`Filter::matches`]. Stamps dedup candidates and emitted subscriptions;
//! a reusable scratch buffer keeps `matches` allocation-free.

use std::collections::HashMap;
use std::sync::Arc;

use crate::filter::Filter;
use crate::message::SubId;
use crate::packed::PackedRef;
use crate::session::SubKey;

type Map<K, V> = HashMap<K, V, foldhash::fast::RandomState>;

fn map<K, V>() -> Map<K, V> {
    Map::with_hasher(foldhash::fast::RandomState::default())
}

/// Which index list a [`FilterSlot`] lives in.
#[derive(Debug, Clone)]
enum SlotKey {
    Id([u8; 32]),
    Author([u8; 32]),
    Tag(u8, Box<[u8]>),
    Kind(u16),
    All,
}

/// One index registration of one filter of one subscription.
#[derive(Debug)]
struct FilterSlot {
    owner: usize,
    filter_index: usize,
    stamp: u64,
    key: SlotKey,
}

/// A live subscription entry.
#[derive(Debug)]
struct Entry<C> {
    conn: C,
    key: SubKey,
    filters: Arc<[Filter]>,
    emit_stamp: u64,
    slots: Vec<usize>,
}

/// The live-subscription index over connection ids `C`.
///
/// `C` is the adapter's connection handle (e.g. a connection counter or
/// slab key); the monitor never inspects it.
#[derive(Debug)]
pub struct Monitors<C: Copy + Eq + std::hash::Hash> {
    slots: slab::Slab<FilterSlot>,
    entries: slab::Slab<Entry<C>>,
    by_conn: Map<C, Map<SubId, usize>>,
    idx_id: Map<[u8; 32], Vec<usize>>,
    idx_author: Map<[u8; 32], Vec<usize>>,
    idx_tag: Map<u8, Map<Box<[u8]>, Vec<usize>>>,
    idx_kind: Map<u16, Vec<usize>>,
    idx_all: Vec<usize>,
    scratch: Vec<usize>,
    epoch: u64,
}

impl<C: Copy + Eq + std::hash::Hash> Default for Monitors<C> {
    fn default() -> Self {
        Self {
            slots: slab::Slab::new(),
            entries: slab::Slab::new(),
            by_conn: map(),
            idx_id: map(),
            idx_author: map(),
            idx_kind: map(),
            idx_tag: map(),
            idx_all: Vec::new(),
            scratch: Vec::new(),
            epoch: 0,
        }
    }
}

impl<C: Copy + Eq + std::hash::Hash> Monitors<C> {
    /// New empty index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of live subscriptions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no subscriptions are live.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Register `sub` for `conn` with its compiled filters; replaces any
    /// existing subscription under the same `(conn, sub.id)`.
    pub fn insert(&mut self, conn: C, sub: SubKey, filters: Arc<[Filter]>) {
        self.remove(conn, &sub.id);
        let sub_id = sub.id.clone();
        let entry = self.entries.insert(Entry {
            conn,
            key: sub,
            filters,
            emit_stamp: 0,
            slots: Vec::new(),
        });
        self.by_conn.entry(conn).or_default().insert(sub_id, entry);
        let Some(entry_ref) = self.entries.get(entry) else {
            return;
        };
        let registered = Arc::clone(&entry_ref.filters);
        let mut new_slots = Vec::new();
        for (filter_index, filter) in registered.iter().enumerate() {
            if filter.is_impossible() {
                continue;
            }
            for key in index_keys(filter) {
                let slot = self.slots.insert(FilterSlot {
                    owner: entry,
                    filter_index,
                    stamp: 0,
                    key: key.clone(),
                });
                self.add_to_index(key, slot);
                new_slots.push(slot);
            }
        }
        if let Some(e) = self.entries.get_mut(entry) {
            e.slots = new_slots;
        }
    }

    /// Remove `conn`'s subscription `sub_id`, if present.
    pub fn remove(&mut self, conn: C, sub_id: &SubId) {
        let Some(entry) = self
            .by_conn
            .get_mut(&conn)
            .and_then(|subs| subs.remove(sub_id))
        else {
            return;
        };
        self.remove_entry(entry);
    }

    /// Drop every subscription of `conn`.
    pub fn remove_connection(&mut self, conn: C) {
        let Some(subs) = self.by_conn.remove(&conn) else {
            return;
        };
        for entry in subs.into_values() {
            self.remove_entry(entry);
        }
    }

    /// Append each `(conn, sub)` whose filters match `ev`, at most once each.
    pub fn matches(&mut self, ev: PackedRef<'_>, out: &mut Vec<(C, SubKey)>) {
        self.epoch += 1;
        self.scratch.clear();
        if let Some(list) = self.idx_id.get(&ev.id().0) {
            self.scratch.extend_from_slice(list);
        }
        if let Some(list) = self.idx_author.get(&ev.pubkey().0) {
            self.scratch.extend_from_slice(list);
        }
        for (name, value) in ev.tags() {
            if let Some(list) = self.idx_tag.get(&name).and_then(|values| values.get(value)) {
                self.scratch.extend_from_slice(list);
            }
        }
        if let Some(list) = self.idx_kind.get(&ev.kind().0) {
            self.scratch.extend_from_slice(list);
        }
        self.scratch.extend_from_slice(&self.idx_all);
        let mut i = 0;
        while let Some(&slot) = self.scratch.get(i) {
            self.probe(slot, ev, out);
            i += 1;
        }
    }

    /// Evaluate one index candidate; emit at most once per subscription.
    fn probe(&mut self, slot: usize, ev: PackedRef<'_>, out: &mut Vec<(C, SubKey)>) {
        let Some(slot_ref) = self.slots.get_mut(slot) else {
            return;
        };
        if slot_ref.stamp == self.epoch {
            return;
        }
        slot_ref.stamp = self.epoch;
        let (owner, filter_index) = (slot_ref.owner, slot_ref.filter_index);
        let Some(entry) = self.entries.get_mut(owner) else {
            return;
        };
        if entry.emit_stamp == self.epoch {
            return;
        }
        let matched = entry
            .filters
            .get(filter_index)
            .is_some_and(|filter| filter.matches(ev));
        if matched {
            entry.emit_stamp = self.epoch;
            out.push((entry.conn, entry.key.clone()));
        }
    }

    fn remove_entry(&mut self, entry: usize) {
        let Some(entry) = self.entries.try_remove(entry) else {
            return;
        };
        for slot in entry.slots {
            if let Some(removed) = self.slots.try_remove(slot) {
                self.remove_from_index(slot, removed.key);
            }
        }
    }

    fn add_to_index(&mut self, key: SlotKey, slot: usize) {
        match key {
            SlotKey::Id(id) => self.idx_id.entry(id).or_default().push(slot),
            SlotKey::Author(pk) => self.idx_author.entry(pk).or_default().push(slot),
            SlotKey::Kind(kind) => self.idx_kind.entry(kind).or_default().push(slot),
            SlotKey::Tag(name, value) => self
                .idx_tag
                .entry(name)
                .or_default()
                .entry(value)
                .or_default()
                .push(slot),
            SlotKey::All => self.idx_all.push(slot),
        }
    }

    fn remove_from_index(&mut self, slot: usize, key: SlotKey) {
        match key {
            SlotKey::Id(id) => remove_from(&mut self.idx_id, &id, slot),
            SlotKey::Author(pk) => remove_from(&mut self.idx_author, &pk, slot),
            SlotKey::Kind(kind) => remove_from(&mut self.idx_kind, &kind, slot),
            SlotKey::Tag(name, value) => {
                let drop_name = self.idx_tag.get_mut(&name).is_some_and(|values| {
                    remove_from(values, &value, slot);
                    values.is_empty()
                });
                if drop_name {
                    self.idx_tag.remove(&name);
                }
            }
            SlotKey::All => {
                if let Some(pos) = self.idx_all.iter().position(|&s| s == slot) {
                    self.idx_all.swap_remove(pos);
                }
            }
        }
    }
}

/// The index key set of one filter, following the field priority.
fn index_keys(filter: &Filter) -> Vec<SlotKey> {
    if let Some(ids) = filter.ids()
        && !ids.is_empty()
    {
        return ids.iter().map(|id| SlotKey::Id(*id)).collect();
    }
    if let Some(authors) = filter.authors()
        && !authors.is_empty()
    {
        return authors.iter().map(|pk| SlotKey::Author(*pk)).collect();
    }
    if let Some((name, values)) = filter.tags().iter().min_by_key(|(_, values)| values.len()) {
        return values
            .iter()
            .map(|value| SlotKey::Tag(*name, value.as_bytes().into()))
            .collect();
    }
    if let Some(kinds) = filter.kinds()
        && !kinds.is_empty()
    {
        return kinds.iter().map(|kind| SlotKey::Kind(*kind)).collect();
    }
    vec![SlotKey::All]
}

fn remove_from<K: Eq + std::hash::Hash>(index: &mut Map<K, Vec<usize>>, key: &K, slot: usize) {
    if let Some(list) = index.get_mut(key) {
        list.retain(|&s| s != slot);
        if list.is_empty() {
            index.remove(key);
        }
    }
}
