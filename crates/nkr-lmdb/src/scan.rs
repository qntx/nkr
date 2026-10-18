//! Time-sliced, snapshot-bounded stored-event scan.

use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nkr::event::{EventId, cmp_newest_first};
use nkr::filter::Filter;
use nkr::packed::PackedRef;

use crate::{Index, Reader, Store, StoreError};

/// Check the wall-clock budget after this many candidates.
const BUDGET_CHECK_EVERY: usize = 64;

/// A `(created_at, id, seq)` match collected for one filter.
type Match = (u64, EventId, u64);

/// A live reverse-range iterator, borrowing the step's read txn.
type RevIter<'a> = Box<dyn Iterator<Item = Result<(&'a [u8], &'a [u8]), heed::Error>> + 'a>;

/// Whether a [`Scan::step`] has work left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// Budget ran out; call [`Scan::step`] again.
    Pending,
    /// Every filter finished.
    Done,
}

/// One descending range cursor: `lo` inclusive, `hi` starts inclusive on
/// `prefix ‖ until ‖ u64::MAX` and becomes exclusive on the last pulled
/// key. The bounds seed one live iterator per [`Scan::step`].
#[derive(Debug)]
struct Cursor {
    lo: Vec<u8>,
    hi: Vec<u8>,
    hi_inclusive: bool,
    exhausted: bool,
}

impl Cursor {
    fn new(prefix: &[u8], since: u64, until: u64) -> Self {
        let mut lo = Vec::with_capacity(prefix.len() + 16);
        lo.extend_from_slice(prefix);
        lo.extend_from_slice(&since.to_be_bytes());
        lo.extend_from_slice(&0u64.to_be_bytes());
        let mut hi = Vec::with_capacity(prefix.len() + 16);
        hi.extend_from_slice(prefix);
        hi.extend_from_slice(&until.to_be_bytes());
        hi.extend_from_slice(&u64::MAX.to_be_bytes());
        Self {
            lo,
            hi,
            hi_inclusive: true,
            exhausted: false,
        }
    }

    /// The bounds for the next probe of this cursor.
    const fn bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        (
            Bound::Included(self.lo.as_slice()),
            if self.hi_inclusive {
                Bound::Included(self.hi.as_slice())
            } else {
                Bound::Excluded(self.hi.as_slice())
            },
        )
    }
}

/// Pull the newest key a live iterator still holds for `cursor`; the
/// pulled key becomes the cursor's exclusive upper bound.
fn pull(
    iter: &mut Option<RevIter<'_>>,
    cursor: &mut Cursor,
) -> Result<Option<Vec<u8>>, StoreError> {
    let Some(it) = iter else {
        return Ok(None);
    };
    if let Some((key, _)) = it.next().transpose()? {
        cursor.hi.clear();
        cursor.hi.extend_from_slice(key);
        cursor.hi_inclusive = false;
        Ok(Some(key.to_vec()))
    } else {
        cursor.exhausted = true;
        *iter = None;
        Ok(None)
    }
}

/// Heap head of a live cursor: newest `(created_at, seq)` first.
#[derive(Debug, PartialEq, Eq)]
struct Head {
    created_at: u64,
    seq: u64,
    key: Vec<u8>,
    cursor: usize,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        (self.created_at, self.seq).cmp(&(other.created_at, other.seq))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-filter scan state.
#[derive(Debug)]
struct Plan {
    /// `None` for finished or impossible filters.
    mode: Option<Mode>,
    /// `(created_at, id, seq)` matches collected so far (`Ids` plans
    /// only; they are bounded by the id count).
    collected: Vec<Match>,
    /// Matches sharing the merge's current `created_at`; flushed when a
    /// lower `created_at` pops or the merge is exhausted.
    group: Vec<Match>,
    /// Entries counted toward `limit` by flushed groups.
    taken: u64,
    /// The last seq considered; duplicates of one event (several tag
    /// values reaching it) share the `(created_at, seq)` suffix and
    /// therefore pop consecutively.
    last_seq: Option<u64>,
    limit: u64,
    finished: bool,
}

#[derive(Debug)]
enum Mode {
    /// `ix_id` point lookups.
    Ids { pending: VecDeque<[u8; 32]> },
    /// A k-way merge over descending range cursors on one index.
    Ranges { db: Index, cursors: Vec<Cursor> },
}

/// Which index a filter scans, one per filter, in strfry order: `ids`,
/// `authors`+`kinds`, `authors`, smallest tag set, `kinds`, `created_at`.
fn plan(filter: &Filter) -> Mode {
    let since = filter.since().unwrap_or(0);
    let until = filter.until().unwrap_or(u64::MAX);
    if let Some(ids) = filter.ids() {
        return Mode::Ids {
            pending: ids.iter().copied().collect(),
        };
    }
    let tag = filter.tags().iter().min_by_key(|(_, values)| values.len());
    match (filter.authors(), filter.kinds(), tag) {
        (Some(authors), Some(kinds), _)
            if (authors.len() as u64).saturating_mul(kinds.len() as u64) <= 256 =>
        {
            Mode::Ranges {
                db: Index::PubkeyKind,
                cursors: authors
                    .iter()
                    .flat_map(|pk| {
                        kinds.iter().map(|kind| {
                            let mut p = Vec::with_capacity(34);
                            p.extend_from_slice(pk);
                            p.extend_from_slice(&kind.to_be_bytes());
                            Cursor::new(&p, since, until)
                        })
                    })
                    .collect(),
            }
        }
        (Some(authors), _, _) => Mode::Ranges {
            db: Index::Pubkey,
            cursors: authors
                .iter()
                .map(|pk| Cursor::new(pk, since, until))
                .collect(),
        },
        (None, _, Some((name, values))) => Mode::Ranges {
            db: Index::Tag,
            cursors: values
                .iter()
                .map(|value| {
                    let mut p = Vec::with_capacity(2 + value.as_bytes().len());
                    p.push(*name);
                    p.push(u8::try_from(value.as_bytes().len()).unwrap_or(u8::MAX));
                    p.extend_from_slice(value.as_bytes());
                    Cursor::new(&p, since, until)
                })
                .collect(),
        },
        (None, Some(kinds), _) => Mode::Ranges {
            db: Index::Kind,
            cursors: kinds
                .iter()
                .map(|kind| Cursor::new(&kind.to_be_bytes(), since, until))
                .collect(),
        },
        (None, None, _) => Mode::Ranges {
            db: Index::Time,
            cursors: vec![Cursor::new(&[], since, until)],
        },
    }
}

/// A snapshot-bounded stored-event scan. Each [`Scan::step`] opens its own
/// read transaction, processes candidates within `budget`, and drops the
/// transaction before returning.
#[derive(Debug)]
pub struct Scan {
    filters: Arc<[Filter]>,
    snapshot: u64,
    plans: Vec<Plan>,
    emitted: HashSet<u64>,
    done: bool,
}

impl Scan {
    /// New scan over `filters`, bounded to entries with `seq <= snapshot`
    /// and per-filter `Filter::effective_limit(max_filter_limit)`.
    #[must_use]
    pub fn new(filters: Arc<[Filter]>, snapshot: u64, max_filter_limit: u64) -> Self {
        let plans = filters
            .iter()
            .map(|filter| {
                let limit = filter.effective_limit(max_filter_limit);
                let finished = filter.is_impossible() || limit == 0;
                Plan {
                    mode: (!finished).then(|| plan(filter)),
                    collected: Vec::new(),
                    group: Vec::new(),
                    taken: 0,
                    last_seq: None,
                    limit,
                    finished,
                }
            })
            .collect();
        Self {
            filters,
            snapshot,
            plans,
            emitted: HashSet::new(),
            done: false,
        }
    }

    /// Process candidates for up to `budget` wall time. `emit` receives
    /// canonical event JSON in scan order (per filter: newest first).
    ///
    /// The budget is checked every 64 candidates, so `Duration::ZERO`
    /// still makes progress.
    ///
    /// # Errors
    ///
    /// [`StoreError::Heed`] on LMDB failure, [`StoreError::Corrupt`] on a
    /// broken index invariant.
    #[allow(
        clippy::excessive_nesting,
        reason = "the per-filter loop keeps cursor state readable"
    )]
    pub fn step(
        &mut self,
        store: &Store,
        budget: Duration,
        emit: &mut dyn FnMut(&str),
    ) -> Result<Progress, StoreError> {
        if self.done {
            return Ok(Progress::Done);
        }
        let start = Instant::now();
        let txn = store.read_txn()?;
        let mut processed = 0usize;
        let mut pending = false;
        {
            let Self {
                filters,
                plans,
                emitted,
                snapshot,
                done: _,
            } = self;
            for (filter, plan) in filters.iter().zip(plans.iter_mut()) {
                if plan.finished {
                    continue;
                }
                run_plan(
                    store,
                    &txn,
                    filter,
                    plan,
                    *snapshot,
                    emitted,
                    &mut processed,
                    start,
                    budget,
                    emit,
                )?;
                if !plan.finished {
                    pending = true;
                    break;
                }
            }
        }
        drop(txn);
        self.done = !pending && self.plans.iter().all(|plan| plan.finished);
        Ok(if self.done {
            Progress::Done
        } else {
            Progress::Pending
        })
    }
}

/// Drive one filter until it finishes or the budget fires.
#[allow(
    clippy::too_many_arguments,
    clippy::excessive_nesting,
    reason = "cursor merge state is clearest inline"
)]
fn run_plan(
    store: &Store,
    txn: &Reader<'_>,
    filter: &Filter,
    plan: &mut Plan,
    snapshot: u64,
    emitted: &mut HashSet<u64>,
    processed: &mut usize,
    start: Instant,
    budget: Duration,
    emit: &mut dyn FnMut(&str),
) -> Result<(), StoreError> {
    let collected = &mut plan.collected;
    let group = &mut plan.group;
    let taken = &mut plan.taken;
    let limit = plan.limit;
    let last_seq = &mut plan.last_seq;
    let Some(mode) = &mut plan.mode else {
        plan.finished = true;
        return Ok(());
    };
    match mode {
        Mode::Ids { pending } => {
            while let Some(id) = pending.pop_front() {
                *processed += 1;
                if let Some(bytes) = store.db(Index::Id).get(txn, &id)? {
                    try_candidate(
                        store,
                        txn,
                        filter,
                        collected,
                        last_seq,
                        snapshot,
                        decode_seq(bytes)?,
                    )?;
                }
                if budget_elapsed(*processed, start, budget) {
                    return Ok(());
                }
            }
            finalize(store, txn, collected, limit, emitted, emit)?;
            plan.finished = true;
        }
        Mode::Ranges { db, cursors } => {
            let db = store.db(*db);
            // One live reverse iterator per cursor, seeded at the saved
            // bounds; the bounds are only (re)derived when the step ends.
            let mut iters: Vec<Option<RevIter<'_>>> = Vec::with_capacity(cursors.len());
            for cursor in cursors.iter() {
                iters.push(if cursor.exhausted {
                    None
                } else {
                    Some(Box::new(db.rev_range(txn, &cursor.bounds())?))
                });
            }
            let mut heap: BinaryHeap<Head> = BinaryHeap::new();
            for (i, (iter, cursor)) in iters.iter_mut().zip(cursors.iter_mut()).enumerate() {
                if let Some(key) = pull(iter, cursor)? {
                    heap.push(Head {
                        created_at: key_created_at(&key),
                        seq: key_seq(&key),
                        key,
                        cursor: i,
                    });
                }
            }
            while let Some(head) = heap.pop() {
                *processed += 1;
                if let Some(cursor) = cursors.get_mut(head.cursor)
                    && let Some(iter) = iters.get_mut(head.cursor)
                    && let Some(next) = pull(iter, cursor)?
                {
                    heap.push(Head {
                        created_at: key_created_at(&next),
                        seq: key_seq(&next),
                        key: next,
                        cursor: head.cursor,
                    });
                }
                // A lower created_at completes the current group; keys
                // are popped in non-increasing order.
                if group.first().is_some_and(|m| head.created_at < m.0) {
                    flush_group(store, txn, group, taken, limit, emitted, emit)?;
                    if *taken >= limit {
                        plan.finished = true;
                        return Ok(());
                    }
                }
                try_candidate(store, txn, filter, group, last_seq, snapshot, head.seq)?;
                if budget_elapsed(*processed, start, budget) {
                    restore_heads(&mut heap, cursors);
                    return Ok(());
                }
            }
            flush_group(store, txn, group, taken, limit, emitted, emit)?;
            plan.finished = true;
        }
    }
    Ok(())
}

/// Emit one complete `created_at` group: id-ascending, at most
/// `limit - taken` entries, each counted toward `taken` whether or not
/// it is re-sent (limit-then-dedup across filters).
fn flush_group(
    store: &Store,
    txn: &Reader<'_>,
    group: &mut Vec<Match>,
    taken: &mut u64,
    limit: u64,
    emitted: &mut HashSet<u64>,
    emit: &mut dyn FnMut(&str),
) -> Result<(), StoreError> {
    group.sort_by_key(|m| m.1);
    let remaining = usize::try_from(limit.saturating_sub(*taken)).unwrap_or(usize::MAX);
    for (_, _, seq) in group.drain(..).take(remaining) {
        *taken += 1;
        if emitted.insert(seq) {
            emit_payload(store, txn, seq, emit)?;
        }
    }
    Ok(())
}

/// Fetch and emit one payload; an entry deleted between slices is
/// skipped but still counts toward the limit.
fn emit_payload(
    store: &Store,
    txn: &Reader<'_>,
    seq: u64,
    emit: &mut dyn FnMut(&str),
) -> Result<(), StoreError> {
    if let Some(bytes) = store.payloads.get(txn, &seq.to_be_bytes())? {
        emit(std::str::from_utf8(bytes).map_err(|_| StoreError::Corrupt("payload is not UTF-8"))?);
    }
    Ok(())
}

/// Heads still in the heap were pulled from their iterators (each became
/// an exclusive `hi` bound) but not consumed; restore them so the next
/// step sees them.
fn restore_heads(heap: &mut BinaryHeap<Head>, cursors: &mut [Cursor]) {
    for parked in heap.drain() {
        if let Some(cursor) = cursors.get_mut(parked.cursor) {
            cursor.hi = parked.key;
            cursor.hi_inclusive = true;
        }
    }
}

/// seq/snapshot/dup checks, packed load, `Filter::matches`, collect.
#[allow(clippy::too_many_arguments, reason = "every input is needed")]
fn try_candidate(
    store: &Store,
    txn: &Reader<'_>,
    filter: &Filter,
    collected: &mut Vec<Match>,
    last_seq: &mut Option<u64>,
    snapshot: u64,
    seq: u64,
) -> Result<(), StoreError> {
    if seq > snapshot || *last_seq == Some(seq) {
        return Ok(());
    }
    *last_seq = Some(seq);
    let Some(bytes) = store.events.get(txn, &seq.to_be_bytes())? else {
        return Ok(()); // deleted between slices
    };
    let packed = PackedRef::new(bytes).ok_or(StoreError::Corrupt("bad packed record"))?;
    if filter.matches(packed) {
        collected.push((packed.created_at(), packed.id(), seq));
    }
    Ok(())
}

/// Sort collected matches newest-first, truncate to the limit, emit the
/// payloads, and record the seqs so later filters skip them.
fn finalize(
    store: &Store,
    txn: &Reader<'_>,
    collected: &mut Vec<Match>,
    limit: u64,
    emitted: &mut HashSet<u64>,
    emit: &mut dyn FnMut(&str),
) -> Result<(), StoreError> {
    collected.sort_by(|a, b| cmp_newest_first((a.0, &a.1), (b.0, &b.1)));
    collected.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    for (_, _, seq) in collected.drain(..) {
        // An earlier filter may already have emitted this seq: it still
        // counts toward this filter's limit boundary, but is not sent
        // twice.
        if !emitted.insert(seq) {
            continue;
        }
        emit_payload(store, txn, seq, emit)?;
    }
    Ok(())
}

/// `created_at` = the 8 bytes right before the trailing seq.
fn key_created_at(key: &[u8]) -> u64 {
    let end = key.len().saturating_sub(8);
    let start = end.saturating_sub(8);
    key.get(start..end)
        .and_then(|s| <&[u8; 8]>::try_from(s).ok())
        .map_or(0, |b| u64::from_be_bytes(*b))
}

/// seq = the last 8 bytes of a range key.
fn key_seq(key: &[u8]) -> u64 {
    key.get(key.len().saturating_sub(8)..)
        .and_then(|s| <&[u8; 8]>::try_from(s).ok())
        .map_or(0, |b| u64::from_be_bytes(*b))
}

fn decode_seq(bytes: &[u8]) -> Result<u64, StoreError> {
    let arr: &[u8; 8] = bytes
        .try_into()
        .map_err(|_| StoreError::Corrupt("bad seq value"))?;
    Ok(u64::from_be_bytes(*arr))
}

fn budget_elapsed(processed: usize, start: Instant, budget: Duration) -> bool {
    processed.is_multiple_of(BUDGET_CHECK_EVERY) && start.elapsed() >= budget
}
