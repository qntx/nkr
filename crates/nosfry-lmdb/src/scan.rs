//! Time-sliced, snapshot-bounded stored-event scan.

use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nosfry_core::event::{EventId, cmp_newest_first};
use nosfry_core::filter::Filter;
use nosfry_core::packed::PackedRef;

use crate::{Index, Reader, Store, StoreError};

/// Check the wall-clock budget after this many candidates.
const BUDGET_CHECK_EVERY: usize = 64;

/// A `(created_at, id, seq)` match collected for one filter.
type Match = (u64, EventId, u64);

/// foldhash for the cross-filter dedup set (server-assigned seq keys).
type SeqSet = HashSet<u64, foldhash::fast::RandomState>;

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

/// Per-filter scan progress shared by both modes.
#[derive(Debug)]
struct PlanState {
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
}

/// Per-filter scan state: the mode machinery plus its progress.
#[derive(Debug)]
struct Plan {
    /// `None` for finished or impossible filters.
    mode: Option<Mode>,
    state: PlanState,
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
    /// NIP-40 cutoff: events with `expiration <= now` are hidden.
    now: u64,
    plans: Vec<Plan>,
    emitted: SeqSet,
    done: bool,
}

impl Scan {
    /// New scan over `filters`, bounded to entries with `seq <= snapshot`
    /// and per-filter `Filter::effective_limit(max_filter_limit)`.
    /// Events with an NIP-40 `expiration` at or before `now` are never
    /// emitted and never count toward limits; `now = 0` disables the
    /// check (expiration values are always `>= 100`).
    #[must_use]
    pub fn new(filters: Arc<[Filter]>, snapshot: u64, max_filter_limit: u64, now: u64) -> Self {
        let plans = filters
            .iter()
            .map(|filter| {
                let limit = filter.effective_limit(max_filter_limit);
                let finished = filter.is_impossible() || limit == 0;
                Plan {
                    mode: (!finished).then(|| plan(filter)),
                    state: PlanState {
                        collected: Vec::new(),
                        group: Vec::new(),
                        taken: 0,
                        last_seq: None,
                        limit,
                    },
                    finished,
                }
            })
            .collect();
        Self {
            filters,
            snapshot,
            now,
            plans,
            emitted: SeqSet::with_hasher(foldhash::fast::RandomState::default()),
            done: false,
        }
    }

    /// Process candidates for up to `budget` wall time. `emit` receives
    /// `(seq, canonical JSON)` in scan order (per filter: newest first).
    ///
    /// The budget is checked every 64 candidates, so `Duration::ZERO`
    /// still makes progress.
    ///
    /// # Errors
    ///
    /// [`StoreError::Heed`] on LMDB failure, [`StoreError::Corrupt`] on a
    /// broken index invariant.
    pub fn step(
        &mut self,
        store: &Store,
        budget: Duration,
        emit: &mut dyn FnMut(u64, &str),
    ) -> Result<Progress, StoreError> {
        if self.done {
            return Ok(Progress::Done);
        }
        let start = Instant::now();
        let txn = store.read_txn()?;
        let pending = {
            let mut step = Step {
                store,
                txn: &txn,
                snapshot: self.snapshot,
                now: self.now,
                emitted: &mut self.emitted,
                processed: 0,
                start,
                budget,
                emit,
            };
            let Self { filters, plans, .. } = self;
            run_filters(&mut step, filters, plans)?
        };
        drop(txn);
        self.done = !pending && self.plans.iter().all(|plan| plan.finished);
        Ok(if self.done {
            Progress::Done
        } else {
            Progress::Pending
        })
    }
}

/// Run one slice over every unfinished plan; `true` when a plan hit
/// the budget mid-filter (its step resumes next call).
fn run_filters(
    step: &mut Step<'_>,
    filters: &[Filter],
    plans: &mut [Plan],
) -> Result<bool, StoreError> {
    for (filter, plan) in filters.iter().zip(plans.iter_mut()) {
        if plan.finished {
            continue;
        }
        run_plan(step, filter, plan)?;
        if !plan.finished {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Per-step state shared by the plan helpers: the store and its read
/// transaction, the scan snapshot, cross-filter dedup, the wall-clock
/// budget, and the emit callback.
struct Step<'a> {
    store: &'a Store,
    txn: &'a Reader<'a>,
    /// Upper bound on seqs this scan may emit.
    snapshot: u64,
    /// NIP-40 cutoff: events with `expiration <= now` are skipped.
    now: u64,
    /// seqs already emitted by an earlier filter.
    emitted: &'a mut SeqSet,
    /// Candidates processed so far this step (budget check cadence).
    processed: usize,
    /// When the step began.
    start: Instant,
    /// Wall-clock budget for this step.
    budget: Duration,
    /// Where emitted payloads go.
    emit: &'a mut dyn FnMut(u64, &str),
}

/// Drive one filter until it finishes or the budget fires.
fn run_plan(step: &mut Step<'_>, filter: &Filter, plan: &mut Plan) -> Result<(), StoreError> {
    let Some(mode) = plan.mode.as_mut() else {
        plan.finished = true;
        return Ok(());
    };
    match mode {
        Mode::Ids { pending } => {
            scan_ids(step, filter, &mut plan.state, pending, &mut plan.finished)
        }
        Mode::Ranges { db, cursors } => scan_ranges(
            step,
            filter,
            &mut plan.state,
            *db,
            cursors,
            &mut plan.finished,
        ),
    }
}

/// `ix_id` point lookups, then the sort/truncate/emit finalize pass.
fn scan_ids(
    step: &mut Step<'_>,
    filter: &Filter,
    state: &mut PlanState,
    pending: &mut VecDeque<[u8; 32]>,
    finished: &mut bool,
) -> Result<(), StoreError> {
    while let Some(id) = pending.pop_front() {
        step.processed += 1;
        if let Some(bytes) = step.store.db(Index::Id).get(step.txn, &id)? {
            try_candidate(
                step,
                filter,
                &mut state.collected,
                &mut state.last_seq,
                decode_seq(bytes)?,
            )?;
        }
        if budget_elapsed(step) {
            return Ok(());
        }
    }
    finalize(step, &mut state.collected, state.limit)?;
    *finished = true;
    Ok(())
}

/// The k-way merge over a `Ranges` plan's live cursors: heap-pop the
/// newest head, refill from its cursor, flush completed `created_at`
/// groups, and park heads back into cursor bounds when the budget fires.
fn scan_ranges(
    step: &mut Step<'_>,
    filter: &Filter,
    state: &mut PlanState,
    index: Index,
    cursors: &mut [Cursor],
    finished: &mut bool,
) -> Result<(), StoreError> {
    let db = step.store.db(index);
    // One live reverse iterator per cursor, seeded at the saved
    // bounds; the bounds are only (re)derived when the step ends.
    let mut iters: Vec<Option<RevIter<'_>>> = Vec::with_capacity(cursors.len());
    for cursor in cursors.iter() {
        iters.push(if cursor.exhausted {
            None
        } else {
            Some(Box::new(db.rev_range(step.txn, &cursor.bounds())?))
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
        step.processed += 1;
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
        if state.group.first().is_some_and(|m| head.created_at < m.0) {
            flush_group(step, state)?;
            if state.taken >= state.limit {
                *finished = true;
                return Ok(());
            }
        }
        try_candidate(
            step,
            filter,
            &mut state.group,
            &mut state.last_seq,
            head.seq,
        )?;
        if budget_elapsed(step) {
            restore_heads(&mut heap, cursors);
            return Ok(());
        }
    }
    flush_group(step, state)?;
    *finished = true;
    Ok(())
}

/// Emit one complete `created_at` group: id-ascending, at most
/// `limit - taken` entries, each counted toward `taken` whether or not
/// it is re-sent (limit-then-dedup across filters).
fn flush_group(step: &mut Step<'_>, state: &mut PlanState) -> Result<(), StoreError> {
    state.group.sort_by_key(|m| m.1);
    let remaining = usize::try_from(state.limit.saturating_sub(state.taken)).unwrap_or(usize::MAX);
    for (_, _, seq) in state.group.drain(..).take(remaining) {
        state.taken += 1;
        if step.emitted.insert(seq) {
            emit_payload(step, seq)?;
        }
    }
    Ok(())
}

/// Fetch and emit one payload; an entry deleted between slices is
/// skipped but still counts toward the limit.
fn emit_payload(step: &mut Step<'_>, seq: u64) -> Result<(), StoreError> {
    if let Some(bytes) = step.store.payloads.get(step.txn, &seq.to_be_bytes())? {
        (step.emit)(
            seq,
            std::str::from_utf8(bytes).map_err(|_| StoreError::Corrupt("payload is not UTF-8"))?,
        );
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
fn try_candidate(
    step: &Step<'_>,
    filter: &Filter,
    collected: &mut Vec<Match>,
    last_seq: &mut Option<u64>,
    seq: u64,
) -> Result<(), StoreError> {
    if seq > step.snapshot || *last_seq == Some(seq) {
        return Ok(());
    }
    *last_seq = Some(seq);
    let Some(bytes) = step.store.events.get(step.txn, &seq.to_be_bytes())? else {
        return Ok(()); // deleted between slices
    };
    let packed = PackedRef::new(bytes).ok_or(StoreError::Corrupt("bad packed record"))?;
    // NIP-40: an expired event is never served and never consumes a
    // limit, even before the sweep removes it.
    let expiration = packed.expiration();
    if expiration != 0 && expiration <= step.now {
        return Ok(());
    }
    if filter.matches(packed) {
        collected.push((packed.created_at(), packed.id(), seq));
    }
    Ok(())
}

/// Sort collected matches newest-first, truncate to the limit, emit the
/// payloads, and record the seqs so later filters skip them.
fn finalize(step: &mut Step<'_>, collected: &mut Vec<Match>, limit: u64) -> Result<(), StoreError> {
    collected.sort_by(|a, b| cmp_newest_first((a.0, &a.1), (b.0, &b.1)));
    collected.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    for (_, _, seq) in collected.drain(..) {
        // An earlier filter may already have emitted this seq: it still
        // counts toward this filter's limit boundary, but is not sent
        // twice.
        if !step.emitted.insert(seq) {
            continue;
        }
        emit_payload(step, seq)?;
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

/// The step's wall-clock budget, checked every `BUDGET_CHECK_EVERY`
/// candidates.
fn budget_elapsed(step: &Step<'_>) -> bool {
    step.processed.is_multiple_of(BUDGET_CHECK_EVERY) && step.start.elapsed() >= step.budget
}
