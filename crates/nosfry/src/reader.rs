//! Reader thread: time-sliced stored-event scans.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nosfry_core::hll::Hll;
use nosfry_core::message::{RelayVerb, count_frame, eose_frame, event_frame};
use nosfry_core::session::SubKey;
use nosfry_lmdb::{CountScan, Progress, Scan, Store, StoreError};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::watch;

use crate::channels::{ConnId, Out, Outbound, ReaderMsg, ScanKind, ShardMsg, route};
use crate::metrics::{Metrics, StoreOp};

/// How long an idle reader waits for new work.
const IDLE_WAIT: Duration = Duration::from_millis(5);

/// Frames per `Out::Frames` chunk pushed by a scan step.
const CHUNK_FRAMES: usize = 64;

/// Accounted bytes per `Out::Frames` chunk.
const CHUNK_BYTES: usize = 256 * 1024;

// Loop iterations, test-only spin detector. Thread-local so readers
// run by concurrent tests count independently.
#[cfg(test)]
thread_local! {
    static LOOP_TICKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The stored-event work one subscription asked for.
#[derive(Debug)]
enum Job {
    /// A REQ's snapshot-bounded scan.
    Scan(Scan),
    /// A NIP-45 `COUNT`.
    Count(Box<CountScan>),
}

/// One running scan.
#[derive(Debug)]
struct ScanEntry {
    conn: ConnId,
    sub: SubKey,
    job: Job,
    out: Outbound,
    /// The scan's first emitted frame already went out as its own
    /// chunk so the first event never waits for a full chunk.
    first_pushed: bool,
}

/// Tunables a reader thread holds for its lifetime (restart-only).
pub(crate) struct Tuning {
    /// Per-slice stored-scan time budget.
    pub(crate) budget: Duration,
    /// Per-connection pending bytes above which its scans pause.
    pub(crate) high_water: usize,
    /// Process-wide pending budget: over it, scans pause whose
    /// connection already holds more than `CHUNK_BYTES` pending —
    /// under memory pressure only connections already behind stop
    /// receiving scan output; clients that keep up are not stalled,
    /// so an attacker holding the budget cannot stall everyone's
    /// queries (0 = unlimited).
    pub(crate) total_budget: usize,
    /// Per-filter cap on stored scan results.
    pub(crate) max_filter_limit: u64,
    /// Index entries a `COUNT` scan may examine before answering
    /// `"approximate": true`.
    pub(crate) max_filter_limit_count: u64,
}

/// Reader thread main loop. `tuning.high_water` pauses scans for
/// connections whose outbound queue is backing up; `total_budget`
/// adds the process-wide rule documented on [`Tuning`]. New and
/// resumed scans alternate slices. `shutdown` ends the idle wait
/// even while shard senders still point at this reader (shards forward
/// into it).
pub(crate) fn run(
    store: &Store,
    rx: &Receiver<ReaderMsg>,
    shards: &[tokio_mpsc::Sender<ShardMsg>],
    tuning: &Tuning,
    metrics: &Metrics,
    shutdown: &watch::Receiver<bool>,
) {
    let mut new_scans: VecDeque<ScanEntry> = VecDeque::new();
    let mut resumed: VecDeque<ScanEntry> = VecDeque::new();
    let mut skipped = 0usize;
    // Alternate between the queues: new scans get their first slice
    // quickly, but a steady stream of them may not starve resumed ones
    // (resumed gets at least every other slice while both are queued).
    let mut prefer_new = true;
    loop {
        #[cfg(test)]
        LOOP_TICKS.with(|t| t.set(t.get() + 1));
        match rx.try_recv() {
            Ok(msg) => {
                apply(msg, &mut new_scans, &mut resumed, tuning);
                skipped = 0;
                continue;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
        }
        let entry = if prefer_new {
            new_scans.pop_front().or_else(|| resumed.pop_front())
        } else {
            resumed.pop_front().or_else(|| new_scans.pop_front())
        };
        prefer_new = !prefer_new;
        let Some(entry) = entry else {
            if !idle(rx, &mut new_scans, &mut resumed, tuning, shutdown) {
                return;
            }
            continue;
        };
        // Pause scans for connections above the outbound high-water
        // mark, or — while the process-wide total is over budget — for
        // connections already more than one chunk behind.
        let pending = entry.out.pending();
        let behind = tuning.total_budget != 0
            && entry.out.total() > tuning.total_budget
            && pending > CHUNK_BYTES;
        if pending <= tuning.high_water && !behind {
            skipped = 0;
            step(entry, store, tuning.budget, shards, metrics, &mut resumed);
            continue;
        }
        resumed.push_back(entry);
        skipped += 1;
        if skipped < new_scans.len() + resumed.len() {
            continue;
        }
        // A full pass with nothing runnable: block for new work
        // instead of spinning.
        if !idle(rx, &mut new_scans, &mut resumed, tuning, shutdown) {
            return;
        }
        skipped = 0;
    }
}

/// Block up to `IDLE_WAIT` for one control message; `false` means the
/// reader exits (senders gone or shutdown raised).
fn idle(
    rx: &Receiver<ReaderMsg>,
    new_scans: &mut VecDeque<ScanEntry>,
    resumed: &mut VecDeque<ScanEntry>,
    tuning: &Tuning,
    shutdown: &watch::Receiver<bool>,
) -> bool {
    match rx.recv_timeout(IDLE_WAIT) {
        Ok(msg) => {
            apply(msg, new_scans, resumed, tuning);
            true
        }
        Err(RecvTimeoutError::Disconnected) => false,
        Err(RecvTimeoutError::Timeout) => !*shutdown.borrow(),
    }
}

/// Run one time-slice of `entry` — chunked `Frames` pushes, EOSE plus
/// `Activate` on completion of a REQ, a single `COUNT` frame on
/// completion of a count, `Closed` on error — and requeue it into
/// `resumed` when it stays pending.
fn step(
    mut entry: ScanEntry,
    store: &Store,
    budget: Duration,
    shards: &[tokio_mpsc::Sender<ShardMsg>],
    metrics: &Metrics,
    resumed: &mut VecDeque<ScanEntry>,
) {
    if let Job::Count(ref mut scan) = entry.job {
        match scan.step(store, budget) {
            Ok(Progress::Pending) => resumed.push_back(entry),
            Ok(Progress::Done) => {
                let result = scan.result();
                entry.out.push(Out::Count {
                    sub: entry.sub.clone(),
                    text: count_frame(
                        &entry.sub.id,
                        result.count,
                        result.approximate,
                        result.hll.as_ref(),
                    ),
                });
            }
            Err(err) => scan_failed(&entry, &err, metrics),
        }
        return;
    }
    let Job::Scan(ref mut scan) = entry.job else {
        unreachable!("count handled above")
    };
    let out = entry.out.clone();
    let sub = entry.sub.clone();
    let mut first = !entry.first_pushed;
    let mut texts = Vec::new();
    let mut bytes = 0usize;
    let result = scan.step(store, budget, &mut |_seq, json| {
        let text = event_frame(&sub.id, json);
        if first {
            first = false;
            out.push(Out::Frames {
                sub: sub.clone(),
                texts: vec![text],
            });
            return;
        }
        bytes += text.len();
        texts.push(text);
        if texts.len() >= CHUNK_FRAMES || bytes >= CHUNK_BYTES {
            out.push(Out::Frames {
                sub: sub.clone(),
                texts: std::mem::take(&mut texts),
            });
            bytes = 0;
        }
    });
    entry.first_pushed |= !first;
    if !texts.is_empty() {
        out.push(Out::Frames {
            sub: sub.clone(),
            texts,
        });
    }
    match result {
        Ok(Progress::Pending) => resumed.push_back(entry),
        Ok(Progress::Done) => {
            entry.out.push(Out::Frame {
                sub: Some(entry.sub.clone()),
                verb: RelayVerb::Eose,
                text: eose_frame(&entry.sub.id),
            });
            drop(route(shards, entry.conn).blocking_send(ShardMsg::Activate {
                conn: entry.conn,
                sub: entry.sub,
            }));
        }
        Err(err) => scan_failed(&entry, &err, metrics),
    }
}

/// A store error on any scan kind: count it, log it, close the sub.
fn scan_failed(entry: &ScanEntry, err: &StoreError, metrics: &Metrics) {
    metrics.store_error(StoreOp::Scan);
    tracing::error!(%err, conn = entry.conn, "scan failed");
    entry.out.push(Out::Closed {
        sub: entry.sub.clone(),
        reason: "error: internal error".to_owned(),
    });
}

/// Apply an inbox message.
fn apply(
    msg: ReaderMsg,
    new_scans: &mut VecDeque<ScanEntry>,
    resumed: &mut VecDeque<ScanEntry>,
    tuning: &Tuning,
) {
    match msg {
        ReaderMsg::Start(start) => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            let job = match start.kind {
                ScanKind::Req => Job::Scan(Scan::new(
                    start.filters,
                    start.gate,
                    start.snapshot,
                    tuning.max_filter_limit,
                    now,
                )),
                ScanKind::Count => {
                    let hll = Hll::for_filters(&start.filters);
                    Job::Count(Box::new(CountScan::new(
                        start.filters,
                        start.gate,
                        start.snapshot,
                        tuning.max_filter_limit_count,
                        now,
                        hll,
                    )))
                }
            };
            new_scans.push_back(ScanEntry {
                conn: start.conn,
                sub: start.sub,
                job,
                out: start.out,
                first_pushed: false,
            });
        }
        ReaderMsg::Cancel { conn, sub_id } => {
            new_scans.retain(|e| e.conn != conn || e.sub.id != sub_id);
            resumed.retain(|e| e.conn != conn || e.sub.id != sub_id);
        }
        ReaderMsg::CancelConn { conn } => {
            new_scans.retain(|e| e.conn != conn);
            resumed.retain(|e| e.conn != conn);
        }
    }
}

#[allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    reason = "unit tests may panic on failure; the spin detector sleeps               between loop-tick samples"
)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use nosfry_core::auth::ReadGate;
    use nosfry_core::filter::Filter;
    use nosfry_core::message::SubId;
    use nosfry_lmdb::StoreOptions;
    use tokio::sync::mpsc;

    use super::*;
    use crate::channels::{ScanKind, ScanStart};

    /// A signed kind-1 event at a distinct `created_at`.
    fn event(i: u64) -> Arc<nosfry_core::event::Event> {
        use nosfry_core::event::{EventLimits, UnverifiedEvent};
        use sha2::Digest;
        let mut secret = [0u8; 32];
        secret[..8].copy_from_slice(&(i + 1).to_be_bytes());
        secret[31] = 0x5a;
        let keypair = secp256k1::Keypair::from_secret_bytes(secret).unwrap();
        let pubkey = faster_hex::hex_string(&keypair.x_only_public_key().0.to_byte_array());
        let created_at = 1_700_000_000 + i;
        let preimage = serde_json::json!([0, pubkey, created_at, 1, [], "x"]);
        let id: [u8; 32] = sha2::Sha256::digest(preimage.to_string().as_bytes()).into();
        let sig = secp256k1::schnorr::sign_no_aux_rand(&id, &keypair);
        let json = serde_json::json!({
            "id": faster_hex::hex_string(&id),
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": 1,
            "tags": [],
            "content": "x",
            "sig": faster_hex::hex_string(&sig.to_byte_array()),
        })
        .to_string();
        Arc::new(
            UnverifiedEvent::parse(&json, &EventLimits::default())
                .unwrap()
                .verify(&EventLimits::default())
                .unwrap(),
        )
    }

    /// The first `Out` a scan produces is a single-frame chunk so the
    /// first event never waits for 64 frames; the rest chunk normally.
    #[test]
    fn first_emit_is_a_single_frame_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
        let events: Vec<Arc<nosfry_core::event::Event>> = (0..10).map(event).collect();
        store.write(&events).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let (shard_tx, _shard_rx) = mpsc::channel(8);
        let (out, mut out_rx) = Outbound::new(1 << 30, Arc::new(AtomicUsize::new(0)));
        let filters: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()]);
        tx.send(ReaderMsg::Start(Box::new(ScanStart {
            conn: 1,
            sub: SubKey {
                id: SubId::parse("s").unwrap(),
                generation: 1,
            },
            kind: ScanKind::Req,
            filters,
            gate: ReadGate::default(),
            snapshot: u64::MAX,
            out,
        })))
        .unwrap();
        let reader = std::thread::spawn(move || {
            run(
                &store,
                &rx,
                &[shard_tx],
                &Tuning {
                    budget: Duration::from_secs(10),
                    high_water: usize::MAX,
                    total_budget: 0,
                    max_filter_limit: 500,
                    max_filter_limit_count: 1_000_000,
                },
                &Metrics::new(),
                &watch::channel(false).1,
            );
        });
        let first = out_rx.blocking_recv().unwrap();
        let second = out_rx.blocking_recv().unwrap();
        let third = out_rx.blocking_recv().unwrap();
        let Out::Frames { texts: first, .. } = first else {
            panic!("first Out was not Frames: {first:?}")
        };
        let Out::Frames { texts: rest, .. } = second else {
            panic!("second Out was not Frames: {second:?}")
        };
        let Out::Frame { text: eose, .. } = third else {
            panic!("third Out was not the EOSE frame: {third:?}")
        };
        assert_eq!(first.len(), 1, "first chunk must carry one frame");
        assert_eq!(rest.len(), 9);
        assert!(eose.contains("EOSE"), "{eose}");
        drop(tx);
        reader.join().unwrap();
    }

    /// Every queued scan sits above the outbound high-water mark: the
    /// reader must block on `recv_timeout` instead of spinning.
    #[test]
    fn paused_scans_do_not_busy_spin() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let (shard_tx, _shard_rx) = mpsc::channel(8);
        let (out, _out_rx) = Outbound::new(1 << 20, Arc::new(AtomicUsize::new(0)));
        // The queued scan's connection is over the high-water mark from
        // the start, so its step can never run.
        out.push(Out::Frame {
            sub: None,
            verb: RelayVerb::Notice,
            text: "x".repeat(1024),
        });
        let filters: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()]);
        tx.send(ReaderMsg::Start(Box::new(ScanStart {
            conn: 1,
            sub: SubKey {
                id: SubId::parse("s").unwrap(),
                generation: 1,
            },
            kind: ScanKind::Req,
            filters,
            gate: ReadGate::default(),
            snapshot: 0,
            out,
        })))
        .unwrap();

        // The reader runs on its own thread and returns its private
        // `LOOP_TICKS` count on join.
        let reader = std::thread::spawn(move || {
            run(
                &store,
                &rx,
                &[shard_tx],
                &Tuning {
                    budget: Duration::ZERO,
                    high_water: 0,
                    total_budget: 0,
                    max_filter_limit: 500,
                    max_filter_limit_count: 1_000_000,
                },
                &Metrics::new(),
                &watch::channel(false).1,
            );
            LOOP_TICKS.with(std::cell::Cell::get)
        });
        std::thread::sleep(Duration::from_millis(300));
        drop(tx);
        let ticks = reader.join().unwrap();
        // IDLE_WAIT is 5ms: roughly 60 passes in 300ms. An unpaused
        // spin would tick millions of times in the same window.
        assert!(ticks < 1000, "reader spun {ticks} times in 300ms");
    }

    /// With the process-wide pending total over a tiny budget, a scan
    /// whose connection is already more than one chunk behind stays
    /// paused while a caught-up connection's scan completes.
    #[test]
    fn total_budget_pauses_only_behind_scans() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
        let events: Vec<Arc<nosfry_core::event::Event>> = (0..3).map(event).collect();
        store.write(&events).unwrap();
        // Pre-inflated shared total: over any tiny budget.
        let total = Arc::new(AtomicUsize::new(2));
        let (tx, rx) = std::sync::mpsc::channel();
        let (shard_tx, mut shard_rx) = mpsc::channel(8);
        // Conn 1 is already more than one chunk behind; conn 2 has
        // nothing pending. Both share the same total.
        let (behind_out, mut behind_rx) = Outbound::new(usize::MAX, Arc::clone(&total));
        behind_out.push(Out::Frame {
            sub: None,
            verb: RelayVerb::Notice,
            text: "x".repeat(CHUNK_BYTES + 1),
        });
        let (caught_out, mut caught_rx) = Outbound::new(1 << 30, Arc::clone(&total));
        let filters: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()]);
        for (conn, out) in [(1, behind_out), (2, caught_out)] {
            tx.send(ReaderMsg::Start(Box::new(ScanStart {
                conn,
                sub: SubKey {
                    id: SubId::parse(&format!("s{conn}")).unwrap(),
                    generation: 1,
                },
                kind: ScanKind::Req,
                filters: Arc::clone(&filters),
                gate: ReadGate::default(),
                snapshot: u64::MAX,
                out,
            })))
            .unwrap();
        }
        let reader = std::thread::spawn(move || {
            run(
                &store,
                &rx,
                &[shard_tx],
                &Tuning {
                    budget: Duration::from_secs(10),
                    high_water: usize::MAX,
                    total_budget: 1,
                    max_filter_limit: 500,
                    max_filter_limit_count: 1_000_000,
                },
                &Metrics::new(),
                &watch::channel(false).1,
            );
        });
        // Conn 2's scan completes (frames + EOSE + Activate); conn 1's
        // scan stays paused and pushes nothing beyond the notice frame
        // already queued above.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut eose = false;
        while !eose && std::time::Instant::now() < deadline {
            match caught_rx.try_recv() {
                Ok(Out::Frame { ref text, .. }) if text.contains("EOSE") => eose = true,
                Ok(_) | Err(mpsc::error::TryRecvError::Empty) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        assert!(eose, "caught-up scan did not complete under total budget");
        assert_eq!(
            shard_rx.blocking_recv().map(|_| ()),
            Some(()),
            "caught-up scan never activated"
        );
        std::thread::sleep(Duration::from_millis(100));
        let mut behind_frames = 0usize;
        while let Ok(out) = behind_rx.try_recv() {
            behind_frames += usize::from(matches!(out, Out::Frames { .. }));
            assert!(
                matches!(out, Out::Frame { ref text, .. } if !text.contains("EOSE")),
                "behind scan pushed while paused: {out:?}"
            );
        }
        assert_eq!(behind_frames, 0, "behind scan emitted frames while paused");
        drop(tx);
        reader.join().unwrap();
    }

    /// A resumed scan must keep receiving slices while a steady stream
    /// of new scans arrives: the scheduler alternates the queues, so a
    /// flood of REQs cannot starve long-running scans.
    #[test]
    fn resumed_scan_progresses_during_new_scan_flood() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
        let events: Vec<Arc<nosfry_core::event::Event>> = (0..4_000).map(event).collect();
        store.write(&events).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        // Every finishing scan `blocking_send`s an `Activate`; keep the
        // inbox bigger than the flood.
        let (shard_tx, mut shard_rx) = mpsc::channel(8_192);
        let (long_out, mut long_rx) = Outbound::new(1 << 30, Arc::new(AtomicUsize::new(0)));
        let all: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()]);
        let one: Arc<[Filter]> =
            Arc::from(vec![Filter::parse("{\"kinds\":[1],\"limit\":1}").unwrap()]);
        tx.send(ReaderMsg::Start(Box::new(ScanStart {
            conn: 1,
            sub: SubKey {
                id: SubId::parse("long").unwrap(),
                generation: 1,
            },
            kind: ScanKind::Req,
            filters: Arc::clone(&all),
            gate: ReadGate::default(),
            snapshot: u64::MAX,
            out: long_out,
        })))
        .unwrap();
        // Queue the flood before the reader starts so every scan is
        // applied before the first step: `new_scans` is [long, n0..n499]
        // and a scheduler that always prefers new scans cannot reach
        // the resumed queue until the whole flood is done.
        for i in 0..500u64 {
            let (out, _rx) = Outbound::new(0, Arc::new(AtomicUsize::new(0)));
            tx.send(ReaderMsg::Start(Box::new(ScanStart {
                conn: 2 + i,
                sub: SubKey {
                    id: SubId::parse(&format!("n{i}")).unwrap(),
                    generation: 1,
                },
                kind: ScanKind::Req,
                filters: Arc::clone(&one),
                gate: ReadGate::default(),
                snapshot: u64::MAX,
                out,
            })))
            .unwrap();
        }
        let reader = std::thread::spawn(move || {
            run(
                &store,
                &rx,
                &[shard_tx],
                &Tuning {
                    budget: Duration::from_micros(50),
                    high_water: usize::MAX,
                    total_budget: 0,
                    max_filter_limit: 5_000,
                    max_filter_limit_count: 1_000_000,
                },
                &Metrics::new(),
                &watch::channel(false).1,
            );
        });
        // Every finished scan reports one `Activate` on `shard_rx`.
        // With alternating slices the long scan finishes long before
        // the flood drains; a new-scans-first scheduler would not emit
        // its EOSE until all 500 flood scans are done.
        let (mut activates, mut eose) = (0usize, false);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !eose && activates < 500 && std::time::Instant::now() < deadline {
            while let Ok(out) = long_rx.try_recv() {
                eose |= matches!(out, Out::Frame { ref text, .. } if text.contains("EOSE"));
            }
            match shard_rx.try_recv() {
                Ok(_) => activates += 1,
                Err(_) => std::thread::sleep(Duration::from_millis(1)),
            }
        }
        assert!(eose, "resumed scan starved during the flood");
        drop(tx);
        reader.join().unwrap();
    }
}
