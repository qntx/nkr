//! Reader thread: time-sliced stored-event scans.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nosfry_core::message::{RelayVerb, eose_frame, event_frame};
use nosfry_core::session::SubKey;
use nosfry_lmdb::{Progress, Scan, Store};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::watch;

use crate::channels::{ConnId, Out, Outbound, ReaderMsg, ShardMsg, route};

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

/// One running scan.
#[derive(Debug)]
struct ScanEntry {
    conn: ConnId,
    sub: SubKey,
    scan: Scan,
    out: Outbound,
    /// The scan's first emitted frame already went out as its own
    /// chunk so the first event never waits for a full chunk.
    first_pushed: bool,
}

/// Reader thread main loop. `high_water` pauses scans for connections
/// whose outbound queue is backing up; new and resumed scans alternate
/// slices. `shutdown` ends the idle wait even while shard
/// senders still point at this reader (shards forward into it).
pub(crate) fn run(
    store: &Store,
    rx: &Receiver<ReaderMsg>,
    shards: &[tokio_mpsc::Sender<ShardMsg>],
    budget: Duration,
    high_water: usize,
    max_filter_limit: u64,
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
                apply(msg, &mut new_scans, &mut resumed, max_filter_limit);
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
            if !idle(rx, &mut new_scans, &mut resumed, max_filter_limit, shutdown) {
                return;
            }
            continue;
        };
        // Pause scans for connections above the outbound high-water mark.
        if entry.out.pending() <= high_water {
            skipped = 0;
            step(entry, store, budget, shards, &mut resumed);
            continue;
        }
        resumed.push_back(entry);
        skipped += 1;
        if skipped < new_scans.len() + resumed.len() {
            continue;
        }
        // A full pass with nothing runnable: block for new work
        // instead of spinning.
        if !idle(rx, &mut new_scans, &mut resumed, max_filter_limit, shutdown) {
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
    max_filter_limit: u64,
    shutdown: &watch::Receiver<bool>,
) -> bool {
    match rx.recv_timeout(IDLE_WAIT) {
        Ok(msg) => {
            apply(msg, new_scans, resumed, max_filter_limit);
            true
        }
        Err(RecvTimeoutError::Disconnected) => false,
        Err(RecvTimeoutError::Timeout) => !*shutdown.borrow(),
    }
}

/// Run one time-slice of `entry` — chunked `Frames` pushes, EOSE plus
/// `Activate` on completion, `Closed` on error — and requeue it into
/// `resumed` when it stays pending.
fn step(
    mut entry: ScanEntry,
    store: &Store,
    budget: Duration,
    shards: &[tokio_mpsc::Sender<ShardMsg>],
    resumed: &mut VecDeque<ScanEntry>,
) {
    let out = entry.out.clone();
    let sub = entry.sub.clone();
    let mut first = !entry.first_pushed;
    let mut texts = Vec::new();
    let mut bytes = 0usize;
    let result = entry.scan.step(store, budget, &mut |_seq, json| {
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
        Err(err) => {
            tracing::error!(%err, conn = entry.conn, "scan failed");
            entry.out.push(Out::Closed {
                sub: entry.sub.clone(),
                reason: "error: internal error".to_owned(),
            });
        }
    }
}

/// Apply an inbox message.
fn apply(
    msg: ReaderMsg,
    new_scans: &mut VecDeque<ScanEntry>,
    resumed: &mut VecDeque<ScanEntry>,
    max_filter_limit: u64,
) {
    match msg {
        ReaderMsg::Start(start) => new_scans.push_back(ScanEntry {
            conn: start.conn,
            sub: start.sub,
            scan: Scan::new(
                start.filters,
                start.gate,
                start.snapshot,
                max_filter_limit,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs()),
            ),
            out: start.out,
            first_pushed: false,
        }),
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

    use nosfry_core::auth::ReadGate;
    use nosfry_core::filter::Filter;
    use nosfry_core::message::SubId;
    use nosfry_lmdb::StoreOptions;
    use tokio::sync::mpsc;

    use super::*;
    use crate::channels::ScanStart;

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
        let (out, mut out_rx) = Outbound::new(1 << 30);
        let filters: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()]);
        tx.send(ReaderMsg::Start(Box::new(ScanStart {
            conn: 1,
            sub: SubKey {
                id: SubId::parse("s").unwrap(),
                generation: 1,
            },
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
                Duration::from_secs(10),
                usize::MAX,
                500,
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
        let (out, _out_rx) = Outbound::new(1 << 20);
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
                Duration::ZERO,
                0,
                500,
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
        let (long_out, mut long_rx) = Outbound::new(1 << 30);
        let all: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()]);
        let one: Arc<[Filter]> =
            Arc::from(vec![Filter::parse("{\"kinds\":[1],\"limit\":1}").unwrap()]);
        tx.send(ReaderMsg::Start(Box::new(ScanStart {
            conn: 1,
            sub: SubKey {
                id: SubId::parse("long").unwrap(),
                generation: 1,
            },
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
            let (out, _rx) = Outbound::new(0);
            tx.send(ReaderMsg::Start(Box::new(ScanStart {
                conn: 2 + i,
                sub: SubKey {
                    id: SubId::parse(&format!("n{i}")).unwrap(),
                    generation: 1,
                },
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
                Duration::from_micros(50),
                usize::MAX,
                5_000,
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
