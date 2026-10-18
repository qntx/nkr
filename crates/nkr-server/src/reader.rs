//! Reader thread: time-sliced stored-event scans.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use nkr::message::{eose_frame, event_frame};
use nkr::session::SubKey;
use nkr_lmdb::{Progress, Scan, Store};
use tokio::sync::mpsc as tokio_mpsc;

use crate::channels::{ConnId, Out, Outbound, ReaderMsg, ShardMsg};

/// How long an idle reader waits for new work.
const IDLE_WAIT: Duration = Duration::from_millis(5);

/// Frames per `Out::Frames` chunk pushed by a scan step.
const CHUNK_FRAMES: usize = 64;

/// Accounted bytes per `Out::Frames` chunk.
const CHUNK_BYTES: usize = 256 * 1024;

/// Loop iterations, test-only spin detector.
#[cfg(test)]
static LOOP_TICKS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

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
/// whose outbound queue is backing up; new scans always run before
/// resumed ones.
#[allow(
    clippy::excessive_nesting,
    reason = "the dispatch/step loop is clearest inline"
)]
pub(crate) fn run(
    store: &Store,
    rx: &Receiver<ReaderMsg>,
    shards: &[tokio_mpsc::Sender<ShardMsg>],
    monitors: usize,
    budget: Duration,
    high_water: usize,
    max_filter_limit: u64,
) {
    let mut new_scans: VecDeque<ScanEntry> = VecDeque::new();
    let mut resumed: VecDeque<ScanEntry> = VecDeque::new();
    let mut skipped = 0usize;
    loop {
        #[cfg(test)]
        LOOP_TICKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match rx.try_recv() {
            Ok(msg) => {
                apply(msg, &mut new_scans, &mut resumed, max_filter_limit);
                skipped = 0;
                continue;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
        }
        // Prefer new scans; then round-robin the resumed ones.
        let Some(mut entry) = new_scans.pop_front().or_else(|| resumed.pop_front()) else {
            match rx.recv_timeout(IDLE_WAIT) {
                Ok(msg) => apply(msg, &mut new_scans, &mut resumed, max_filter_limit),
                Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => {}
            }
            continue;
        };
        // Pause scans for connections above the outbound high-water mark.
        if entry.out.pending() > high_water {
            resumed.push_back(entry);
            skipped += 1;
            if skipped >= new_scans.len() + resumed.len() {
                // A full pass with nothing runnable: block for new work
                // instead of spinning.
                match rx.recv_timeout(IDLE_WAIT) {
                    Ok(msg) => apply(msg, &mut new_scans, &mut resumed, max_filter_limit),
                    Err(RecvTimeoutError::Disconnected) => return,
                    Err(RecvTimeoutError::Timeout) => {}
                }
                skipped = 0;
            }
            continue;
        }
        skipped = 0;
        let out = entry.out.clone();
        let sub = entry.sub.clone();
        let mut first = !entry.first_pushed;
        let mut texts = Vec::new();
        let mut bytes = 0usize;
        let result = entry.scan.step(store, budget, &mut |json| {
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
                    text: eose_frame(&entry.sub.id),
                });
                let index = usize::try_from(entry.conn).unwrap_or(usize::MAX) % monitors.max(1);
                if let Some(tx) = shards.get(index) {
                    drop(tx.blocking_send(ShardMsg::Activate {
                        conn: entry.conn,
                        sub: entry.sub,
                    }));
                }
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
}

/// Apply an inbox message.
fn apply(
    msg: ReaderMsg,
    new_scans: &mut VecDeque<ScanEntry>,
    resumed: &mut VecDeque<ScanEntry>,
    max_filter_limit: u64,
) {
    match msg {
        ReaderMsg::Start {
            conn,
            sub,
            filters,
            snapshot,
            out,
        } => new_scans.push_back(ScanEntry {
            conn,
            sub,
            scan: Scan::new(filters, snapshot, max_filter_limit),
            out,
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
    use std::sync::atomic::Ordering;

    use nkr::filter::Filter;
    use nkr::message::SubId;
    use nkr_lmdb::StoreOptions;

    use super::*;

    /// A signed kind-1 event at a distinct `created_at`.
    fn event(i: u64) -> Arc<nkr::event::Event> {
        use nkr::event::{EventLimits, UnverifiedEvent};
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
        let events: Vec<Arc<nkr::event::Event>> = (0..10).map(event).collect();
        store.write(&events).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let (out, mut out_rx) = Outbound::new(1 << 30);
        let filters: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()]);
        tx.send(ReaderMsg::Start {
            conn: 1,
            sub: SubKey {
                id: SubId::parse("s").unwrap(),
                generation: 1,
            },
            filters,
            snapshot: u64::MAX,
            out,
        })
        .unwrap();
        let reader = std::thread::spawn(move || {
            run(
                &store,
                &rx,
                &[],
                1,
                Duration::from_secs(10),
                usize::MAX,
                500,
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
        let (out, _out_rx) = Outbound::new(1 << 20);
        // The queued scan's connection is over the high-water mark from
        // the start, so its step can never run.
        out.push(Out::Frame {
            sub: None,
            text: "x".repeat(1024),
        });
        let filters: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()]);
        tx.send(ReaderMsg::Start {
            conn: 1,
            sub: SubKey {
                id: SubId::parse("s").unwrap(),
                generation: 1,
            },
            filters,
            snapshot: 0,
            out,
        })
        .unwrap();

        let before = LOOP_TICKS.load(Ordering::Relaxed);
        let reader = std::thread::spawn(move || {
            run(&store, &rx, &[], 1, Duration::ZERO, 0, 500);
        });
        std::thread::sleep(Duration::from_millis(300));
        drop(tx);
        reader.join().unwrap();
        let ticks = LOOP_TICKS.load(Ordering::Relaxed) - before;
        // IDLE_WAIT is 5ms: roughly 60 passes in 300ms. An unpaused
        // spin would tick millions of times in the same window.
        assert!(ticks < 1000, "reader spun {ticks} times in 300ms");
    }
}
