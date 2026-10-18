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
        let result = entry.scan.step(store, budget, &mut |json| {
            out.push(Out::Frame {
                sub: Some(sub.clone()),
                text: event_frame(&sub.id, json),
            });
        });
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
