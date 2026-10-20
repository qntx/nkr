//! Monitor shard thread: the live-subscription inverted index plus the
//! snapshot/pending machinery that makes delivery exactly-once.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use nosfry_core::event::Event;
use nosfry_core::message::SubId;
use nosfry_core::monitor::Monitors;
use nosfry_core::session::SubKey;
use nosfry_lmdb::Store;
use tokio::sync::mpsc;

use crate::channels::{ConnId, Out, Outbound, ReaderMsg, ShardMsg, route};

/// Buffered matches per pending subscription before `CLOSED`.
const PENDING_CAP: usize = 4096;

/// foldhash for the hot-path maps (server-assigned conn ids and client
/// sub ids); randomized seeding keeps `HashDoS` resistance.
type Map<K, V> = HashMap<K, V, foldhash::fast::RandomState>;

fn map<K, V>() -> Map<K, V> {
    Map::with_hasher(foldhash::fast::RandomState::default())
}

/// A subscription registered but not yet activated: its live matches
/// buffer until the stored scan finishes.
#[derive(Debug)]
struct PendingSub {
    /// Generation the pending state belongs to; `Activate` must match.
    generation: u64,
    /// Buffered live matches in arrival order.
    buffer: VecDeque<(SubKey, Arc<Event>)>,
}

/// Per-connection state inside one shard; created on `Register`, which
/// always carries the connection's outbound queue.
#[derive(Debug)]
struct ConnState {
    /// Where frames go.
    out: Outbound,
    /// Pending subscriptions: sub id -> generation + buffered matches.
    pending: Map<SubId, PendingSub>,
}

/// One monitor shard's state: the monitor index, connection outbound
/// queues, the reader routing table it forwards scan control to, and
/// the authoritative `last_seq` snapshot source.
#[derive(Debug)]
struct Shard {
    monitors: Monitors<ConnId>,
    conns: Map<ConnId, ConnState>,
    /// Reader inboxes, routed `conn % len`; forwarding Start/Cancel from
    /// here keeps a subscription's reader messages in shard order.
    readers: Arc<[std::sync::mpsc::Sender<ReaderMsg>]>,
    last_seq: u64,
    hits: Vec<(ConnId, SubKey)>,
}

impl Shard {
    fn new(store: &Store, readers: Arc<[std::sync::mpsc::Sender<ReaderMsg>]>) -> Self {
        Self {
            monitors: Monitors::new(),
            conns: map(),
            readers,
            last_seq: store.snapshot().unwrap_or(0),
            hits: Vec::new(),
        }
    }

    /// Apply one inbox message.
    fn handle(&mut self, msg: ShardMsg) {
        match msg {
            ShardMsg::Register(mut start) => {
                let state = self
                    .conns
                    .entry(start.conn)
                    .and_modify(|state| state.out = start.out.clone())
                    .or_insert_with(|| ConnState {
                        out: start.out.clone(),
                        pending: map(),
                    });
                state.pending.insert(
                    start.sub.id.clone(),
                    PendingSub {
                        generation: start.sub.generation,
                        buffer: VecDeque::new(),
                    },
                );
                self.monitors.insert(
                    start.conn,
                    start.sub.clone(),
                    Arc::clone(&start.filters),
                    start.gate.clone(),
                );
                start.snapshot = self.last_seq;
                drop(route(&self.readers, start.conn).send(ReaderMsg::Start(start)));
            }
            ShardMsg::Activate { conn, sub } => {
                let Some(state) = self.conns.get_mut(&conn) else {
                    return;
                };
                // A stale Activate (an old generation's scan finishing
                // after a re-REQ) must not flush or activate the new
                // subscription before its own scan completes.
                if state
                    .pending
                    .get(&sub.id)
                    .is_some_and(|pending| pending.generation != sub.generation)
                {
                    return;
                }
                if let Some(pending) = state.pending.remove(&sub.id) {
                    flush(&state.out, pending.buffer);
                }
            }
            ShardMsg::Remove { conn, sub_id } => {
                self.monitors.remove(conn, &sub_id);
                if let Some(state) = self.conns.get_mut(&conn) {
                    state.pending.remove(&sub_id);
                }
                drop(route(&self.readers, conn).send(ReaderMsg::Cancel { conn, sub_id }));
            }
            ShardMsg::RemoveConn { conn } => {
                self.monitors.remove_connection(conn);
                self.conns.remove(&conn);
                drop(route(&self.readers, conn).send(ReaderMsg::CancelConn { conn }));
            }
            ShardMsg::Batch(batch) => {
                for (seq, event) in batch.iter() {
                    self.last_seq = self.last_seq.max(*seq);
                    self.deliver(event);
                }
            }
            ShardMsg::Ephemeral(event) => {
                self.deliver(&event);
            }
        }
    }

    /// Match the event against the monitor index and route each hit.
    fn deliver(&mut self, event: &Arc<Event>) {
        self.monitors.matches(event.packed(), &mut self.hits);
        let Self {
            monitors,
            conns,
            hits,
            ..
        } = self;
        for (conn, sub) in hits.drain(..) {
            let Some(state) = conns.get_mut(&conn) else {
                continue;
            };
            route_hit(monitors, state, conn, sub, event);
        }
    }
}

/// Route one monitor hit: buffered while the subscription's stored scan
/// runs (closed and dropped when the buffer overflows), one shared
/// `Arc` pushed to the connection once active.
fn route_hit(
    monitors: &mut Monitors<ConnId>,
    state: &mut ConnState,
    conn: ConnId,
    sub: SubKey,
    event: &Arc<Event>,
) {
    let Some(pending) = state.pending.get_mut(&sub.id) else {
        state.out.push(Out::Event {
            sub,
            event: Arc::clone(event),
        });
        return;
    };
    if pending.buffer.len() < PENDING_CAP {
        pending.buffer.push_back((sub, Arc::clone(event)));
        return;
    }
    state.out.push(Out::Closed {
        sub: sub.clone(),
        reason: "error: subscription fell behind".to_owned(),
    });
    state.pending.remove(&sub.id);
    monitors.remove(conn, &sub.id);
}

/// Push every buffered live match to the connection.
fn flush(out: &Outbound, buffer: VecDeque<(SubKey, Arc<Event>)>) {
    for (sub, event) in buffer {
        out.push(Out::Event { sub, event });
    }
}

/// Shard thread main loop. `last_seq` starts at the store snapshot —
/// every later seq arrives only via [`ShardMsg::Batch`]. `readers` is
/// the routing table the shard forwards scan control into.
pub(crate) fn run(
    store: &Store,
    mut rx: mpsc::Receiver<ShardMsg>,
    readers: Arc<[std::sync::mpsc::Sender<ReaderMsg>]>,
) {
    let mut shard = Shard::new(store, readers);
    while let Some(msg) = rx.blocking_recv() {
        shard.handle(msg);
    }
}

#[allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "unit tests may panic on failure"
)]
#[cfg(test)]
mod tests {
    use nosfry_core::auth::ReadGate;
    use nosfry_core::event::{EventLimits, UnverifiedEvent};
    use nosfry_core::filter::Filter;
    use nosfry_lmdb::StoreOptions;
    use secp256k1::{Keypair, SecretKey, schnorr};
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::channels::ScanStart;

    fn event(kind: u16, content: &str) -> Arc<Event> {
        let keypair = Keypair::from_secret_key(&SecretKey::from_secret_bytes([7u8; 32]).unwrap());
        let pk_hex = faster_hex::hex_string(&keypair.x_only_public_key().0.to_byte_array());
        let created_at = 1_700_000_000u64;
        let preimage =
            serde_json::to_string(&json!([0, pk_hex, created_at, kind, [], content])).unwrap();
        let id = Sha256::digest(preimage.as_bytes());
        let sig = schnorr::sign_no_aux_rand(&id, &keypair);
        let json = serde_json::to_string(&json!({
            "id": faster_hex::hex_string(&id),
            "pubkey": pk_hex,
            "created_at": created_at,
            "kind": kind,
            "tags": [],
            "content": content,
            "sig": faster_hex::hex_string(&sig.to_byte_array()),
        }))
        .unwrap();
        Arc::new(
            UnverifiedEvent::parse(&json, &EventLimits::default())
                .unwrap()
                .verify(&EventLimits::default())
                .unwrap(),
        )
    }

    fn sub_key(id: &str, generation: u64) -> SubKey {
        SubKey {
            id: SubId::parse(id).unwrap(),
            generation,
        }
    }

    fn filters() -> Arc<[Filter]> {
        Arc::from(vec![Filter::parse("{\"kinds\":[1]}").unwrap()])
    }

    /// Remove(gen1) + Register(gen2) + Activate(gen1) must not flush or
    /// activate the new subscription before its own scan completes.
    #[test]
    fn stale_activate_does_not_flush_new_generation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
        let (reader_tx, _reader_rx) = std::sync::mpsc::channel();
        let mut shard = Shard::new(&store, Arc::from([reader_tx]));
        let (out, mut out_rx) = Outbound::new(1 << 20);

        shard.handle(ShardMsg::Register(Box::new(ScanStart {
            conn: 1,
            sub: sub_key("s", 1),
            filters: filters(),
            out: out.clone(),
            gate: ReadGate::default(),
            snapshot: 0,
        })));
        shard.handle(ShardMsg::Remove {
            conn: 1,
            sub_id: SubId::parse("s").unwrap(),
        });
        shard.handle(ShardMsg::Register(Box::new(ScanStart {
            conn: 1,
            sub: sub_key("s", 2),
            filters: filters(),
            out,
            gate: ReadGate::default(),
            snapshot: 0,
        })));
        // The stale generation's scan finished late; Activate is ignored.
        shard.handle(ShardMsg::Activate {
            conn: 1,
            sub: sub_key("s", 1),
        });

        // A live batch must still buffer for gen2, not deliver.
        let batch: crate::channels::Batch = vec![(1u64, event(1, "hi"))];
        shard.handle(ShardMsg::Batch(Arc::new(batch)));
        assert!(
            out_rx.try_recv().is_err(),
            "stale Activate delivered frames to gen2"
        );

        // The real Activate flushes the buffered event.
        shard.handle(ShardMsg::Activate {
            conn: 1,
            sub: sub_key("s", 2),
        });
        match out_rx.try_recv() {
            Ok(Out::Event { sub, .. }) => assert_eq!(sub.generation, 2),
            other => panic!("expected buffered Event, got {other:?}"),
        }
    }

    /// `Register` after a `Batch` must send the reader a `Start`
    /// carrying the shard's post-batch `last_seq` as the snapshot.
    #[test]
    fn register_starts_scan_at_current_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
        let (reader_tx, reader_rx) = std::sync::mpsc::channel();
        let mut shard = Shard::new(&store, Arc::from([reader_tx]));
        let (out, _out_rx) = Outbound::new(1 << 20);

        let batch: crate::channels::Batch = vec![(42u64, event(1, "hi"))];
        shard.handle(ShardMsg::Batch(Arc::new(batch)));
        shard.handle(ShardMsg::Register(Box::new(ScanStart {
            conn: 1,
            sub: sub_key("s", 1),
            filters: filters(),
            out,
            gate: ReadGate::default(),
            snapshot: 0,
        })));

        match reader_rx.try_recv() {
            Ok(ReaderMsg::Start(start)) => assert_eq!(start.snapshot, 42),
            other => panic!("expected Start with snapshot 42, got {other:?}"),
        }
    }

    /// `Register` then `Remove` must reach the reader as `Start` then
    /// `Cancel` in that order — the FIFO channel keeps a fast REQ+CLOSE
    /// from spawning a scan after its cancel.
    #[test]
    fn register_then_remove_orders_start_before_cancel() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
        let (reader_tx, reader_rx) = std::sync::mpsc::channel();
        let mut shard = Shard::new(&store, Arc::from([reader_tx]));
        let (out, _out_rx) = Outbound::new(1 << 20);

        shard.handle(ShardMsg::Register(Box::new(ScanStart {
            conn: 1,
            sub: sub_key("s", 1),
            filters: filters(),
            out,
            gate: ReadGate::default(),
            snapshot: 0,
        })));
        shard.handle(ShardMsg::Remove {
            conn: 1,
            sub_id: SubId::parse("s").unwrap(),
        });

        match reader_rx.try_recv() {
            Ok(ReaderMsg::Start(start)) if start.conn == 1 => {}
            other => panic!("expected Start first, got {other:?}"),
        }
        match reader_rx.try_recv() {
            Ok(ReaderMsg::Cancel { conn: 1, .. }) => {}
            other => panic!("expected Cancel second, got {other:?}"),
        }
    }
}
