//! Monitor shard thread: the live-subscription inverted index plus the
//! snapshot/pending machinery that makes delivery exactly-once.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use nkr::event::Event;
use nkr::message::SubId;
use nkr::monitor::Monitors;
use nkr::session::SubKey;
use nkr_lmdb::Store;
use tokio::sync::mpsc;

use crate::channels::{ConnId, Out, Outbound, ShardMsg};

/// Buffered matches per pending subscription before `CLOSED`.
const PENDING_CAP: usize = 4096;

/// A subscription registered but not yet activated: its live matches
/// buffer until the stored scan finishes.
#[derive(Debug)]
struct PendingSub {
    /// Generation the pending state belongs to; `Activate` must match.
    generation: u64,
    /// Buffered live matches in arrival order.
    buffer: VecDeque<(SubKey, Arc<Event>)>,
}

/// Per-connection state inside one shard.
#[derive(Debug, Default)]
struct ConnState {
    /// Where frames go.
    out: Option<Outbound>,
    /// Pending subscriptions: sub id -> generation + buffered matches.
    pending: HashMap<SubId, PendingSub>,
}

/// One monitor shard's state: the monitor index, connection outbound
/// queues, and the authoritative `last_seq` snapshot source.
#[derive(Debug)]
struct Shard {
    monitors: Monitors<ConnId>,
    conns: HashMap<ConnId, ConnState>,
    last_seq: u64,
    hits: Vec<(ConnId, SubKey)>,
}

impl Shard {
    fn new(store: &Store) -> Self {
        Self {
            monitors: Monitors::new(),
            conns: HashMap::new(),
            last_seq: store.snapshot().unwrap_or(0),
            hits: Vec::new(),
        }
    }

    /// Apply one inbox message.
    fn handle(&mut self, msg: ShardMsg) {
        match msg {
            ShardMsg::Register {
                conn,
                sub,
                filters,
                out,
                reply,
            } => {
                let state = self.conns.entry(conn).or_default();
                state.out = Some(out);
                state.pending.insert(
                    sub.id.clone(),
                    PendingSub {
                        generation: sub.generation,
                        buffer: VecDeque::new(),
                    },
                );
                self.monitors.insert(conn, sub, filters);
                reply.send(self.last_seq).ok();
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
                if let Some(pending) = state.pending.remove(&sub.id)
                    && let Some(out) = &state.out
                {
                    flush(out, pending.buffer);
                }
            }
            ShardMsg::Remove { conn, sub_id } => {
                self.monitors.remove(conn, &sub_id);
                if let Some(state) = self.conns.get_mut(&conn) {
                    state.pending.remove(&sub_id);
                }
            }
            ShardMsg::RemoveConn { conn } => {
                self.monitors.remove_connection(conn);
                self.conns.remove(&conn);
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

    /// Match the event against the monitor index and route it: buffered
    /// for pending subs, one shared `Arc` per match for active ones.
    #[allow(
        clippy::excessive_nesting,
        reason = "pending/active routing stays readable inline"
    )]
    fn deliver(&mut self, event: &Arc<Event>) {
        self.monitors.matches(event.packed(), &mut self.hits);
        for (conn, sub) in self.hits.drain(..) {
            let Some(state) = self.conns.get_mut(&conn) else {
                continue;
            };
            if let Some(pending) = state.pending.get_mut(&sub.id) {
                if pending.buffer.len() >= PENDING_CAP {
                    if let Some(out) = &state.out {
                        out.push(Out::Closed {
                            sub: sub.clone(),
                            reason: "error: subscription fell behind".to_owned(),
                        });
                    }
                    state.pending.remove(&sub.id);
                    self.monitors.remove(conn, &sub.id);
                } else {
                    pending.buffer.push_back((sub, Arc::clone(event)));
                }
            } else if let Some(out) = &state.out {
                out.push(Out::Event {
                    sub,
                    event: Arc::clone(event),
                });
            }
        }
    }
}

/// Push every buffered live match to the connection.
fn flush(out: &Outbound, buffer: VecDeque<(SubKey, Arc<Event>)>) {
    for (sub, event) in buffer {
        out.push(Out::Event { sub, event });
    }
}

/// Shard thread main loop. `last_seq` starts at the store snapshot —
/// every later seq arrives only via [`ShardMsg::Batch`].
pub(crate) fn run(store: &Store, mut rx: mpsc::Receiver<ShardMsg>) {
    let mut shard = Shard::new(store);
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
    use nkr::event::{EventLimits, UnverifiedEvent};
    use nkr::filter::Filter;
    use nkr_lmdb::StoreOptions;
    use secp256k1::{Keypair, SecretKey, schnorr};
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::*;

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
        let mut shard = Shard::new(&store);
        let (out, mut out_rx) = Outbound::new(1 << 20);

        let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
        shard.handle(ShardMsg::Register {
            conn: 1,
            sub: sub_key("s", 1),
            filters: filters(),
            out: out.clone(),
            reply: reply_tx,
        });
        shard.handle(ShardMsg::Remove {
            conn: 1,
            sub_id: SubId::parse("s").unwrap(),
        });
        let (reply_tx2, _reply_rx2) = tokio::sync::oneshot::channel();
        shard.handle(ShardMsg::Register {
            conn: 1,
            sub: sub_key("s", 2),
            filters: filters(),
            out,
            reply: reply_tx2,
        });
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
}
